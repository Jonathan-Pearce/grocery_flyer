# Plan: Deal Quality Scoring System

Build a 0–100 deal grade + 0.0–1.0 confidence factor for every active flyer item, using all historical observations as feature inputs. Four new pipeline modules, three new DB tables, one config file.

---

## Files to create / modify

**New files:**
- `pipeline/product_resolver.py`
- `pipeline/price_history.py`
- `pipeline/deal_scorer.py`
- `config/scoring.yaml`

**Modified files:**
- `pipeline/build_db.py` — add `--score` flag and `build_scores()` call
- `requirements.txt` — add `pyyaml`

**DB outputs:**
- `db/dimensions/products.parquet`
- `db/features/price_history.parquet`
- `db/scores/active_scores.parquet`
- `db/scores/archived_scores.parquet`

---

## Phase A — Product Identity Resolution
*(no dependencies — can start immediately)*

**`pipeline/product_resolver.py`**

Assigns a `canonical_product_id` to every observation using three-tier progressive matching:

1. **Strict:** `(store_chain, sku)` exact match → match_tier_conf `1.0`
2. **Probable:** normalized name fingerprint + brand + weight_unit + category_l3 → match_tier_conf `0.6`
   - Name fingerprint: lowercase, strip punctuation, remove weight/size tokens (e.g. `1L`, `500g`, `pkg`), sort tokens, hash
   - Requires ≥ 2 of: brand match, weight_unit match, category_l3 match to qualify as probable
3. **Category fallback:** assign a synthetic ID keyed on `category_l3` (or `category_l1` if L3 absent) → match_tier_conf `0.2`; scores rely entirely on category-level baselines

Writes **`db/dimensions/products.parquet`**:

| Column | Notes |
|---|---|
| `canonical_product_id` | stable hash string |
| `canonical_name` | most-common name across observations |
| `canonical_brand` | most-common brand |
| `category_l1/l2/l3` | from most-common observation |
| `is_food`, `is_human_food` | |
| `weight_value`, `weight_unit` | normalised |
| `match_tier` | `"strict"` / `"probable"` / `"category"` |
| `observation_count` | how many records resolved here |

---

## Phase B — Price History & Feature Engineering
*(depends on Phase A)*

**`pipeline/price_history.py`**

Reads all observations from `db/observations/` + product IDs from Phase A. Computes one row per `(canonical_product_id, store_chain, week_start)`.

**Regular price estimation** (in priority order):
1. `regular_price` observed directly in a recent `no_promo` observation → source: `"observed"`, price_basis_conf: `1.0`
2. Max `sale_price` where `promo_type == "no_promo"`, ≥ 4 observations → source: `"own_history"`, price_basis_conf: `0.8`
3. Same as above with 1–3 observations → price_basis_conf: `0.5`
4. Cross-chain price for same `canonical_product_id` → source: `"cross_chain"`, price_basis_conf: `0.4`
5. Median `sale_price` of sibling products in `category_l3` → source: `"category_median"`, price_basis_conf: `0.2`
6. No estimate → source: `"none"`, price_basis_conf: `0.0` (affected components get neutral values)

**Computed features per row:**
- `regular_price_estimated`, `regular_price_source`, `price_basis_conf`
- `sale_freq_chain` — `n_sale_weeks / n_observed_weeks` over trailing 52w for this chain
- `sale_freq_market` — same across all chains
- `cycle_low_52w`, `cycle_high_52w` — min/max `sale_price` in trailing 52w
- `weeks_observed` — total weeks this product has been seen
- `chain_count` — number of distinct chains carrying this product
- `category_sibling_count` — number of observations in same `category_l3`

Writes **`db/features/price_history.parquet`** partitioned by `(store_chain, year, week)`.

---

## Phase C — Scoring
*(depends on Phase B)*

**`pipeline/deal_scorer.py`**

Loads active observations (`flyer_valid_from <= today <= flyer_valid_to`), joins features from Phase B, computes six score components + five confidence sub-signals.

### Score components (sum to 100)

**1. Discount Depth (0–25)**
- `pct_off = (regular_price_estimated - sale_price) / regular_price_estimated`
- ≥50% → 18 pts; 30–49% → 13; 20–29% → 8; 10–19% → 4; <10% → 1
- Absolute dollar bonus (stacks): ≥$5 → +7; $2–$4.99 → +4; $1–$1.99 → +2; <$1 → 0
- Multi-buy: normalise to effective unit price before applying
- Cold-start (no regular price): substitute neutral 8 pts

**2. Deal Rarity (0–20)**
- Uses `sale_freq_chain` from trailing 52 weeks
- <10% → 20; 10–25% → 15; 26–50% → 8; 51–75% → 3; >75% → 0
- Cross-chain exclusive bonus: if no other chain has this deal active this week → +3 (capped at 20)
- Cold-start (< 4 weeks history): neutral 10 pts

**3. Item Essentiality (0–20)**
- Static lookup from `config/scoring.yaml` keyed on `category_l1/l2/l3` + name-keyword list
- Tiers:
  - Core staples (flour, eggs, butter, sugar, rice, milk, oil, pasta, bread) → 20
  - Produce / Fresh meat & seafood → 17
  - Pantry essentials (canned goods, sauces, spices) → 14
  - Dairy & Eggs (non-staple, e.g. yogurt, cheese) → 12
  - Frozen essentials (vegetables, plain proteins) → 10
  - Non-soda beverages (coffee, tea, juice) → 8
  - Bakery / Deli (specialty) → 7
  - Non-food useful (Household, Health & Beauty) → 5
  - Snacks & Confectionery → 4
  - Soda / sports drinks → 3
  - Non-food discretionary (Apparel, General Merchandise) → 1
- Name-keyword list in config can override category for core-staple boost

**4. Price Cycle Position (0–15)**
- `price_percentile = (sale_price - cycle_low_52w) / (cycle_high_52w - cycle_low_52w)`
- At/below 52w low → 15; bottom 25% → 11; 25–50% → 7; 50–75% → 3; at/near high → 0
- Cold-start: neutral 7 pts

**5. Deal Authenticity (0–15)**
- Regular price validation (0–8): stated `regular_price` within 20% of estimated → 8; 20–50% inflated → graduated; >50% inflated → 0
- Promo type quality (0–5): `bogo` / `percentage_off` → 5; `multi_buy` / `dollar_off` → 3; `rollback` → 2; `clearance` → 4; `no_promo` → 0
- Purchase limit penalty: `purchase_limit == 1` → -2
- Deal freshness: week 1 of flyer run → +2; week 2 → 0; week 3+ → -2

**6. Loyalty & Stacking Bonus (0–5)**
- Points value ≥ $2 equiv → +3; $1–$1.99 → +2; <$1 → +1
- `member_price < sale_price` → +2 (stacks)
- Capped at 5

---

### Confidence sub-signals (each 0.0–1.0)

**1. `history_depth_conf`**
| Weeks observed | Value |
|---|---|
| ≥ 26 | 1.0 |
| 12–25 | 0.7 |
| 4–11 | 0.4 |
| 1–3 | 0.2 |
| 0 (cold start) | 0.0 |

**2. `price_basis_conf`** — derived from regular price estimation source (see Phase B above)

**3. `match_tier_conf`**
| Tier | Value |
|---|---|
| Strict SKU | 1.0 |
| Probable match | 0.6 |
| Category fallback | 0.2 |

**4. `chain_coverage_conf`**
| Chains | Value |
|---|---|
| ≥ 4 | 1.0 |
| 2–3 | 0.7 |
| 1 | 0.4 |

**5. `category_coverage_conf`**
| Sibling observations in L3 | Value |
|---|---|
| ≥ 100 | 1.0 |
| 20–99 | 0.7 |
| 5–19 | 0.4 |
| < 5 | 0.1 |

**Aggregation (weights in `config/scoring.yaml`):**
```
confidence_weights:
  history_depth:     0.35
  price_basis:       0.30
  match_tier:        0.20
  chain_coverage:    0.10
  category_coverage: 0.05
```

`confidence = weighted_average(sub-signals)`
`confidence_label`: ≥ 0.75 → `"High"`; 0.45–0.74 → `"Medium"`; < 0.45 → `"Low"`

Confidence **never suppresses** output — every active deal appears; consumer filters by threshold.

---

### Output schema additions (appended to FlyerItem fields)

| Column | Type | Description |
|---|---|---|
| `deal_score` | float | 0–100 composite grade |
| `score_discount_depth` | float | Component 1 (0–25) |
| `score_deal_rarity` | float | Component 2 (0–20) |
| `score_essentiality` | float | Component 3 (0–20) |
| `score_cycle_position` | float | Component 4 (0–15) |
| `score_authenticity` | float | Component 5 (0–15) |
| `score_loyalty_bonus` | float | Component 6 (0–5) |
| `confidence` | float | 0.0–1.0 overall |
| `confidence_history_depth` | float | Sub-signal 1 |
| `confidence_price_basis` | float | Sub-signal 2 |
| `confidence_match_tier` | float | Sub-signal 3 |
| `confidence_chain_coverage` | float | Sub-signal 4 |
| `confidence_category_coverage` | float | Sub-signal 5 |
| `confidence_label` | str | `"High"` / `"Medium"` / `"Low"` |
| `match_tier` | str | `"strict"` / `"probable"` / `"category"` |
| `regular_price_estimated` | float | Baseline used for discount calc |
| `regular_price_source` | str | How it was derived |
| `scored_on` | str | ISO date of scoring run |

Writes to:
- **`db/scores/active_scores.parquet`** — `flyer_valid_from <= today <= flyer_valid_to`; overwritten each run
- **`db/scores/archived_scores.parquet`** — all past scored deals; append-only, deduplicated on `(flyer_id, sku, store_id)`

---

## Phase D — Config File

**`config/scoring.yaml`** sections:
- `score_weights` — component point maxima
- `discount_depth_thresholds` — pct_off breakpoints and points
- `absolute_dollar_thresholds` — dollar-save breakpoints and points
- `essentiality_tiers` — L1/L2/L3 → tier mapping
- `staple_keywords` — name-keyword list → tier override
- `confidence_weights` — sub-signal weights
- `confidence_thresholds` — breakpoints for each sub-signal
- `cold_start_neutrals` — neutral point values per component

---

## Phase E — Build Pipeline Integration
*(depends on Phase C)*

Extend `pipeline/build_db.py` with:
- `build_products()` — calls product_resolver, writes `products.parquet`
- `build_price_history()` — calls price_history, writes `price_history.parquet`
- `build_scores()` — calls deal_scorer, writes active/archived parquet
- `--score` CLI flag on `main()` — runs phases A→C after observations; off by default

---

## Phase F — Tests
*(parallel with Phase E)*

- `tests/test_product_resolver.py` — strict match, probable match, category fallback, name fingerprint collisions
- `tests/test_price_history.py` — regular price estimation priority cascade, sale_freq calculation, cold-start
- `tests/test_deal_scorer.py` — each score component with known inputs; confidence sub-signal edge cases; "Low conf, High score" scenario

---

## Verification

1. `python -m pipeline.build_db --score` completes without error on current `cleaned/` data
2. `db/scores/active_scores.parquet` contains only flyers where `flyer_valid_from <= today <= flyer_valid_to`
3. Any product appearing for the first time → `confidence_label == "Low"`, all `neutral` component values applied
4. A SKU-matched product in 4+ chains → `confidence_chain_coverage == 1.0`
5. Manual spot-check: top 10 deals are big-discount staples; bottom 10 are tiny-discount chips/soda
6. Re-running scorer produces identical `active_scores.parquet` and no duplicate rows in `archived_scores.parquet`

---

## Confirmed decisions

- Product resolver: strict + probable + category fallback tiers
- Active flyer definition: `flyer_valid_from <= today <= flyer_valid_to`
- Scoring scope: only active flyers scored; historical observations feed features only
- Essentiality config: YAML at `config/scoring.yaml` (also holds confidence weights and all thresholds)
- Cold-start: neutral mid-range values substituted for unknown components (not excluded, not zeroed)
- Score storage: separate `active_scores.parquet` (overwritten) and `archived_scores.parquet` (append-only)
- Confidence: always show, never suppress — consumer sets own threshold
- New dependency: `pyyaml` added to `requirements.txt`
