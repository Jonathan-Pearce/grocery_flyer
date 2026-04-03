# Plan: Grocery Flyer Data Cleaning & Normalization Pipeline

## TL;DR
Build a Python pipeline that ingests raw flyer JSON from two API sources (Flipp Enterprise + Metro Azure), normalizes all items into a unified schema, and outputs cleaned records ready for price tracking, cross-store comparison, and recipe planning applications. The pipeline handles schema unification, price parsing, product name normalization, weight/unit extraction, multi-item splitting, category harmonization, and promo/deal classification.

---

## Data Nuances Identified

### Price Challenges
- Two completely different schemas (Flipp 3-part: pre/price/post vs Metro: salePrice/regularPrice/alternatePrice)
- French comma decimals ("14,99" vs "14.99")
- Compound regularPrice strings ("3,99/lb - 8,80/kg")
- Multi-buy encoding ("2/" in pre_price_text → "2/$5.00")
- Percentage-only discounts with no base price ("SAVE 25%", "15% off")
- "starting at" prefix meaning price is a floor
- Missing prices entirely (some items are promo-only)
- Over-limit pricing in disclaimers ("LIMIT 4 OVER LIMIT PAY 10.49 EA")
- Cents indicator (priceSign: "¢")
- Member prices vs regular sale prices (Metro memberPriceEn)

### Product Name Challenges
- Multi-product entries joined by "OR", "or", "OU" ("SCHNEIDERS OR MAPLE LEAF BACON")
- Completely different products in one entry ("CHESTNUTS, 85 G OR CORN KERNEL, 340-410 G")
- OCR artifacts ("ORCRUSHED" → "OR CRUSHED")
- Trademark symbols (™, ®)
- Weight embedded in name ("153 G", "3x70 mL", "24/341-355 ml")
- Brand sometimes in name, sometimes in separate field
- ALL CAPS vs mixed case vs French names
- Bilingual mixing in single fields

### Weight/Unit Challenges
- Weight ranges ("65 - 375 g", "70-79 G")
- Multi-pack notation ("6x355 mL", "24/341-355 ml", "3's")
- Unit inconsistencies (mL vs L, g vs kg, lb vs /lb)
- Obvious data errors ("1.89 mL" should be "1.89 L")
- "SELECTED VARIETIES" means weight/size varies
- Dual imperial/metric ("$/lb" + "$/kg")

### Category Challenges
- Flipp has Google taxonomy (l1→l7) + simple categories array
- Metro has mainCategoryEn/Fr + subCategoryEn/Fr (often null)
- Maxi/Provigo categories in French only
- Non-grocery items mixed in (bikes, dog toys, software, diapers)
- Pet food vs human food distinction needed

### Promotional Offer Challenges
- Loyalty points ("100 Scene+ PTS when you buy 2", "PC Optimum 6,000 pts")
- Complex earn rules ("$6 back in points for every $30 spent on...")
- Rollback, BOGO, percentage off, dollar off, multi-buy
- Day-restricted deals ("jeudi et vendredi" / Thursday and Friday)
- "WHILE SUPPLIES LAST" availability caveats

### Non-Product Records
- Banner/ad items (item_type=5 in Flipp, actionType="Inblock"/"URL" in Metro)
- Video content blocks
- Null-heavy placeholder records

### Temporal/Regional
- Flyer validity windows (validFrom/validTo) in different timezone formats
- Store-specific pricing (same chain, different stores)
- Province-level variation (ON, QC, etc.)

---

## Steps

### Phase 1: Schema Unification & Record Filtering
*Goal: Get all records into one schema, discard non-products*

1. **Define the unified output schema** as a Python dataclass or Pydantic model with fields:
   - `source_api` (flipp | metro), `store_chain`, `store_id`, `flyer_id`
   - `flyer_valid_from`, `flyer_valid_to`, `fetched_on`
   - `raw_name`, `raw_description`, `raw_body`
   - `sale_price`, `regular_price`, `price_unit`, `price_per_kg`, `price_per_lb`
   - `alternate_price`, `alternate_unit`
   - `pre_price_text`, `post_price_text` (keep originals for audit)
   - `currency` (CAD always, but explicit)
   - `brand`, `sku`, `product_url`, `image_url`
   - `category_l1` through `category_l4` (harmonized)
   - `raw_categories` (preserve originals)
   - `promo_type`, `promo_details`, `loyalty_program`, `loyalty_points`
   - `purchase_limit`, `over_limit_price`
   - `weight_value`, `weight_unit`, `weight_is_range`, `weight_min`, `weight_max`
   - `pack_count`, `pack_unit_size`, `pack_unit`
   - `is_multi_product`, `multi_product_variants` (list)
   - `language` (en | fr | bil)
   - `name_en`, `name_fr`, `description_en`, `description_fr`
   - `province`, `tax_indicator`
   - `is_food`, `is_human_food`

2. **Build a Flipp normalizer** (`normalize_flipp.py`) that maps Flipp product fields → unified schema
   - Map `pre_price_text + price_text + post_price_text` → price fields
   - Map `item_categories.l1-l4` → category hierarchy
   - Map `sale_story` → promo fields
   - Map `disclaimer_text` → purchase limit fields
   - Filter out `item_type=5` (banners)

3. **Build a Metro normalizer** (`normalize_metro.py`) that maps Metro product fields → unified schema
   - Map `salePrice`, `regularPrice`, `alternatePrice`, `memberPrice*` → price fields
   - Map `mainCategoryEn/Fr`, `subCategoryEn/Fr` → category hierarchy
   - Map `waysToSave_EN`, `savings*` → promo fields
   - Filter out `actionType="Inblock"` and `actionType="URL"` records
   - Handle French comma decimals

4. **Build a loader** (`load_raw.py`) that walks `data/<store>/flyers/*.json`, detects API source from top-level keys (`publication_id` → Flipp, `job` → Metro), and routes to the correct normalizer
   - *Depends on steps 1-3*

### Phase 2: Price Parsing & Normalization
*Goal: Every record has clean numeric prices and unit info*

5. **Implement price parser** (`parsers/price_parser.py`):
   - Strip `$`, commas→periods, whitespace
   - Handle "2/$5.00" → unit_price=2.50, multi_buy_qty=2, multi_buy_total=5.00
   - Handle "starting at" → `price_is_floor=True`
   - Handle "¢" → divide by 100
   - Handle compound strings like "3,99/lb - 8,80/kg" → extract both
   - Parse over-limit pricing from disclaimer text ("LIMIT 4 OVER LIMIT PAY 10.49 EA")
   - Validate lb↔kg consistency (1 lb = 0.453592 kg)
   - *Parallel with step 6*

6. **Implement promo/deal classifier** (`parsers/promo_parser.py`):
   - Classify into: rollback, percentage_off, dollar_off, multi_buy, bogo, loyalty_points, member_price, clearance, no_promo
   - Extract loyalty details: program (Scene+, PC Optimum), points amount, trigger condition
   - Extract percentage/dollar savings amounts
   - Parse day restrictions ("jeudi et vendredi")
   - *Parallel with step 5*

### Phase 3: Product Name & Entity Extraction
*Goal: Clean names, extract brand/weight/pack info, identify multi-product entries*

7. **Implement name cleaner** (`parsers/name_parser.py`):
   - Normalize case (title case for product names)
   - Strip ™, ®, special chars
   - Fix common OCR artifacts ("ORCRUSHED" → "OR CRUSHED")
   - Separate brand from product name when brand is embedded
   - Detect language (EN/FR/bilingual) per field
   - *Depends on Phase 1 schema*

8. **Implement weight/size extractor** (`parsers/weight_parser.py`):
   - Regex patterns for: "NNN g", "NNN mL", "N.N L", "NNN kg", "N lb"
   - Handle ranges: "65 - 375 g" → min=65, max=375, unit=g, is_range=True
   - Handle multi-packs: "6x355 mL" → pack_count=6, pack_unit_size=355, pack_unit=mL
   - Handle pack notation: "24/341-355 ml" → pack_count=24, size_range
   - Handle count notation: "3's", "24's" → pack_count=N
   - Normalize units to metric (lb→kg, oz→g)
   - Sanity-check values (flag "1.89 mL" as likely "1.89 L")
   - *Parallel with step 7*

9. **Implement multi-product splitter** (`parsers/multi_product_parser.py`):
   - Detect separators: " OR ", " or ", " OU ", " / ", " AND ", " ET "
   - Heuristic: if products on each side have independent weights, split into separate records
   - Link split records back to parent via `parent_record_id`
   - Preserve original combined name in `raw_name`
   - Use `custom_id_field_1-6` SKUs to identify individual variants
   - *Depends on steps 7, 8*

### Phase 4: Category Harmonization
*Goal: Unified category hierarchy across all stores*

10. **Build category mapping** (`categories/category_map.py`):
    - Use Flipp's Google taxonomy as the base hierarchy (it's the richest)
    - Map Metro's `mainCategoryEn` values to Google taxonomy L1/L2
    - Translate French-only categories to English equivalents
    - Add `is_food` and `is_human_food` boolean flags
    - Provide a top-level grocery taxonomy: Produce, Meat & Seafood, Dairy, Bakery, Pantry, Frozen, Beverages, Snacks, Deli, Health & Beauty, Household, Pet, Baby, Other
    - *Parallel with Phase 3*

### Phase 5: Output & Validation
*Goal: Write cleaned data, validate quality*

11. **Build output writer** that saves cleaned records as:
    - One JSON file per store per flyer: `cleaned/<store>/<flyer_id>.json`
    - A combined CSV/Parquet for analytical queries
    - *Depends on all above*

12. **Build validation/QA report** (`validate.py`):
    - Count records processed, filtered, split
    - Flag records with missing prices (expected vs unexpected)
    - Flag suspicious weight values (sanity checks)
    - Flag records that failed parsing (keep raw, mark as needs_review)
    - Compare sale_price < regular_price when both present
    - Cross-validate alternatePrice lb↔kg math
    - Report category coverage (% categorized)
    - *Depends on step 11*

---

## Relevant Files

- `data/<store>/flyers/*.json` — Raw input files (two schemas: Flipp and Metro)
- `data/<store>/store_flyers.json` — Flyer metadata (dates, province, store name)
- `data/<store>/stores.json` — Store registry (banner, name)
- `flipp.py` — Existing Flipp API client (reference for field names)
- `azure.py` — Existing Metro API client (reference for field names)
- `fetch_flyers.py` — Fetch orchestrator (reference for store→API routing)
- `Stores.md` — Store list and portfolio groupings

## New Files to Create
- `clean.py` — Main entry point / orchestrator
- `schema.py` — Unified output schema (Pydantic model)
- `normalize_flipp.py` — Flipp → unified schema mapper
- `normalize_metro.py` — Metro → unified schema mapper
- `load_raw.py` — Raw file loader + API source detector
- `parsers/price_parser.py` — Price string parsing
- `parsers/promo_parser.py` — Promo/deal classification
- `parsers/name_parser.py` — Name cleaning & brand extraction
- `parsers/weight_parser.py` — Weight/size extraction
- `parsers/multi_product_parser.py` — Multi-product splitting
- `categories/category_map.py` — Category harmonization mappings
- `validate.py` — QA report generator

---

## Verification

1. Run `python clean.py` end-to-end on all existing data in `data/` — should complete without crashes
2. Spot-check 10+ items from each store in cleaned output for correctness:
   - Food Basics bacon ("65 - 375 g") → weight_is_range=True, weight_min=65, weight_max=375
   - Adonis turkey (French, comma pricing) → regular_price=3.99, price_unit=lb
   - Loblaws multi-product chestnuts/corn → is_multi_product=True, 2 split records
   - Walmart banner items → filtered out (not in output)
   - Sobeys Scene+ points items → loyalty_program="Scene+", loyalty_points=100
   - Atlantic Superstore limit items → purchase_limit=4, over_limit_price=10.49
   - Metro Inblock video items → filtered out
3. Validate no price has comma decimals in output (all normalized to period)
4. Validate all weight_unit values are from allowed set: g, kg, mL, L, lb, oz, count
5. Validate category_l1 coverage > 80% of food items
6. Run validate.py and review QA report for anomalies

---

## Decisions

- **Python-only pipeline** — matches existing codebase (no Spark/dbt overhead)
- **Pydantic for schema** — provides validation, serialization, clear field documentation
- **Google taxonomy as category base** — Flipp already provides it; map Metro into it
- **Keep raw fields alongside cleaned** — enables debugging and iterative improvement
- **Split multi-product entries** into separate records with parent linkage
- **Metric normalization** — store both original and metric-normalized weights
- **No ML/NLP in v1** — use regex/rule-based parsing; ML entity extraction can come later
- **Parquet output** for analytics — efficient columnar format for price tracking queries

## Further Considerations

1. **Product identity/deduplication across stores**: Same product (e.g., "Heinz Beans 398mL") appears across Loblaws, No Frills, Walmart with slightly different names. A future step could create a canonical product ID using brand + normalized name + weight. → Recommend deferring to Phase 2 of the project, but designing the schema to support it now.

2. **Historical price tracking schema**: For tracking prices over time, the cleaned records need a composite key (store_chain + store_id + sku + flyer_valid_from). Should we define this key now or leave it to the application layer? → Recommend defining it in the schema as `price_observation_key`.

3. **French language handling depth**: Should French names be translated to English for unified search, or kept as parallel fields? → Recommend keeping parallel `name_en`/`name_fr` fields and adding search to the application layer.
