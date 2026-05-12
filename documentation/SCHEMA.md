# Data Schema Contracts

This document defines the stable data contracts used by the fetch, pipeline,
ranking, and frontend export flow.

## Scope

- Raw ingestion artifacts under `data/<chain>/`
- Cleaned unified records under `cleaned/`
- Database outputs under `db/`
- Geo/regional helper outputs under `data/`
- Frontend JSON payloads under `frontend/public/data/`

## 1) Raw Ingestion Layer (`data/<chain>/`)

### `stores.parquet`

Columns:

- `store_code` (string)
- `province` (string)
- `store_name` (string)
- `raw_json` (string JSON blob)

Notes:

- `raw_json` preserves source-specific payload fields.
- Flipp and Metro chains both use this schema.

### `store_flyers.parquet`

Columns:

- `store_code` (string)
- `flyer_id` (string)
- `raw_json` (string JSON blob)

Notes:

- `flyer_id` is Flipp publication ID or Metro job/title.

### `flyers.parquet`

Stable envelope columns:

- `flyer_id` (string)
- `source_api` (`flipp` or `metro`)
- `fetched_on` (string/date)
- `products_url` (string)

Flipp envelope extras:

- `pub_valid_from` (string/date)
- `pub_valid_to` (string/date)
- `pub_locale` (string)

Metro envelope extras:

- `store_id` (string)

Product fields:

- Source product attributes are flattened as additional columns.
- List/dict product fields are JSON-encoded strings.
- Column set is source-dependent and may evolve as upstream APIs change.

## 2) Cleaned Unified Layer (`cleaned/*.parquet`)

Each chain writes one cleaned parquet file with `FlyerItem` schema fields from
`pipeline/schema.py`.

Representative columns include:

- Provenance: `source_api`, `store_chain`, `store_id`, `flyer_id`,
  `flyer_valid_from`, `flyer_valid_to`, `fetched_on`, `province`
- Identity: `name_en`, `name_fr`, `brand`, `sku`, `language`, `product_url`
- Pricing: `sale_price`, `regular_price`, `price_unit`, `price_per_kg`,
  `price_per_lb`, `multi_buy_qty`, `multi_buy_total`, `member_price`
- Promotion: `promo_type`, `promo_details`, `loyalty_program`, `loyalty_points`
- Package/weight: `weight_value`, `weight_unit`, `pack_count`, `pack_unit_size`
- Categories: `category_l1`..`category_l4`, `is_food`, `is_human_food`
- Multi-product: `is_multi_product`, `parent_record_id`, `multi_product_variants`
- Tracking: `price_observation_key`

## 3) DB Layer (`db/`)

### Observations (`db/observations/.../*.parquet`)

Partitioning:

- `store_chain=<chain>/year=<iso_year>/week=<iso_week>/<flyer_id>.parquet`

Row schema:

- Derived from cleaned `FlyerItem` records.

### Dimensions

`db/dimensions/stores.parquet`:

- `store_chain`, `store_id`, `store_name`, `banner`, `province`, `city`, `postal_code`

`db/dimensions/flyers.parquet`:

- `flyer_id`, `store_chain`, `store_id`, `valid_from`, `valid_to`, `language`, `province`

### Scores (`db/scores/active_scores.parquet`)

Current columns:

- `flyer_id`, `sku`, `store_id`, `store_chain`, `name_en`, `name_fr`
- `sale_price`, `regular_price`, `regular_price_estimated`, `regular_price_source`
- `flyer_valid_from`, `flyer_valid_to`, `price_unit`, `promo_type`, `image_url`
- `deal_score`
- component scores:
  `score_discount_depth`, `score_deal_rarity`, `score_essentiality`,
  `score_cycle_position`, `score_authenticity`, `score_loyalty_bonus`
- confidence outputs:
  `confidence`, `confidence_history_depth`, `confidence_price_basis`,
  `confidence_match_tier`, `confidence_chain_coverage`,
  `confidence_category_coverage`, `confidence_label`
- metadata:
  `match_tier`, `scored_on`, `category_l1`, `category_l2`, `brand`

Notes:

- `active_scores.parquet` is overwritten each scoring run.

## 4) Rankings Layer (`db/rankings/`)

### `current_chain_rankings.parquet`

- `week_label`, `store_chain`, `flyer_count`, `item_count`, `hot_count`,
  `hot_ratio`, `avg_flyer_grade`, `letter_grade`, `rank`

### `current_flyer_rankings.parquet`

- `flyer_id`, `store_chain`, `flyer_valid_from`, `flyer_valid_to`, `item_count`,
  `hot_count`, `good_count`, `avg_score`, `top10_avg`, `hot_ratio`,
  `flyer_grade`, `letter_grade`, `week_label`

### `weekly_history.parquet`

- append-only history keyed by `(week_label, store_chain)`
- shape aligns with chain ranking rows plus week metadata

## 5) Geo/Regional Helper Layer (`data/`)

### `stores_geo.parquet`

- `chain`, `store_code`, `store_name`, `address`, `city`, `province`,
  `postal_code`, `lat`, `lon`, `source_api`, `geo_source`

### `flyer_regions.parquet`

- `chain`, `region_id`, `valid_from`, `valid_to`, `store_codes`,
  `postal_fsas`, `postal_codes`, `store_count`, `multi_flyer_stores`

Notes:

- `store_codes`, `postal_fsas`, `postal_codes`, `multi_flyer_stores` are
  JSON-encoded arrays.

## 6) Frontend Export JSON Contracts (`frontend/public/data/`)

### `active_scores.json.gz`

- Selected subset of score fields exported from `db/scores/active_scores.parquet`
- sorted descending by `deal_score`

### `stores_geo.json`

Per-store records with map-ready fields:

- `chain`, `store_code`, `store_name`, `address`, `city`, `province`,
  `postal_code`, `lat`, `lon`, `geo_source`

### `flyer_regions.json`

- Region-level records with decoded list fields:
  `store_codes`, `postal_fsas`, `postal_codes`, `multi_flyer_stores`

### `postal_centroids.json`

- object map: `{FSA: [lat, lon]}`

### `rankings.json`

- object with keys:
  - `chains`: current chain ranking rows
  - `flyers`: current flyer ranking rows

### `rankings_history.json`

- list of week buckets:
  - `week_label`
  - `chains`: ranking rows for that week

## Compatibility Notes

- Raw product columns in `data/<chain>/flyers.parquet` are intentionally flexible.
- Cleaned, score, ranking, and frontend export schemas are the contracts that
  downstream consumers should rely on.
- If a contract field changes, update this file and corresponding parser/export
  tests in the same change.