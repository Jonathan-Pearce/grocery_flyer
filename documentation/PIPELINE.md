# Pipeline Contracts

This document defines the expected inputs, outputs, and failure modes for the weekly deal pipeline.

## End-to-end flow

1. `python -m scripts.fetch_flyers`
2. `python -m pipeline.clean`
3. `python -m pipeline.build_db --score`
4. `python -m pipeline.flyer_ranker`
5. `python -m scripts.export_frontend_data`

Shortcut orchestrator:

```bash
python scripts/run_pipeline.py
```

## Stage contracts

### Stage 1: Fetch flyers

Entrypoint: `scripts/fetch_flyers.py`

Input:
- `data/<chain>/stores.parquet`
- Source APIs (Flipp, Metro Digital)

Output:
- `data/<chain>/store_flyers.parquet`
- `data/<chain>/flyers.parquet`
- `logs/<chain>/<date>_verbose.log`
- `logs/<chain>/<date>_summary.log`

Idempotency:
- Appends only new flyer IDs/jobs and skips existing rows by known IDs.

Common failures:
- API credential/auth failures
- transient network failures
- empty stores layer for a chain

### Stage 2: Normalize and clean

Entrypoint: `pipeline.clean`

Input:
- `data/*/flyers.parquet`

Output:
- `cleaned/<chain>.parquet`

Idempotency:
- Safe to rerun; rewrites cleaned brand outputs from current raw inputs.

Common failures:
- malformed raw records
- parser edge cases (price/name/weight/promo)

### Stage 3: Build DB and scores

Entrypoint: `pipeline.build_db --score`

Input:
- `cleaned/*.parquet`

Output:
- `db/observations/**/*.parquet`
- `db/dimensions/**/*.parquet`
- `db/features/*.parquet`
- `db/scores/active_scores.parquet`

Idempotency:
- Safe to rerun; supports incremental behavior and `--force` where implemented.

Common failures:
- missing cleaned outputs
- schema mismatches in cleaned data

### Stage 4: Flyer ranking

Entrypoint: `pipeline.flyer_ranker`

Input:
- `db/scores/active_scores.parquet`

Output:
- `db/rankings/rankings.parquet`
- `db/rankings/rankings_history.parquet`

Common failures:
- missing or empty active scores

### Stage 5: Frontend export

Entrypoint: `scripts/export_frontend_data.py`

Input:
- `db/scores/active_scores.parquet`
- `db/rankings/*.parquet`
- store geo/region intermediate outputs

Output:
- `frontend/public/data/active_scores.json`
- `frontend/public/data/stores_geo.json`
- `frontend/public/data/flyer_regions.json`
- `frontend/public/data/chain_regions.json`
- `frontend/public/data/postal_centroids.json`
- `frontend/public/data/rankings.json`
- `frontend/public/data/rankings_history.json`

## Verification checklist

After a full run:

1. `db/scores/active_scores.parquet` exists and has current-week rows.
2. `db/rankings/rankings.parquet` exists.
3. `frontend/public/data/active_scores.json` and `frontend/public/data/rankings.json` were updated.
4. Frontend build succeeds (`docker compose run --rm frontend npm run build`).

## Safe partial runs

- Single chain test:

```bash
python scripts/run_pipeline.py --store loblaws
```

- Export only frontend artifacts after DB updates:

```bash
python scripts/export_frontend_data.py
```
