# Operations Runbook

Weekly operation for grocery flyer ingestion and ranking.

## Standard weekly run

### A. Fetch latest flyers

```bash
python -m scripts.fetch_flyers
```

### B. Build clean + scored pipeline outputs

```bash
python scripts/run_pipeline.py
```

### C. Validate frontend payload build

```bash
docker compose run --rm frontend npm run build
```

## Docker-based execution

If running through existing backend container:

```bash
docker exec grocery-flyer-backend python3 scripts/fetch_flyers.py
docker exec grocery-flyer-backend python3 scripts/run_pipeline.py
```

## Recovery playbooks

### 1. One chain failed in fetch step

Symptoms:
- logs show auth or API errors for one brand
- that chain has stale `flyers.parquet`

Recovery:
1. rerun stores scan if store set may be stale:
   `python -m scripts.fetch_stores --portfolio <portfolio> --brand <folder>`
2. rerun targeted fetch:
   `python -m scripts.fetch_flyers --portfolio <portfolio> --brand <folder>`
3. rerun pipeline for one chain:
   `python scripts/run_pipeline.py --store <folder>`
4. rerun full export:
   `python scripts/export_frontend_data.py`

### 2. Pipeline succeeded but rankings missing

Symptoms:
- `db/scores/active_scores.parquet` exists
- `frontend/public/data/rankings.json` missing/old

Recovery:
1. run ranker:
   `python -m pipeline.flyer_ranker`
2. export rankings:
   `python scripts/export_frontend_data.py --rankings-only`

### 3. Frontend shows empty deals

Symptoms:
- UI renders but no deals

Checks:
1. verify `frontend/public/data/active_scores.json` exists and non-empty
2. verify active week data in `db/scores/active_scores.parquet`
3. rerun export:
   `python scripts/export_frontend_data.py`

### 4. Geo/region map data missing

Recovery sequence:
1. run `python scripts/build_stores_geo.py`
2. run `python scripts/analyze_regions.py`
3. run `python scripts/export_frontend_data.py`

## Operational checks after each weekly run

1. Confirm current-date log files exist under `logs/<chain>/`.
2. Confirm `db/scores/active_scores.parquet` timestamp is current.
3. Confirm `frontend/public/data/active_scores.json` and `rankings.json` updated.
4. Confirm frontend build passes.

## Incident notes

- Fetch and pipeline stages are designed to be rerunnable.
- Prioritize chain-specific reruns before full reruns to reduce runtime.
- Avoid manual edits to generated outputs under `db/`, `cleaned/`, or `frontend/public/data/`.
