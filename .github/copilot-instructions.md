# Copilot Instructions

## Project overview

This project fetches and archives weekly grocery flyers from 25 Canadian supermarket chains. It uses two source APIs — the Flipp FlyerKit API (for Loblaws, Sobeys, and Walmart groups) and the Metro Digital Azure API (for the Metro group) — and saves product data as structured JSON files under `data/`.

## Repository layout

```
fetch_flyers.py     # Main entry point — orchestrates all portfolio/brand fetches
fetch_stores.py     # Store scanner — builds data/<folder>/stores.json
flipp.py            # Flipp brand dataclass, portfolios, HTTP helpers, logger
azure.py            # Metro MetroBrand dataclass, portfolios, HTTP helpers
requirements.txt    # Python dependencies (requests only)
data/<folder>/
  stores.json         # Known stores, keyed by store_code or store_id (append-only)
  store_flyers.json   # Accumulated publication/flyer listings per store (append-only)
  flyers/<id>.json    # Products for one publication or flyer job (never overwritten)
logs/<folder>/
  YYYY-MM-DD_verbose.log
  YYYY-MM-DD_summary.log
documentation/
  METRO_API.md        # Metro Digital API reference
  Stores.md           # Raw credential reference per brand
```

## Portfolios and brands

### Flipp-based (`flipp.py`)
- **LOBLAWS_PORTFOLIO** — Loblaws, No Frills, Provigo, Real Canadian Superstore, Maxi, Zehrs, Fortinos, Atlantic Superstore, Dominion, Independent Grocer, Independent City Market, Freshmart
- **SOBEYS_PORTFOLIO** — Sobeys, Safeway, IGA, Freshco, Foodland, Longos, Farm Boy
- **WALMART_PORTFOLIO** — Walmart (filtered to `flyer_type="groceryflyer"`)

### Metro-based (`azure.py`)
- **METRO_PORTFOLIO** — Metro Ontario, Metro Quebec, Food Basics, Super C, Adonis

## Key dataclasses

### `Brand` (flipp.py)
| Field | Type | Purpose |
|---|---|---|
| `name` | `str` | Display name |
| `slug` | `str \| None` | Flipp URL slug; `None` = brand skipped |
| `folder` | `str` | Subdirectory under `data/` and `logs/` |
| `access_token` | `str` | Per-brand Flipp access token |
| `code_range` | `range` | Store-code sweep range for `fetch_stores.py` |
| `flyer_type_filter` | `str \| None` | Filter publications by `flyer_type` (Walmart only) |

### `MetroBrand` (azure.py)
| Field | Type | Purpose |
|---|---|---|
| `name` | `str` | Display name |
| `folder` | `str` | Subdirectory under `data/` and `logs/` |
| `app_config_url` | `str` | SPA `app.json` URL for credential discovery |
| `banner_id` | `str \| None` | Azure APIM `Banner` header value |
| `api_key` | `str \| None` | Azure APIM `Ocp-Apim-Subscription-Key` value |
| `id_range` | `range` | Integer store-ID sweep range |
| `locale` | `str` | `"en"` for ON brands, `"fr"` for QC brands |

## Two-phase data collection

Both `fetch_flipp_brand()` and `fetch_metro_brand()` follow the same two-phase pattern:

1. **Phase 1 — store → flyers/publications**: For each store in `stores.json`, fetch active flyers/publications and append any new ones to `store_flyers.json`. New publication IDs/job numbers are collected for Phase 2.
2. **Phase 2 — download products**: For each unique new publication/job, download the full product list and save to `flyers/<id>.json`. Existing files are skipped.

Both phases are idempotent and safe to re-run.

## Flipp API conventions

- Base URL: `https://dam.flippenterprise.net/flyerkit`
- Store lookup: `GET /flyerkit/store/{slug}?store_code={code}&access_token={token}`
- Publication list: `GET /flyerkit/publications/{slug}?store_code={code}&access_token={token}`
- Products: `GET /flyerkit/publication/{id}/products?display_type=all&locale=en&access_token={token}`
- Each brand has its own `access_token`; never share tokens across brands.
- `DELAY = 0.05` seconds is applied between all HTTP requests to avoid rate limiting.

## Metro API conventions

- Base URL: `https://metrodigital-apim.azure-api.net/api`
- Every request needs two headers: `Ocp-Apim-Subscription-Key` (api_key) and `Banner` (banner_id).
- Flyer list: `GET /api/flyers/{store_id}/{locale}?date={YYYY-MM-DD}`
- Products: `POST /api/Pages/{job}/{store_id}/{locale}/search`
- The job number comes from `flyers[].title` in the flyer list response (not `flyerId`).
- Credentials are hardcoded in `METRO_PORTFOLIO` or dynamically fetched from `app_config_url` via `metro_load_credentials()`.

## Logging

`FlippLogger` (in `flipp.py`) is shared across both Flipp and Metro fetchers:
- `logger.log(msg)` — writes to stdout and the verbose log file.
- `logger.summary(msg)` — writes to stdout, verbose log, and the summary log file.
- Always call `logger.close()` at the end (handled in `_log_phase2_summary()`).

## Coding conventions

- All data files are written with `save_json()` from `flipp.py` (pretty-printed with `indent=2`).
- Store codes are always stored and compared as strings (`str(code)`).
- Never overwrite an existing flyer file; always check `os.path.exists(out_path)` before writing.
- Portfolios are plain lists; to add a new brand, append a `Brand` or `MetroBrand` to the appropriate list.
- A `Brand` with `slug=None` is silently skipped by the portfolio runner (for brands not yet confirmed).
- Keep `fetch_flyers.py` and `fetch_stores.py` thin — business logic belongs in `flipp.py` or `azure.py`.
