# grocery_flyer

Fetches and archives weekly grocery flyers from 25 Canadian supermarket chains, saving product data as structured JSON files.

## Stores

### Sobeys Group
| Store | Folder | Source API |
|---|---|---|
| Sobeys | `sobeys` | Flipp (`dam.flippenterprise.net`) |
| Safeway | `safeway` | Flipp (`dam.flippenterprise.net`) |
| IGA | `iga` | Flipp (`dam.flippenterprise.net`) |
| Freshco | `freshco` | Flipp (`dam.flippenterprise.net`) |
| Foodland | `foodland` | Flipp (`dam.flippenterprise.net`) |
| Longos | `longos` | Flipp (`dam.flippenterprise.net`) |
| Farm Boy | `farm_boy` | Flipp (`dam.flippenterprise.net`) |

### Loblaws Group
| Store | Folder | Source API |
|---|---|---|
| Loblaws | `loblaws` | Flipp (`dam.flippenterprise.net`) |
| No Frills | `nofrills` | Flipp (`dam.flippenterprise.net`) |
| Real Canadian Superstore | `real_canadian_superstore` | Flipp (`dam.flippenterprise.net`) |
| Provigo | `provigo` | Flipp (`dam.flippenterprise.net`) |
| Maxi | `maxi` | Flipp (`dam.flippenterprise.net`) |
| Zehrs | `zehrs` | Flipp (`dam.flippenterprise.net`) |
| Fortinos | `fortinos` | Flipp (`dam.flippenterprise.net`) |
| Atlantic Superstore | `atlantic_superstore` | Flipp (`dam.flippenterprise.net`) |
| Dominion | `dominion` | Flipp (`dam.flippenterprise.net`) |
| Independent Grocer | `independent_grocer` | Flipp (`dam.flippenterprise.net`) |
| Independent City Market | `independent_city_market` | Flipp (`dam.flippenterprise.net`) |
| Freshmart | `freshmart` | Flipp (`dam.flippenterprise.net`) |

### Metro Group
| Store | Folder | Source API |
|---|---|---|
| Metro Ontario | `metro` | Metro Digital (`metrodigital-apim.azure-api.net`) |
| Metro Quebec | `metro_qc` | Metro Digital (`metrodigital-apim.azure-api.net`) |
| Food Basics | `food_basics` | Metro Digital (`metrodigital-apim.azure-api.net`) |
| Super C | `super_c` | Metro Digital (`metrodigital-apim.azure-api.net`) |
| Adonis | `adonis` | Metro Digital (`metrodigital-apim.azure-api.net`) |

### Other
| Store | Folder | Source API |
|---|---|---|
| Walmart | `walmart` | Flipp (`dam.flippenterprise.net`) |

## Usage

```bash
pip install -r requirements.txt

# Fetch flyers for all stores
python fetch_flyers.py

# Fetch flyers for a single portfolio
python fetch_flyers.py --portfolio loblaws
python fetch_flyers.py --portfolio sobeys
python fetch_flyers.py --portfolio metro

# Fetch flyers for a single brand within a portfolio
python fetch_flyers.py --portfolio metro --brand food_basics
python fetch_flyers.py --portfolio loblaws --brand nofrills
```

Run `fetch_stores.py` first for any new brand to populate `data/<folder>/stores.json` before fetching flyers.

```bash
python fetch_stores.py                                         # all portfolios
python fetch_stores.py --portfolio loblaws                     # all Loblaws brands
python fetch_stores.py --portfolio loblaws --brand nofrills
python fetch_stores.py --portfolio metro --brand food_basics --start 1 --end 500
```

## Output structure

Each brand folder under `data/` contains:

```
data/<folder>/
  stores.json          # All known stores for this brand (keyed by store code/ID)
  store_flyers.json    # Accumulated flyer/publication listings per store
  flyers/
    <id>.json          # Full product list for a single publication or flyer job
```

Both `stores.json` and `store_flyers.json` are **append-only** — existing entries are never overwritten. Individual flyer files are also skipped if they already exist on disk, making every run safe to re-run.

```
data/
  sobeys/          safeway/          iga/              freshco/
  foodland/        longos/           farm_boy/
  loblaws/         nofrills/         real_canadian_superstore/
  provigo/         maxi/             zehrs/            fortinos/
  atlantic_superstore/  dominion/   independent_grocer/
  independent_city_market/           freshmart/
  metro/           metro_qc/         food_basics/      super_c/   adonis/
  walmart/
```

## Logs

Each run writes two log files under `logs/<folder>/`:

| File | Contents |
|---|---|
| `YYYY-MM-DD_verbose.log` | Full request-level detail |
| `YYYY-MM-DD_summary.log` | High-level counts and timing |

## Files

| File | Description |
|---|---|
| `fetch_flyers.py` | Main entry point — fetches all stores and saves flyer products |
| `fetch_stores.py` | Store scanner — sweeps store code/ID ranges to build `stores.json` |
| `flipp.py` | Flipp API helpers: `Brand` dataclass, portfolio configs, HTTP utils, logger |
| `azure.py` | Metro Azure API helpers: `MetroBrand` dataclass, portfolio config, HTTP utils |
| `requirements.txt` | Python dependencies (`requests`) |
| `documentation/METRO_API.md` | Metro Digital API reference (endpoints, auth, response shapes) |
| `documentation/Stores.md` | Raw API credential reference per brand |

## APIs

### Flipp FlyerKit API

Base URL: `https://dam.flippenterprise.net/flyerkit`

| Endpoint | Description |
|---|---|
| `GET /flyerkit/store/{slug}?store_code={code}&access_token={token}` | Look up a store by code |
| `GET /flyerkit/publications/{slug}?store_code={code}&access_token={token}` | List active publications for a store |
| `GET /flyerkit/publication/{id}/products?display_type=all&locale=en&access_token={token}` | Fetch all products in a publication |

Each brand has its own `access_token`. Walmart uses `flyer_type_filter="groceryflyer"` to exclude non-grocery publications.

### Metro Digital API

Base URL: `https://metrodigital-apim.azure-api.net/api`

Every request requires two headers: `Ocp-Apim-Subscription-Key` (the brand's `api_key`) and `Banner` (the brand's `banner_id`). Credentials are hardcoded per brand in `azure.py` or fetched at runtime from each brand's `app.json` config URL.

| Endpoint | Description |
|---|---|
| `GET /api/flyers/{store_id}/{locale}?date={YYYY-MM-DD}` | List flyers active on a given date |
| `POST /api/Pages/{job}/{store_id}/{locale}/search` | Fetch all ad-block products for a flyer job |

See [documentation/METRO_API.md](documentation/METRO_API.md) for full request/response details.
