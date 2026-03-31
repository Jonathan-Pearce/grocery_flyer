# grocery_flyer

Fetches and archives weekly grocery flyers from 19 Canadian supermarket chains, saving product data as dated JSON files.

## Stores

### Sobeys Group
| Store | Source API |
|---|---|
| Sobeys | Flipp (`dam.flippenterprise.net`) |
| Safeway | Flipp (`dam.flippenterprise.net`) |
| IGA | Flipp (`dam.flippenterprise.net`) |
| Freshco | Flipp (`dam.flippenterprise.net`) |
| Foodland | Flipp (`dam.flippenterprise.net`) |
| Longos | Flipp (`dam.flippenterprise.net`) |
| Farm Boy | Flipp (`dam.flippenterprise.net`) |

### Loblaws Group
| Store | Source API |
|---|---|
| Loblaws | Flipp (`dam.flippenterprise.net`) |
| No Frills | Flipp (`dam.flippenterprise.net`) |
| Real Canadian Superstore | Flipp (`dam.flippenterprise.net`) |
| Provigo | Flipp (`dam.flippenterprise.net`) |
| Maxi | Flipp (`dam.flippenterprise.net`) |
| Zehrs | Flipp (`dam.flippenterprise.net`) |
| Fortinos | Flipp (`dam.flippenterprise.net`) |
| Atlantic Superstore | Flipp (`dam.flippenterprise.net`) |
| Dominion | Flipp (`dam.flippenterprise.net`) |
| Independent Grocer | Flipp (`dam.flippenterprise.net`) |

### Other
| Store | Source API |
|---|---|
| Walmart | Flipp (`dam.flippenterprise.net`) |
| Metro | Metro Digital (`metrodigital-apim.azure-api.net`) |

## Usage

```bash
pip install -r requirements.txt
python fetch_flyers.py
```

Each run saves today's flyer data under `data/<store>/YYYY-MM-DD.json`. Flyers update weekly.

## Output structure

```
data/
  sobeys/
  safeway/
  iga/
  freshco/
  foodland/
  longos/
  farm_boy/
  loblaws/
  nofrills/
  real_canadian_superstore/
  provigo/
  maxi/
  zehrs/
  fortinos/
  atlantic_superstore/
  dominion/
  independent_grocer/
  walmart/
  metro/
```

Each folder contains dated JSON files (e.g. `2026-03-31.json`) with the full list of flyer products.

## Files

| File | Description |
|---|---|
| `fetch_flyers.py` | Main entry point — fetches all stores and saves results |
| `metro.py` | Standalone Metro API explorer (used for development/debugging) |
| `requirements.txt` | Python dependencies (`requests`) |

## APIs

**All Flipp stores** use the [Flipp FlyerKit API](https://dam.flippenterprise.net/flyerkit):
- Publication endpoint: `/flyerkit/publication/{id}/products?display_type=all&locale=en&access_token={token}`

**Metro** uses the Metro Digital API:
- Flyer list: `GET /api/flyers/{store_id}/en?date={date}`
- Products: `POST /api/Pages/{job}/{store_id}/en/search`
