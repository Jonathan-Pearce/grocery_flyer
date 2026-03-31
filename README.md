# grocery_flyer

Fetches and archives weekly grocery flyers from three Canadian supermarket chains — **Freshco**, **No Frills**, and **Metro** — saving product data as dated JSON files.

## Stores

| Store | Source API |
|---|---|
| Freshco | Flipp (`dam.flippenterprise.net`) |
| No Frills | Flipp (`dam.flippenterprise.net`) |
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
  freshco/
    2026-03-31.json   # list of flyer products from the Flipp API
  nofrills/
    2026-03-31.json
  metro/
    2026-03-31.json   # list of products from the Metro Digital API
```

## Files

| File | Description |
|---|---|
| `fetch_flyers.py` | Main entry point — fetches all three stores and saves results |
| `metro.py` | Standalone Metro API explorer (used for development/debugging) |
| `requirements.txt` | Python dependencies (`requests`) |

## APIs

**Freshco & No Frills** use the [Flipp FlyerKit API](https://dam.flippenterprise.net/flyerkit):
- Publication endpoint: `/flyerkit/publication/{id}/products?display_type=all&locale=en&access_token={token}`

**Metro** uses the Metro Digital API:
- Flyer list: `GET /api/flyers/{store_id}/en?date={date}`
- Products: `POST /api/Pages/{job}/{store_id}/en/search`
