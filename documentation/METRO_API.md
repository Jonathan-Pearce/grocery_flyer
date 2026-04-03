# Metro Grocery Flyer API

## Overview

Metro's flyer app (`flyer.metro.ca`) is a Vue.js SPA backed by an Azure API Management (APIM) endpoint at `https://metrodigital-apim.azure-api.net/api`. Products are not served from a REST products endpoint — they are embedded in ad block ("search") data fetched per-flyer.

---

## Authentication

Every request requires two headers:

| Header | Value |
|---|---|
| `Ocp-Apim-Subscription-Key` | `021027e7c41548bcba5d2315a155816b` |
| `Banner` | `62e3eddbffe0e6f10778a56d` |

> **Note:** The `Banner` header (capital B, no prefix) is required by Azure APIM middleware on every route. All other header name variants (`X-Banner-Id`, `banner_id`, `BannerId`, etc.) return 400 `"Banner Id is null or empty"`. The value `62e3eddbffe0e6f10778a56d` is the Metro Ontario banner ID from `app.json`.

These values were discovered by fetching and grepping the compiled Vue app bundle at `https://flyer.metro.ca/js/app.3a018fde.js`, which contained:
```js
headers: {
  "Ocp-Apim-Subscription-Key": AppConf.apikey || "021027e7c41548bcba5d2315a155816b",
  Banner: AppConf.banner_id || "62e3eddbffe0e6f10778a56d"
}
```

---

## Config Sources

| URL | Description |
|---|---|
| `https://flyer.metro.ca/config/app.json?v=...` | Returns `api`, `banner_id`, `apikey`, `api_version` |
| `https://flyer.metro.ca/js/app.3a018fde.js` | Compiled Vue app bundle — contains all API call patterns |

---

## Endpoints

### 1. List Flyers for a Store

```
GET /api/flyers/{store_id}/{locale}?date={YYYY-MM-DD}
```

| Parameter | Example | Notes |
|---|---|---|
| `store_id` | `85` | Integer store ID |
| `locale` | `en` | `en` or `fr` |
| `date` | `2026-03-31` | Returns flyers active on this date |

**Response:**
```json
{
  "serverTime": "2026-03-31T00:00:00",
  "client": "Metro",
  "banner": "Metro Ontario",
  "flyers": [
    {
      "id": "69c9f2b84a589e215a4942f5",
      "flyerId": "69c9f25b4a589e215a49424b",
      "title": "82846",
      "flyerTitle": "Weekly Flyer",
      "flyerCategory": "Weekly Flyer",
      "language": "bil",
      "startDate": "2026-03-26T00:00:00Z",
      "endDate": "2026-04-01T23:59:00Z",
      "storeName": "#052 North York (Bathurst)",
      "pageCount": 23,
      ...
    }
  ]
}
```

> `flyers[].title` is the **job number** (e.g. `82846`) used in all subsequent endpoints.  
> `flyers[].flyerCategory` can be `"Weekly Flyer"`, `"Other Publication - Responsive"`, etc.

---

### 2. Get Page Layout

```
GET /api/pages/{job}/{store_id}/{locale}?date={YYYY-MM-DD}
```

| Parameter | Example | Notes |
|---|---|---|
| `job` | `82846` | From `flyers[].title` |
| `store_id` | `85` | Integer store ID |
| `locale` | `en` or `bil` | Both work |

Returns an array of page objects with layout grids, block positions, and image URLs. Does **not** include product data.

---

### 3. Get Products (All Deals)

```
POST /api/Pages/{job}/{store_id}/{locale}/search
Content-Type: application/json

{"display_type": "all"}
```

| Parameter | Example | Notes |
|---|---|---|
| `job` | `82846` | From `flyers[].title` |
| `store_id` | `85` | Integer store ID |
| `locale` | `en` | `en` or `bil` |

**Response:** Array of ad blocks. Products are nested under `blocks[].products[]`:

```json
[
  {
    "zones": ["M[!SCA]"],
    "products": [
      {
        "sku": "10003349",
        "productEn": "RASPBERRIES 170 g OR BLACKBERRIES 170 g",
        "salePrice": "2.49",
        "regularPrice": null,
        "promoUnitEn": "ea.",
        "mainCategoryEn": "Fruit and Vegetables",
        "bodyEn": "PRODUCT OF U.S.A. OR MEXICO",
        "waysToSave_EN": "New lower price",
        "validFrom": "2026-03-26T04:00:00Z",
        "validTo": "2026-04-01T04:00:00Z",
        "productImage": "https://promo-omni.net/cdn-cgi/image/...",
        "contents": "RASPBERRIES 170 g OR BLACKBERRIES 170 g. 2.49 ea.",
        ...
      }
    ],
    "images": [...],
    "placement": {...}
  }
]
```

> Flatten with: `products = [p for block in blocks for p in block.get("products", [])]`

---

## Key Fields in Product Objects

| Field | Description |
|---|---|
| `sku` | Product SKU |
| `productEn` / `productFr` | Product name |
| `salePrice` / `salePriceFr` | Sale price |
| `regularPrice` | Regular price (null if not on sale) |
| `promoUnitEn` | Unit label (e.g. `ea.`, `lb.`) |
| `mainCategoryEn` | Category |
| `bodyEn` | Additional description |
| `waysToSave_EN` | Promo type (e.g. `"New lower price"`) |
| `validFrom` / `validTo` | Deal validity window |
| `productImage` | Product image URL |
| `save` / `savingsEn` | Savings amount (if applicable) |

---

## What Doesn't Work

| Attempted | Result |
|---|---|
| `GET /api/flyers/{id}/en/products` | 404 — this endpoint does not exist |
| `GET /api/flyers/{objectId}/en` with ObjectId from response | 400 — must use numeric job `title` not ObjectId |
| Banner ID as any query param name | 400 — must be `Banner` header |
| Banner ID as any `X-*` header | 400 — only bare `Banner` works |
| Azure Blob Storage direct access | 403 — SAS token has `sp=rl` but signature includes hostname, can't reuse |
| `GET /api/flyers` with `banner_id` query param | 400 — requires `Banner` header instead |

---

## Python Helper (`metro.py`)

```python
import json, requests

API_URL   = "https://metrodigital-apim.azure-api.net/api"
API_KEY   = "021027e7c41548bcba5d2315a155816b"
BANNER_ID = "62e3eddbffe0e6f10778a56d"  # Metro Ontario

HEADERS = {
    "Ocp-Apim-Subscription-Key": API_KEY,
    "Banner": BANNER_ID,
}

def get_flyers(store_id, date):
    r = requests.get(f"{API_URL}/flyers/{store_id}/en",
                     headers=HEADERS, params={"date": date})
    r.raise_for_status()
    return r.json()

def get_products(job, store_id, date, locale="en"):
    r = requests.post(f"{API_URL}/Pages/{job}/{store_id}/{locale}/search",
                      headers=HEADERS, json={"display_type": "all"})
    r.raise_for_status()
    return [p for block in r.json() for p in block.get("products", [])]
```
