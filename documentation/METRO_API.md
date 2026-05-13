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
| `salePrice` / `salePriceFr` | Sale price — always a plain numeric string (e.g. `"3.99"`) |
| `regularPrice` / `regularPriceFr` | Regular/was price. **Varies by brand and locale** (see below) |
| `savingsEn` / `savingsFr` | Dollar savings amount (e.g. `"$10.99"`). Only present when Metro explicitly advertises the discount amount. When populated, `save` mirrors the same value. |
| `save` | Mirror of `savingsEn`; use `savingsEn` instead |
| `savingsPrefix` | Display prefix, e.g. `"SAVE"` — not numeric, for display only |
| `waysToSave_EN` / `waysToSave_FR` | Promo category string, e.g. `"New lower price"`, `"This Week Only"`, `"Weekly specials"`, `"Locked Down"` |
| `promoUnitEn` / `promoUnitFr` | Unit label (e.g. `"ea."`, `"/lb"`) |
| `alternatePrice` / `alternatePriceFr` | Secondary per-unit price (e.g. `"$13.21/kg"`, `"22,02$/kg"`) — includes unit suffix |
| `memberPriceEn` / `memberPriceFr` | Loyalty/membership price — plain numeric string when present |
| `mainCategoryEn` / `mainCategoryFr` | Top-level product category |
| `subCategoryEn` / `subCategoryFr` | Sub-category |
| `bodyEn` / `bodyFr` | Additional description / size info |
| `validFrom` / `validTo` | Deal validity window (ISO 8601) |
| `productImage` | Product image URL |
| `contents` | Full promotional body text |
| `tx` | Tax flag exactly as shown (e.g. `"+TX"`) |
| `limitQty` | Maximum units at promotional price |
| `afterLimitPrice` | Price per unit for quantities above `limitQty` |
| `priceQuantity` | Required quantity for multi-buy deals |
| `pts` / `loyalty` | Loyalty points fields |
| `brand` | Manufacturer/brand name |
| `upc` | Product UPC/barcode |
| `zone` | Store zone string (e.g. `"M[!SCA]"`) — not parsed |

---

## Pricing Field Formats by Brand

**Critical**: `regularPrice` format differs significantly between brands and locales.

| Brand | `regularPrice` example | Notes |
|---|---|---|
| Metro Ontario | `null` | Almost always null; "New lower price" doesn't expose the old price |
| Metro QC | `"17,99/lb - 39,66/kg"` | French comma decimal + per-unit label + dual-unit range |
| Metro QC (simple) | `"5,49"` | French comma decimal, parseable directly |
| Food Basics | `null` | Not populated in observed data |
| Super C | (unknown) | Not yet observed |

**Parser note**: `regularPrice` must be treated as an optional text field. The leading numeric portion should be extracted by stripping unit suffixes (`/kg`, `/lb`, ` - ...`). French comma decimal notation (`17,99` → `17.99`) must be handled.

Similarly, `alternatePrice` always includes a unit suffix (`"$13.21/kg"`, `"22,02$/kg"`) and requires the same leading-number extraction.

`savingsEn` is a dollar-prefixed string (`"$10.99"`) and parses cleanly to a float after stripping `$`.

---

## `waysToSave_EN` Values Observed

| Value | Brands | Meaning |
|---|---|---|
| `"New lower price"` | Metro ON | Permanent price reduction |
| `"This Week Only"` | Food Basics | Weekly flyer deal |
| `"Locked Down"` | Food Basics | Extended lock-in price |
| `"Weekly specials"` | Metro QC | Weekly flyer deal |
| `"Long term specials"` | Metro QC | Extended promotional price |
| `null` / absent | All | Regular shelf pricing, no explicit promo type |

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
