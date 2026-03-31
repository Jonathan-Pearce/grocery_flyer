import json
import requests

from datetime import date

API_URL   = "https://metrodigital-apim.azure-api.net/api"
API_KEY   = "021027e7c41548bcba5d2315a155816b"
BANNER_ID = "62e3eddbffe0e6f10778a56d"  # Metro Ontario
DATE      = date.today().isoformat()
LOCALE    = "en"

HEADERS = {
    "Ocp-Apim-Subscription-Key": API_KEY,
    "Banner": BANNER_ID,
    "User-Agent": "Mozilla/5.0",
}


def get_flyers(store_id: int):
    """Return flyer list for a store."""
    resp = requests.get(
        f"{API_URL}/flyers/{store_id}/{LOCALE}",
        headers=HEADERS,
        params={"date": DATE},
    )
    resp.raise_for_status()
    return resp.json()


def get_pages(job: str, store_id: int, locale: str = "en"):
    """Return page layout data for a flyer job."""
    resp = requests.get(
        f"{API_URL}/pages/{job}/{store_id}/{locale}",
        headers=HEADERS,
        params={"date": DATE},
    )
    resp.raise_for_status()
    return resp.json()


def get_products(job: str, store_id: int, locale: str = "en"):
    """Return all product blocks for a flyer job by POSTing to /Pages/.../search."""
    resp = requests.post(
        f"{API_URL}/Pages/{job}/{store_id}/{locale}/search",
        headers=HEADERS,
        json={"display_type": "all"},
    )
    resp.raise_for_status()
    blocks = resp.json()
    # Flatten: collect all products across all blocks
    products = []
    for block in blocks:
        products.extend(block.get("products", []))
    return products


def save(filename, data):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} items -> {filename}")


if __name__ == "__main__":
    flyers_data = get_flyers(85)
    flyer = next(f for f in flyers_data["flyers"] if f["flyerCategory"] == "Weekly Flyer")
    job = flyer["title"]
    print(f"Weekly flyer: {flyer['flyerTitle']}  job={job}  {flyer['startDate'][:10]} to {flyer['endDate'][:10]}")

    products = get_products(job, 85)
    save("metro_products.json", products)
    print(f"\nSample product:")
    print(json.dumps(products[0], indent=2))


