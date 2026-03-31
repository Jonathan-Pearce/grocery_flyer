import json
import os
from datetime import date

import requests

# ── Metro API config ──────────────────────────────────────────────────────────
METRO_API_URL = "https://metrodigital-apim.azure-api.net/api"
METRO_HEADERS = {
    "Ocp-Apim-Subscription-Key": "021027e7c41548bcba5d2315a155816b",
    "Banner": "62e3eddbffe0e6f10778a56d",
    "User-Agent": "Mozilla/5.0",
}
METRO_STORE_ID = 85
METRO_LOCALE = "en"

TODAY = date.today().isoformat()


def save_json(folder: str, data) -> None:
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{TODAY}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    size = os.path.getsize(path)
    print(f"  Saved -> {path}  ({size:,} bytes)")


# ── Freshco / No Frills (Flipp API) ──────────────────────────────────────────
def fetch_flipp_flyer(publication_id: str, access_token: str) -> list:
    url = f"https://dam.flippenterprise.net/flyerkit/publication/{publication_id}/products"
    resp = requests.get(
        url,
        params={"display_type": "all", "locale": "en", "access_token": access_token},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ── Metro ─────────────────────────────────────────────────────────────────────
def metro_get_flyers(store_id: int) -> dict:
    resp = requests.get(
        f"{METRO_API_URL}/flyers/{store_id}/{METRO_LOCALE}",
        headers=METRO_HEADERS,
        params={"date": TODAY},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def metro_get_products(job: str, store_id: int) -> list:
    resp = requests.post(
        f"{METRO_API_URL}/Pages/{job}/{store_id}/{METRO_LOCALE}/search",
        headers=METRO_HEADERS,
        json={"display_type": "all"},
        timeout=15,
    )
    resp.raise_for_status()
    products = []
    for block in resp.json():
        products.extend(block.get("products", []))
    return products


def fetch_metro() -> list:
    flyers_data = metro_get_flyers(METRO_STORE_ID)
    flyer = next(
        f for f in flyers_data["flyers"] if f["flyerCategory"] == "Weekly Flyer"
    )
    job = flyer["title"]
    print(
        f"  Metro Weekly Flyer: {flyer['flyerTitle']}"
        f"  ({flyer['startDate'][:10]} to {flyer['endDate'][:10]})"
    )
    return metro_get_products(job, METRO_STORE_ID)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Fetching flyers for {TODAY}\n")

    print("Freshco...")
    save_json("data/freshco", fetch_flipp_flyer("7852849", "881f0b9feea3693a704952a69b2a037a"))

    print("No Frills...")
    save_json("data/nofrills", fetch_flipp_flyer("7855364", "1063f92aaf17b3dfa830cd70a685a52b"))

    print("Metro...")
    save_json("data/metro", fetch_metro())

    print("\nDone.")
