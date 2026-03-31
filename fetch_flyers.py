import json
import os
from datetime import date

import requests

# ── Metro API config ──────────────────────────────────────────────────────────
METRO_API_URL = "https://metrodigital-apim.azure-api.net/api"
# (display_name, data_folder, banner_id, api_key, store_id, locale)
METRO_STORES = [
    ("Metro",       "metro",       "62e3eddbffe0e6f10778a56d", "021027e7c41548bcba5d2315a155816b", 85,  "en"),
    ("Super C",     "super_c",     "6141fa7157f8c212fc19dddc", "021027e7c41548bcba5d2315a155816b", 910, "en"),
    ("Food Basics", "food_basics", "62015981ed29a2a604a206b4", "0defd42b9de9412488327864774fbfca", 917,   "en"),
    ("Adonis",      "adonis",      "63fe18ec3e7cd81e86393c61", "0a112db32b2f42588b54063b05dfbc90", 21937, "en"),
]

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


# ── Metro Group ──────────────────────────────────────────────────────────────
def _metro_headers(banner_id: str, api_key: str) -> dict:
    return {
        "Ocp-Apim-Subscription-Key": api_key,
        "Banner": banner_id,
        "User-Agent": "Mozilla/5.0",
    }


def metro_get_flyers(store_id: int, banner_id: str, api_key: str, locale: str) -> dict:
    resp = requests.get(
        f"{METRO_API_URL}/flyers/{store_id}/{locale}",
        headers=_metro_headers(banner_id, api_key),
        params={"date": TODAY},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def metro_get_products(job: str, store_id: int, banner_id: str, api_key: str, locale: str) -> list:
    resp = requests.post(
        f"{METRO_API_URL}/Pages/{job}/{store_id}/{locale}/search",
        headers=_metro_headers(banner_id, api_key),
        json={"display_type": "all"},
        timeout=15,
    )
    resp.raise_for_status()
    products = []
    for block in resp.json():
        products.extend(block.get("products", []))
    return products


def fetch_metro_store(banner_id: str, api_key: str, store_id: int, locale: str) -> list:
    flyers_data = metro_get_flyers(store_id, banner_id, api_key, locale)
    flyer = next(
        f for f in flyers_data["flyers"] if f["flyerCategory"] == "Weekly Flyer"
    )
    print(
        f"  Weekly Flyer: {flyer['flyerTitle']}"
        f"  ({flyer['startDate'][:10]} to {flyer['endDate'][:10]})"
    )
    return metro_get_products(flyer["title"], store_id, banner_id, api_key, locale)


# ── Flipp store registry ──────────────────────────────────────────────────────
# (display_name, data_folder, publication_id, access_token)
FLIPP_STORES = [
    # Sobeys Group
    ("Sobeys",                  "sobeys",                "7850450", "afbc75b4e335236182ac2fba092a0d4a"),
    ("Safeway",                 "safeway",               "7850234", "41073822c1e3a003da36de785443fa0f"),
    ("IGA",                     "iga",                   "7857184", "692be3f8ba9e9247dc13d064cb89e7f9"),
    ("Freshco",                 "freshco",               "7852849", "881f0b9feea3693a704952a69b2a037a"),
    ("Foodland",                "foodland",              "7858789", "07ca28af93a0585f05575bf41ce92a6d"),
    ("Longos",                  "longos",                "7852767", "5b4ad9bb0148449f25dbb0b76b976c1b"),
    ("Farm Boy",                "farm_boy",              "7860241", "633f9e9fe2eae3e7b4a811dd9690ac4b"),
    # Loblaws Group
    ("Loblaws",                 "loblaws",               "7856259", "fd66ddd31b95e07b9ad2744424e9fd32"),
    ("No Frills",               "nofrills",              "7855364", "1063f92aaf17b3dfa830cd70a685a52b"),
    ("Real Canadian Superstore","real_canadian_superstore","7853464","a6e07e290f469d032d54a252f7582de2"),
    ("Provigo",                 "provigo",               "7855517", "31c52dc6a419dc10959261a5a210fccf"),
    ("Maxi",                    "maxi",                  "7855553", "75a33b973cc2e856dd0f2cd629d80a19"),
    ("Zehrs",                   "zehrs",                 "7856233", "fef2a837ffeee9e5e5d02f31db81f209"),
    ("Fortinos",                "fortinos",              "7853481", "ff3274ff57f481a8fcfac9c6c968fe67"),
    ("Atlantic Superstore",     "atlantic_superstore",   "7855775", "4d9c0561f7abbf53ad6eca20dad201c7"),
    ("Dominion",                "dominion",              "7855727", "23d83ed8a192329f29749c3b86c707fc"),
    ("Independent Grocer",      "independent_grocer",    "7853659", "fa31161a375478b68b2ec0f8f8edd65a"),
    # Walmart
    ("Walmart",                  "walmart",               "7847887", "92bcff5f7d07c3aaa4b33e2c048d7728"),
]


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Fetching flyers for {TODAY}\n")

    for name, folder, pub_id, token in FLIPP_STORES:
        print(f"{name}...")
        save_json(f"data/{folder}", fetch_flipp_flyer(pub_id, token))

    for name, folder, banner_id, api_key, store_id, locale in METRO_STORES:
        print(f"{name}...")
        save_json(f"data/{folder}", fetch_metro_store(banner_id, api_key, store_id, locale))

    print("\nDone.")
