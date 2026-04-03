"""Probe Metro portfolio app.json endpoints to gather banner_id and apikey."""
import json
import requests

BRANDS = {
    "metro_ontario": "https://flyer.metro.ca/config/app.json",
    "metro_quebec":  "https://depliant.metro.ca/config/app.json",
    "super_c":       "https://depliant.superc.ca/config/app.json",
    "food_basics":   "https://flyer.foodbasics.ca/config/app.json",
}

for brand, url in BRANDS.items():
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        print(f"=== {brand} — HTTP {r.status_code} ===")
        if r.status_code == 200:
            print(json.dumps(r.json(), indent=2))
        else:
            print(r.text[:400])
    except Exception as e:
        print(f"ERROR: {e}")
    print()
