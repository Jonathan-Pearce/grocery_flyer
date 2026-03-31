import json
import requests

URLS = {
    "freshco_flyer": "https://dam.flippenterprise.net/flyerkit/publication/7852849/products?display_type=all&locale=en&access_token=881f0b9feea3693a704952a69b2a037a",
    "freshco_item":  "https://dam.flippenterprise.net/flyerkit/product/1001962693?locale=en&access_token=881f0b9feea3693a704952a69b2a037a",
    "nofrills_flyer": "https://dam.flippenterprise.net/flyerkit/publication/7855364/products?display_type=all&locale=en&access_token=1063f92aaf17b3dfa830cd70a685a52b",
    "nofrills_item":  "https://dam.flippenterprise.net/flyerkit/product/1002418542?locale=en&access_token=1063f92aaf17b3dfa830cd70a685a52b",
}

for name, url in URLS.items():
    print(f"Fetching {name}...")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    filename = f"{name}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved {filename} ({len(resp.content)} bytes)")

print("Done.")
