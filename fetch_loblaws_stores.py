"""
Scan Loblaws store codes via the Flipp Enterprise store endpoint and
save all discovered locations to data/loblaws/stores.json.

URL format:
  https://dam.flippenterprise.net/flyerkit/store/loblaws
    ?locale=en
    &access_token=fd66ddd31b95e07b9ad2744424e9fd32
    &store_code=<CODE>

A valid store returns a JSON object with address/location data.
An invalid / non-existent store returns a non-200 status or an empty body.
"""

import json
import os
import time

import requests

ACCESS_TOKEN = "fd66ddd31b95e07b9ad2744424e9fd32"
BASE_URL = "https://dam.flippenterprise.net/flyerkit/store/loblaws"
OUTPUT_PATH = "data/loblaws/stores.json"

# Loblaws store codes appear to be 4-digit numbers (example: 1158).
# Sweep a wide range and tighten after seeing results.
CODE_RANGE = range(1, 3000)

DELAY = 0.05  # seconds between requests to be polite


def fetch_store(code: int) -> dict | None:
    """Return parsed JSON for a valid store code, or None if not found."""
    try:
        resp = requests.get(
            BASE_URL,
            params={"locale": "en", "access_token": ACCESS_TOKEN, "store_code": code},
            timeout=10,
        )
    except requests.RequestException as exc:
        print(f"  [!] code {code}: request error – {exc}")
        return None

    if resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    # An empty dict / empty list means the code doesn't exist
    if not data:
        return None

    return data


def main():
    stores = {}
    total = len(CODE_RANGE)
    found = 0

    print(f"Scanning {total} Loblaws store codes ({CODE_RANGE.start}–{CODE_RANGE.stop - 1})…\n")

    for i, code in enumerate(CODE_RANGE, 1):
        data = fetch_store(code)
        if data is not None:
            stores[str(code)] = data
            found += 1
            name     = data.get("name", "?")
            city     = data.get("city", "?")
            province = data.get("province", "?")
            address  = data.get("address", "?")
            postal   = data.get("postal_code", "?")
            print(f"  [{code}] {name} — {address}, {city}, {province}  {postal}")

        if i % 100 == 0:
            print(f"  … {i}/{total} scanned, {found} stores found so far")

        time.sleep(DELAY)

    print(f"\nScan complete: {found} stores found out of {total} codes checked.")

    # ── Save results ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(stores, f, indent=2)
    size = os.path.getsize(OUTPUT_PATH)
    print(f"Saved → {OUTPUT_PATH}  ({size:,} bytes, {found} stores)")


if __name__ == "__main__":
    main()
