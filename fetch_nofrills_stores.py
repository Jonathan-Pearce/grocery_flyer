"""
Scan No Frills store codes via the Flipp Enterprise store endpoint and
save all discovered locations to data/nofrills/stores.json.

URL format:
  https://dam.flippenterprise.net/flyerkit/store/nofrills
    ?locale=en
    &access_token=1063f92aaf17b3dfa830cd70a685a52b
    &store_code=<CODE>

A valid store returns a JSON object with address/location data.
An invalid / non-existent store returns a non-200 status or an empty body.
"""

import json
import os
import time

import requests

ACCESS_TOKEN = "1063f92aaf17b3dfa830cd70a685a52b"
BASE_URL = "https://dam.flippenterprise.net/flyerkit/store/nofrills"
OUTPUT_PATH = "data/nofrills/stores.json"

# Extending the sweep into the 6000–10000 range.
CODE_RANGE = range(10000, 12001)

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
    # Load existing stores so new results are merged in, not overwritten
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH) as f:
            stores = json.load(f)
        print(f"Loaded {len(stores)} existing stores from {OUTPUT_PATH}")
    else:
        stores = {}

    total = len(CODE_RANGE)
    found = 0

    print(f"Scanning {total} No Frills store codes ({CODE_RANGE.start}–{CODE_RANGE.stop - 1})…\n")

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

        if i % 200 == 0:
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
