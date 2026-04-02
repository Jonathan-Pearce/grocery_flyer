"""
Scan Provigo store codes via the Flipp Enterprise store endpoint and
save all discovered locations to data/provigo/stores.json.

URL format:
  https://dam.flippenterprise.net/flyerkit/store/provigo
    ?locale=en
    &access_token=31c52dc6a419dc10959261a5a210fccf
    &store_code=<CODE>

A valid store returns a JSON object with address/location data.
An invalid / non-existent store returns a non-200 status or an empty body.
"""

import json
import os
import time

import requests

ACCESS_TOKEN = "31c52dc6a419dc10959261a5a210fccf"
BASE_URL = "https://dam.flippenterprise.net/flyerkit/store/provigo"
OUTPUT_PATH = "data/provigo/stores.json"

# Provigo store codes: example 7198, scan a wide range.
CODE_RANGE = range(1, 12001)

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

    print(f"Scanning {total} Provigo store codes ({CODE_RANGE.start}–{CODE_RANGE.stop - 1})…\n")

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

        if i % 500 == 0:
            print(f"  … {i}/{total} scanned, {found} stores found so far")

        time.sleep(DELAY)

    print(f"\nScan complete: {found} new stores found out of {total} codes checked.")

    # ── Save results ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(stores, f, indent=2)
    size = os.path.getsize(OUTPUT_PATH)
    print(f"Saved → {OUTPUT_PATH}  ({size:,} bytes, {len(stores)} total stores)")


if __name__ == "__main__":
    main()
