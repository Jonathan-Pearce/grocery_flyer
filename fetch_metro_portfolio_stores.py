"""
Scan Metro portfolio store IDs and merge results into data/<folder>/stores.json.

Unlike Flipp brands (which use slug-based store codes), Metro's API identifies
stores by a plain integer ID via:

  GET /api/flyers/{store_id}/en?date={YYYY-MM-DD}

A valid ID with an active flyer returns storeName; an unknown ID returns 404
or an empty flyer list.  Credentials (banner_id, api_key) are read from
hardcoded values or fetched from each brand's app.json at startup.

Usage:
  python fetch_metro_portfolio_stores.py                          # all brands
  python fetch_metro_portfolio_stores.py --brand metro
  python fetch_metro_portfolio_stores.py --brand food_basics --start 1 --end 500
"""

import argparse
import json
import os
import time

from datetime import date

from flipp import (
    DELAY,
    METRO_PORTFOLIO,
    MetroBrand,
    metro_fetch_store,
    metro_load_credentials,
    save_json,
)


def scan_brand(brand: MetroBrand, id_range: range, today: str) -> None:
    output_path = f"data/{brand.folder}/stores.json"

    if os.path.exists(output_path):
        with open(output_path) as f:
            stores = json.load(f)
        print(f"  Loaded {len(stores)} existing stores from {output_path}")
    else:
        stores = {}

    total = len(id_range)
    found = 0
    print(f"  Scanning {total} IDs ({id_range.start}–{id_range.stop - 1})…\n")

    for i, store_id in enumerate(id_range, 1):
        data = metro_fetch_store(brand, store_id, today)
        if data is not None:
            stores[str(store_id)] = data
            found += 1
            print(
                f"    [{store_id}] {data.get('store_name', '?')}"
                f"  ({data.get('banner', '?')})"
            )

        if i % 100 == 0:
            print(f"    … {i}/{total} scanned, {found} stores found so far")

        time.sleep(DELAY)

    print(f"\n  Scan complete: {found} stores found out of {total} IDs checked.")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    save_json(output_path, stores)
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan Metro portfolio store IDs")
    parser.add_argument(
        "--brand",
        metavar="FOLDER",
        help="Folder name of a single brand to scan (e.g. metro, food_basics). "
             "Omit to scan all brands.",
    )
    parser.add_argument("--start", type=int, help="Override ID range start (inclusive)")
    parser.add_argument("--end",   type=int, help="Override ID range end (inclusive)")
    args = parser.parse_args()

    today = date.today().isoformat()

    brands = METRO_PORTFOLIO
    if args.brand:
        brands = [b for b in brands if b.folder == args.brand]
        if not brands:
            parser.error(
                f"Unknown brand folder '{args.brand}'. "
                f"Valid: {[b.folder for b in METRO_PORTFOLIO]}"
            )

    for brand in brands:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        if not metro_load_credentials(brand):
            print(f"  [!] Could not load credentials for {brand.name} — skipping.\n")
            continue

        print(f"  banner_id : {brand.banner_id}")
        print(f"  api_key   : {brand.api_key[:8]}…")

        id_range = brand.id_range
        if args.start is not None or args.end is not None:
            start = args.start if args.start is not None else id_range.start
            end   = (args.end + 1) if args.end is not None else id_range.stop
            id_range = range(start, end)

        scan_brand(brand, id_range, today)

    print("Done.")


if __name__ == "__main__":
    main()
