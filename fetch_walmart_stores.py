"""
Scan Flipp store codes for Walmart Canada and merge results into
data/walmart/stores.json.

Usage:
  python fetch_walmart_stores.py
  python fetch_walmart_stores.py --start 3000 --end 4000
"""

import argparse
import json
import os
import time

from flipp import DELAY, WALMART_PORTFOLIO, Brand, fetch_store


def scan_brand(brand: Brand, code_range: range) -> None:
    output_path = f"data/{brand.folder}/stores.json"

    if os.path.exists(output_path):
        with open(output_path) as f:
            stores = json.load(f)
        print(f"  Loaded {len(stores)} existing stores from {output_path}")
    else:
        stores = {}

    total = len(code_range)
    found = 0
    print(f"  Scanning {total} codes ({code_range.start}–{code_range.stop - 1})…\n")

    for i, code in enumerate(code_range, 1):
        data = fetch_store(brand, code)
        if data is not None:
            stores[str(code)] = data
            found += 1
            print(
                f"    [{code}] {data.get('name', '?')} — "
                f"{data.get('address', '?')}, {data.get('city', '?')}, "
                f"{data.get('province', '?')}  {data.get('postal_code', '?')}"
            )

        if i % 500 == 0:
            print(f"    … {i}/{total} scanned, {found} stores found so far")

        time.sleep(DELAY)

    print(f"\n  Scan complete: {found} stores found out of {total} codes checked.")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(stores, f, indent=2)
    size = os.path.getsize(output_path)
    print(f"  Saved → {output_path}  ({size:,} bytes, {len(stores)} total stores)\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan Walmart Canada store codes")
    parser.add_argument("--start", type=int, help="Override code range start (inclusive)")
    parser.add_argument("--end",   type=int, help="Override code range end (inclusive)")
    args = parser.parse_args()

    brand = WALMART_PORTFOLIO[0]

    print(f"{'─' * 60}")
    print(f"Brand: {brand.name}  (folder: {brand.folder})")

    code_range = brand.code_range
    if args.start is not None or args.end is not None:
        start = args.start if args.start is not None else code_range.start
        end   = (args.end + 1) if args.end is not None else code_range.stop
        code_range = range(start, end)

    scan_brand(brand, code_range)
    print("Done.")


if __name__ == "__main__":
    main()
