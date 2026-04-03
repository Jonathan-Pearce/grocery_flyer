"""
Scan store codes/IDs for all portfolio brands and merge results into
data/<folder>/stores.json.

Portfolios:
  loblaws   — Flipp API store-code sweep
  sobeys    — Flipp API store-code sweep
  walmart   — Flipp API store-code sweep
  metro     — Azure API integer-ID sweep

Usage:
  python -m scripts.fetch_stores                                          # all portfolios
  python -m scripts.fetch_stores --portfolio loblaws                      # all Loblaws brands
  python -m scripts.fetch_stores --portfolio loblaws --brand nofrills
  python -m scripts.fetch_stores --portfolio metro --brand food_basics --start 1 --end 500
"""

import argparse
import json
import os
import time
from datetime import date

from fetchers.azure import METRO_PORTFOLIO, MetroBrand, metro_fetch_store, metro_load_credentials
from fetchers.flipp import DELAY, LOBLAWS_PORTFOLIO, SOBEYS_PORTFOLIO, WALMART_PORTFOLIO, Brand, fetch_store, save_json


# ── Flipp store scanner ───────────────────────────────────────────────────────

def scan_flipp_brand(brand: Brand, code_range: range) -> None:
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
                f"    [{code}] {data.get('name','?')} — "
                f"{data.get('address','?')}, {data.get('city','?')}, "
                f"{data.get('province','?')}  {data.get('postal_code','?')}"
            )

        if i % 500 == 0:
            print(f"    … {i}/{total} scanned, {found} new stores found so far")

        time.sleep(DELAY)

    print(f"\n  Scan complete: {found} new stores found out of {total} codes checked.")
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    save_json(output_path, stores)
    print()


# ── Metro store scanner ───────────────────────────────────────────────────────

def scan_metro_brand(brand: MetroBrand, id_range: range, today: str) -> None:
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


# ── Portfolio runners ─────────────────────────────────────────────────────────

def run_flipp_portfolio(
    brands: list[Brand],
    start: int | None,
    end: int | None,
) -> None:
    for brand in brands:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        if brand.slug is None:
            print(f"  [!] Slug not yet confirmed for {brand.name} — skipping.\n")
            continue

        code_range = brand.code_range
        if start is not None or end is not None:
            s = start if start is not None else code_range.start
            e = (end + 1) if end is not None else code_range.stop
            code_range = range(s, e)

        scan_flipp_brand(brand, code_range)


def run_metro_portfolio(
    brands: list[MetroBrand],
    start: int | None,
    end: int | None,
    today: str,
) -> None:
    for brand in brands:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        if not metro_load_credentials(brand):
            print(f"  [!] Could not load credentials for {brand.name} — skipping.\n")
            continue

        print(f"  banner_id : {brand.banner_id}")
        print("  api_key   : (loaded)")

        id_range = brand.id_range
        if start is not None or end is not None:
            s = start if start is not None else id_range.start
            e = (end + 1) if end is not None else id_range.stop
            id_range = range(s, e)

        scan_metro_brand(brand, id_range, today)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _filter_brands(portfolio: list, brand_arg: str | None, portfolio_name: str) -> list:
    if brand_arg is None:
        return portfolio
    filtered = [b for b in portfolio if b.folder == brand_arg]
    if not filtered:
        valid = [b.folder for b in portfolio]
        raise SystemExit(
            f"Unknown brand folder '{brand_arg}' for portfolio '{portfolio_name}'. "
            f"Valid: {valid}"
        )
    return filtered


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan store codes/IDs for grocery portfolio brands"
    )
    parser.add_argument(
        "--portfolio",
        choices=["loblaws", "sobeys", "walmart", "metro"],
        help="Portfolio to scan. Omit to scan all.",
    )
    parser.add_argument(
        "--brand",
        metavar="FOLDER",
        help="Single brand folder to scan (e.g. nofrills, food_basics).",
    )
    parser.add_argument("--start", type=int, help="Override range start (inclusive)")
    parser.add_argument("--end",   type=int, help="Override range end (inclusive)")
    args = parser.parse_args()

    today = date.today().isoformat()
    portfolios = [args.portfolio] if args.portfolio else ["loblaws", "sobeys", "walmart", "metro"]

    for portfolio in portfolios:
        if portfolio == "loblaws":
            brands = _filter_brands(LOBLAWS_PORTFOLIO, args.brand, "loblaws")
            run_flipp_portfolio(brands, args.start, args.end)
        elif portfolio == "sobeys":
            brands = _filter_brands(SOBEYS_PORTFOLIO, args.brand, "sobeys")
            run_flipp_portfolio(brands, args.start, args.end)
        elif portfolio == "walmart":
            brands = _filter_brands(WALMART_PORTFOLIO, args.brand, "walmart")
            run_flipp_portfolio(brands, args.start, args.end)
        elif portfolio == "metro":
            brands = _filter_brands(METRO_PORTFOLIO, args.brand, "metro")
            run_metro_portfolio(brands, args.start, args.end, today)

    print("Done.")


if __name__ == "__main__":
    main()
