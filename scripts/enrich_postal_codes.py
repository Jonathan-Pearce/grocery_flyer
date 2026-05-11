"""
Enrich stores.parquet for all brands with postal codes.

For Flipp-based brands (Loblaws, Sobeys, Walmart), postal codes are already
present in the raw_json returned by the Flipp API and are extracted directly.

For Metro-based brands (Metro, Food Basics, Adonis, Super C, Metro QC), the
Metro Azure API does not return postal codes.  This script geocodes each Metro
store via the OpenStreetMap Nominatim API (free, no key required) using the
store name and province, then writes the recovered postal code back into the
stores.parquet file.

Writes:
  data/<folder>/stores.parquet  — updated in-place; postal_code column filled
  data/postal_codes_summary.parquet  — cross-brand postal code summary
  data/postal_codes_summary.csv      — human-readable version of the above

Usage:
  python -m scripts.enrich_postal_codes                             # all brands
  python -m scripts.enrich_postal_codes --portfolio loblaws         # Flipp only
  python -m scripts.enrich_postal_codes --portfolio metro           # Metro only
  python -m scripts.enrich_postal_codes --portfolio metro --dry-run # preview only
"""

import argparse
import csv
import json
import os
import re
import time

import requests

import pyarrow as pa
import pyarrow.parquet as pq

from fetchers.azure import METRO_PORTFOLIO
from fetchers.flipp import (
    LOBLAWS_PORTFOLIO,
    SOBEYS_PORTFOLIO,
    WALMART_PORTFOLIO,
    DELAY,
)

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_HEADERS = {"User-Agent": "grocery_flyer_enrich/1.0 (github.com/Jonathan-Pearce/grocery_flyer)"}
# Nominatim usage policy: max 1 request/second
_NOMINATIM_DELAY = 1.1

_CANADIAN_PC_RE = re.compile(r"[A-Z]\d[A-Z]\s?\d[A-Z]\d", re.IGNORECASE)


# ── Parquet helpers ───────────────────────────────────────────────────────────

def _load_stores(parquet_path: str) -> dict:
    """Load ``{store_code: info_dict}`` from stores.parquet."""
    if not os.path.exists(parquet_path):
        return {}
    table = pq.read_table(parquet_path)
    result = {}
    for row in table.to_pylist():
        code = str(row.get("store_code", ""))
        if code:
            result[code] = json.loads(row["raw_json"]) if "raw_json" in row else dict(row)
    return result


def _save_stores(parquet_path: str, stores: dict) -> None:
    """Write enriched stores dict back to parquet."""
    rows = []
    for code, info in stores.items():
        rows.append({
            "store_code": str(code),
            "postal_code": info.get("postal_code") or None,
            "province": info.get("province") or info.get("banner") or None,
            "store_name": info.get("name") or info.get("store_name") or None,
            "raw_json": json.dumps(info),
        })
    os.makedirs(os.path.dirname(parquet_path) or ".", exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), parquet_path)


# ── Nominatim geocoding ───────────────────────────────────────────────────────

def _clean_store_name(raw_name: str) -> str:
    """Strip leading store number from Metro store names like '#052 North York (Bathurst)'."""
    return re.sub(r"^#\d+\s*", "", raw_name).strip()


def _geocode_postal_code(store_name: str, province: str | None) -> str | None:
    """Look up the Canadian postal code for a store using Nominatim.

    Returns a normalised postal code (no space, uppercase) or None if not found.
    """
    clean_name = _clean_store_name(store_name)
    query_parts = [clean_name]
    if province:
        query_parts.append(province)
    query_parts.append("Canada")
    query = ", ".join(query_parts)

    try:
        resp = requests.get(
            _NOMINATIM_URL,
            params={
                "q": query,
                "countrycodes": "ca",
                "format": "json",
                "addressdetails": 1,
                "limit": 1,
            },
            headers=_NOMINATIM_HEADERS,
            timeout=10,
        )
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    try:
        results = resp.json()
    except ValueError:
        return None

    if not results:
        return None

    postcode = results[0].get("address", {}).get("postcode", "")
    if not postcode:
        return None

    # Nominatim can return "K1A 0A6" — normalise to no-space uppercase
    match = _CANADIAN_PC_RE.search(postcode)
    if match:
        return match.group().replace(" ", "").upper()
    return None


# ── Per-brand enrichment ──────────────────────────────────────────────────────

def enrich_flipp_brand(folder: str, dry_run: bool = False) -> list[dict]:
    """Extract postal_code from raw_json for a Flipp brand (no HTTP calls needed)."""
    parquet_path = f"data/{folder}/stores.parquet"
    stores = _load_stores(parquet_path)
    if not stores:
        print(f"  [{folder}] No stores found — skipping.")
        return []

    enriched = 0
    rows_out = []
    for code, info in stores.items():
        pc = info.get("postal_code") or None
        if pc and not info.get("postal_code"):
            info["postal_code"] = pc
            enriched += 1
        rows_out.append({
            "folder": folder,
            "store_code": code,
            "store_name": info.get("name") or info.get("store_name") or "",
            "postal_code": pc or "",
            "province": info.get("province") or "",
            "address": info.get("address") or "",
            "city": info.get("city") or "",
        })

    print(f"  [{folder}] {len(stores)} stores, {sum(1 for r in rows_out if r['postal_code'])} with postal codes.")
    if not dry_run:
        _save_stores(parquet_path, stores)
    return rows_out


def enrich_metro_brand(folder: str, dry_run: bool = False) -> list[dict]:
    """Geocode Metro stores that are missing a postal code via Nominatim."""
    parquet_path = f"data/{folder}/stores.parquet"
    stores = _load_stores(parquet_path)
    if not stores:
        print(f"  [{folder}] No stores found — skipping.")
        return []

    already_have = sum(1 for info in stores.values() if info.get("postal_code"))
    need_geocode = [(code, info) for code, info in stores.items() if not info.get("postal_code")]

    print(f"  [{folder}] {len(stores)} stores: {already_have} have postal codes, "
          f"{len(need_geocode)} need geocoding.")

    geocoded = 0
    failed = 0
    for code, info in need_geocode:
        store_name = info.get("store_name") or info.get("name") or ""
        province = info.get("province") or None
        pc = _geocode_postal_code(store_name, province)
        if pc:
            info["postal_code"] = pc
            geocoded += 1
            print(f"    [{code}] {store_name} → {pc}")
        else:
            failed += 1
        time.sleep(_NOMINATIM_DELAY)

    print(f"  [{folder}] Geocoded {geocoded} new postal codes, {failed} not found.")

    if not dry_run and geocoded > 0:
        _save_stores(parquet_path, stores)

    rows_out = []
    for code, info in stores.items():
        rows_out.append({
            "folder": folder,
            "store_code": code,
            "store_name": info.get("store_name") or info.get("name") or "",
            "postal_code": info.get("postal_code") or "",
            "province": info.get("province") or "",
            "address": info.get("address") or "",
            "city": info.get("city") or "",
        })
    return rows_out


# ── Summary writer ────────────────────────────────────────────────────────────

def _write_summary(rows: list[dict], dry_run: bool) -> None:
    """Write cross-brand postal code summary to parquet and CSV."""
    if not rows or dry_run:
        if dry_run:
            print("\n[dry-run] Would write data/postal_codes_summary.{parquet,csv}")
        return

    os.makedirs("data", exist_ok=True)

    # Parquet
    pq.write_table(pa.Table.from_pylist(rows), "data/postal_codes_summary.parquet")

    # CSV
    fieldnames = ["folder", "store_code", "store_name", "postal_code", "province", "address", "city"]
    with open("data/postal_codes_summary.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    total = len(rows)
    with_pc = sum(1 for r in rows if r["postal_code"])
    print(f"\nSummary: {with_pc}/{total} stores have postal codes "
          f"({100 * with_pc // total if total else 0}%).")
    print("Wrote data/postal_codes_summary.parquet and data/postal_codes_summary.csv")


# ── Portfolio runners ─────────────────────────────────────────────────────────

def run_flipp_portfolio(dry_run: bool = False) -> list[dict]:
    all_rows: list[dict] = []
    for portfolio in (LOBLAWS_PORTFOLIO, SOBEYS_PORTFOLIO, WALMART_PORTFOLIO):
        for brand in portfolio:
            all_rows.extend(enrich_flipp_brand(brand.folder, dry_run=dry_run))
    return all_rows


def run_metro_portfolio(dry_run: bool = False) -> list[dict]:
    all_rows: list[dict] = []
    for brand in METRO_PORTFOLIO:
        all_rows.extend(enrich_metro_brand(brand.folder, dry_run=dry_run))
    return all_rows


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich stores.parquet with postal codes for all brands"
    )
    parser.add_argument(
        "--portfolio",
        choices=["loblaws", "sobeys", "walmart", "metro", "flipp"],
        help="Portfolio to enrich (flipp = all Flipp brands). Omit for all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing any files.",
    )
    args = parser.parse_args()

    all_rows: list[dict] = []

    if args.portfolio in (None, "loblaws", "sobeys", "walmart", "flipp"):
        print("── Flipp brands ─────────────────────────────────────────────")
        all_rows.extend(run_flipp_portfolio(dry_run=args.dry_run))

    if args.portfolio in (None, "metro"):
        print("── Metro brands ─────────────────────────────────────────────")
        all_rows.extend(run_metro_portfolio(dry_run=args.dry_run))

    _write_summary(all_rows, dry_run=args.dry_run)
    print("Done.")


if __name__ == "__main__":
    main()
