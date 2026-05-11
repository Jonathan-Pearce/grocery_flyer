"""
Analyse regional flyer clustering across all portfolio brands.

For Flipp brands (Loblaws, Sobeys, Walmart):
  Groups stores by their Forward Sortation Area (FSA — first three characters
  of the postal code, e.g. "M5V", "L1V") to show which stores share the same
  regional delivery zone.

For Metro brands (Metro, Food Basics, Adonis, Super C, Metro QC):
  Parses the ``zonedPublicationName`` field from store_flyers.parquet to
  cluster stores by their assigned flyer-distribution zone.  Stores that
  consistently appear together in the same zone pattern are in the same
  regional cluster.  Special section codes (e.g. M[KOS], M[ITL], M[ALC])
  are extracted separately to identify stores with supplementary inserts.

Writes:
  data/flyer_clusters.json    — human-readable cluster report
  data/flyer_clusters.parquet — one row per (brand, cluster, store)

Usage:
  python -m scripts.analyze_flyer_zones                  # all brands
  python -m scripts.analyze_flyer_zones --portfolio metro
  python -m scripts.analyze_flyer_zones --portfolio loblaws
"""

import argparse
import json
import os
import re
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

from fetchers.azure import METRO_PORTFOLIO
from fetchers.flipp import LOBLAWS_PORTFOLIO, SOBEYS_PORTFOLIO, WALMART_PORTFOLIO


# ── Flipp FSA clustering ──────────────────────────────────────────────────────

def _fsa(postal_code: str) -> str | None:
    """Return the Forward Sortation Area (first 3 chars) of a Canadian postal code."""
    pc = (postal_code or "").replace(" ", "").upper()
    if len(pc) >= 3:
        return pc[:3]
    return None


def cluster_flipp_brand(folder: str) -> list[dict]:
    """Cluster stores for a Flipp brand by FSA.

    Returns a list of row dicts suitable for the summary parquet.
    """
    stores_path = f"data/{folder}/stores.parquet"
    if not os.path.exists(stores_path):
        print(f"  [{folder}] stores.parquet not found — skipping.")
        return []

    table = pq.read_table(stores_path)
    rows = table.to_pylist()

    fsa_to_stores: dict[str, list[str]] = defaultdict(list)
    no_postal = 0
    for row in rows:
        info = json.loads(row["raw_json"]) if "raw_json" in row else row
        pc = info.get("postal_code") or row.get("postal_code") or ""
        fsa = _fsa(pc)
        store_name = info.get("name") or info.get("store_name") or row.get("store_name") or ""
        code = row.get("store_code", "")
        if fsa:
            fsa_to_stores[fsa].append(f"{code}:{store_name}")
        else:
            no_postal += 1

    print(f"  [{folder}] {len(rows)} stores, {len(fsa_to_stores)} FSA clusters, "
          f"{no_postal} stores without postal code.")

    out_rows = []
    for fsa, stores in sorted(fsa_to_stores.items()):
        out_rows.append({
            "brand": folder,
            "cluster_type": "FSA",
            "cluster_id": fsa,
            "store_count": len(stores),
            "stores": json.dumps(stores),
        })
    return out_rows


# ── Metro zone clustering ─────────────────────────────────────────────────────

# Section-specific add-on codes embedded in zone expressions.
# These appear as modifier tokens attached to a base zone code.
_SECTION_CODES: set[str] = {
    "KOS",   # Kosher section
    "ITL",   # Italian section
    "ALC",   # Alcohol / LCBO section
    "DEL",   # Deli section
    "PUP",   # Pick-up / online section
    "NEO",   # Neo / new products section
    "SKP",   # Skip-the-dishes / delivery section
    "SOA",   # ?
    "MSF",   # ?
    "PHA",   # Pharmacy section
    "EON",   # Eastern Ontario bilingual section
    "NOR",   # Northern Ontario section
    "GTA",   # GTA sub-region add-on
    "REB",   # Rebranded / renovation section
    "HOT",   # Hot deals section
    "SCA",   # Scarborough section
}

# Base zone codes that correspond to geographic distribution regions.
_BASE_ZONE_RE = re.compile(r"([A-Z]+)(?:\[([^\]]*)\])?")


def _parse_zone_expression(zone_expr: str) -> tuple[str, list[str]]:
    """Split a Metro zone expression into a base region code and section modifiers.

    For example::

        "R[!(HOT^REB)]_M_R[!HOT]_M[DEL^PUP^NEO]_M[ITL]_M[ALC]"
        → base_zone = "R_M", sections = ["DEL", "PUP", "NEO", "ITL", "ALC"]

    The base zone is the first non-section token; section codes are anything
    inside brackets that match the ``_SECTION_CODES`` set.
    """
    base_parts: list[str] = []
    sections: list[str] = []

    for token in zone_expr.split("_"):
        m = _BASE_ZONE_RE.match(token)
        if not m:
            continue
        base = m.group(1)       # e.g. "M", "R", "OTB", "MB"
        modifiers = m.group(2)  # e.g. "DEL^PUP^NEO", "!(HOT^REB)", None

        if modifiers:
            # Extract individual modifier codes
            codes = re.findall(r"[A-Z]+", modifiers)
            # If ALL extracted codes are section codes, this token is a section modifier
            non_section = [c for c in codes if c not in _SECTION_CODES]
            if non_section:
                # Geographic qualifier (e.g. R[!HOT], R[GTA])
                base_parts.append(base)
            else:
                # Pure section modifier (e.g. M[KOS], M[ITL])
                sections.extend(c for c in codes if c in _SECTION_CODES)
        else:
            base_parts.append(base)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_base: list[str] = []
    for p in base_parts:
        if p not in seen:
            seen.add(p)
            unique_base.append(p)

    return "_".join(unique_base), sorted(set(sections))


def cluster_metro_brand(folder: str) -> list[dict]:
    """Cluster Metro stores by their dominant zone pattern.

    Returns rows for the summary parquet.
    """
    flyers_path = f"data/{folder}/store_flyers.parquet"
    stores_path = f"data/{folder}/stores.parquet"

    if not os.path.exists(flyers_path):
        print(f"  [{folder}] store_flyers.parquet not found — skipping.")
        return []

    # Load store names
    store_names: dict[str, str] = {}
    if os.path.exists(stores_path):
        for row in pq.read_table(stores_path).to_pylist():
            info = json.loads(row["raw_json"]) if "raw_json" in row else row
            store_names[str(row.get("store_code", ""))] = (
                info.get("store_name") or info.get("name") or row.get("store_name") or ""
            )

    # Tally base zone per store
    store_zones: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    store_sections: dict[str, set[str]] = defaultdict(set)

    for row in pq.read_table(flyers_path).to_pylist():
        code = str(row.get("store_code", ""))
        info = json.loads(row["raw_json"]) if "raw_json" in row else row
        zone_expr = info.get("zonedPublicationName", "")
        if not zone_expr:
            continue
        base_zone, sections = _parse_zone_expression(zone_expr)
        if base_zone:
            store_zones[code][base_zone] += 1
        store_sections[code].update(sections)

    # Assign each store to its most-frequent base zone
    zone_to_stores: dict[str, list[str]] = defaultdict(list)
    for code, zone_counts in store_zones.items():
        dominant = max(zone_counts, key=lambda z: zone_counts[z])
        name = store_names.get(code, "")
        zone_to_stores[dominant].append(f"{code}:{name}")

    # Collect section codes across stores
    section_summary: dict[str, list[str]] = defaultdict(list)
    for code, sections in store_sections.items():
        name = store_names.get(code, "")
        for sec in sections:
            section_summary[sec].append(f"{code}:{name}")

    total_stores = len(store_zones)
    print(f"  [{folder}] {total_stores} stores across {len(zone_to_stores)} zone clusters; "
          f"{len(section_summary)} section types found.")

    out_rows = []
    for zone, stores in sorted(zone_to_stores.items()):
        out_rows.append({
            "brand": folder,
            "cluster_type": "zone",
            "cluster_id": zone,
            "store_count": len(stores),
            "stores": json.dumps(stores),
        })
    for sec, stores in sorted(section_summary.items()):
        out_rows.append({
            "brand": folder,
            "cluster_type": "section",
            "cluster_id": sec,
            "store_count": len(stores),
            "stores": json.dumps(stores),
        })
    return out_rows


# ── Output writers ────────────────────────────────────────────────────────────

def _write_outputs(all_rows: list[dict]) -> None:
    """Write cluster summary to parquet and JSON."""
    if not all_rows:
        print("No cluster data to write.")
        return

    os.makedirs("data", exist_ok=True)

    # Parquet
    pq.write_table(pa.Table.from_pylist(all_rows), "data/flyer_clusters.parquet")

    # JSON (grouped by brand for readability)
    by_brand: dict[str, dict] = defaultdict(dict)
    for row in all_rows:
        brand = row["brand"]
        ctype = row["cluster_type"]
        cid = row["cluster_id"]
        if ctype not in by_brand[brand]:
            by_brand[brand][ctype] = {}
        by_brand[brand][ctype][cid] = {
            "store_count": row["store_count"],
            "stores": json.loads(row["stores"]),
        }

    with open("data/flyer_clusters.json", "w", encoding="utf-8") as f:
        json.dump(by_brand, f, indent=2, ensure_ascii=False)

    print(f"\nWrote data/flyer_clusters.parquet and data/flyer_clusters.json "
          f"({len(all_rows)} cluster rows across {len(by_brand)} brands).")


# ── Portfolio runners ─────────────────────────────────────────────────────────

def run_flipp_portfolio() -> list[dict]:
    all_rows: list[dict] = []
    for portfolio in (LOBLAWS_PORTFOLIO, SOBEYS_PORTFOLIO, WALMART_PORTFOLIO):
        for brand in portfolio:
            all_rows.extend(cluster_flipp_brand(brand.folder))
    return all_rows


def run_metro_portfolio() -> list[dict]:
    all_rows: list[dict] = []
    for brand in METRO_PORTFOLIO:
        all_rows.extend(cluster_metro_brand(brand.folder))
    return all_rows


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyse regional flyer clusters by postal FSA (Flipp) and zone expression (Metro)"
    )
    parser.add_argument(
        "--portfolio",
        choices=["loblaws", "sobeys", "walmart", "metro", "flipp"],
        help="Portfolio to analyse (flipp = all Flipp brands). Omit for all.",
    )
    args = parser.parse_args()

    all_rows: list[dict] = []

    if args.portfolio in (None, "loblaws", "sobeys", "walmart", "flipp"):
        print("── Flipp brands ─────────────────────────────────────────────")
        all_rows.extend(run_flipp_portfolio())

    if args.portfolio in (None, "metro"):
        print("── Metro brands ─────────────────────────────────────────────")
        all_rows.extend(run_metro_portfolio())

    _write_outputs(all_rows)
    print("Done.")


if __name__ == "__main__":
    main()
