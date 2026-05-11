#!/usr/bin/env python3
"""
Analyse flyer-sharing patterns to identify regional flyer editions and
flag stores that receive more than one flyer simultaneously.

Regional key
------------
Flipp chains  — ``flyer_run_id`` in store_flyers.parquet raw_json.
Metro chains  — job ``title`` in store_flyers.parquet raw_json.

Output
------
data/flyer_regions.parquet  — one row per (chain, region_id, valid_from/to)

  chain               str    e.g. "nofrills"
  region_id           str    flyer_run_id (Flipp) or job title (Metro)
  valid_from          str    ISO date YYYY-MM-DD
  valid_to            str    ISO date YYYY-MM-DD
  store_codes         str    JSON array of store_code strings
  postal_fsas         str    JSON array of distinct FSAs (first 3 chars)
  postal_codes        str    JSON array of distinct full postal codes
  store_count         int32
  multi_flyer_stores  str    JSON array of store_codes that also received
                             a second simultaneous flyer (same week)

Usage
-----
    python3 -m scripts.analyze_regions
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR  = Path("data")
GEO_PATH  = DATA_DIR / "stores_geo.parquet"
OUT_PATH  = DATA_DIR / "flyer_regions.parquet"

FLIPP_FOLDERS = [
    "loblaws", "nofrills", "provigo", "real_canadian_superstore", "maxi",
    "zehrs", "fortinos", "atlantic_superstore", "dominion",
    "independent_grocer", "independent_city_market", "freshmart",
    "sobeys", "safeway", "iga", "freshco", "foodland", "longos",
    "farm_boy", "walmart",
]
METRO_FOLDERS = ["metro", "metro_qc", "food_basics", "super_c", "adonis"]

OUTPUT_SCHEMA = pa.schema([
    pa.field("chain",              pa.string()),
    pa.field("region_id",          pa.string()),
    pa.field("valid_from",         pa.string()),
    pa.field("valid_to",           pa.string()),
    pa.field("store_codes",        pa.string()),   # JSON array
    pa.field("postal_fsas",        pa.string()),   # JSON array of distinct FSAs
    pa.field("postal_codes",       pa.string()),   # JSON array of distinct full codes
    pa.field("store_count",        pa.int32()),
    pa.field("multi_flyer_stores", pa.string()),   # JSON array
])


# ── Load geo lookup ───────────────────────────────────────────────────────────

def _load_geo_lookup() -> dict[tuple[str, str], str]:
    """Return {(chain, store_code): postal_code}."""
    if not GEO_PATH.exists():
        print(f"  [!] {GEO_PATH} not found — run build_stores_geo.py first", file=sys.stderr)
        return {}
    t = pq.read_table(str(GEO_PATH))
    d = t.to_pydict()
    return {
        (chain, code): postal
        for chain, code, postal in zip(d["chain"], d["store_code"], d["postal_code"])
        if postal
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fsa(postal: str) -> str:
    """Return forward sortation area (first 3 chars) of a postal code."""
    p = (postal or "").replace(" ", "").upper()
    return p[:3] if len(p) >= 3 else ""


def _week_key(date_str: str) -> str:
    """Truncate ISO datetime to YYYY-MM-DD."""
    return (date_str or "")[:10]


# ── Flipp ─────────────────────────────────────────────────────────────────────

def _analyse_flipp(
    folder: str,
    geo: dict[tuple[str, str], str],
) -> list[dict]:
    """Return region rows for one Flipp chain."""
    path = DATA_DIR / folder / "store_flyers.parquet"
    if not path.exists():
        return []
    t = pq.read_table(str(path))
    d = t.to_pydict()
    codes = d["store_code"]
    raws  = d["raw_json"]

    # Group by (flyer_run_id, valid_from, valid_to)
    # Key: (run_id, valid_from_date, valid_to_date) → set of store_codes
    run_stores: dict[tuple, set[str]] = defaultdict(set)

    for code, raw in zip(codes, raws):
        obj = json.loads(raw) if raw else {}
        run_id = obj.get("flyer_run_id")
        vf = _week_key(obj.get("valid_from", ""))
        vt = _week_key(obj.get("valid_to", ""))
        if run_id and vf:
            run_stores[(str(run_id), vf, vt)].add(str(code))

    # Detect multi-flyer stores: a store that appears in 2+ runs for the same week
    # Build: store_code → set of run_ids active in same week
    store_runs_by_week: dict[tuple[str, str], set[str]] = defaultdict(set)
    for (run_id, vf, vt), store_set in run_stores.items():
        for code in store_set:
            store_runs_by_week[(code, vf)].add(run_id)

    multi_stores_by_run: dict[tuple, set[str]] = defaultdict(set)
    for (code, vf), run_set in store_runs_by_week.items():
        if len(run_set) > 1:
            for run_id in run_set:
                # Find the matching (run_id, vf, *) key
                for key in run_stores:
                    if key[0] == run_id and key[1] == vf and code in run_stores[key]:
                        multi_stores_by_run[key].add(code)

    rows = []
    for (run_id, vf, vt), store_set in run_stores.items():
        postal_codes = sorted({
            geo.get((folder, c), "")
            for c in store_set
            if geo.get((folder, c))
        })
        fsas = sorted({_fsa(p) for p in postal_codes if p})
        multi = sorted(multi_stores_by_run.get((run_id, vf, vt), set()))
        rows.append({
            "chain":              folder,
            "region_id":          run_id,
            "valid_from":         vf,
            "valid_to":           vt,
            "store_codes":        json.dumps(sorted(store_set)),
            "postal_fsas":        json.dumps(fsas),
            "postal_codes":       json.dumps(postal_codes),
            "store_count":        len(store_set),
            "multi_flyer_stores": json.dumps(multi),
        })

    return rows


# ── Metro ─────────────────────────────────────────────────────────────────────

def _analyse_metro(
    folder: str,
    geo: dict[tuple[str, str], str],
) -> list[dict]:
    """Return region rows for one Metro chain."""
    path = DATA_DIR / folder / "store_flyers.parquet"
    if not path.exists():
        return []
    t = pq.read_table(str(path))
    d = t.to_pydict()
    codes = d["store_code"]
    raws  = d["raw_json"]

    # Group by (title/job, startDate, endDate)
    job_stores: dict[tuple, set[str]] = defaultdict(set)

    for code, raw in zip(codes, raws):
        obj = json.loads(raw) if raw else {}
        title = obj.get("title")
        start = _week_key(obj.get("startDate", ""))
        end   = _week_key(obj.get("endDate",   ""))
        if title and start:
            job_stores[(str(title), start, end)].add(str(code))

    # Multi-flyer: store appears in 2+ jobs in the same week
    store_jobs_by_week: dict[tuple[str, str], set[str]] = defaultdict(set)
    for (title, start, end), store_set in job_stores.items():
        for code in store_set:
            store_jobs_by_week[(code, start)].add(title)

    multi_stores_by_job: dict[tuple, set[str]] = defaultdict(set)
    for (code, start), job_set in store_jobs_by_week.items():
        if len(job_set) > 1:
            for title in job_set:
                for key in job_stores:
                    if key[0] == title and key[1] == start and code in job_stores[key]:
                        multi_stores_by_job[key].add(code)

    rows = []
    for (title, start, end), store_set in job_stores.items():
        postal_codes = sorted({
            geo.get((folder, c), "")
            for c in store_set
            if geo.get((folder, c))
        })
        fsas = sorted({_fsa(p) for p in postal_codes if p})
        multi = sorted(multi_stores_by_job.get((title, start, end), set()))
        rows.append({
            "chain":              folder,
            "region_id":          title,
            "valid_from":         start,
            "valid_to":           end,
            "store_codes":        json.dumps(sorted(store_set)),
            "postal_fsas":        json.dumps(fsas),
            "postal_codes":       json.dumps(postal_codes),
            "store_count":        len(store_set),
            "multi_flyer_stores": json.dumps(multi),
        })

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def analyze_regions() -> None:
    print("Loading geo lookup …")
    geo = _load_geo_lookup()
    print(f"  {len(geo):,} (chain, store_code) → postal_code entries")

    all_rows: list[dict] = []

    print("── Flipp chains ──────────────────────────────────────")
    for folder in FLIPP_FOLDERS:
        rows = _analyse_flipp(folder, geo)
        if rows:
            store_total = sum(r["store_count"] for r in rows)
            multi_total = sum(len(json.loads(r["multi_flyer_stores"])) for r in rows)
            print(f"  {folder}: {len(rows)} regions, {store_total} store-week records, {multi_total} multi-flyer instances")
            all_rows.extend(rows)

    print("── Metro chains ──────────────────────────────────────")
    for folder in METRO_FOLDERS:
        rows = _analyse_metro(folder, geo)
        if rows:
            store_total = sum(r["store_count"] for r in rows)
            multi_total = sum(len(json.loads(r["multi_flyer_stores"])) for r in rows)
            print(f"  {folder}: {len(rows)} jobs, {store_total} store-week records, {multi_total} multi-flyer instances")
            all_rows.extend(rows)

    if not all_rows:
        print("No rows produced.", file=sys.stderr)
        return

    def _col(key: str, cast=None) -> list:
        vals = [r.get(key) for r in all_rows]
        return vals

    table = pa.table(
        {
            "chain":              pa.array(_col("chain"),              type=pa.string()),
            "region_id":          pa.array(_col("region_id"),          type=pa.string()),
            "valid_from":         pa.array(_col("valid_from"),         type=pa.string()),
            "valid_to":           pa.array(_col("valid_to"),           type=pa.string()),
            "store_codes":        pa.array(_col("store_codes"),        type=pa.string()),
            "postal_fsas":        pa.array(_col("postal_fsas"),        type=pa.string()),
            "postal_codes":       pa.array(_col("postal_codes"),       type=pa.string()),
            "store_count":        pa.array(_col("store_count"),        type=pa.int32()),
            "multi_flyer_stores": pa.array(_col("multi_flyer_stores"), type=pa.string()),
        },
        schema=OUTPUT_SCHEMA,
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(OUT_PATH))
    print(f"\n✓ Wrote {len(all_rows):,} region rows → {OUT_PATH}")

    # Quick sanity check: no region should span more than 2 provinces
    prov_issue = 0
    for row in all_rows:
        fsas = json.loads(row["postal_fsas"])
        # FSA first char indicates province group (rough check)
        # A=NL, B=NS, C=PEI, E=NB, G/H/J=QC, K/L/M/N/P=ON, R=MB, S=SK, T=AB, V=BC, X=NT/NU, Y=YT
        first_chars = {f[0] for f in fsas if f}
        # Group QC (G/H/J) and ON (K/L/M/N/P) separately; flag if mixing distant regions
        if first_chars and len(first_chars) > 3:
            prov_issue += 1

    if prov_issue:
        print(f"  ⚠  {prov_issue} regions span 4+ FSA prefix groups (may be national flyers)")
    else:
        print("  ✓ Province sanity check passed")


if __name__ == "__main__":
    analyze_regions()
