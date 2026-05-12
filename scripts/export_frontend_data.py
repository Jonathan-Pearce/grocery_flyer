#!/usr/bin/env python3
"""
Export pipeline data from Parquet → JSON for the frontend.

Usage:
    python scripts/export_frontend_data.py [--geo-only] [--scores-only]

Outputs:
    frontend/public/data/active_scores.json     — scored deals (requires pandas)
    frontend/public/data/stores_geo.json        — store locations
    frontend/public/data/flyer_regions.json     — regional flyer groupings
    frontend/public/data/postal_centroids.json  — FSA → [lat, lon] for offline geocoding
"""
import argparse
import csv
import sys
import json
from pathlib import Path

SCORES_PATH = Path("db/scores/active_scores.parquet")
OUT_PATH = Path("frontend/public/data/active_scores.json")

GEO_PATH         = Path("data/stores_geo.parquet")
GEO_OUT_PATH     = Path("frontend/public/data/stores_geo.json")
REGIONS_PATH     = Path("data/flyer_regions.parquet")
REGIONS_OUT_PATH = Path("frontend/public/data/flyer_regions.json")

GEONAMES_PATH         = Path("data/geonames_ca_cache.tsv")
CENTROIDS_OUT_PATH    = Path("frontend/public/data/postal_centroids.json")

KEEP_FIELDS = [
    "flyer_id", "sku", "store_chain", "store_id",
    "name_en", "name_fr", "brand",
    "sale_price", "regular_price", "price_unit",
    "promo_type",
    "flyer_valid_from", "flyer_valid_to",
    "deal_score", "confidence", "confidence_label",
    "category_l1", "category_l2",
    "image_url",
]


# ── Geo exports ───────────────────────────────────────────────────────────────

def export_stores_geo() -> None:
    """Export data/stores_geo.parquet → frontend/public/data/stores_geo.json."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required.", file=sys.stderr)
        sys.exit(1)

    if not GEO_PATH.exists():
        print(
            f"ERROR: {GEO_PATH} not found.\n"
            "Run: python3 -m scripts.build_stores_geo",
            file=sys.stderr,
        )
        return

    print(f"Reading {GEO_PATH}…")
    t = pq.read_table(str(GEO_PATH))
    d = t.to_pydict()

    records = []
    for i in range(t.num_rows):
        lat = d["lat"][i]
        lon = d["lon"][i]
        records.append({
            "chain":       d["chain"][i],
            "store_code":  d["store_code"][i],
            "store_name":  d["store_name"][i],
            "address":     d["address"][i] or None,
            "city":        d["city"][i] or None,
            "province":    d["province"][i] or None,
            "postal_code": d["postal_code"][i] or None,
            "lat":         lat if lat == lat else None,   # NaN → None
            "lon":         lon if lon == lon else None,
            "geo_source":  d["geo_source"][i],
        })

    GEO_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(GEO_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))

    with_coords = sum(1 for r in records if r["lat"] is not None)
    with_postal  = sum(1 for r in records if r["postal_code"])
    print(f"✓ Exported {len(records):,} stores → {GEO_OUT_PATH}")
    print(f"  postal_code: {with_postal:,}  lat/lon: {with_coords:,}")


def export_flyer_regions() -> None:
    """Export data/flyer_regions.parquet → frontend/public/data/flyer_regions.json."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required.", file=sys.stderr)
        sys.exit(1)

    if not REGIONS_PATH.exists():
        print(
            f"ERROR: {REGIONS_PATH} not found.\n"
            "Run: python3 -m scripts.analyze_regions",
            file=sys.stderr,
        )
        return

    print(f"Reading {REGIONS_PATH}…")
    t = pq.read_table(str(REGIONS_PATH))
    d = t.to_pydict()

    records = []
    for i in range(t.num_rows):
        records.append({
            "chain":              d["chain"][i],
            "region_id":          d["region_id"][i],
            "valid_from":         d["valid_from"][i],
            "valid_to":           d["valid_to"][i],
            "store_codes":        json.loads(d["store_codes"][i] or "[]"),
            "postal_fsas":        json.loads(d["postal_fsas"][i] or "[]"),
            "postal_codes":       json.loads(d["postal_codes"][i] or "[]"),
            "store_count":        d["store_count"][i],
            "multi_flyer_stores": json.loads(d["multi_flyer_stores"][i] or "[]"),
        })

    REGIONS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REGIONS_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))

    multi_count = sum(1 for r in records if r["multi_flyer_stores"])
    print(f"✓ Exported {len(records):,} flyer regions → {REGIONS_OUT_PATH}")
    print(f"  regions with multi-flyer stores: {multi_count:,}")


def export_postal_centroids() -> None:
    """Build FSA → [lat, lon] map from geonames_ca_cache.tsv for offline geocoding.

    The GeoNames Canada file has one row per full postal code (A1A 1A1 format),
    with tab-separated columns:
        0: country_code
        1: postal_code   (e.g. "T0A" for FSA-only rows, or full postal)
        2: place_name
        3: state_name
        4: state_code    (province abbrev)
        9: latitude
        10: longitude

    We average all rows sharing the same 3-char FSA prefix and emit:
        {"M5V": [43.649, -79.394], ...}
    """
    if not GEONAMES_PATH.exists():
        print(f"WARNING: {GEONAMES_PATH} not found — skipping postal_centroids export", file=sys.stderr)
        return

    print(f"Reading {GEONAMES_PATH}…")
    fsa_lats: dict[str, list[float]] = {}
    fsa_lons: dict[str, list[float]] = {}

    with open(GEONAMES_PATH, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) < 11:
                continue
            postal = row[1].strip().upper().replace(" ", "")
            fsa = postal[:3]
            if len(fsa) != 3:
                continue
            try:
                lat = float(row[9])
                lon = float(row[10])
            except ValueError:
                continue
            fsa_lats.setdefault(fsa, []).append(lat)
            fsa_lons.setdefault(fsa, []).append(lon)

    centroids: dict[str, list[float]] = {}
    for fsa in fsa_lats:
        lats = fsa_lats[fsa]
        lons = fsa_lons[fsa]
        centroids[fsa] = [
            round(sum(lats) / len(lats), 5),
            round(sum(lons) / len(lons), 5),
        ]

    CENTROIDS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CENTROIDS_OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(centroids, f, ensure_ascii=False, separators=(",", ":"))

    print(f"✓ Exported {len(centroids):,} FSA centroids → {CENTROIDS_OUT_PATH}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Export pipeline data to frontend JSON")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--geo-only",    action="store_true", help="Only export geo/region data")
    grp.add_argument("--scores-only", action="store_true", help="Only export deal scores")
    args = parser.parse_args()

    run_scores = not args.geo_only
    run_geo    = not args.scores_only

    if run_scores:
        _export_scores()
    if run_geo:
        export_stores_geo()
        export_flyer_regions()
        export_postal_centroids()


def _export_scores() -> None:
    """Export db/scores/active_scores.parquet → frontend/public/data/active_scores.json."""
    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas is required. Run: pip install pandas pyarrow", file=sys.stderr)
        sys.exit(1)

    if not SCORES_PATH.exists():
        print(
            f"ERROR: {SCORES_PATH} not found.\n"
            "Run the full pipeline first to generate scored deals.",
            file=sys.stderr,
        )
        return

    print(f"Reading {SCORES_PATH}…")
    df = pd.read_parquet(SCORES_PATH)

    # Select only the columns the frontend needs
    available = [c for c in KEEP_FIELDS if c in df.columns]
    missing = [c for c in KEEP_FIELDS if c not in df.columns]
    if missing:
        print(f"Note: fields not in parquet (will be omitted): {missing}")

    df = df[available].copy()

    # Drop rows with no deal_score
    if "deal_score" in df.columns:
        df = df.dropna(subset=["deal_score"])
        df["deal_score"] = df["deal_score"].astype(int)

    # Sort best deals first
    if "deal_score" in df.columns:
        df = df.sort_values("deal_score", ascending=False)

    # Convert date columns to ISO strings
    for col in ["flyer_valid_from", "flyer_valid_to"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # Fill NaN with None (→ JSON null)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"✓ Exported {len(records):,} deals → {OUT_PATH}")


if __name__ == "__main__":
    main()
