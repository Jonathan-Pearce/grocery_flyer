#!/usr/bin/env python3
"""
Export active deal scores from Parquet → JSON for the frontend.

Usage:
    python scripts/export_frontend_data.py

Output:
    frontend/public/data/active_scores.json
"""
import sys
import json
from pathlib import Path

SCORES_PATH = Path("db/scores/active_scores.parquet")
OUT_PATH = Path("frontend/public/data/active_scores.json")

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


def main():
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
        sys.exit(1)

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
