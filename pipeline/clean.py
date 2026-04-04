"""
Pipeline orchestrator — CLI entry point.

Usage::

    python -m pipeline.clean [--store <name>] [--dry-run] [--output-dir <path>] [--force]

Options
-------
--store <name>
    Restrict processing to a single store folder (e.g. ``food_basics``).
    Useful for targeted testing without walking every brand directory.
--dry-run
    Print the total record count and per-store breakdown to stdout and exit 0
    without writing any output files.
--output-dir <path>
    Root directory for cleaned output (default: ``cleaned``).
--force
    Regenerate all output files even if they are already up-to-date.

Pipeline
--------
load_raw → normalize → parse_price → classify_promo → clean_name →
extract_weight → split_multi_product → map_category → write_output

Idempotency
-----------
Per-flyer JSON files are skipped when the cleaned file already exists and its
``generated_at`` date matches the raw file's ``fetched_on`` date, indicating
that the raw data has not changed since the last run.  Pass ``--force`` to
override and regenerate all files regardless.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys

from categories.category_map import get_food_flags, map_google_taxonomy, map_metro_category
from pipeline.load_raw import iter_flyers
from parsers.multi_product_parser import split_multi_product
from parsers.name_parser import parse_name
from parsers.price_parser import parse_price
from parsers.promo_parser import parse_promo
from parsers.weight_parser import parse_weight
from pipeline.schema import FlyerItem


# ── Pipeline helpers ──────────────────────────────────────────────────────────


def _apply_pipeline(item: FlyerItem) -> list[FlyerItem]:
    """Apply all enrichment steps to one :class:`~schema.FlyerItem`.

    Steps applied in order:

    1. ``parse_price``  — enriches multi-buy, floor-price, and per-weight fields.
    2. ``classify_promo`` — sets ``promo_type`` and loyalty fields.
    3. ``clean_name``   — title-cases and language-splits ``name_en``/``name_fr``.
    4. ``extract_weight`` — populates ``weight_value``, ``pack_count``, etc.
    5. ``map_category`` — harmonises ``category_l1`` to the shared taxonomy and
       sets ``is_food``/``is_human_food``.
    6. ``split_multi_product`` — expands combined entries into parent + children.

    Returns
    -------
    list[FlyerItem]
        One item for single-product records; two or more for multi-product
        entries (parent record first, children following).
    """
    # 1. parse_price
    price_fields = parse_price(
        price_text=str(item.sale_price) if item.sale_price is not None else None,
        pre_text=item.pre_price_text,
        post_text=item.post_price_text,
        original_price=str(item.regular_price) if item.regular_price is not None else None,
    )
    price_fields.pop("parse_warnings", None)

    # 2. classify_promo
    promo_fields = parse_promo(
        item.promo_details,
        member_price=item.member_price,
    )
    promo_fields.pop("parse_warnings", None)

    # 3. clean_name
    name_fields = parse_name(item.raw_name, brand=item.brand)

    # 4. extract_weight
    weight_fields = parse_weight(
        raw_name=item.raw_name,
        raw_description=item.raw_description,
        raw_body=item.raw_body,
    )
    weight_fields.pop("parse_warnings", None)

    # 5. map_category
    if item.source_api == "metro":
        mapped_l1 = map_metro_category(item.category_l1, None)
    else:
        mapped_l1 = map_google_taxonomy(item.category_l1, item.category_l2)

    if mapped_l1 is None:
        mapped_l1 = item.category_l1  # keep existing value when unmapped
    is_food, is_human_food = get_food_flags(mapped_l1)

    category_fields: dict = {"is_food": is_food, "is_human_food": is_human_food}
    if mapped_l1 is not None:
        category_fields["category_l1"] = mapped_l1

    # Merge all enrichment fields and update the record
    merged = {
        **price_fields,
        **promo_fields,
        **name_fields,
        **weight_fields,
        **category_fields,
    }
    enriched = item.model_copy(update=merged)

    # 6. split_multi_product (may expand one record into parent + children)
    return split_multi_product(enriched)


# ── Idempotency ───────────────────────────────────────────────────────────────


def _is_up_to_date(out_path: str, fetched_on: str | None) -> bool:
    """Return ``True`` if *out_path* exists and was generated from the same fetch.

    The comparison uses only the **date portion** (``YYYY-MM-DD``) of both
    ``fetched_on`` (from the raw file) and ``fetched_on`` stored in the cleaned
    file.  If they match, the raw source has not changed since the last run.
    """
    if not os.path.exists(out_path):
        return False
    if not fetched_on:
        return False
    try:
        with open(out_path, encoding="utf-8") as fh:
            cleaned = json.load(fh)
        stored_fetched_on = str(cleaned.get("fetched_on", ""))
        return stored_fetched_on[:10] == str(fetched_on)[:10]
    except Exception:
        return False


# ── Output writers ────────────────────────────────────────────────────────────


def _write_flyer_json(
    out_path: str,
    flyer_id: str | None,
    store_chain: str,
    fetched_on: str | None,
    records: list[FlyerItem],
) -> None:
    """Write the per-flyer JSON output file at *out_path*."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload = {
        "flyer_id": flyer_id,
        "store_chain": store_chain,
        "fetched_on": fetched_on,
        "generated_at": generated_at,
        "record_count": len(records),
        "records": [r.model_dump() for r in records],
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _write_parquet(out_path: str, all_records: list[FlyerItem]) -> None:
    """Write *all_records* to a combined Parquet file at *out_path*.

    List-valued fields (e.g. ``multi_product_variants``, ``raw_categories``)
    are JSON-serialised to strings so that the resulting Parquet schema is
    flat and compatible with standard analytics tools.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not all_records:
        return

    rows = []
    for record in all_records:
        row = record.model_dump()
        # Flatten list fields to JSON strings for a uniform Parquet schema
        for key, val in row.items():
            if isinstance(val, list):
                row[key] = json.dumps(val)
        rows.append(row)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, out_path)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.clean",
        description="Walk raw flyer files, normalise records, and write output.",
    )
    parser.add_argument(
        "--store",
        metavar="NAME",
        default=None,
        help="Process only this store folder (e.g. food_basics).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print record count and per-store breakdown, then exit without writing output.",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        default="cleaned",
        help="Root directory for cleaned output files (default: cleaned).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate all output files even if already up-to-date.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    output_dir: str = args.output_dir

    store_counts: dict[str, int] = {}
    all_records: list[FlyerItem] = []
    any_json_written = False

    for store_chain, flyer_id, fetched_on, raw_items in iter_flyers(store=args.store):
        # Apply the full enrichment pipeline to every record in this flyer
        processed: list[FlyerItem] = []
        for item in raw_items:
            processed.extend(_apply_pipeline(item))

        # Accumulate counts for --dry-run breakdown
        store_counts[store_chain] = store_counts.get(store_chain, 0) + len(processed)

        if args.dry_run:
            continue

        # Collect for combined Parquet output
        all_records.extend(processed)

        # Idempotency: skip writing this flyer's JSON if already up-to-date
        out_path = os.path.join(output_dir, store_chain, f"{flyer_id}.json")
        if not args.force and _is_up_to_date(out_path, fetched_on):
            continue

        _write_flyer_json(out_path, flyer_id, store_chain, fetched_on, processed)
        any_json_written = True

    total = sum(store_counts.values())

    if args.dry_run:
        print(f"{total} records")
        for store, count in sorted(store_counts.items()):
            print(f"  {store}: {count}")
        return 0

    # Write combined Parquet when there is something to write
    if all_records:
        parquet_path = os.path.join(output_dir, "all_flyers.parquet")
        if args.force or any_json_written or not os.path.exists(parquet_path):
            _write_parquet(parquet_path, all_records)

    print(f"{total} records processed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
