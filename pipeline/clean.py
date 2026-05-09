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

Output
------
One Parquet file per grocery chain: ``<output-dir>/<chain>.parquet``.
Each row is a single :class:`~schema.FlyerItem` record with list-valued
fields (e.g. ``multi_product_variants``, ``raw_categories``) JSON-encoded
as strings for a flat, uniform schema.

Idempotency
-----------
The ``flyer_id`` column in the existing per-chain Parquet is read at the
start of each run.  Any flyer whose ID is already present is skipped.
Pass ``--force`` to override and regenerate everything from raw data.

Pipeline
--------
load_raw → normalize → parse_price → classify_promo → clean_name →
extract_weight → split_multi_product → map_category → write_output
"""

from __future__ import annotations

import argparse
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


def _load_processed_ids(output_dir: str, store_chain: str) -> set[str]:
    """Return the set of ``flyer_id`` values already in *cleaned/<chain>.parquet*.

    Returns an empty set when the file does not exist or cannot be read.
    """
    import pyarrow.parquet as pq

    path = os.path.join(output_dir, f"{store_chain}.parquet")
    if not os.path.exists(path):
        return set()
    try:
        table = pq.read_table(path, columns=["flyer_id"])
        return {str(v) for v in table.column("flyer_id").to_pylist() if v is not None}
    except Exception:
        return set()


# ── Output writers ────────────────────────────────────────────────────────────


def _records_to_rows(records: list[FlyerItem]) -> list[dict]:
    """Serialise *records* to plain dicts with list fields JSON-encoded."""
    rows = []
    for record in records:
        row = record.model_dump()
        for key, val in row.items():
            if isinstance(val, list):
                row[key] = json.dumps(val)
        rows.append(row)
    return rows


def _write_parquet(out_path: str, records: list[FlyerItem]) -> None:
    """Write *records* to *out_path*, creating or overwriting the file.

    List-valued fields (e.g. ``multi_product_variants``, ``raw_categories``)
    are JSON-serialised to strings for a flat, uniform Parquet schema.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not records:
        return

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    table = pa.Table.from_pylist(_records_to_rows(records))
    pq.write_table(table, out_path)


def _append_to_parquet(out_path: str, new_records: list[FlyerItem]) -> None:
    """Append *new_records* to the per-chain Parquet file, creating it if absent.

    Reads the existing file (when present), concatenates the new rows, and
    writes the result back atomically.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not new_records:
        return

    new_table = pa.Table.from_pylist(_records_to_rows(new_records))
    if os.path.exists(out_path):
        existing = pq.read_table(out_path)
        combined = pa.concat_tables([existing, new_table], promote_options="default")
    else:
        combined = new_table

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    pq.write_table(combined, out_path)


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

    # Cache of already-processed flyer_ids per chain (loaded on first encounter).
    # When --force, all caches are initialised empty so every flyer is reprocessed.
    processed_cache: dict[str, set[str]] = {}
    # New enriched records accumulated per chain during this run.
    new_by_chain: dict[str, list[FlyerItem]] = {}
    store_counts: dict[str, int] = {}

    for store_chain, flyer_id, _fetched_on, raw_items in iter_flyers(store=args.store):
        # Lazily load the set of already-processed IDs for this chain.
        if store_chain not in processed_cache:
            processed_cache[store_chain] = (
                set()  # --force treats every flyer as new
                if args.force
                else _load_processed_ids(output_dir, store_chain)
            )

        # Idempotency: skip flyers already present in the cleaned Parquet.
        if flyer_id is not None and flyer_id in processed_cache[store_chain]:
            continue

        # Apply the full enrichment pipeline to every record in this flyer.
        processed: list[FlyerItem] = []
        for item in raw_items:
            processed.extend(_apply_pipeline(item))

        store_counts[store_chain] = store_counts.get(store_chain, 0) + len(processed)

        if args.dry_run:
            continue

        new_by_chain.setdefault(store_chain, []).extend(processed)

    total = sum(store_counts.values())

    if args.dry_run:
        print(f"{total} records")
        for store, count in sorted(store_counts.items()):
            print(f"  {store}: {count}")
        return 0

    # Write per-chain Parquet files.
    os.makedirs(output_dir, exist_ok=True)
    for store_chain, new_records in new_by_chain.items():
        out_path = os.path.join(output_dir, f"{store_chain}.parquet")
        if args.force:
            # Overwrite: only records produced in this run.
            _write_parquet(out_path, new_records)
        else:
            # Append: merge with whatever was already on disk.
            _append_to_parquet(out_path, new_records)

    print(f"{total} records processed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
