"""
One-time migration: convert per-file JSON archives to per-brand Parquet files.

Usage::

    python -m scripts.migrate_to_parquet [--data-dir data] [--cleaned-dir cleaned]
                                          [--store <name>] [--delete-json]

Options
-------
--data-dir PATH
    Root directory of the raw data (default: ``data``).
--cleaned-dir PATH
    Root directory of cleaned per-flyer JSON envelopes (default: ``cleaned``).
--store NAME
    Restrict migration to a single brand folder (e.g. ``loblaws``).
--delete-json
    Remove source JSON files after successfully writing Parquet.

What it does
------------
**Raw layer** — for each brand under ``data/<brand>/flyers/*.json``:

* Reads each flyer JSON, detects Flipp (``publication_id``) vs Metro (``job``).
* Flattens every product in the file to one row, adding envelope columns
  (``flyer_id``, ``source_api``, ``fetched_on``, ``pub_valid_from`` /
  ``pub_valid_to`` / ``pub_locale`` for Flipp, or ``store_id`` for Metro,
  and ``products_url``).
* Nested / list product fields are JSON-encoded to strings.
* Writes all rows for the brand to ``data/<brand>/flyers.parquet``.

**Cleaned layer** — for each ``cleaned/<brand>/<id>.json`` envelope:

* Reads the ``records[]`` array, JSON-encodes any list-valued fields.
* Appends all records for the brand to ``cleaned/<brand>.parquet``.

Both layers are idempotent when re-run without ``--delete-json``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


# ── Row builders ──────────────────────────────────────────────────────────────

_FLIPP_ENVELOPE_COLS = frozenset(
    {"flyer_id", "source_api", "fetched_on", "pub_valid_from", "pub_valid_to", "pub_locale", "products_url"}
)
_METRO_ENVELOPE_COLS = frozenset(
    {"flyer_id", "source_api", "fetched_on", "store_id", "products_url"}
)


def _encode_product(product: dict, envelope: dict) -> dict:
    """Merge envelope cols with one product, JSON-encoding list/dict values."""
    row = dict(envelope)
    for key, val in product.items():
        if isinstance(val, (list, dict)):
            row[key] = json.dumps(val)
        else:
            row[key] = val
    return row


def _flipp_rows(flyer_id: str, flyer_data: dict) -> list[dict]:
    """Return one row per product for a Flipp flyer file."""
    pub_meta = flyer_data.get("publication_meta") or {}
    envelope = {
        "flyer_id": flyer_id,
        "source_api": "flipp",
        "fetched_on": flyer_data.get("fetched_on"),
        "pub_valid_from": pub_meta.get("valid_from"),
        "pub_valid_to": pub_meta.get("valid_to"),
        "pub_locale": pub_meta.get("locale"),
        "products_url": flyer_data.get("products_url"),
    }
    return [_encode_product(p, envelope) for p in flyer_data.get("products", [])]


def _metro_rows(flyer_id: str, flyer_data: dict) -> list[dict]:
    """Return one row per product for a Metro flyer file."""
    store_id = flyer_data.get("store_id")
    envelope = {
        "flyer_id": flyer_id,
        "source_api": "metro",
        "fetched_on": flyer_data.get("fetched_on"),
        "store_id": str(store_id) if store_id is not None else None,
        "products_url": flyer_data.get("products_url"),
    }
    return [_encode_product(p, envelope) for p in flyer_data.get("products", [])]


# ── Parquet writer helpers ─────────────────────────────────────────────────────


def _write_parquet(path: str, rows: list[dict]) -> None:
    """Write *rows* to *path*, appending to an existing file when present."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    new_table = pa.Table.from_pylist(rows)
    if os.path.exists(path):
        existing = pq.read_table(path)
        combined = pa.concat_tables([existing, new_table], promote_options="default")
    else:
        combined = new_table
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pq.write_table(combined, path)


# ── Raw layer migration ────────────────────────────────────────────────────────


def migrate_raw(data_dir: str, store: str | None, delete_json: bool) -> None:
    """Convert ``data/<brand>/flyers/*.json`` → ``data/<brand>/flyers.parquet``."""
    try:
        brand_dirs = sorted(os.listdir(data_dir))
    except FileNotFoundError:
        print(f"[raw] data dir not found: {data_dir}")
        return

    for brand in brand_dirs:
        if store is not None and brand != store:
            continue
        flyers_dir = os.path.join(data_dir, brand, "flyers")
        if not os.path.isdir(flyers_dir):
            continue

        json_files = sorted(f for f in os.listdir(flyers_dir) if f.endswith(".json"))
        if not json_files:
            continue

        out_path = os.path.join(data_dir, brand, "flyers.parquet")
        all_rows: list[dict] = []
        deleted: list[str] = []

        for fname in json_files:
            fpath = os.path.join(flyers_dir, fname)
            try:
                with open(fpath, encoding="utf-8") as fh:
                    flyer_data = json.load(fh)
            except Exception as exc:
                print(f"  [!] {fpath}: skipped — {exc}")
                continue

            if "publication_id" in flyer_data:
                flyer_id = str(flyer_data["publication_id"])
                rows = _flipp_rows(flyer_id, flyer_data)
            elif "job" in flyer_data:
                flyer_id = str(flyer_data["job"])
                rows = _metro_rows(flyer_id, flyer_data)
            else:
                print(f"  [!] {fpath}: unknown format — skipping")
                continue

            all_rows.extend(rows)
            deleted.append(fpath)

        if not all_rows:
            print(f"{brand}: no products found — skipped")
            continue

        _write_parquet(out_path, all_rows)
        print(f"{brand}: {len(json_files)} files → {len(all_rows)} rows → {out_path}")

        if delete_json:
            for fpath in deleted:
                os.remove(fpath)
            # Remove the now-empty flyers/ directory if possible
            try:
                os.rmdir(flyers_dir)
            except OSError:
                pass
            print(f"  deleted {len(deleted)} JSON files")


# ── Cleaned layer migration ───────────────────────────────────────────────────


def migrate_cleaned(cleaned_dir: str, store: str | None, delete_json: bool) -> None:
    """Convert ``cleaned/<brand>/<id>.json`` → ``cleaned/<brand>.parquet``."""
    try:
        brand_dirs = sorted(os.listdir(cleaned_dir))
    except FileNotFoundError:
        print(f"[cleaned] cleaned dir not found: {cleaned_dir}")
        return

    for brand in brand_dirs:
        if store is not None and brand != store:
            continue
        brand_path = os.path.join(cleaned_dir, brand)
        if not os.path.isdir(brand_path):
            continue

        json_files = sorted(f for f in os.listdir(brand_path) if f.endswith(".json"))
        if not json_files:
            continue

        out_path = os.path.join(cleaned_dir, f"{brand}.parquet")
        all_rows: list[dict] = []
        deleted: list[str] = []

        for fname in json_files:
            fpath = os.path.join(brand_path, fname)
            try:
                with open(fpath, encoding="utf-8") as fh:
                    envelope = json.load(fh)
            except Exception as exc:
                print(f"  [!] {fpath}: skipped — {exc}")
                continue

            for record in envelope.get("records") or []:
                row = dict(record)
                for key, val in row.items():
                    if isinstance(val, list):
                        row[key] = json.dumps(val)
                all_rows.append(row)
            deleted.append(fpath)

        if not all_rows:
            print(f"{brand}: no records found — skipped")
            continue

        _write_parquet(out_path, all_rows)
        print(f"{brand}: {len(json_files)} files → {len(all_rows)} rows → {out_path}")

        if delete_json:
            for fpath in deleted:
                os.remove(fpath)
            try:
                os.rmdir(brand_path)
            except OSError:
                pass
            print(f"  deleted {len(deleted)} JSON files")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.migrate_to_parquet",
        description="Convert per-file JSON archives to per-brand Parquet files.",
    )
    parser.add_argument("--data-dir", default="data", metavar="PATH")
    parser.add_argument("--cleaned-dir", default="cleaned", metavar="PATH")
    parser.add_argument("--store", default=None, metavar="NAME")
    parser.add_argument(
        "--delete-json",
        action="store_true",
        help="Remove source JSON files after writing Parquet.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    print("── Raw layer ────────────────────────────────────────────────────")
    migrate_raw(args.data_dir, args.store, args.delete_json)
    print("── Cleaned layer ────────────────────────────────────────────────")
    migrate_cleaned(args.cleaned_dir, args.store, args.delete_json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
