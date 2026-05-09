"""
Hugging Face dataset pull — download grocery flyer data from HF repos.

Allows a fresh clone to restore data from Hugging Face before running
pipeline stages like ``build_db`` or ``deal_scorer``.

    jpearce610/grocery-flyers-raw      → data/<brand>/raw.parquet
    jpearce610/grocery-flyers-processed → cleaned/<brand>/<id>.json (reconstructed)
                                          db/ parquet files

Usage
-----
    python -m pipeline.hf_pull --cleaned --brand adonis
    python -m pipeline.hf_pull --raw --all
    python -m pipeline.hf_pull --db
    python -m pipeline.hf_pull --all

Auth
----
Requires the ``HF_TOKEN`` environment variable to be set.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys

import pyarrow.parquet as pq

from pipeline.hf_sync import (
    ALL_BRANDS,
    PROCESSED_REPO_ID,
    RAW_REPO_ID,
    _DB_FILES,
    _hf_api,
    _hf_enabled,
    _load_hf_parquet,
)


# ── Pull helpers ──────────────────────────────────────────────────────────────


def pull_raw_brand(brand: str, data_dir: str = "data") -> bool:
    """Download ``<brand>.parquet`` from the raw HF repo.

    Writes to ``data/<brand>/raw.parquet``.  Useful for analytics queries
    against the raw product data without having the original JSON files.

    Returns True if the file was downloaded, False otherwise.
    """
    if not _hf_enabled():
        return False

    table = _load_hf_parquet(RAW_REPO_ID, f"{brand}.parquet")
    if table is None:
        print(f"[hf_pull] {brand}: no raw parquet found on HF — skipping")
        return False

    out_path = os.path.join(data_dir, brand, "raw.parquet")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pq.write_table(table, out_path)
    print(f"[hf_pull] {brand}: downloaded {len(table)} raw rows → {out_path}")
    return True


def pull_cleaned_brand(brand: str, cleaned_dir: str = "cleaned") -> int:
    """Download ``cleaned/<brand>.parquet`` from the processed HF repo and
    reconstruct individual ``cleaned/<brand>/<flyer_id>.json`` envelope files.

    Reconstruction groups rows by ``flyer_id`` so that ``build_db.py`` can run
    unchanged after a fresh clone.  List fields are JSON-deserialized back from
    the string columns in the parquet.

    Returns the number of flyer JSON files written.
    """
    if not _hf_enabled():
        return 0

    table = _load_hf_parquet(PROCESSED_REPO_ID, f"cleaned/{brand}.parquet")
    if table is None:
        print(f"[hf_pull] {brand}: no cleaned parquet found on HF — skipping")
        return 0

    rows = table.to_pylist()

    # Group by flyer_id
    flyer_groups: dict[str, list[dict]] = {}
    for row in rows:
        fid = str(row.get("flyer_id", "unknown"))
        flyer_groups.setdefault(fid, []).append(row)

    brand_dir = os.path.join(cleaned_dir, brand)
    os.makedirs(brand_dir, exist_ok=True)

    written = 0
    for flyer_id, records in flyer_groups.items():
        out_path = os.path.join(brand_dir, f"{flyer_id}.json")

        # Restore list fields that were serialised as JSON strings
        restored_records = []
        for row in records:
            restored = dict(row)
            # Remove the internal dedup key if present
            restored.pop("_product_idx", None)
            # Try to deserialise fields that look like JSON lists/dicts
            for key, val in restored.items():
                if isinstance(val, str) and val.startswith("["):
                    try:
                        restored[key] = json.loads(val)
                    except (json.JSONDecodeError, ValueError):
                        pass
            restored_records.append(restored)

        # Determine fetched_on from the first record
        fetched_on = restored_records[0].get("fetched_on") if restored_records else None

        envelope = {
            "flyer_id": flyer_id,
            "store_chain": brand,
            "fetched_on": fetched_on,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "record_count": len(restored_records),
            "records": restored_records,
        }

        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(envelope, fh, indent=2)
        written += 1

    print(f"[hf_pull] {brand}: reconstructed {written} cleaned JSON files in {brand_dir}")
    return written


def pull_db(db_dir: str = "db") -> None:
    """Download db/ parquet files from the processed HF repo to the local ``db/`` tree."""
    if not _hf_enabled():
        return

    api = _hf_api()
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError

    token = os.environ.get("HF_TOKEN")

    for local_rel, hf_path in _DB_FILES:
        local_path = os.path.join(db_dir, local_rel.lstrip("db/"))
        try:
            downloaded = hf_hub_download(
                repo_id=PROCESSED_REPO_ID,
                filename=hf_path,
                repo_type="dataset",
                token=token,
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            import shutil
            shutil.copy2(downloaded, local_path)
            print(f"[hf_pull] Downloaded {hf_path} → {local_path}")
        except (EntryNotFoundError, RepositoryNotFoundError, FileNotFoundError):
            print(f"[hf_pull] {hf_path} not found on HF — skipping")
        except Exception as exc:  # noqa: BLE001
            print(f"[hf_pull] Warning: failed to download {hf_path}: {exc}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.hf_pull",
        description="Download grocery flyer data from Hugging Face dataset repos.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Download raw parquet files (data/<brand>/raw.parquet).",
    )
    parser.add_argument(
        "--cleaned",
        action="store_true",
        help="Download and reconstruct cleaned JSON envelopes.",
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Download db/ parquet files (scores, price history, dims).",
    )
    parser.add_argument(
        "--all",
        dest="all_flags",
        action="store_true",
        help="Equivalent to --raw --cleaned --db.",
    )
    parser.add_argument(
        "--brand",
        metavar="BRAND",
        default=None,
        help="Restrict to a single brand folder (e.g. adonis). Default: all brands.",
    )
    parser.add_argument(
        "--data-dir",
        metavar="PATH",
        default="data",
        help="Root directory for raw data output (default: data).",
    )
    parser.add_argument(
        "--cleaned-dir",
        metavar="PATH",
        default="cleaned",
        help="Root directory for cleaned JSON output (default: cleaned).",
    )
    parser.add_argument(
        "--db-dir",
        metavar="PATH",
        default="db",
        help="Root directory for db/ parquet output (default: db).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if not _hf_enabled():
        print("[hf_pull] HF_TOKEN not set — cannot pull from HF.", file=sys.stderr)
        return 1

    do_raw = args.raw or args.all_flags
    do_cleaned = args.cleaned or args.all_flags
    do_db = args.db or args.all_flags

    if not (do_raw or do_cleaned or do_db):
        print("[hf_pull] Nothing to do. Use --raw, --cleaned, --db, or --all.", file=sys.stderr)
        return 1

    brands = [args.brand] if args.brand else ALL_BRANDS

    if do_raw:
        for brand in brands:
            try:
                pull_raw_brand(brand, data_dir=args.data_dir)
            except Exception as exc:  # noqa: BLE001
                print(f"[hf_pull] Error pulling raw {brand}: {exc}", file=sys.stderr)

    if do_cleaned:
        total = 0
        for brand in brands:
            try:
                total += pull_cleaned_brand(brand, cleaned_dir=args.cleaned_dir)
            except Exception as exc:  # noqa: BLE001
                print(f"[hf_pull] Error pulling cleaned {brand}: {exc}", file=sys.stderr)
        print(f"[hf_pull] Cleaned pull complete. {total} flyer files reconstructed.")

    if do_db:
        try:
            pull_db(db_dir=args.db_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"[hf_pull] Error pulling db files: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
