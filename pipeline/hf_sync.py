"""
Hugging Face dataset sync — push and manage grocery flyer data in HF repos.

Two dataset repos are used:
  jpearce610/grocery-flyers-raw      — raw product rows (one parquet per chain)
  jpearce610/grocery-flyers-processed — cleaned records and db/ parquet files

Raw flyer JSONs are flattened to rows, appended-to/merged-with any existing
parquet on HF, pushed, and then the local JSON files are deleted.

Auth
----
Requires the ``HF_TOKEN`` environment variable to be set.  All public
functions silently no-op when it is absent (allowing pipeline stages to call
them unconditionally without failing in environments without HF access).

Usage
-----
    python -m pipeline.hf_sync --raw --brand adonis
    python -m pipeline.hf_sync --cleaned --all
    python -m pipeline.hf_sync --db
    python -m pipeline.hf_sync --create-repos   # one-time setup
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

RAW_REPO_ID = "jpearce610/grocery-flyers-raw"
PROCESSED_REPO_ID = "jpearce610/grocery-flyers-processed"

ALL_BRANDS = [
    "adonis",
    "atlantic_superstore",
    "dominion",
    "farm_boy",
    "food_basics",
    "foodland",
    "fortinos",
    "freshco",
    "freshmart",
    "iga",
    "independent_city_market",
    "independent_grocer",
    "loblaws",
    "longos",
    "maxi",
    "metro",
    "metro_qc",
    "nofrills",
    "provigo",
    "real_canadian_superstore",
    "safeway",
    "sobeys",
    "super_c",
    "walmart",
    "zehrs",
]

# Parquet files in db/ that are pushed verbatim (overwrite on each run)
_DB_FILES = [
    ("db/features/price_history.parquet", "db/features/price_history.parquet"),
    ("db/scores/active_scores.parquet", "db/scores/active_scores.parquet"),
    ("db/scores/archived_scores.parquet", "db/scores/archived_scores.parquet"),
    ("db/dimensions/products.parquet", "db/dimensions/products.parquet"),
    ("db/dimensions/stores.parquet", "db/dimensions/stores.parquet"),
    ("db/dimensions/flyers.parquet", "db/dimensions/flyers.parquet"),
]


# ── Auth / enable check ───────────────────────────────────────────────────────


def _hf_enabled() -> bool:
    """Return True only when HF_TOKEN is set in the environment."""
    return bool(os.environ.get("HF_TOKEN"))


def _hf_api():
    """Return an authenticated HfApi instance.  Raises if HF_TOKEN is unset."""
    from huggingface_hub import HfApi

    token = os.environ.get("HF_TOKEN")
    if not token:
        raise RuntimeError(
            "HF_TOKEN environment variable is not set. "
            "Export your Hugging Face write token before running hf_sync."
        )
    return HfApi(token=token)


# ── Parquet helpers ───────────────────────────────────────────────────────────


def _load_hf_parquet(repo_id: str, path_in_repo: str) -> pa.Table | None:
    """Download and read a parquet file from a HF dataset repo.

    Returns None when the file does not yet exist on HF (404 / entry not found).
    """
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError

    token = os.environ.get("HF_TOKEN")
    try:
        local_path = hf_hub_download(
            repo_id=repo_id,
            filename=path_in_repo,
            repo_type="dataset",
            token=token,
        )
        return pq.read_table(local_path)
    except (EntryNotFoundError, RepositoryNotFoundError, FileNotFoundError):
        return None
    except Exception as exc:  # noqa: BLE001
        # Treat unexpected errors (e.g. transient network) as missing
        print(f"[hf_sync] Warning: could not load {repo_id}/{path_in_repo}: {exc}")
        return None


def _push_hf_parquet(table: pa.Table, repo_id: str, path_in_repo: str) -> None:
    """Write *table* to a temp file and upload it to *repo_id* as *path_in_repo*."""
    api = _hf_api()
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        pq.write_table(table, tmp_path)
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type="dataset",
        )
    finally:
        os.unlink(tmp_path)


# ── Manifest helpers (track which flyer IDs have been synced) ─────────────────


def _manifest_path_raw(brand: str) -> str:
    return f"data/{brand}/hf_raw_synced.json"


def _manifest_path_cleaned(brand: str) -> str:
    return f"data/{brand}/hf_cleaned_synced.json"


def _load_manifest(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as fh:
            return set(json.load(fh))
    except Exception:
        return set()


def _save_manifest(path: str, ids: set[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(sorted(ids), fh, indent=2)


# ── Raw flyer flatteners ──────────────────────────────────────────────────────


def _flatten_flipp_flyer(brand: str, flyer_id: str, data: dict[str, Any]) -> list[dict]:
    """Convert a Flipp raw flyer JSON dict to a list of flat product rows."""
    fetched_on = data.get("fetched_on")
    publication_meta = data.get("publication_meta") or {}
    valid_from = publication_meta.get("valid_from")
    valid_to = publication_meta.get("valid_to")
    publication_meta_json = json.dumps(publication_meta)

    rows = []
    for i, product in enumerate(data.get("products") or []):
        rows.append({
            # Routing metadata
            "source_api": "flipp",
            "store_chain": brand,
            "flyer_id": flyer_id,
            "fetched_on": fetched_on,
            "product_idx": i,
            "publication_meta_json": publication_meta_json,
            "flyer_valid_from": valid_from,
            "flyer_valid_to": valid_to,
            # Store context
            "store_id": None,  # Flipp products aren't store-scoped in the raw file
            # Flipp product fields
            "product_id": str(product.get("id")) if product.get("id") is not None else None,
            "sku": product.get("sku"),
            "name": product.get("name"),
            "description": product.get("description"),
            "price_text": product.get("price_text"),
            "original_price": product.get("original_price"),
            "pre_price_text": product.get("pre_price_text"),
            "post_price_text": product.get("post_price_text"),
            "sale_story": product.get("sale_story"),
            "valid_from": product.get("valid_from"),
            "valid_to": product.get("valid_to"),
            "item_type": product.get("item_type"),
            "item_categories_json": json.dumps(product.get("item_categories")),
            "image_url": product.get("image_url"),
            "item_web_url": product.get("item_web_url"),
            # Metro-only fields — absent in Flipp
            "product_en": None,
            "product_fr": None,
            "body_en": None,
            "body_fr": None,
            "main_category_en": None,
            "main_category_fr": None,
            "sub_category_en": None,
            "sub_category_fr": None,
            "regular_price": None,
            "sale_price": None,
            "savings_en": None,
            "savings_fr": None,
            "price_unit": None,
            "alternate_price": None,
            "product_image": None,
        })
    return rows


def _flatten_metro_flyer(brand: str, flyer_id: str, data: dict[str, Any]) -> list[dict]:
    """Convert a Metro raw flyer JSON dict to a list of flat product rows."""
    fetched_on = data.get("fetched_on")
    store_id = str(data.get("store_id")) if data.get("store_id") is not None else None

    rows = []
    for i, product in enumerate(data.get("products") or []):
        # Determine validity window from first product that has it, or from
        # product-level fields (Metro products carry their own dates)
        valid_from = product.get("validFrom")
        valid_to = product.get("validTo")
        rows.append({
            # Routing metadata
            "source_api": "metro",
            "store_chain": brand,
            "flyer_id": flyer_id,
            "fetched_on": fetched_on,
            "product_idx": i,
            "publication_meta_json": None,
            "flyer_valid_from": valid_from,
            "flyer_valid_to": valid_to,
            # Store context
            "store_id": store_id,
            # Flipp-only fields — absent in Metro
            "product_id": None,
            "name": None,
            "description": None,
            "price_text": None,
            "original_price": None,
            "pre_price_text": None,
            "post_price_text": None,
            "sale_story": None,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "item_type": None,
            "item_categories_json": None,
            "image_url": None,
            "item_web_url": None,
            # Metro product fields
            "sku": product.get("sku"),
            "product_en": product.get("productEn"),
            "product_fr": product.get("productFr"),
            "body_en": product.get("bodyEn"),
            "body_fr": product.get("bodyFr"),
            "main_category_en": product.get("mainCategoryEn"),
            "main_category_fr": product.get("mainCategoryFr"),
            "sub_category_en": product.get("subCategoryEn"),
            "sub_category_fr": product.get("subCategoryFr"),
            "regular_price": product.get("regularPrice"),
            "sale_price": product.get("salePrice"),
            "savings_en": product.get("savingsEn"),
            "savings_fr": product.get("savingsFr"),
            "price_unit": product.get("priceUnit"),
            "alternate_price": product.get("alternatePrice"),
            "product_image": product.get("productImage"),
        })
    return rows


def _flatten_flyer(brand: str, flyer_id: str, data: dict[str, Any]) -> list[dict]:
    """Detect API source and dispatch to the appropriate flattener."""
    if "publication_id" in data:
        return _flatten_flipp_flyer(brand, flyer_id, data)
    if "job" in data:
        return _flatten_metro_flyer(brand, flyer_id, data)
    raise ValueError(
        f"Cannot detect API source for {brand}/{flyer_id}.json "
        f"(expected 'publication_id' or 'job' key at top level)"
    )


# ── Public sync functions ─────────────────────────────────────────────────────


def push_raw_brand(brand: str) -> int:
    """Convert new raw flyer JSONs for *brand* to Parquet and push to HF.

    Steps:
    1. Load the sync manifest (``data/<brand>/hf_raw_synced.json``).
    2. Identify unsynced flyer JSON files in ``data/<brand>/flyers/``.
    3. Flatten each new flyer to product rows.
    4. Download existing parquet from HF (if any), merge, dedup, re-upload.
    5. Delete the local JSON files that were successfully synced.
    6. Update the manifest.

    Returns the number of flyer files synced (0 means nothing new).
    """
    if not _hf_enabled():
        return 0

    flyers_dir = f"data/{brand}/flyers"
    if not os.path.isdir(flyers_dir):
        return 0

    manifest = _load_manifest(_manifest_path_raw(brand))

    new_json_files = [
        f for f in os.listdir(flyers_dir)
        if f.endswith(".json") and f[:-5] not in manifest
    ]

    if not new_json_files:
        return 0

    new_rows: list[dict] = []
    synced_ids: list[str] = []

    for fname in new_json_files:
        flyer_id = fname[:-5]
        fpath = os.path.join(flyers_dir, fname)
        try:
            with open(fpath, encoding="utf-8") as fh:
                data = json.load(fh)
            rows = _flatten_flyer(brand, flyer_id, data)
            new_rows.extend(rows)
            synced_ids.append(flyer_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[hf_sync] Warning: skipping {brand}/{fname}: {exc}")

    if not new_rows:
        return 0

    hf_path = f"{brand}.parquet"
    existing = _load_hf_parquet(RAW_REPO_ID, hf_path)
    new_table = pa.Table.from_pylist(new_rows)

    if existing is not None:
        # Align schemas before concat (fill missing columns with nulls)
        combined = _concat_tables(existing, new_table)
    else:
        combined = new_table

    combined = _dedup_table(combined, ["store_chain", "flyer_id", "product_idx"])

    _push_hf_parquet(combined, RAW_REPO_ID, hf_path)
    print(f"[hf_sync] {brand}: pushed {len(new_rows)} raw rows ({len(synced_ids)} flyers) → {RAW_REPO_ID}/{hf_path}")

    # Delete local JSON files that have been synced
    for fname in new_json_files:
        flyer_id = fname[:-5]
        if flyer_id in synced_ids:
            try:
                os.unlink(os.path.join(flyers_dir, fname))
            except OSError as exc:
                print(f"[hf_sync] Warning: could not delete {flyers_dir}/{fname}: {exc}")

    manifest.update(synced_ids)
    _save_manifest(_manifest_path_raw(brand), manifest)

    return len(synced_ids)


def push_cleaned_brand(brand: str, cleaned_dir: str = "cleaned") -> int:
    """Append new cleaned flyer records for *brand* to HF processed repo.

    Reads ``cleaned/<brand>/*.json`` envelopes, extracts the ``records[]``
    arrays, and appends to ``cleaned/<brand>.parquet`` in the processed repo.

    Returns the number of flyer files synced.
    """
    if not _hf_enabled():
        return 0

    brand_dir = os.path.join(cleaned_dir, brand)
    if not os.path.isdir(brand_dir):
        return 0

    manifest = _load_manifest(_manifest_path_cleaned(brand))

    new_json_files = [
        f for f in os.listdir(brand_dir)
        if f.endswith(".json") and f[:-5] not in manifest
    ]

    if not new_json_files:
        return 0

    new_rows: list[dict] = []
    synced_ids: list[str] = []

    for fname in new_json_files:
        flyer_id = fname[:-5]
        fpath = os.path.join(brand_dir, fname)
        try:
            with open(fpath, encoding="utf-8") as fh:
                envelope = json.load(fh)
            records = envelope.get("records") or []
            for i, record in enumerate(records):
                row = dict(record)
                # Flatten list fields to JSON strings for a uniform schema
                for key, val in row.items():
                    if isinstance(val, list):
                        row[key] = json.dumps(val)
                # Ensure dedup key fields are present
                row.setdefault("store_chain", brand)
                row.setdefault("flyer_id", flyer_id)
                row["_product_idx"] = i
                new_rows.append(row)
            synced_ids.append(flyer_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[hf_sync] Warning: skipping cleaned/{brand}/{fname}: {exc}")

    if not new_rows:
        return 0

    hf_path = f"cleaned/{brand}.parquet"
    existing = _load_hf_parquet(PROCESSED_REPO_ID, hf_path)
    new_table = pa.Table.from_pylist(new_rows)

    if existing is not None:
        combined = _concat_tables(existing, new_table)
    else:
        combined = new_table

    combined = _dedup_table(combined, ["store_chain", "flyer_id", "_product_idx"])

    _push_hf_parquet(combined, PROCESSED_REPO_ID, hf_path)
    print(f"[hf_sync] {brand}: pushed {len(new_rows)} cleaned rows ({len(synced_ids)} flyers) → {PROCESSED_REPO_ID}/{hf_path}")

    manifest.update(synced_ids)
    _save_manifest(_manifest_path_cleaned(brand), manifest)

    return len(synced_ids)


def push_db_files(db_dir: str = "db") -> None:
    """Upload db/ parquet files verbatim (overwrite) to the processed HF repo.

    This covers price history, scores, and dimension tables — each is fully
    rebuilt on every pipeline run so append logic is not needed.
    """
    if not _hf_enabled():
        return

    api = _hf_api()
    for local_rel, hf_path in _DB_FILES:
        local_path = os.path.join(db_dir, local_rel.lstrip("db/"))
        if not os.path.isfile(local_path):
            continue
        try:
            api.upload_file(
                path_or_fileobj=local_path,
                path_in_repo=hf_path,
                repo_id=PROCESSED_REPO_ID,
                repo_type="dataset",
            )
            print(f"[hf_sync] Uploaded {hf_path} → {PROCESSED_REPO_ID}")
        except Exception as exc:  # noqa: BLE001
            print(f"[hf_sync] Warning: failed to upload {hf_path}: {exc}")


def create_repos() -> None:
    """One-time helper: create both HF dataset repos if they do not exist."""
    api = _hf_api()
    for repo_id in (RAW_REPO_ID, PROCESSED_REPO_ID):
        api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        print(f"[hf_sync] Repo ready: {repo_id}")


# ── Table helpers ─────────────────────────────────────────────────────────────


def _concat_tables(t1: pa.Table, t2: pa.Table) -> pa.Table:
    """Concatenate two Arrow tables, adding null columns where schemas differ."""
    cols1 = set(t1.schema.names)
    cols2 = set(t2.schema.names)

    for missing in cols2 - cols1:
        null_arr = pa.array([None] * len(t1), type=pa.string())
        t1 = t1.append_column(missing, null_arr)

    for missing in cols1 - cols2:
        null_arr = pa.array([None] * len(t2), type=pa.string())
        t2 = t2.append_column(missing, null_arr)

    # Reorder t2 columns to match t1
    t2 = t2.select(t1.schema.names)

    return pa.concat_tables([t1, t2])


def _dedup_table(table: pa.Table, key_cols: list[str]) -> pa.Table:
    """Remove duplicate rows by *key_cols*, keeping the last occurrence."""
    import pyarrow.compute as pc

    # Build a composite string key for dedup
    present_keys = [k for k in key_cols if k in table.schema.names]
    if not present_keys:
        return table

    key_parts = [
        pc.cast(table.column(k), pa.string()) if k in table.schema.names
        else pa.array([""] * len(table), type=pa.string())
        for k in present_keys
    ]
    composite = key_parts[0]
    for part in key_parts[1:]:
        composite = pc.binary_join_element_wise(composite, part, "||")

    # Keep last occurrence for each key (iterate in reverse, track seen)
    keys_list = composite.to_pylist()
    seen: set = set()
    keep_indices = []
    for i in range(len(keys_list) - 1, -1, -1):
        k = keys_list[i]
        if k not in seen:
            seen.add(k)
            keep_indices.append(i)
    keep_indices.reverse()

    return table.take(keep_indices)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.hf_sync",
        description="Push grocery flyer data to Hugging Face dataset repos.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Push raw flyer parquet files to the raw HF repo.",
    )
    parser.add_argument(
        "--cleaned",
        action="store_true",
        help="Push cleaned flyer parquet files to the processed HF repo.",
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Push db/ parquet files (scores, price history, dims) to processed repo.",
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
        "--cleaned-dir",
        metavar="PATH",
        default="cleaned",
        help="Root directory for cleaned JSON envelopes (default: cleaned).",
    )
    parser.add_argument(
        "--db-dir",
        metavar="PATH",
        default="db",
        help="Root directory for db/ parquet files (default: db).",
    )
    parser.add_argument(
        "--create-repos",
        action="store_true",
        help="Create HF dataset repos (one-time setup). Does not push any data.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if not _hf_enabled():
        print("[hf_sync] HF_TOKEN not set — nothing to do.", file=sys.stderr)
        return 0

    if args.create_repos:
        create_repos()
        return 0

    do_raw = args.raw or args.all_flags
    do_cleaned = args.cleaned or args.all_flags
    do_db = args.db or args.all_flags

    if not (do_raw or do_cleaned or do_db):
        print("[hf_sync] Nothing to do. Use --raw, --cleaned, --db, or --all.", file=sys.stderr)
        return 1

    brands = [args.brand] if args.brand else ALL_BRANDS

    if do_raw:
        total = 0
        for brand in brands:
            try:
                total += push_raw_brand(brand)
            except Exception as exc:  # noqa: BLE001
                print(f"[hf_sync] Error syncing raw {brand}: {exc}", file=sys.stderr)
        print(f"[hf_sync] Raw sync complete. {total} flyer files synced.")

    if do_cleaned:
        total = 0
        for brand in brands:
            try:
                total += push_cleaned_brand(brand, cleaned_dir=args.cleaned_dir)
            except Exception as exc:  # noqa: BLE001
                print(f"[hf_sync] Error syncing cleaned {brand}: {exc}", file=sys.stderr)
        print(f"[hf_sync] Cleaned sync complete. {total} flyer files synced.")

    if do_db:
        try:
            push_db_files(db_dir=args.db_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"[hf_sync] Error syncing db files: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
