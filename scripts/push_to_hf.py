"""
Push aggregated grocery flyer datasets to Hugging Face.

Pipeline steps
--------------
When ``--skip-pipeline`` is not set, the script runs:

1. ``python -m pipeline.clean``  — normalise and enrich raw flyer JSONs into
   ``cleaned/all_flyers.parquet``.
2. ``python -m pipeline.build_db --score`` — ingest cleaned data into Parquet
   observations, rebuild dimension tables, and run product resolution,
   price-history, and deal-scoring in one pass.

Usage::

    python -m scripts.push_to_hf [--repo <id>] [--token <hf_token>]
                                   [--skip-pipeline] [--dry-run]

Options
-------
--repo <id>
    Hugging Face dataset repo ID, e.g. ``jpearce610/canadian-grocery-flyers``.
    Defaults to the ``HF_REPO`` environment variable.
--token <hf_token>
    Hugging Face access token.  Defaults to the ``HF_TOKEN`` environment
    variable.  Must have *write* access to the target repo.
--skip-pipeline
    Skip running the clean/build/score pipeline and push whatever Parquet
    files are already present on disk.  Useful for re-uploading after a
    failed push.
--dry-run
    Print the files that would be uploaded without actually uploading them.

Datasets pushed
---------------
The script pushes five Parquet tables to ``<repo>/data/``:

raw_archive.parquet
    Slim historical archive — one row per product observation, keeping only
    the columns needed for price-trend analysis.  Debug fields (``raw_name``,
    ``raw_body``, ``raw_categories``, etc.) and rarely-populated internals are
    excluded.  Appended each week; never overwritten.
    Built from ``cleaned/all_flyers.parquet``.

observations.parquet
    Full cleaned product observation table — all columns from the enrichment
    pipeline (source: ``cleaned/all_flyers.parquet``).
    This is the widest table, best for ML or feature experimentation.

active_deals.parquet
    Scored active deals from the current week
    (source: ``db/scores/active_scores.parquet``).
    Overwritten on every push.

price_history.parquet
    Per-product per-chain per-week price features — estimated regular price,
    sale frequency, 52-week high/low (source: ``db/features/price_history.parquet``).
    Grows as more history accumulates.

stores.parquet
    Store directory — chain, province, city, postal code
    (source: ``db/dimensions/stores.parquet``).
    Updated whenever new stores are discovered.
"""

from __future__ import annotations

import argparse
import os
import sys

# ── Raw-archive column selection ──────────────────────────────────────────────
#
# Only these columns are kept in raw_archive.parquet.  The selection covers
# everything needed for price-trend analysis and historical research while
# dropping:
#   • debug/audit fields  (raw_name, raw_description, raw_body,
#                          pre_price_text, post_price_text, raw_categories)
#   • rarely-populated fields  (alternate_price, alternate_unit, over_limit_price,
#                               tax_indicator, purchase_limit, loyalty_trigger,
#                               weight_is_range, weight_min, weight_max,
#                               category_l3, category_l4, parent_record_id,
#                               multi_product_variants)
#   • pipeline-internal flags  (is_multi_product, price_is_floor)
#   • redundant constants      (currency — always "CAD")
#
_RAW_ARCHIVE_COLS: list[str] = [
    # ── Provenance ────────────────────────────────────────────────────────────
    "source_api",
    "store_chain",
    "store_id",
    "province",
    "flyer_id",
    "flyer_valid_from",
    "flyer_valid_to",
    "fetched_on",
    # ── Product identity ──────────────────────────────────────────────────────
    "sku",
    "brand",
    "name_en",
    "name_fr",
    "language",
    # ── Pricing ───────────────────────────────────────────────────────────────
    "sale_price",
    "regular_price",
    "price_unit",
    "price_per_kg",
    "price_per_lb",
    "member_price",
    "multi_buy_qty",
    "multi_buy_total",
    # ── Promotion ─────────────────────────────────────────────────────────────
    "promo_type",
    "loyalty_program",
    "loyalty_points",
    # ── Package size ──────────────────────────────────────────────────────────
    "weight_value",
    "weight_unit",
    "pack_count",
    "pack_unit_size",
    "pack_unit",
    # ── Category ──────────────────────────────────────────────────────────────
    "category_l1",
    "category_l2",
    "is_food",
    "is_human_food",
    # ── Deduplication ─────────────────────────────────────────────────────────
    "price_observation_key",
]

# Temp path for the slimmed archive built during each push run
_RAW_ARCHIVE_TMP = "cleaned/raw_archive.parquet"

# Files to push: (local_path, path_in_repo)
_UPLOADS: list[tuple[str, str]] = [
    (_RAW_ARCHIVE_TMP,                           "data/raw_archive.parquet"),
    ("cleaned/all_flyers.parquet",               "data/observations.parquet"),
    ("db/scores/active_scores.parquet",          "data/active_deals.parquet"),
    ("db/features/price_history.parquet",        "data/price_history.parquet"),
    ("db/dimensions/stores.parquet",             "data/stores.parquet"),
]

# Pipeline steps, run in order when --skip-pipeline is not set.
# Scoring is triggered via the --score flag on build_db rather than a separate step.
_PIPELINE_STEPS: list[str] = [
    "pipeline.clean",
    "pipeline.build_db --score",
]


def _build_raw_archive(src_parquet: str, out_path: str) -> int:
    """Project *src_parquet* down to :data:`_RAW_ARCHIVE_COLS` and write *out_path*.

    Columns that are present in ``_RAW_ARCHIVE_COLS`` but absent from the
    source file are silently skipped so that the function stays forward-
    compatible as the schema evolves.

    Parameters
    ----------
    src_parquet:
        Path to the full cleaned observations file (``cleaned/all_flyers.parquet``).
    out_path:
        Destination path for the slim archive.

    Returns
    -------
    int
        Number of rows written, or 0 when *src_parquet* does not exist.
    """
    import pyarrow.parquet as pq

    if not os.path.exists(src_parquet):
        return 0

    # Read schema only (no data) to determine which requested columns exist,
    # then pass the column list to read_table so only those columns are
    # deserialised — avoids loading the full wide table into memory.
    schema = pq.read_schema(src_parquet)
    available = set(schema.names)
    keep = [c for c in _RAW_ARCHIVE_COLS if c in available]
    table = pq.read_table(src_parquet, columns=keep)

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    pq.write_table(table, out_path, compression="snappy")
    return table.num_rows


def _run_pipeline() -> None:
    """Run the clean and build_db pipeline steps."""
    import subprocess

    for step in _PIPELINE_STEPS:
        cmd = [sys.executable, "-m"] + step.split()
        print(f"  $ python -m {step}")
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"Pipeline step 'python -m {step}' exited with code {result.returncode}"
            )


def _push_files(
    repo_id: str,
    token: str,
    uploads: list[tuple[str, str]],
    dry_run: bool = False,
) -> None:
    """Upload local Parquet files to a Hugging Face dataset repository.

    Parameters
    ----------
    repo_id:
        HF dataset repo, e.g. ``"jpearce610/canadian-grocery-flyers"``.
    token:
        HF access token with write permissions.
    uploads:
        List of ``(local_path, path_in_repo)`` pairs.
    dry_run:
        When ``True``, print what would be uploaded without uploading.
    """
    from huggingface_hub import HfApi

    api = HfApi()

    # Ensure the dataset repo exists; create it if not
    if not dry_run:
        api.create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
            exist_ok=True,
            private=False,
        )

    for local_path, repo_path in uploads:
        if not os.path.exists(local_path):
            print(f"  [skip] {local_path} — file not found")
            continue

        size_mb = os.path.getsize(local_path) / 1_048_576
        print(f"  {'[dry-run] ' if dry_run else ''}→ {repo_path}  ({size_mb:.1f} MB)")

        if not dry_run:
            api.upload_file(
                path_or_fileobj=local_path,
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type="dataset",
                token=token,
                commit_message=f"chore: weekly data refresh — {repo_path}",
            )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.push_to_hf",
        description=(
            "Run the analytics pipeline and push aggregated Parquet datasets "
            "to a Hugging Face dataset repository."
        ),
    )
    parser.add_argument(
        "--repo",
        default=os.environ.get("HF_REPO", ""),
        metavar="REPO_ID",
        help=(
            "Hugging Face dataset repo ID "
            "(e.g. jpearce610/canadian-grocery-flyers). "
            "Defaults to the HF_REPO environment variable."
        ),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("HF_TOKEN", ""),
        metavar="TOKEN",
        help=(
            "Hugging Face write-access token. "
            "Defaults to the HF_TOKEN environment variable."
        ),
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip the clean/build/score pipeline and push existing files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print files that would be uploaded without uploading them.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if not args.dry_run:
        if not args.repo:
            print(
                "Error: --repo or HF_REPO environment variable is required.",
                file=sys.stderr,
            )
            return 1
        if not args.token:
            print(
                "Error: --token or HF_TOKEN environment variable is required.",
                file=sys.stderr,
            )
            return 1

    # ── 1. Run pipeline ───────────────────────────────────────────────────────
    if not args.skip_pipeline:
        print("Running pipeline…")
        try:
            _run_pipeline()
        except RuntimeError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
    else:
        print("Skipping pipeline (--skip-pipeline).")

    # ── 2. Build slim raw archive ─────────────────────────────────────────────
    print("Building raw archive (key columns only)…")
    try:
        n_rows = _build_raw_archive("cleaned/all_flyers.parquet", _RAW_ARCHIVE_TMP)
        if n_rows:
            size_mb = os.path.getsize(_RAW_ARCHIVE_TMP) / 1_048_576
            print(f"  raw_archive.parquet: {n_rows:,} rows, {size_mb:.1f} MB")
        else:
            print("  [skip] cleaned/all_flyers.parquet not found — raw archive skipped")
    except (ImportError, OSError, ValueError) as exc:
        print(f"  Warning: could not build raw archive: {exc}", file=sys.stderr)

    # ── 3. Push files ─────────────────────────────────────────────────────────
    if args.dry_run:
        print("Files that would be uploaded:")
    else:
        print(f"Uploading to {args.repo}…")

    try:
        _push_files(
            repo_id=args.repo,
            token=args.token,
            uploads=_UPLOADS,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Error during upload: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("Dry run complete — no files uploaded.")
    else:
        print(f"Done. Data available at https://huggingface.co/datasets/{args.repo}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
