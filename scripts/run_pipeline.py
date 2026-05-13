#!/usr/bin/env python3
"""
scripts/run_pipeline.py — Weekly deal-scoring pipeline orchestration.

Chains four steps end-to-end:
  1. pipeline.clean     — raw flyer JSON → cleaned/<chain>.parquet
  2. pipeline.build_db  — cleaned/ → observations, dimensions, price history, scores
  3. pipeline.flyer_ranker — active_scores.parquet → flyer/chain rankings
  4. export_frontend_data  — db/ → frontend/public/data/*.json

Fetching (scripts/fetch_flyers.py) is intentionally excluded; it runs on its
own periodic schedule and must complete before this script is called.

Usage::

    python scripts/run_pipeline.py [--store <name>] [--db-dir <path>]
                                   [--cleaned-dir <path>]

Options
-------
--store <name>
    Restrict steps 1 and 2 to a single chain slug (e.g. ``loblaws``).
    Useful for incremental testing without reprocessing all 25 chains.
--db-dir <path>
    Root Parquet database directory (default: ``db``).
--cleaned-dir <path>
    Directory for cleaned per-chain Parquet files (default: ``cleaned``).
"""

from __future__ import annotations

import argparse
import datetime
import os
from pathlib import Path
import sys


# Allow direct execution via "python scripts/run_pipeline.py" by adding
# the repository root (parent of scripts/) to the import path.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ── Helpers ───────────────────────────────────────────────────────────────────


def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def _step(n: int, total: int, label: str) -> None:
    bar = "─" * 50
    print(f"\n[{_ts()}]  Step {n}/{total} — {label}")
    print(f"           {bar}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="Run the full weekly deal-scoring pipeline end-to-end.",
    )
    parser.add_argument(
        "--store",
        metavar="NAME",
        default=None,
        help="Restrict clean + build_db to one chain slug (e.g. loblaws).",
    )
    parser.add_argument(
        "--db-dir",
        metavar="PATH",
        default="db",
        help="Root Parquet database directory (default: db).",
    )
    parser.add_argument(
        "--cleaned-dir",
        metavar="PATH",
        default="cleaned",
        help="Cleaned Parquet directory (default: cleaned).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    print(f"[{_ts()}]  Pipeline start")
    if args.store:
        print(f"           (restricted to store: {args.store})")

    # ── Step 1: pipeline.clean ────────────────────────────────────────────────
    _step(1, 4, "pipeline.clean  →  cleaned/<chain>.parquet")
    from pipeline.clean import main as clean_main  # noqa: PLC0415

    clean_argv: list[str] = []
    if args.store:
        clean_argv += ["--store", args.store]
    if args.cleaned_dir != "cleaned":
        clean_argv += ["--output-dir", args.cleaned_dir]

    rc = clean_main(clean_argv or None)
    if rc:
        print(f"\nERROR: pipeline.clean exited with code {rc}", file=sys.stderr)
        return rc

    # ── Step 2: pipeline.build_db --score ────────────────────────────────────
    _step(2, 4, "pipeline.build_db --score  →  db/observations, dimensions, features, scores")
    from pipeline.build_db import main as build_db_main  # noqa: PLC0415

    build_argv: list[str] = ["--score", "--db-dir", args.db_dir, "--cleaned-dir", args.cleaned_dir]
    if args.store:
        build_argv += ["--store", args.store]

    rc = build_db_main(build_argv)
    if rc:
        print(f"\nERROR: pipeline.build_db exited with code {rc}", file=sys.stderr)
        return rc

    # ── Step 3: flyer_ranker ──────────────────────────────────────────────────
    _step(3, 4, "pipeline.flyer_ranker  →  db/rankings/")
    from pipeline.flyer_ranker import rank_flyers  # noqa: PLC0415

    scores_path = os.path.join(args.db_dir, "scores", "active_scores.parquet")
    rankings_dir = os.path.join(args.db_dir, "rankings")

    if not os.path.exists(scores_path):
        print(
            f"\nWARNING: {scores_path} not found — no active deals scored this run.\n"
            "Rankings step skipped.",
            file=sys.stderr,
        )
    else:
        flyer_rows, chain_rows = rank_flyers(scores_path=scores_path, out_dir=rankings_dir)
        print(f"✓ Ranked {len(flyer_rows)} flyers across {len(chain_rows)} chains")
        for row in chain_rows:
            print(
                f"  #{row['rank']:2d}  {row['store_chain']:<25s}"
                f"  grade={row['avg_flyer_grade']:5.1f}  ({row['letter_grade']})"
                f"  hot={row['hot_count']}  flyers={row['flyer_count']}  avg_items={row['avg_items_per_flyer']}"
            )

    # ── Step 4: export_frontend_data ──────────────────────────────────────────
    _step(4, 4, "export_frontend_data  →  frontend/public/data/*.json")
    from scripts.export_frontend_data import (  # noqa: PLC0415
        _export_scores,
        export_flyer_regions,
        export_postal_centroids,
        export_rankings,
        export_stores_geo,
    )

    _export_scores()
    export_stores_geo()
    export_flyer_regions()
    export_postal_centroids()
    export_rankings()

    print(f"\n[{_ts()}]  Pipeline complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
