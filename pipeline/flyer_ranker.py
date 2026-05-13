"""
pipeline/flyer_ranker.py — Weekly flyer and chain-level ranking engine.

Reads scored deal rows (from ``db/scores/active_scores.parquet`` or the
archived file) and aggregates them into per-flyer and per-chain grades for
the week.

Grade formula
-------------
For each flyer (identified by ``flyer_id``):

  item_count    — total scored items in the flyer
  hot_count     — items with deal_score >= 80
  good_count    — items with deal_score >= 65
  avg_score     — mean deal_score across all items
  top10_avg     — mean deal_score of the top-10 items (or all if < 10)
  hot_ratio     — hot_count / item_count (clamped to [0, 1])

  flyer_grade (0–100) =
      0.40 * avg_score
    + 0.30 * hot_ratio * 100
    + 0.30 * top10_avg

Chain grade for a given week is the average flyer_grade across all flyers
belonging to that chain that week.

Letter grades
-------------
  A  ≥ 75   B  ≥ 60   C  ≥ 45   D  ≥ 30   F  < 30

Public API
----------
rank_flyers(scores_path, out_dir, week_label) -> list[dict]
    Read scored rows, compute rankings, write output files, return flyer rows.

Output files (written to ``out_dir``)
--------------------------------------
current_flyer_rankings.parquet  — overwritten each run; one row per flyer.
current_chain_rankings.parquet  — overwritten each run; one row per chain.
weekly_history.parquet          — append-only; one row per (week_label, store_chain).
"""

from __future__ import annotations

import os
import sys
import datetime
from statistics import mean
from typing import Sequence


# ── Grade helpers ─────────────────────────────────────────────────────────────

_GRADE_THRESHOLDS = [
    (75, "A"),
    (60, "B"),
    (45, "C"),
    (30, "D"),
]


def letter_grade(score: float) -> str:
    """Map a 0–100 numeric grade to a letter grade."""
    for threshold, letter in _GRADE_THRESHOLDS:
        if score >= threshold:
            return letter
    return "F"


def flyer_grade(scores: Sequence[float | int]) -> float:
    """Compute the overall flyer grade (0–100) from a list of deal scores."""
    if not scores:
        return 0.0
    scores_f = [float(s) for s in scores]
    n = len(scores_f)
    hot = sum(1 for s in scores_f if s >= 80)
    avg = mean(scores_f)
    top10 = mean(sorted(scores_f, reverse=True)[:10])
    hot_ratio = hot / n
    grade = 0.40 * avg + 0.30 * hot_ratio * 100.0 + 0.30 * top10
    return round(grade, 2)


# ── Parquet I/O helpers ───────────────────────────────────────────────────────

def _read_parquet(path: str) -> list[dict]:
    """Read a Parquet file to a list of dicts. Returns [] if file missing."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required.", file=sys.stderr)
        raise
    if not os.path.exists(path):
        return []
    return pq.read_table(path).to_pylist()


def _write_parquet(rows: list[dict], path: str) -> None:
    """Write *rows* to a Parquet file, inferring schema."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("ERROR: pyarrow is required.", file=sys.stderr)
        raise
    if not rows:
        return
    table = pa.Table.from_pylist(rows)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    pq.write_table(table, path, compression="snappy")


# ── Core ranking logic ────────────────────────────────────────────────────────

def _aggregate_flyer_rows(scored_rows: list[dict]) -> list[dict]:
    """Aggregate deal-level rows into one row per flyer."""
    # Group by flyer_id
    groups: dict[str, list[dict]] = {}
    for row in scored_rows:
        fid = str(row.get("flyer_id") or "")
        if not fid:
            continue
        groups.setdefault(fid, []).append(row)

    flyer_rows: list[dict] = []
    for fid, items in groups.items():
        first = items[0]
        scores = [r["deal_score"] for r in items if r.get("deal_score") is not None]
        n = len(scores)
        if n == 0:
            continue

        scores_f = [float(s) for s in scores]
        hot = sum(1 for s in scores_f if s >= 80)
        good = sum(1 for s in scores_f if s >= 65)
        grade = flyer_grade(scores_f)

        flyer_rows.append({
            "flyer_id":        fid,
            "store_chain":     str(first.get("store_chain") or ""),
            "flyer_valid_from": str(first.get("flyer_valid_from") or ""),
            "flyer_valid_to":   str(first.get("flyer_valid_to") or ""),
            "item_count":      n,
            "hot_count":       hot,
            "good_count":      good,
            "avg_score":       round(mean(scores_f), 2),
            "top10_avg":       round(mean(sorted(scores_f, reverse=True)[:10]), 2),
            "hot_ratio":       round(hot / n, 4),
            "flyer_grade":     grade,
            "letter_grade":    letter_grade(grade),
        })

    return flyer_rows


def _aggregate_chain_rows(flyer_rows: list[dict], week_label: str) -> list[dict]:
    """Aggregate flyer rows into one row per chain for the given week."""
    chain_groups: dict[str, list[dict]] = {}
    for row in flyer_rows:
        chain = row["store_chain"]
        chain_groups.setdefault(chain, []).append(row)

    chain_rows: list[dict] = []
    for chain, flyers in chain_groups.items():
        grades = [f["flyer_grade"] for f in flyers]
        chain_grade = round(mean(grades), 2)
        total_items = sum(f["item_count"] for f in flyers)
        total_hot = sum(f["hot_count"] for f in flyers)
        hot_ratio = round(total_hot / total_items, 4) if total_items else 0.0
        avg_items_per_flyer = round(total_items / len(flyers)) if flyers else 0

        chain_rows.append({
            "week_label":          week_label,
            "store_chain":         chain,
            "flyer_count":         len(flyers),
            "avg_items_per_flyer": avg_items_per_flyer,
            "hot_count":           total_hot,
            "hot_ratio":           hot_ratio,
            "avg_flyer_grade":     chain_grade,
            "letter_grade":        letter_grade(chain_grade),
        })

    # Sort by grade descending
    chain_rows.sort(key=lambda r: r["avg_flyer_grade"], reverse=True)
    for i, row in enumerate(chain_rows, start=1):
        row["rank"] = i

    return chain_rows


# ── History helpers ────────────────────────────────────────────────────────────

def _dedup_key(row: dict) -> tuple:
    return (row.get("week_label", ""), row.get("store_chain", ""))


def _append_history(
    new_chain_rows: list[dict],
    history_path: str,
) -> None:
    """Append new chain-level ranking rows to the history file, deduplicating
    on (week_label, store_chain)."""
    existing: list[dict] = _read_parquet(history_path)
    existing_keys = {_dedup_key(r) for r in existing}
    to_add = [r for r in new_chain_rows if _dedup_key(r) not in existing_keys]
    if not to_add:
        return
    merged = existing + to_add
    _write_parquet(merged, history_path)


# ── Public API ────────────────────────────────────────────────────────────────

def rank_flyers(
    scores_path: str,
    out_dir: str,
    week_label: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Read scored deals, compute flyer and chain rankings, write outputs.

    Parameters
    ----------
    scores_path:
        Path to the active_scores.parquet (or archived_scores.parquet) file.
    out_dir:
        Directory for output files (created if absent).
    week_label:
        ISO week string used as the history key, e.g. ``"2026-W20"``.
        Defaults to the current UTC ISO week.

    Returns
    -------
    (flyer_rows, chain_rows)
        Lists of dicts — one per flyer / one per chain.

    Side-effects
    ------------
    * Writes ``<out_dir>/current_flyer_rankings.parquet`` — overwritten each run.
    * Writes ``<out_dir>/current_chain_rankings.parquet`` — overwritten each run.
    * Appends to ``<out_dir>/weekly_history.parquet`` — keyed on (week_label, store_chain).
    """
    if week_label is None:
        today = datetime.date.today()
        year, week, _ = today.isocalendar()
        week_label = f"{year}-W{week:02d}"

    os.makedirs(out_dir, exist_ok=True)

    # 1. Load scored deal rows
    scored_rows = _read_parquet(scores_path)

    # 2. Flyer-level aggregation
    flyer_rows = _aggregate_flyer_rows(scored_rows)
    # Attach week label
    for row in flyer_rows:
        row["week_label"] = week_label

    # 3. Chain-level aggregation
    chain_rows = _aggregate_chain_rows(flyer_rows, week_label)

    # 4. Write current snapshots (overwrite)
    flyer_out = os.path.join(out_dir, "current_flyer_rankings.parquet")
    chain_out  = os.path.join(out_dir, "current_chain_rankings.parquet")
    _write_parquet(flyer_rows, flyer_out)
    _write_parquet(chain_rows, chain_out)

    # 5. Append chain-level history
    history_path = os.path.join(out_dir, "weekly_history.parquet")
    _append_history(chain_rows, history_path)

    return flyer_rows, chain_rows


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Rank flyers and chains from scored deal data."
    )
    parser.add_argument(
        "--scores",
        default="db/scores/active_scores.parquet",
        help="Path to active_scores.parquet (default: db/scores/active_scores.parquet)",
    )
    parser.add_argument(
        "--out-dir",
        default="db/rankings",
        help="Output directory for ranking Parquet files (default: db/rankings)",
    )
    parser.add_argument(
        "--week",
        default=None,
        help="Override week label, e.g. '2026-W20'. Defaults to current UTC week.",
    )
    args = parser.parse_args()

    flyer_rows, chain_rows = rank_flyers(
        scores_path=args.scores,
        out_dir=args.out_dir,
        week_label=args.week,
    )

    print(f"✓ Ranked {len(flyer_rows)} flyers across {len(chain_rows)} chains")
    print(f"  Output: {args.out_dir}/")
    for row in chain_rows:
        print(
            f"  #{row['rank']:2d}  {row['store_chain']:<25s}"
            f"  grade={row['avg_flyer_grade']:5.1f}  "
            f"({row['letter_grade']})  "
            f"hot={row['hot_count']}/{row['item_count']}"
        )


if __name__ == "__main__":
    main()
