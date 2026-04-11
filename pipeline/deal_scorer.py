"""
pipeline/deal_scorer.py — Deal scoring engine.

Loads all active observations (``flyer_valid_from <= today <= flyer_valid_to``),
joins price-history features from Phase B, and computes a 0–100 ``deal_score``
plus a 0.0–1.0 ``confidence`` value for each row.

All thresholds and point values are read from ``config/scoring.yaml`` at
runtime — no magic numbers in code.

Score components
----------------
1. Discount Depth      (0–25)  — pct_off + dollar-saved bonus
2. Deal Rarity         (0–20)  — sale frequency + cross-chain exclusivity
3. Item Essentiality   (0–20)  — category tier / staple-keyword override
4. Price Cycle Position (0–15) — position within 52-week price range
5. Deal Authenticity   (0–15)  — inflation check + promo type + freshness
6. Loyalty & Stacking Bonus (0–5) — loyalty-points CAD value + member-price stacking

Confidence sub-signals
----------------------
history_depth_conf, price_basis_conf, match_tier_conf,
chain_coverage_conf, category_coverage_conf

Output tables
-------------
- ``db/scores/active_scores.parquet``   — active deals only; **overwritten** each run
- ``db/scores/archived_scores.parquet`` — all past scored deals; **append-only**,
  deduplicated on ``(flyer_id, sku, store_id)``

Public API
----------
``score_deals(observations_dir, price_history_path, config_path, out_dir)``
``main(argv)`` — CLI entry point
"""

from __future__ import annotations

import datetime
import os
import sys
from typing import Any

# ── Config loader ─────────────────────────────────────────────────────────────


def _load_config(config_path: str) -> dict:
    """Load and return the YAML scoring config as a plain dict.

    Parameters
    ----------
    config_path:
        Path to ``config/scoring.yaml``.

    Returns
    -------
    dict
        Parsed YAML document.
    """
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "PyYAML is required for deal_scorer. Install it with: pip install pyyaml"
        ) from exc
    with open(config_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ── Parquet helpers ───────────────────────────────────────────────────────────


def _load_parquet_dir(path: str) -> list[dict]:
    """Walk a directory (or single file) of Parquet files and return rows."""
    import pyarrow.parquet as pq

    records: list[dict] = []
    if not os.path.exists(path):
        return records
    if os.path.isfile(path):
        try:
            records.extend(pq.read_table(path).to_pylist())
        except Exception as exc:  # noqa: BLE001
            print(f"Warning: skipping {path}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return records
    for dirpath, _dirs, filenames in os.walk(path):
        for fname in sorted(filenames):
            if not fname.endswith(".parquet"):
                continue
            fpath = os.path.join(dirpath, fname)
            try:
                table = pq.ParquetFile(fpath).read()
                records.extend(table.to_pylist())
            except Exception as exc:  # noqa: BLE001
                print(
                    f"Warning: skipping {fpath}: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
    return records


def _write_parquet(rows: list[dict], out_path: str, schema: Any) -> None:
    """Write *rows* to a single Parquet file at *out_path*."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, out_path)


# ── Date helpers ──────────────────────────────────────────────────────────────


def _parse_date(value: Any) -> datetime.date | None:
    """Parse a date from a string, datetime, or date object."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def _days_since(date_from: datetime.date | None, today: datetime.date) -> int | None:
    """Return number of days between *date_from* and *today*, or None."""
    if date_from is None:
        return None
    return (today - date_from).days


# ── Bracket lookup helpers ────────────────────────────────────────────────────


def _bracket_pts(value: float | None, brackets: list[dict], key_min: str, key_pts: str) -> int:
    """Return pts for the highest bracket whose *key_min* <= value.

    Brackets should be ordered highest-min first.
    Returns 0 when *value* is None.
    """
    if value is None:
        return 0
    for b in brackets:
        if value >= b[key_min]:
            return b[key_pts]
    return 0


def _bracket_pts_max(value: float | None, brackets: list[dict], key_max: str, key_pts: str) -> int:
    """Return pts for the first bracket whose *key_max* > value.

    Brackets should be ordered lowest-max first (ascending max threshold).
    Returns 0 when *value* is None.
    """
    if value is None:
        return 0
    for b in brackets:
        if value < b[key_max]:
            return b[key_pts]
    return 0


def _bracket_conf(value: float | None, brackets: list[dict], key_min: str, key_conf: str) -> float:
    """Return conf for the highest bracket whose *key_min* <= value."""
    if value is None:
        return 0.0
    for b in brackets:
        if value >= b[key_min]:
            return b[key_conf]
    return 0.0


# ── Component scoring functions ───────────────────────────────────────────────


def _score_discount_depth(
    sale_price: float | None,
    regular_price_estimated: float | None,
    multi_buy_qty: int | None,
    multi_buy_total: float | None,
    cfg: dict,
) -> int:
    """Component 1 — Discount Depth (0–25 pts).

    Multi-buy deals are normalised to effective unit price before scoring.
    Returns cold-start neutral when regular price is unavailable.
    """
    dc = cfg["discount_depth"]
    max_pts: int = dc["max_pts"]
    cold: int = dc["cold_start_pts"]

    # Normalise multi-buy to unit price
    eff_sale = sale_price
    if multi_buy_qty and multi_buy_qty > 1 and multi_buy_total is not None:
        eff_sale = multi_buy_total / multi_buy_qty
    elif multi_buy_qty and multi_buy_qty > 1 and sale_price is not None:
        eff_sale = sale_price  # sale_price already per-unit

    if eff_sale is None or regular_price_estimated is None or regular_price_estimated <= 0:
        return cold

    pct_off = (regular_price_estimated - eff_sale) / regular_price_estimated * 100.0
    dollar_saved = regular_price_estimated - eff_sale

    pts = _bracket_pts(pct_off, dc["pct_off_brackets"], "min_pct", "pts")
    bonus = _bracket_pts(dollar_saved, dc["dollar_saved_bonus"], "min_saved", "bonus")
    return min(max_pts, pts + bonus)


def _score_deal_rarity(
    sale_freq_chain: float | None,
    is_cross_chain_exclusive: bool,
    cfg: dict,
) -> int:
    """Component 2 — Deal Rarity (0–20 pts).

    Cold-start neutral when no sale frequency data is available.
    """
    dc = cfg["deal_rarity"]
    max_pts: int = dc["max_pts"]
    cold: int = dc["cold_start_pts"]

    if sale_freq_chain is None:
        return cold

    pts = _bracket_pts_max(sale_freq_chain, dc["freq_brackets"], "max_freq", "pts")
    if is_cross_chain_exclusive:
        pts += dc["cross_chain_exclusive_bonus"]
    return min(max_pts, pts)


def _score_essentiality(
    category_l1: str | None,
    name_en: str | None,
    name_fr: str | None,
    cfg: dict,
) -> int:
    """Component 3 — Item Essentiality (0–20 pts).

    Staple keyword list overrides to tier-1 (max tier).
    Always computable (no cold-start neutral needed).
    """
    dc = cfg["essentiality"]
    max_pts: int = dc["max_pts"]

    # Staple keyword override — check both name fields
    combined = " ".join(
        t.lower()
        for t in (name_en or "", name_fr or "")
        if t
    )
    for kw in dc["staple_keywords"]:
        if kw.lower() in combined:
            return dc["tier_pts"][1]

    # Category tier lookup
    tier = dc["category_tier_map"].get(category_l1 or "", 5)
    return min(max_pts, dc["tier_pts"].get(tier, 0))


def _score_cycle_position(
    sale_price: float | None,
    cycle_low_52w: float | None,
    cycle_high_52w: float | None,
    cfg: dict,
) -> int:
    """Component 4 — Price Cycle Position (0–15 pts).

    ``price_percentile = (sale_price - cycle_low_52w) / (cycle_high_52w - cycle_low_52w)``

    Cold-start neutral when no 52-week cycle data is available.
    """
    dc = cfg["cycle_position"]
    cold: int = dc["cold_start_pts"]

    if (
        sale_price is None
        or cycle_low_52w is None
        or cycle_high_52w is None
        or cycle_high_52w <= cycle_low_52w
    ):
        return cold

    percentile = (sale_price - cycle_low_52w) / (cycle_high_52w - cycle_low_52w)
    percentile = max(0.0, min(1.0, percentile))  # clamp to [0, 1]

    return _bracket_pts_max(percentile, dc["percentile_brackets"], "max_percentile", "pts")


def _score_authenticity(
    sale_price: float | None,
    regular_price_estimated: float | None,
    promo_type: str | None,
    purchase_limit: int | None,
    flyer_valid_from: datetime.date | None,
    today: datetime.date,
    cfg: dict,
) -> int:
    """Component 5 — Deal Authenticity (0–15 pts).

    Regular price inflation check + promo type quality + purchase limit
    penalty + deal freshness bonus/penalty.
    """
    dc = cfg["authenticity"]
    max_pts: int = dc["max_pts"]
    total = 0

    # Sub-component 1: regular price inflation check (0–8 pts)
    if sale_price and regular_price_estimated and sale_price > 0:
        ratio = regular_price_estimated / sale_price
        total += _bracket_pts(ratio, dc["inflation_brackets"], "min_ratio", "pts")

    # Sub-component 2: promo type quality
    total += dc["promo_type_pts"].get(promo_type or "no_promo", 0)

    # Sub-component 3: purchase limit penalty
    if purchase_limit == 1:
        total += dc["purchase_limit_1_penalty"]

    # Sub-component 4: deal freshness
    if flyer_valid_from is not None:
        days_running = (today - flyer_valid_from).days
        if days_running < 7:
            total += dc["freshness_week1_bonus"]
        elif days_running < 14:
            total += dc["freshness_week2_pts"]
        else:
            total += dc["freshness_week3plus_penalty"]

    return max(0, min(max_pts, total))


def _score_loyalty_bonus(
    loyalty_program: str | None,
    loyalty_points: int | None,
    sale_price: float | None,
    member_price: float | None,
    cfg: dict,
) -> int:
    """Component 6 — Loyalty & Stacking Bonus (0–5 pts)."""
    dc = cfg["loyalty_bonus"]
    max_pts: int = dc["max_pts"]
    total = 0

    # Loyalty-points CAD value
    if loyalty_program and loyalty_points and loyalty_points > 0:
        prog_cfg = dc["loyalty_programs"].get(loyalty_program, {})
        cad_per_point: float = prog_cfg.get("cad_per_point", 0.0)
        cad_value = loyalty_points * cad_per_point
        total += _bracket_pts(cad_value, dc["loyalty_value_brackets"], "min_cad", "pts")

    # Member-price stacking bonus
    if member_price is not None and sale_price is not None and member_price < sale_price:
        total += dc["member_price_stack_bonus"]

    return min(max_pts, total)


# ── Confidence calculation ────────────────────────────────────────────────────


def _calc_confidence(
    weeks_observed: int | None,
    price_basis_conf: float | None,
    match_tier: str | None,
    chain_count: int | None,
    category_sibling_count: int | None,
    cfg: dict,
) -> tuple[float, float, float, float, float, float]:
    """Return ``(confidence, history_depth_conf, price_basis_conf_out,
    match_tier_conf, chain_coverage_conf, category_coverage_conf)``.
    """
    cc = cfg["confidence"]

    history_depth_conf = _bracket_conf(
        float(weeks_observed) if weeks_observed is not None else None,
        cc["history_depth_brackets"],
        "min_weeks",
        "conf",
    )

    price_basis_conf_out = float(price_basis_conf) if price_basis_conf is not None else 0.0

    match_tier_conf = cc["match_tier_conf"].get(match_tier or "category", 0.2)

    chain_coverage_conf = _bracket_conf(
        float(chain_count) if chain_count is not None else None,
        cc["chain_coverage_brackets"],
        "min_chains",
        "conf",
    )

    category_coverage_conf = _bracket_conf(
        float(category_sibling_count) if category_sibling_count is not None else None,
        cc["category_coverage_brackets"],
        "min_siblings",
        "conf",
    )

    weights = cc["weights"]
    confidence = (
        weights["history_depth_conf"] * history_depth_conf
        + weights["price_basis_conf"] * price_basis_conf_out
        + weights["match_tier_conf"] * match_tier_conf
        + weights["chain_coverage_conf"] * chain_coverage_conf
        + weights["category_coverage_conf"] * category_coverage_conf
    )
    confidence = max(0.0, min(1.0, confidence))

    return (
        confidence,
        history_depth_conf,
        price_basis_conf_out,
        match_tier_conf,
        chain_coverage_conf,
        category_coverage_conf,
    )


def _confidence_label(confidence: float, cfg: dict) -> str:
    """Return 'High', 'Medium', or 'Low' label for *confidence*."""
    cc = cfg["confidence"]
    if confidence >= cc["label_high_threshold"]:
        return cc["label_high"]
    if confidence >= cc["label_medium_threshold"]:
        return cc["label_medium"]
    return cc["label_low"]


# ── Cross-chain exclusivity check ─────────────────────────────────────────────


def _build_active_chain_product_set(
    active_rows: list[dict],
) -> set[tuple[str, str]]:
    """Return a set of ``(canonical_product_id, store_chain)`` pairs that are
    active this week.  Used to detect cross-chain exclusivity.
    """
    result: set[tuple[str, str]] = set()
    for row in active_rows:
        cid = row.get("canonical_product_id") or ""
        chain = row.get("store_chain") or ""
        if cid:
            result.add((cid, chain))
    return result


# ── Main scoring function ─────────────────────────────────────────────────────


def _score_row(
    obs: dict,
    features: dict,
    match_tier: str,
    is_cross_chain_exclusive: bool,
    today: datetime.date,
    cfg: dict,
) -> dict:
    """Score one observation row and return the full scored output dict."""
    sale_price: float | None = obs.get("sale_price")
    regular_price_estimated: float | None = features.get("regular_price_estimated")
    regular_price_source: str | None = features.get("regular_price_source") or "none"

    # Component scores
    c1 = _score_discount_depth(
        sale_price=sale_price,
        regular_price_estimated=regular_price_estimated,
        multi_buy_qty=obs.get("multi_buy_qty"),
        multi_buy_total=obs.get("multi_buy_total"),
        cfg=cfg,
    )
    c2 = _score_deal_rarity(
        sale_freq_chain=features.get("sale_freq_chain"),
        is_cross_chain_exclusive=is_cross_chain_exclusive,
        cfg=cfg,
    )
    c3 = _score_essentiality(
        category_l1=obs.get("category_l1"),
        name_en=obs.get("name_en"),
        name_fr=obs.get("name_fr"),
        cfg=cfg,
    )
    c4 = _score_cycle_position(
        sale_price=sale_price,
        cycle_low_52w=features.get("cycle_low_52w"),
        cycle_high_52w=features.get("cycle_high_52w"),
        cfg=cfg,
    )
    c5 = _score_authenticity(
        sale_price=sale_price,
        regular_price_estimated=regular_price_estimated,
        promo_type=obs.get("promo_type"),
        purchase_limit=obs.get("purchase_limit"),
        flyer_valid_from=_parse_date(obs.get("flyer_valid_from")),
        today=today,
        cfg=cfg,
    )
    c6 = _score_loyalty_bonus(
        loyalty_program=obs.get("loyalty_program"),
        loyalty_points=obs.get("loyalty_points"),
        sale_price=sale_price,
        member_price=obs.get("member_price"),
        cfg=cfg,
    )

    deal_score = min(100, c1 + c2 + c3 + c4 + c5 + c6)

    # Confidence
    (
        confidence,
        history_depth_conf,
        price_basis_conf_out,
        match_tier_conf,
        chain_coverage_conf,
        category_coverage_conf,
    ) = _calc_confidence(
        weeks_observed=features.get("weeks_observed"),
        price_basis_conf=features.get("price_basis_conf"),
        match_tier=match_tier,
        chain_count=features.get("chain_count"),
        category_sibling_count=features.get("category_sibling_count"),
        cfg=cfg,
    )

    label = _confidence_label(confidence, cfg)

    return {
        # Provenance
        "flyer_id": obs.get("flyer_id"),
        "sku": obs.get("sku"),
        "store_id": obs.get("store_id"),
        "store_chain": obs.get("store_chain"),
        "name_en": obs.get("name_en"),
        "name_fr": obs.get("name_fr"),
        "sale_price": sale_price,
        "flyer_valid_from": str(obs.get("flyer_valid_from") or ""),
        "flyer_valid_to": str(obs.get("flyer_valid_to") or ""),
        # Scores
        "deal_score": deal_score,
        "score_discount_depth": c1,
        "score_deal_rarity": c2,
        "score_essentiality": c3,
        "score_cycle_position": c4,
        "score_authenticity": c5,
        "score_loyalty_bonus": c6,
        # Confidence
        "confidence": confidence,
        "confidence_history_depth": history_depth_conf,
        "confidence_price_basis": price_basis_conf_out,
        "confidence_match_tier": match_tier_conf,
        "confidence_chain_coverage": chain_coverage_conf,
        "confidence_category_coverage": category_coverage_conf,
        "confidence_label": label,
        # Supporting fields
        "match_tier": match_tier,
        "regular_price_estimated": regular_price_estimated,
        "regular_price_source": regular_price_source,
        "scored_on": today.isoformat(),
    }


# ── Archive deduplication key ─────────────────────────────────────────────────


def _archive_dedup_key(row: dict) -> tuple[str, str, str]:
    """Return the deduplication key for a scored row in the archive.

    Two rows with the same ``(flyer_id, sku, store_id)`` triple are considered
    the same deal observation and only one copy is kept in
    ``archived_scores.parquet``.
    """
    return (row.get("flyer_id") or "", row.get("sku") or "", row.get("store_id") or "")





def _output_schema():
    import pyarrow as pa

    return pa.schema(
        [
            ("flyer_id", pa.string()),
            ("sku", pa.string()),
            ("store_id", pa.string()),
            ("store_chain", pa.string()),
            ("name_en", pa.string()),
            ("name_fr", pa.string()),
            ("sale_price", pa.float64()),
            ("flyer_valid_from", pa.string()),
            ("flyer_valid_to", pa.string()),
            ("deal_score", pa.int32()),
            ("score_discount_depth", pa.int32()),
            ("score_deal_rarity", pa.int32()),
            ("score_essentiality", pa.int32()),
            ("score_cycle_position", pa.int32()),
            ("score_authenticity", pa.int32()),
            ("score_loyalty_bonus", pa.int32()),
            ("confidence", pa.float64()),
            ("confidence_history_depth", pa.float64()),
            ("confidence_price_basis", pa.float64()),
            ("confidence_match_tier", pa.float64()),
            ("confidence_chain_coverage", pa.float64()),
            ("confidence_category_coverage", pa.float64()),
            ("confidence_label", pa.string()),
            ("match_tier", pa.string()),
            ("regular_price_estimated", pa.float64()),
            ("regular_price_source", pa.string()),
            ("scored_on", pa.string()),
        ]
    )


# ── Public API ────────────────────────────────────────────────────────────────


def score_deals(
    observations_dir: str,
    price_history_path: str,
    config_path: str,
    out_dir: str,
    today: datetime.date | None = None,
) -> int:
    """Score all active flyer deals and write output Parquet files.

    Parameters
    ----------
    observations_dir:
        Root of the observations Parquet tree, e.g. ``"db/observations"``.
    price_history_path:
        Path to (or root of) the price_history Parquet file/dataset from Phase B,
        e.g. ``"db/features/price_history.parquet"``.
    config_path:
        Path to ``config/scoring.yaml``.
    out_dir:
        Directory for output files, e.g. ``"db/scores"``.
    today:
        Reference date for "active" filtering (default: today's UTC date).

    Returns
    -------
    int
        Number of active rows scored.

    Side-effects
    ------------
    * Writes ``<out_dir>/active_scores.parquet`` — overwritten each run.
    * Appends to ``<out_dir>/archived_scores.parquet``, deduplicated on
      ``(flyer_id, sku, store_id)``.
    """
    import pyarrow.parquet as pq

    if today is None:
        today = datetime.date.today()

    cfg = _load_config(config_path)

    # ── 1. Load observations ──────────────────────────────────────────────────

    all_obs = _load_parquet_dir(observations_dir)

    # ── 2. Filter active observations ─────────────────────────────────────────

    active_obs: list[dict] = []
    for obs in all_obs:
        valid_from = _parse_date(obs.get("flyer_valid_from"))
        valid_to = _parse_date(obs.get("flyer_valid_to"))
        if valid_from is not None and valid_to is not None:
            if valid_from <= today <= valid_to:
                active_obs.append(obs)

    # ── 3. Load price-history features ────────────────────────────────────────

    ph_rows = _load_parquet_dir(price_history_path)

    # Index price-history by (canonical_product_id, store_chain)
    # Use the most recent week_start entry for each key
    ph_index: dict[tuple[str, str], dict] = {}
    for row in ph_rows:
        cid = row.get("canonical_product_id") or ""
        chain = row.get("store_chain") or ""
        key = (cid, chain)
        existing = ph_index.get(key)
        if existing is None:
            ph_index[key] = row
        else:
            # Keep the most recent week_start
            cur_ws = str(row.get("week_start") or "")
            ex_ws = str(existing.get("week_start") or "")
            if cur_ws > ex_ws:
                ph_index[key] = row

    # ── 4. Resolve canonical_product_id and match_tier for each observation ───

    # _resolve_record is the single source of truth for canonical product IDs in
    # this codebase (used by price_history.py as well).  It lives in
    # product_resolver as a semi-public helper; both modules intentionally share
    # the same resolution logic so that deal scores align with price-history rows.
    from pipeline.product_resolver import _resolve_record

    # Augment active observations with canonical ID and match tier
    augmented: list[tuple[dict, str, str, float]] = []
    for obs in active_obs:
        cid, tier, _conf = _resolve_record(obs)
        augmented.append((obs, cid, tier, _conf))

    # ── 5. Build cross-chain exclusivity lookup ───────────────────────────────

    # For each canonical_product_id, collect the set of chains with active deals
    cid_to_chains: dict[str, set[str]] = {}
    for obs, cid, _tier, _conf in augmented:
        chain = obs.get("store_chain") or ""
        if cid not in cid_to_chains:
            cid_to_chains[cid] = set()
        cid_to_chains[cid].add(chain)

    # ── 6. Score each active observation ──────────────────────────────────────

    scored_rows: list[dict] = []
    for obs, cid, tier, _conf in augmented:
        chain = obs.get("store_chain") or ""
        features = ph_index.get((cid, chain), {})

        # Cross-chain exclusivity: only this chain has the deal active
        active_chains = cid_to_chains.get(cid, set())
        is_exclusive = len(active_chains) == 1

        row = _score_row(
            obs=obs,
            features=features,
            match_tier=tier,
            is_cross_chain_exclusive=is_exclusive,
            today=today,
            cfg=cfg,
        )
        scored_rows.append(row)

    # ── 7. Write active_scores.parquet (overwrite) ────────────────────────────

    active_path = os.path.join(out_dir, "active_scores.parquet")
    schema = _output_schema()
    _write_parquet(scored_rows, active_path, schema)

    # ── 8. Append to archived_scores.parquet (deduplicate) ────────────────────

    archive_path = os.path.join(out_dir, "archived_scores.parquet")

    # Load existing archive
    existing_archive: list[dict] = []
    if os.path.exists(archive_path):
        try:
            existing_archive = pq.read_table(archive_path).to_pylist()
        except Exception as exc:  # noqa: BLE001
            print(
                f"Warning: could not read archive {archive_path}: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )

    # Build deduplication key set from existing archive
    existing_keys: set[tuple] = {_archive_dedup_key(r) for r in existing_archive}

    # Append only new rows
    new_rows = [r for r in scored_rows if _archive_dedup_key(r) not in existing_keys]
    merged = existing_archive + new_rows

    _write_parquet(merged, archive_path, schema)

    return len(scored_rows)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m pipeline.deal_scorer",
        description="Score active grocery flyer deals.",
    )
    parser.add_argument(
        "--db-dir",
        metavar="PATH",
        default="db",
        help="Root directory for the Parquet database (default: db).",
    )
    parser.add_argument(
        "--price-history",
        metavar="PATH",
        default=None,
        help=(
            "Path to price_history Parquet dataset "
            "(default: <db-dir>/features/price_history.parquet)."
        ),
    )
    parser.add_argument(
        "--config",
        metavar="PATH",
        default=None,
        help=(
            "Path to scoring YAML config "
            "(default: config/scoring.yaml relative to the working directory)."
        ),
    )
    parser.add_argument(
        "--out-dir",
        metavar="PATH",
        default=None,
        help="Output directory for score Parquet files (default: <db-dir>/scores).",
    )
    parser.add_argument(
        "--today",
        metavar="YYYY-MM-DD",
        default=None,
        help="Override today's date for testing (default: system date).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = _build_parser().parse_args(argv)

    db_dir: str = args.db_dir
    price_history_path: str = args.price_history or os.path.join(
        db_dir, "features", "price_history.parquet"
    )
    config_path: str = args.config or os.path.join("config", "scoring.yaml")
    out_dir: str = args.out_dir or os.path.join(db_dir, "scores")
    observations_dir: str = os.path.join(db_dir, "observations")

    today: datetime.date | None = None
    if args.today:
        try:
            today = datetime.date.fromisoformat(args.today)
        except ValueError:
            print(f"Error: invalid --today value: {args.today!r}", file=sys.stderr)
            return 1

    try:
        n = score_deals(
            observations_dir=observations_dir,
            price_history_path=price_history_path,
            config_path=config_path,
            out_dir=out_dir,
            today=today,
        )
        print(f"Done. {n} active deals scored. Output written to {out_dir}")
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
