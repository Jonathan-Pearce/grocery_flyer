"""
pipeline/price_history.py — Price history & feature engineering.

Reads all Parquet observations from ``db/observations/``, joins with
``db/dimensions/products.parquet`` from Phase A, and computes one feature
row per ``(canonical_product_id, store_chain, week_start)``.

Regular price estimation priority cascade
-----------------------------------------
1. ``regular_price`` directly observed in a ``no_promo`` row
   → source ``"observed"``, conf ``1.0``
2. Median ``regular_price`` stated by the API on promo rows (e.g. Metro
   ``regularPrice`` field), where ``regular_price > sale_price``
   → source ``"api_stated"``, conf ``0.85``
3. Max ``sale_price`` where ``promo_type == "no_promo"``, ≥ 4 rows
   → source ``"own_history"``, conf ``0.8``
4. Same, 1–3 rows
   → source ``"own_history_sparse"``, conf ``0.5``
5. Cross-chain price for same ``canonical_product_id``
   → source ``"cross_chain"``, conf ``0.4``
6. Median ``sale_price`` of sibling products in ``category_l3``
   → source ``"category_median"``, conf ``0.2``
7. No estimate possible
   → source ``"none"``, conf ``0.0``

Computed feature columns
------------------------
- ``regular_price_estimated``  — float
- ``regular_price_source``     — string
- ``price_basis_conf``         — float 0.0–1.0
- ``sale_freq_chain``          — n_sale_weeks / n_observed_weeks, trailing 52 weeks, this chain
- ``sale_freq_market``         — same aggregated across all chains
- ``cycle_low_52w``            — min sale_price in trailing 52 weeks
- ``cycle_high_52w``           — max sale_price in trailing 52 weeks
- ``weeks_observed``           — total distinct weeks this product has been seen (all chains)
- ``chain_count``              — distinct chains carrying this product
- ``category_sibling_count``   — observation count in the same ``category_l3``

Output
------
Writes ``db/features/price_history.parquet``, partitioned by
``(store_chain, year, week)``.

Public API
----------
``build_price_history(observations_dir, products_path, out_path)``
"""

from __future__ import annotations

import datetime
import os
import shutil
import sys
from collections import defaultdict

from pipeline.product_resolver import _resolve_record

# 52 weeks expressed as a timedelta
_TRAILING_WINDOW = datetime.timedelta(weeks=52)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _week_start(date_str: str | None) -> datetime.date | None:
    """Return the Monday of the ISO week that contains *date_str*.

    Parameters
    ----------
    date_str:
        ISO 8601 date string or ``None``.

    Returns
    -------
    datetime.date | None
        The Monday of the week, or ``None`` when *date_str* is absent or
        cannot be parsed.
    """
    if not date_str:
        return None
    try:
        d = datetime.date.fromisoformat(str(date_str)[:10])
        return d - datetime.timedelta(days=d.weekday())
    except ValueError:
        return None


def _median(values: list[float]) -> float | None:
    """Return the median of *values*, or ``None`` when the list is empty."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2.0
    return float(s[mid])


def _load_observations(observations_dir: str) -> list[dict]:
    """Walk all ``.parquet`` files under *observations_dir* and return rows.

    Uses ``ParquetFile.read()`` (not ``read_table``) to avoid PyArrow merging
    Hive partition columns (e.g. ``store_chain=loblaws`` in the directory
    name) with same-named columns already inside the file.
    """
    import pyarrow.parquet as pq

    records: list[dict] = []
    for dirpath, _dirs, filenames in os.walk(observations_dir):
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


def _load_products(products_path: str) -> dict[str, dict]:
    """Load ``products.parquet`` and return a dict keyed by
    ``canonical_product_id``.

    Returns an empty dict when the file does not exist or cannot be read.
    """
    import pyarrow.parquet as pq

    if not os.path.exists(products_path):
        return {}
    try:
        table = pq.read_table(products_path)
        return {
            r["canonical_product_id"]: r
            for r in table.to_pylist()
            if r.get("canonical_product_id")
        }
    except Exception as exc:  # noqa: BLE001
        print(
            f"Warning: could not load {products_path}: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return {}


def _compute_regular_price(
    chain_no_promo_regular: list[float],
    chain_no_promo_sale: list[float],
    cross_chain_no_promo: list[float],
    category_sale_prices: list[float],
    promo_item_regular: list[float] | None = None,
) -> tuple[float | None, str, float]:
    """Apply the seven-tier regular price cascade.

    Parameters
    ----------
    chain_no_promo_regular:
        ``regular_price`` values from ``no_promo`` rows at *this* chain.
    chain_no_promo_sale:
        ``sale_price`` values from ``no_promo`` rows at *this* chain.
    cross_chain_no_promo:
        Regular or sale prices from ``no_promo`` rows at *other* chains
        for the same ``canonical_product_id``.
    category_sale_prices:
        All ``sale_price`` values for sibling products in the same
        ``category_l3``.
    promo_item_regular:
        ``regular_price`` values from *promo* rows where
        ``regular_price > sale_price`` (API-stated strikethrough prices,
        e.g. Metro ``regularPrice`` field).  ``None`` is treated as ``[]``.

    Returns
    -------
    tuple[float | None, str, float]
        ``(regular_price_estimated, regular_price_source, price_basis_conf)``
    """
    # Priority 1 — regular_price directly observed in a no_promo row
    if chain_no_promo_regular:
        return max(chain_no_promo_regular), "observed", 1.0

    # Priority 2 — API-stated strikethrough price on promo rows
    _promo_reg = promo_item_regular or []
    if _promo_reg:
        med = _median(_promo_reg)
        if med is not None:
            return med, "api_stated", 0.85

    # Priority 3 & 4 — max sale_price where promo_type == "no_promo"
    if chain_no_promo_sale:
        if len(chain_no_promo_sale) >= 4:
            return max(chain_no_promo_sale), "own_history", 0.8
        return max(chain_no_promo_sale), "own_history_sparse", 0.5

    # Priority 5 — cross-chain price for same canonical_product_id
    if cross_chain_no_promo:
        return max(cross_chain_no_promo), "cross_chain", 0.4

    # Priority 6 — median sale_price of sibling products in category_l3
    med = _median(category_sale_prices)
    if med is not None:
        return med, "category_median", 0.2

    # Priority 7 — no estimate possible
    return None, "none", 0.0


# ── Main API ──────────────────────────────────────────────────────────────────


def build_price_history(
    observations_dir: str,
    products_path: str,
    out_path: str,
) -> int:
    """Compute price-history features and write to *out_path*.

    Parameters
    ----------
    observations_dir:
        Root of the observations Parquet tree, e.g. ``"db/observations"``.
    products_path:
        Path to ``db/dimensions/products.parquet`` from Phase A.
    out_path:
        Destination path (or dataset root directory) for the feature table,
        e.g. ``"db/features/price_history.parquet"``.

    Returns
    -------
    int
        Number of feature rows written.

    Side-effects
    ------------
    *   Creates parent directories of *out_path* if they do not exist.
    *   Always overwrites *out_path* for idempotency (removes the existing
        file/directory before writing).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    # ── 1. Load raw observations ──────────────────────────────────────────────

    raw_records = _load_observations(observations_dir)

    # ── 2. Assign canonical_product_id and week_start ─────────────────────────

    records: list[dict] = []
    for rec in raw_records:
        cid, _tier, _conf = _resolve_record(rec)
        ws = _week_start(rec.get("flyer_valid_from"))
        if ws is None:
            continue  # cannot place row in a week → skip
        records.append(
            {
                "canonical_product_id": cid,
                "store_chain": rec.get("store_chain") or "",
                "week_start": ws,
                "sale_price": rec.get("sale_price"),
                "regular_price": rec.get("regular_price"),
                "promo_type": rec.get("promo_type"),
            }
        )

    # ── 3. Load products.parquet for category_l3 ──────────────────────────────

    products = _load_products(products_path)

    # ── 4. Build in-memory indices ────────────────────────────────────────────

    # chain_data[canonical_product_id][store_chain] → list of slim record dicts
    chain_data: dict[str, dict[str, list[dict]]] = defaultdict(
        lambda: defaultdict(list)
    )

    # category_sale_prices[category_l3] → list of sale_price floats
    category_sale_prices: dict[str, list[float]] = defaultdict(list)

    for r in records:
        cid = r["canonical_product_id"]
        chain = r["store_chain"]
        chain_data[cid][chain].append(r)

        # Accumulate per-category sale prices for the sibling median
        prod_info = products.get(cid)
        cat_l3 = prod_info.get("category_l3") if prod_info else None
        if cat_l3 and r["sale_price"] is not None:
            category_sale_prices[cat_l3].append(r["sale_price"])

    # ── 5. Collect unique (canonical_product_id, store_chain, week_start) keys

    seen: set[tuple] = set()
    output_keys: list[tuple[str, str, datetime.date]] = []
    for r in records:
        key = (r["canonical_product_id"], r["store_chain"], r["week_start"])
        if key not in seen:
            seen.add(key)
            output_keys.append(key)
    output_keys.sort()

    # ── 6. Compute features for each output key ───────────────────────────────

    feature_rows: list[dict] = []

    for canonical_product_id, store_chain, week_start in output_keys:
        this_chain_recs = chain_data[canonical_product_id][store_chain]

        # --- Regular price cascade inputs -----------------------------------
        no_promo_at_chain = [
            r for r in this_chain_recs if r["promo_type"] == "no_promo"
        ]
        chain_no_promo_regular = [
            r["regular_price"]
            for r in no_promo_at_chain
            if r["regular_price"] is not None
        ]
        chain_no_promo_sale = [
            r["sale_price"]
            for r in no_promo_at_chain
            if r["sale_price"] is not None
        ]

        # API-stated strikethrough prices: promo rows where regular_price > sale_price
        promo_item_regular = [
            r["regular_price"]
            for r in this_chain_recs
            if r["promo_type"] != "no_promo"
            and r["regular_price"] is not None
            and r["sale_price"] is not None
            and r["regular_price"] > r["sale_price"]
        ]

        # Cross-chain: other chains for the same canonical_product_id
        cross_chain_no_promo: list[float] = []
        for other_chain, other_recs in chain_data[canonical_product_id].items():
            if other_chain == store_chain:
                continue
            for r in other_recs:
                if r["promo_type"] == "no_promo":
                    p = (
                        r["regular_price"]
                        if r["regular_price"] is not None
                        else r["sale_price"]
                    )
                    if p is not None:
                        cross_chain_no_promo.append(p)

        prod_info = products.get(canonical_product_id)
        cat_l3 = prod_info.get("category_l3") if prod_info else None
        cat_siblings = category_sale_prices.get(cat_l3, []) if cat_l3 else []

        reg_price, reg_source, reg_conf = _compute_regular_price(
            chain_no_promo_regular,
            chain_no_promo_sale,
            cross_chain_no_promo,
            cat_siblings,
            promo_item_regular,
        )

        # --- Trailing 52-week window -----------------------------------------
        window_start = week_start - _TRAILING_WINDOW

        chain_52w = [
            r
            for r in this_chain_recs
            if window_start <= r["week_start"] <= week_start
        ]

        # sale_freq_chain
        chain_weeks_obs: set[datetime.date] = {r["week_start"] for r in chain_52w}
        chain_weeks_sale: set[datetime.date] = {
            r["week_start"]
            for r in chain_52w
            if r["promo_type"] is not None and r["promo_type"] != "no_promo"
        }
        n_obs_chain = len(chain_weeks_obs)
        sale_freq_chain = (
            len(chain_weeks_sale) / n_obs_chain if n_obs_chain > 0 else 0.0
        )

        # sale_freq_market — aggregate across all chains
        market_chain_weeks_obs: set[tuple] = set()
        market_chain_weeks_sale: set[tuple] = set()
        for c_chain, c_recs in chain_data[canonical_product_id].items():
            for r in c_recs:
                if window_start <= r["week_start"] <= week_start:
                    market_chain_weeks_obs.add((c_chain, r["week_start"]))
                    if r["promo_type"] is not None and r["promo_type"] != "no_promo":
                        market_chain_weeks_sale.add((c_chain, r["week_start"]))
        n_obs_market = len(market_chain_weeks_obs)
        sale_freq_market = (
            len(market_chain_weeks_sale) / n_obs_market if n_obs_market > 0 else 0.0
        )

        # cycle_low_52w / cycle_high_52w
        prices_52w = [r["sale_price"] for r in chain_52w if r["sale_price"] is not None]
        cycle_low: float | None = min(prices_52w) if prices_52w else None
        cycle_high: float | None = max(prices_52w) if prices_52w else None

        # weeks_observed (all chains, all time)
        all_weeks: set[datetime.date] = set()
        for c_recs in chain_data[canonical_product_id].values():
            for r in c_recs:
                all_weeks.add(r["week_start"])
        weeks_observed = len(all_weeks)

        # chain_count and category_sibling_count
        chain_count = len(chain_data[canonical_product_id])
        category_sibling_count = len(cat_siblings)

        iso_year, iso_week, _ = week_start.isocalendar()

        feature_rows.append(
            {
                "canonical_product_id": canonical_product_id,
                "store_chain": store_chain,
                "week_start": week_start.isoformat(),
                "year": iso_year,
                "week": iso_week,
                "regular_price_estimated": reg_price,
                "regular_price_source": reg_source,
                "price_basis_conf": reg_conf,
                "sale_freq_chain": sale_freq_chain,
                "sale_freq_market": sale_freq_market,
                "cycle_low_52w": cycle_low,
                "cycle_high_52w": cycle_high,
                "weeks_observed": weeks_observed,
                "chain_count": chain_count,
                "category_sibling_count": category_sibling_count,
            }
        )

    # ── 7. Write output ───────────────────────────────────────────────────────

    # Remove any pre-existing file or directory at out_path for idempotency
    if os.path.exists(out_path):
        if os.path.isdir(out_path):
            shutil.rmtree(out_path)
        else:
            os.remove(out_path)

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    schema = pa.schema(
        [
            ("canonical_product_id", pa.string()),
            ("store_chain", pa.string()),
            ("week_start", pa.string()),
            ("year", pa.int32()),
            ("week", pa.int32()),
            ("regular_price_estimated", pa.float64()),
            ("regular_price_source", pa.string()),
            ("price_basis_conf", pa.float64()),
            ("sale_freq_chain", pa.float64()),
            ("sale_freq_market", pa.float64()),
            ("cycle_low_52w", pa.float64()),
            ("cycle_high_52w", pa.float64()),
            ("weeks_observed", pa.int64()),
            ("chain_count", pa.int64()),
            ("category_sibling_count", pa.int64()),
        ]
    )

    table = pa.Table.from_pylist(feature_rows, schema=schema)
    pq.write_to_dataset(
        table,
        root_path=out_path,
        partition_cols=["store_chain", "year", "week"],
    )

    return len(feature_rows)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m pipeline.price_history",
        description="Compute price-history features from Parquet observations.",
    )
    parser.add_argument(
        "--db-dir",
        metavar="PATH",
        default="db",
        help="Root directory for the Parquet database (default: db).",
    )
    parser.add_argument(
        "--products",
        metavar="PATH",
        default=None,
        help=(
            "Path to products.parquet dimension table "
            "(default: <db-dir>/dimensions/products.parquet)."
        ),
    )
    parser.add_argument(
        "--out",
        metavar="PATH",
        default=None,
        help=(
            "Destination path for price_history.parquet "
            "(default: <db-dir>/features/price_history.parquet)."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    db_dir: str = args.db_dir
    products_path: str = args.products or os.path.join(
        db_dir, "dimensions", "products.parquet"
    )
    out_path: str = args.out or os.path.join(
        db_dir, "features", "price_history.parquet"
    )
    observations_dir: str = os.path.join(db_dir, "observations")

    try:
        n = build_price_history(
            observations_dir=observations_dir,
            products_path=products_path,
            out_path=out_path,
        )
        print(f"Done. {n} feature rows written to {out_path}")
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
