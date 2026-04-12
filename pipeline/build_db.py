"""
Observations ingestion — writes cleaned flyer envelopes to partitioned Parquet.

Usage::

    python -m pipeline.build_db [--db-dir <path>] [--cleaned-dir <path>]
                                 [--store <name>] [--force] [--score]

Options
-------
--db-dir <path>
    Root directory for the Parquet database (default: ``db``).
--cleaned-dir <path>
    Root directory of cleaned JSON envelopes (default: ``cleaned``).
--store <name>
    Restrict processing to a single store folder (e.g. ``loblaws``).
--force
    Overwrite existing Parquet files even if they already exist.
--score
    Run product resolution, price history, and deal scoring after
    observations and dimensions have been built.

Output layout
-------------
Each cleaned ``<store>/<flyer_id>.json`` envelope is written to::

    <db_dir>/observations/store_chain=<store>/year=<YYYY>/week=<WW>/<flyer_id>.parquet

The partition key is derived from ``flyer_valid_from`` on the first record in
the envelope's ``records[]`` array.  When that field is absent the
``fetched_on`` date is used instead; if that is also absent today's date is
used as a last resort.
"""

from __future__ import annotations

import datetime
import json
import os
import sys


# ── Partition helper ──────────────────────────────────────────────────────────


def _partition_dir(db_dir: str, store_chain: str, flyer_valid_from: str | None) -> str:
    """Return a Hive-style partition path for *store_chain* and *flyer_valid_from*.

    Parameters
    ----------
    db_dir:
        Root directory of the Parquet database, e.g. ``"db"``.
    store_chain:
        Normalised chain slug, e.g. ``"loblaws"``.
    flyer_valid_from:
        ISO 8601 date string (``"YYYY-MM-DD"``), or ``None``.

    Returns
    -------
    str
        Path of the form
        ``db/observations/store_chain=loblaws/year=2026/week=14``.

    Notes
    -----
    *   ``year`` and ``week`` are derived from the ISO week calendar so that
        the partition boundaries align with flyer publication weeks.
    *   When *flyer_valid_from* is ``None`` the fallback date is today.
    """
    if flyer_valid_from:
        try:
            # Slice to 10 characters to handle both "YYYY-MM-DD" date strings
            # and full ISO 8601 timestamps like "2026-04-02T10:30:00+00:00".
            date = datetime.date.fromisoformat(str(flyer_valid_from)[:10])
        except ValueError:
            date = datetime.date.today()
    else:
        date = datetime.date.today()

    iso_year, iso_week, _ = date.isocalendar()
    return os.path.join(
        db_dir,
        "observations",
        f"store_chain={store_chain}",
        f"year={iso_year}",
        f"week={iso_week}",
    )


# ── Main ingest loop ──────────────────────────────────────────────────────────


def build_observations(
    db_dir: str,
    cleaned_dir: str,
    store: str | None = None,
    force: bool = False,
) -> tuple[int, int]:
    """Ingest cleaned flyer envelopes into partitioned Parquet files.

    Parameters
    ----------
    db_dir:
        Root directory for the Parquet database output.
    cleaned_dir:
        Root directory of cleaned JSON envelopes produced by
        ``pipeline.clean``.
    store:
        When given, only process the sub-directory matching this brand slug.
    force:
        When ``True``, overwrite existing Parquet files.  When ``False``
        (default) existing files are skipped.

    Returns
    -------
    tuple[int, int]
        A ``(written, skipped)`` tuple with the total counts across all brands.

    Side-effects
    ------------
    *   Creates ``<db_dir>/observations/…/<flyer_id>.parquet`` files.
    *   Prints a per-brand summary line to stdout, e.g.
        ``loblaws: 15 written, 3 skipped``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    total_written = 0
    total_skipped = 0

    # Determine which store directories to walk
    if store:
        store_dirs = [(store, os.path.join(cleaned_dir, store))]
    else:
        try:
            entries = os.listdir(cleaned_dir)
        except FileNotFoundError:
            entries = []
        store_dirs = [
            (entry, os.path.join(cleaned_dir, entry))
            for entry in sorted(entries)
            if os.path.isdir(os.path.join(cleaned_dir, entry))
        ]

    for store_chain, store_path in store_dirs:
        written = 0
        skipped = 0

        # Enumerate all cleaned JSON envelopes for this brand
        try:
            json_files = sorted(
                f for f in os.listdir(store_path) if f.endswith(".json")
            )
        except (FileNotFoundError, NotADirectoryError):
            json_files = []

        for fname in json_files:
            flyer_id = fname[:-5]  # strip ".json"
            envelope_path = os.path.join(store_path, fname)

            try:
                with open(envelope_path, encoding="utf-8") as fh:
                    envelope = json.load(fh)
            except Exception:
                skipped += 1
                continue

            records = envelope.get("records") or []

            # Derive partition date from the first record in the envelope
            flyer_valid_from: str | None = None
            fetched_on: str | None = None
            if records:
                first = records[0]
                flyer_valid_from = first.get("flyer_valid_from")
                fetched_on = first.get("fetched_on")

            # Fall back to fetched_on when flyer_valid_from is absent;
            # _partition_dir will use today's date if partition_date is also None.
            partition_date = flyer_valid_from or fetched_on  # may still be None

            part_dir = _partition_dir(db_dir, store_chain, partition_date)
            out_path = os.path.join(part_dir, f"{flyer_id}.parquet")

            if os.path.exists(out_path) and not force:
                skipped += 1
                continue

            if not records:
                skipped += 1
                continue

            # Serialise list fields to JSON strings for a flat Parquet schema
            rows = []
            for record in records:
                row = dict(record)
                for key, val in row.items():
                    if isinstance(val, list):
                        row[key] = json.dumps(val)
                rows.append(row)

            os.makedirs(part_dir, exist_ok=True)
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, out_path)
            written += 1

        print(f"{store_chain}: {written} written, {skipped} skipped")
        total_written += written
        total_skipped += skipped

    return total_written, total_skipped


# ── Dimension tables ──────────────────────────────────────────────────────────


def build_dimensions(db_dir: str, data_dir: str) -> None:
    """Build dimension tables for stores and flyers.

    Parameters
    ----------
    db_dir:
        Root directory for the Parquet database output.
    data_dir:
        Root directory of the raw data, containing one sub-directory per
        brand (e.g. ``data/loblaws``, ``data/adonis``).

    Side-effects
    ------------
    *   Writes ``<db_dir>/dimensions/stores.parquet`` — one row per store,
        across all brands.
    *   Writes ``<db_dir>/dimensions/flyers.parquet`` — one row per unique
        flyer/job, deduplicated across stores that share the same flyer.
    *   Both files are fully overwritten on every run.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    dim_dir = os.path.join(db_dir, "dimensions")
    os.makedirs(dim_dir, exist_ok=True)

    # Determine chain directories
    try:
        chain_dirs = sorted(
            entry
            for entry in os.listdir(data_dir)
            if os.path.isdir(os.path.join(data_dir, entry))
        )
    except FileNotFoundError:
        chain_dirs = []

    # ── stores.parquet ────────────────────────────────────────────────────────

    store_rows: list[dict] = []

    for chain in chain_dirs:
        stores_path = os.path.join(data_dir, chain, "stores.json")
        if not os.path.isfile(stores_path):
            continue
        try:
            with open(stores_path, encoding="utf-8") as fh:
                stores: dict = json.load(fh)
        except Exception:
            continue
        for store_id, store_data in stores.items():
            store_rows.append(
                {
                    "store_chain": chain,
                    "store_id": str(store_id),
                    # Metro uses "store_name"; Flipp uses "name"
                    "store_name": (
                        store_data.get("store_name")
                        if store_data.get("store_name") is not None
                        else store_data.get("name")
                    ),
                    "banner": store_data.get("banner"),
                    "province": store_data.get("province"),
                    "city": store_data.get("city"),
                    "postal_code": store_data.get("postal_code"),
                }
            )

    stores_schema = pa.schema(
        [
            ("store_chain", pa.string()),
            ("store_id", pa.string()),
            ("store_name", pa.string()),
            ("banner", pa.string()),
            ("province", pa.string()),
            ("city", pa.string()),
            ("postal_code", pa.string()),
        ]
    )
    stores_table = pa.Table.from_pylist(store_rows, schema=stores_schema)
    pq.write_table(stores_table, os.path.join(dim_dir, "stores.parquet"))

    # ── flyers.parquet ────────────────────────────────────────────────────────

    flyer_rows: list[dict] = []
    seen_flyer_ids: set[str] = set()

    for chain in chain_dirs:
        flyers_path = os.path.join(data_dir, chain, "store_flyers.json")
        if not os.path.isfile(flyers_path):
            continue
        try:
            with open(flyers_path, encoding="utf-8") as fh:
                store_flyers: dict = json.load(fh)
        except Exception:
            continue
        for store_id, flyers in store_flyers.items():
            for flyer in flyers or []:
                # Metro uses "title" (job number); Flipp uses "id"
                raw_id = flyer.get("title") or flyer.get("id")
                if raw_id is None:
                    continue
                flyer_id = str(raw_id)
                if flyer_id in seen_flyer_ids:
                    continue
                seen_flyer_ids.add(flyer_id)
                flyer_rows.append(
                    {
                        "flyer_id": flyer_id,
                        "store_chain": chain,
                        "store_id": str(store_id),
                        # Metro uses "startDate"/"endDate"; Flipp uses "valid_from"/"valid_to"
                        "valid_from": (
                            flyer.get("startDate")
                            if flyer.get("startDate") is not None
                            else flyer.get("valid_from")
                        ),
                        "valid_to": (
                            flyer.get("endDate")
                            if flyer.get("endDate") is not None
                            else flyer.get("valid_to")
                        ),
                        # Flipp uses "locale" instead of "language"
                        "language": (
                            flyer.get("language")
                            if flyer.get("language") is not None
                            else flyer.get("locale")
                        ),
                        "province": flyer.get("province"),
                    }
                )

    flyers_schema = pa.schema(
        [
            ("flyer_id", pa.string()),
            ("store_chain", pa.string()),
            ("store_id", pa.string()),
            ("valid_from", pa.string()),
            ("valid_to", pa.string()),
            ("language", pa.string()),
            ("province", pa.string()),
        ]
    )
    flyers_table = pa.Table.from_pylist(flyer_rows, schema=flyers_schema)
    pq.write_table(flyers_table, os.path.join(dim_dir, "flyers.parquet"))


# ── Scoring pipeline steps ────────────────────────────────────────────────────


def build_products(db_dir: str, observations_dir: str) -> None:
    """Phase A — resolve canonical product IDs, write products.parquet.

    Parameters
    ----------
    db_dir:
        Root directory for the Parquet database output.
    observations_dir:
        Root of the observations Parquet tree,
        e.g. ``"<db_dir>/observations"``.

    Side-effects
    ------------
    *   Writes ``<db_dir>/dimensions/products.parquet`` — always overwritten.
    """
    from pipeline.product_resolver import resolve_products

    out_path = os.path.join(db_dir, "dimensions", "products.parquet")
    obs_to_product_mapping = resolve_products(observations_dir=observations_dir, out_path=out_path)
    print(f"Products resolved. {len(obs_to_product_mapping)} observation keys mapped. Written to {out_path}")


def build_price_history(db_dir: str) -> None:
    """Phase B — compute price history features, write price_history.parquet.

    Parameters
    ----------
    db_dir:
        Root directory for the Parquet database output.

    Side-effects
    ------------
    *   Writes (or overwrites) ``<db_dir>/features/price_history.parquet``.
    """
    from pipeline.price_history import build_price_history as _build_price_history

    observations_dir = os.path.join(db_dir, "observations")
    products_path = os.path.join(db_dir, "dimensions", "products.parquet")
    out_path = os.path.join(db_dir, "features", "price_history.parquet")
    n = _build_price_history(
        observations_dir=observations_dir,
        products_path=products_path,
        out_path=out_path,
    )
    print(f"Price history built. {n} feature rows written to {out_path}")


def build_scores(db_dir: str, today: str | None = None) -> None:
    """Phase C — score active deals, write active/archived_scores.parquet.

    Parameters
    ----------
    db_dir:
        Root directory for the Parquet database output.
    today:
        Reference date as an ISO 8601 string (``"YYYY-MM-DD"``) for
        "active" deal filtering.  Defaults to today's UTC date when
        ``None``.

    Side-effects
    ------------
    *   Writes ``<db_dir>/scores/active_scores.parquet`` — overwritten each run.
    *   Appends to ``<db_dir>/scores/archived_scores.parquet``.
    """
    import datetime as _dt

    from pipeline.deal_scorer import score_deals

    observations_dir = os.path.join(db_dir, "observations")
    price_history_path = os.path.join(db_dir, "features", "price_history.parquet")
    config_path = os.path.join("config", "scoring.yaml")
    out_dir = os.path.join(db_dir, "scores")

    today_date: _dt.date | None = None
    if today is not None:
        today_date = _dt.date.fromisoformat(today)

    n = score_deals(
        observations_dir=observations_dir,
        price_history_path=price_history_path,
        config_path=config_path,
        out_dir=out_dir,
        today=today_date,
    )
    print(f"Scoring complete. {n} active deals scored. Output in {out_dir}")


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m pipeline.build_db",
        description="Ingest cleaned flyer envelopes into partitioned Parquet files.",
    )
    parser.add_argument(
        "--db-dir",
        metavar="PATH",
        default="db",
        help="Root directory for the Parquet database (default: db).",
    )
    parser.add_argument(
        "--cleaned-dir",
        metavar="PATH",
        default="cleaned",
        help="Root directory of cleaned JSON envelopes (default: cleaned).",
    )
    parser.add_argument(
        "--store",
        metavar="NAME",
        default=None,
        help="Process only this store folder (e.g. loblaws).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing Parquet files.",
    )
    parser.add_argument(
        "--data-dir",
        metavar="PATH",
        default="data",
        help="Root directory for raw store/flyer metadata (default: data).",
    )
    parser.add_argument(
        "--dimensions-only",
        action="store_true",
        help="Rebuild dimension tables only; skip observations entirely.",
    )
    parser.add_argument(
        "--score",
        action="store_true",
        help=(
            "Run product resolution, price history, and deal scoring "
            "after observations and dimensions have been built."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.dimensions_only:
            build_dimensions(db_dir=args.db_dir, data_dir=args.data_dir)
            print("Done. Dimensions rebuilt.")
        else:
            written, skipped = build_observations(
                db_dir=args.db_dir,
                cleaned_dir=args.cleaned_dir,
                store=args.store,
                force=args.force,
            )
            build_dimensions(db_dir=args.db_dir, data_dir=args.data_dir)
            print(f"Done. {written} flyers written, {skipped} skipped. Dimensions rebuilt.")
            if args.score:
                observations_dir = os.path.join(args.db_dir, "observations")
                build_products(db_dir=args.db_dir, observations_dir=observations_dir)
                build_price_history(db_dir=args.db_dir)
                build_scores(db_dir=args.db_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
