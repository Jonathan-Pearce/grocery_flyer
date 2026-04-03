"""
Observations ingestion — writes cleaned flyer envelopes to partitioned Parquet.

Usage::

    python -m pipeline.build_db [--db-dir <path>] [--cleaned-dir <path>]
                                 [--store <name>] [--force]

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
) -> None:
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

    Side-effects
    ------------
    *   Creates ``<db_dir>/observations/…/<flyer_id>.parquet`` files.
    *   Prints a per-brand summary line to stdout, e.g.
        ``loblaws: 15 written, 3 skipped``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    build_observations(
        db_dir=args.db_dir,
        cleaned_dir=args.cleaned_dir,
        store=args.store,
        force=args.force,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
