"""
Raw-file loader and pipeline router.

Reads ``data/<store>/flyers.parquet`` for every store directory, detects which
API each flyer came from (Flipp or Metro) via the ``source_api`` column,
resolves store metadata from ``stores.json`` and ``store_flyers.json``, and
yields a flat stream of unified :class:`~schema.FlyerItem` records.

Public API
----------
``iter_records(data_dir, store=None)``
    Generator — yields :class:`~schema.FlyerItem` instances one at a time.

Detection rules
---------------
* ``source_api == "flipp"`` → Flipp API (row group reconstructed into
  ``publication_id`` / ``publication_meta`` envelope for the normaliser)
* ``source_api == "metro"`` → Metro API (row group reconstructed into
  ``job`` / ``store_id`` envelope for the normaliser)
* Unrecognised ``source_api`` → raises :class:`ValueError`.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from pipeline.normalize_flipp import normalize_flipp_file
from pipeline.normalize_metro import normalize_metro_file
from pipeline.schema import FlyerItem


# ── Envelope column names ─────────────────────────────────────────────────────

#: Columns written by the fetcher that carry per-flyer envelope metadata.
#: Everything else in a row is a raw product field.
_ALL_ENVELOPE_COLS: frozenset[str] = frozenset(
    {
        "flyer_id",
        "source_api",
        "fetched_on",
        # Flipp-specific
        "pub_valid_from",
        "pub_valid_to",
        "pub_locale",
        # Metro-specific
        "store_id",
        # Shared
        "products_url",
    }
)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _load_json(path: str) -> Any:
    """Load and return a JSON file, or return an empty dict on missing file."""
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _store_province(stores: dict, store_id: str | None) -> str | None:
    """Look up the two-letter province code for *store_id* in *stores*."""
    if not store_id:
        return None
    entry = stores.get(str(store_id))
    if not isinstance(entry, dict):
        return None
    return entry.get("province") or None


def _flipp_store_id(store_flyers: dict, publication_id: str) -> str | None:
    """Return the first store code that references *publication_id*.

    The ``store_flyers.json`` dict is keyed by store code; each value is a list
    of publication objects, each of which has an ``"id"`` field.
    """
    pub_int: int | None
    try:
        pub_int = int(publication_id)
    except (TypeError, ValueError):
        pub_int = None

    for store_code, pubs in store_flyers.items():
        if not isinstance(pubs, list):
            continue
        for pub in pubs:
            if not isinstance(pub, dict):
                continue
            pid = pub.get("id")
            if str(pid) == str(publication_id):
                return str(store_code)
            if pub_int is not None and pid == pub_int:
                return str(store_code)
    return None


def _decode_product_row(row: dict) -> dict:
    """Strip envelope columns and JSON-decode any string values that look like
    JSON arrays or objects back into native Python types."""
    product: dict = {}
    for key, val in row.items():
        if key in _ALL_ENVELOPE_COLS:
            continue
        if isinstance(val, str) and val and val[0] in ("[", "{"):
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, ValueError):
                pass
        product[key] = val
    return product


def _rows_to_flyer_data(flyer_id: str, rows: list[dict]) -> dict:
    """Reconstruct a flyer envelope dict from a group of Parquet rows.

    The returned dict has the same shape as the original per-flyer JSON file
    so that the existing :func:`normalize_flipp_file` /
    :func:`normalize_metro_file` functions can consume it without changes.

    Raises
    ------
    ValueError
        When ``source_api`` is absent or unrecognised.
    """
    first = rows[0]
    source_api = first.get("source_api")
    fetched_on = first.get("fetched_on")
    products = [_decode_product_row(r) for r in rows]

    if source_api == "flipp":
        return {
            "publication_id": flyer_id,
            "fetched_on": fetched_on,
            "publication_meta": {
                "id": int(flyer_id) if flyer_id and flyer_id.isdigit() else flyer_id,
                "valid_from": first.get("pub_valid_from"),
                "valid_to": first.get("pub_valid_to"),
                "locale": first.get("pub_locale"),
            },
            "products_url": first.get("products_url"),
            "products": products,
        }

    if source_api == "metro":
        return {
            "job": flyer_id,
            "store_id": first.get("store_id"),
            "fetched_on": fetched_on,
            "products_url": first.get("products_url"),
            "products": products,
        }

    raise ValueError(
        f"Cannot determine API source for flyer '{flyer_id}': "
        f"unrecognised source_api value {source_api!r}. "
        "Expected 'flipp' or 'metro'."
    )


def _iter_flyer_parquet(
    data_dir: str, store: str | None
) -> Iterator[tuple[str, dict]]:
    """Yield ``(store_chain, flyer_data)`` pairs from per-brand Parquet files.

    Parameters
    ----------
    data_dir:
        Path to the top-level ``data/`` directory.
    store:
        When not ``None``, only the matching store folder is visited.
    """
    import pyarrow.parquet as pq

    if not os.path.isdir(data_dir):
        return

    for entry in sorted(os.listdir(data_dir)):
        if store is not None and entry != store:
            continue
        parquet_path = os.path.join(data_dir, entry, "flyers.parquet")
        if not os.path.isfile(parquet_path):
            continue

        try:
            table = pq.read_table(parquet_path)
        except Exception:
            continue

        rows = table.to_pylist()

        # Group rows by flyer_id preserving order of first occurrence
        by_flyer: dict[str, list[dict]] = defaultdict(list)
        flyer_order: list[str] = []
        for row in rows:
            fid = str(row.get("flyer_id", ""))
            if fid not in by_flyer:
                flyer_order.append(fid)
            by_flyer[fid].append(row)

        for flyer_id in flyer_order:
            yield entry, _rows_to_flyer_data(flyer_id, by_flyer[flyer_id])


# ── Public API ────────────────────────────────────────────────────────────────


def iter_flyers(
    data_dir: str = "data",
    store: str | None = None,
) -> Iterator[tuple[str, str | None, str | None, list[FlyerItem]]]:
    """Yield ``(store_chain, flyer_id, fetched_on, items)`` for each raw flyer file.

    Unlike :func:`iter_records`, this generator groups all records that share
    a ``flyer_id`` together so that callers can check idempotency at the flyer
    level and access the raw ``fetched_on`` date without iterating all records.

    Parameters
    ----------
    data_dir:
        Root data directory (default: ``"data"``).
    store:
        When provided, only the named store sub-folder is processed.

    Yields
    ------
    tuple[str, str | None, str | None, list[FlyerItem]]
        ``(store_chain, flyer_id, fetched_on, items)`` where *items* contains
        all normalised :class:`~schema.FlyerItem` records for the flyer.

    Raises
    ------
    ValueError
        If a row group has an unrecognised ``source_api`` value.
    """
    for store_chain, flyer_data in _iter_flyer_parquet(data_dir, store):
        store_dir = os.path.join(data_dir, store_chain)
        stores: dict = _load_json(os.path.join(store_dir, "stores.json"))
        store_flyers: dict = _load_json(os.path.join(store_dir, "store_flyers.json"))

        fetched_on: str | None = flyer_data.get("fetched_on") or None

        if "publication_id" in flyer_data:
            publication_id = str(flyer_data["publication_id"])
            store_id = _flipp_store_id(store_flyers, publication_id)
            province = _store_province(stores, store_id)
            items = normalize_flipp_file(
                flyer_data,
                store_chain=store_chain,
                store_id=store_id,
                province=province,
            )
            flyer_id: str | None = publication_id

        elif "job" in flyer_data:
            file_store_id = flyer_data.get("store_id")
            store_id = str(file_store_id) if file_store_id is not None else None
            province = _store_province(stores, store_id)
            items = normalize_metro_file(
                flyer_data,
                store_chain=store_chain,
                store_id=store_id,
                province=province,
            )
            flyer_id = str(flyer_data["job"]) or None

        else:
            raise ValueError(
                f"Cannot determine API source for flyer "
                f"'{flyer_data.get('flyer_id', '?')}' in '{store_chain}': "
                "unrecognised source_api value. "
                "Expected 'flipp' or 'metro'."
            )

        yield store_chain, flyer_id, fetched_on, items


def iter_records(
    data_dir: str = "data",
    store: str | None = None,
) -> Iterator[FlyerItem]:
    """Yield :class:`~schema.FlyerItem` records from every raw flyer Parquet.

    Parameters
    ----------
    data_dir:
        Root data directory (default: ``"data"``).
    store:
        When provided, only the named store sub-folder is processed (e.g.
        ``"food_basics"``).  Useful for targeted testing.

    Raises
    ------
    ValueError
        If a row group has an unrecognised ``source_api`` value.
    """
    for _, flyer_id, fetched_on, items in iter_flyers(data_dir=data_dir, store=store):
        yield from items
