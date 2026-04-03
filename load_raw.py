"""
Raw-file loader and pipeline router.

Walks ``data/<store>/flyers/*.json`` for every store directory, detects which
API each file came from (Flipp or Metro), resolves store metadata from
``stores.json`` and ``store_flyers.json``, and yields a flat stream of unified
:class:`~schema.FlyerItem` records.

Public API
----------
``iter_records(data_dir, store=None)``
    Generator — yields :class:`~schema.FlyerItem` instances one at a time.

Detection rules
---------------
* ``"publication_id"`` present in the file → Flipp API
* ``"job"`` present in the file → Metro API
* Neither key → raises :class:`ValueError` with a descriptive message.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any

from normalize_flipp import normalize_flipp_file
from normalize_metro import normalize_metro_file
from schema import FlyerItem


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


def _iter_flyer_files(data_dir: str, store: str | None) -> Iterator[tuple[str, str]]:
    """Yield ``(store_folder_name, flyer_file_path)`` pairs.

    Parameters
    ----------
    data_dir:
        Path to the top-level ``data/`` directory.
    store:
        When not ``None``, only the matching store folder is visited.
    """
    if not os.path.isdir(data_dir):
        return

    for entry in sorted(os.listdir(data_dir)):
        if store is not None and entry != store:
            continue
        folder_path = os.path.join(data_dir, entry)
        if not os.path.isdir(folder_path):
            continue
        flyers_dir = os.path.join(folder_path, "flyers")
        if not os.path.isdir(flyers_dir):
            continue
        for fname in sorted(os.listdir(flyers_dir)):
            if fname.endswith(".json"):
                yield entry, os.path.join(flyers_dir, fname)


# ── Public API ────────────────────────────────────────────────────────────────


def iter_flyers(
    data_dir: str = "data",
    store: str | None = None,
) -> Iterator[tuple[str, str | None, str | None, list[FlyerItem]]]:
    """Yield ``(store_chain, flyer_id, fetched_on, items)`` for each raw flyer file.

    Unlike :func:`iter_records`, this generator groups all records from a
    single flyer file together so that callers can write one output file per
    flyer, check idempotency at the file level, and access the raw
    ``fetched_on`` date without iterating all records first.

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
        all normalised :class:`~schema.FlyerItem` records from the file.

    Raises
    ------
    ValueError
        If a flyer file contains neither ``"publication_id"`` nor ``"job"``.
    """
    for store_chain, flyer_path in _iter_flyer_files(data_dir, store):
        store_dir = os.path.join(data_dir, store_chain)
        stores: dict = _load_json(os.path.join(store_dir, "stores.json"))
        store_flyers: dict = _load_json(os.path.join(store_dir, "store_flyers.json"))

        with open(flyer_path, encoding="utf-8") as fh:
            flyer_data: dict = json.load(fh)

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
                f"Cannot determine API source for '{flyer_path}': "
                "file contains neither 'publication_id' (Flipp) nor 'job' (Metro). "
                "Expected one of these top-level keys to be present."
            )

        yield store_chain, flyer_id, fetched_on, items


def iter_records(
    data_dir: str = "data",
    store: str | None = None,
) -> Iterator[FlyerItem]:
    """Yield :class:`~schema.FlyerItem` records from every raw flyer file.

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
        If a flyer file contains neither ``"publication_id"`` (Flipp) nor
        ``"job"`` (Metro) — i.e. the source API cannot be determined.
    """
    for store_chain, flyer_path in _iter_flyer_files(data_dir, store):
        store_dir = os.path.join(data_dir, store_chain)
        stores: dict = _load_json(os.path.join(store_dir, "stores.json"))
        store_flyers: dict = _load_json(os.path.join(store_dir, "store_flyers.json"))

        with open(flyer_path, encoding="utf-8") as fh:
            flyer_data: dict = json.load(fh)

        # ── API source detection ──────────────────────────────────────────────
        if "publication_id" in flyer_data:
            # ── Flipp ─────────────────────────────────────────────────────────
            publication_id = str(flyer_data["publication_id"])
            store_id = _flipp_store_id(store_flyers, publication_id)
            province = _store_province(stores, store_id)

            items = normalize_flipp_file(
                flyer_data,
                store_chain=store_chain,
                store_id=store_id,
                province=province,
            )
            yield from items

        elif "job" in flyer_data:
            # ── Metro ─────────────────────────────────────────────────────────
            file_store_id = flyer_data.get("store_id")
            store_id = str(file_store_id) if file_store_id is not None else None
            province = _store_province(stores, store_id)

            items = normalize_metro_file(
                flyer_data,
                store_chain=store_chain,
                store_id=store_id,
                province=province,
            )
            yield from items

        else:
            raise ValueError(
                f"Cannot determine API source for '{flyer_path}': "
                "file contains neither 'publication_id' (Flipp) nor 'job' (Metro). "
                "Expected one of these top-level keys to be present."
            )
