"""
Flipp Enterprise API normaliser — maps raw Flipp product records to FlyerItem.

The public API is two functions:

* ``normalize_flipp_product`` — maps one raw product dict to a ``FlyerItem``
  (filtering must be applied by the caller first).
* ``normalize_flipp_file`` — full pipeline: loads a flyer dict, applies all
  filters, and returns a list of validated ``FlyerItem`` instances.

Usage::

    import json
    from normalize_flipp import normalize_flipp_file

    with open("data/loblaws/flyers/7838648.json") as f:
        flyer_data = json.load(f)

    items = normalize_flipp_file(
        flyer_data,
        store_chain="loblaws",
        store_id="1000",
        province="ON",
    )
"""

from __future__ import annotations

from typing import Any

from schema import FlyerItem


# ── Private helpers ───────────────────────────────────────────────────────────


def _parse_price(value: Any) -> float | None:
    """Coerce a raw price value to ``float``, returning ``None`` on failure."""
    if value is None:
        return None
    try:
        return float(str(value).strip().lstrip("$").replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _iso_date(value: str | None) -> str | None:
    """Return the ``YYYY-MM-DD`` prefix of an ISO 8601 datetime string."""
    if not value:
        return None
    return value[:10]


def _category_name(item_categories: dict | None, level: str) -> str | None:
    """Extract ``category_name`` from an ``item_categories`` dict at *level*."""
    if not item_categories:
        return None
    level_data = item_categories.get(level)
    if not isinstance(level_data, dict):
        return None
    return level_data.get("category_name") or None


# ── Public API ────────────────────────────────────────────────────────────────


def normalize_flipp_product(
    raw: dict,
    *,
    store_chain: str | None = None,
    store_id: str | None = None,
    flyer_id: str | None = None,
    fetched_on: str | None = None,
    province: str | None = None,
    flyer_valid_from: str | None = None,
    flyer_valid_to: str | None = None,
) -> FlyerItem:
    """Map a single raw Flipp product dict to a :class:`~schema.FlyerItem`.

    Filtering (``item_type == 5``, no ``name`` + no ``sku``) must be applied
    *before* calling this function.  See :func:`normalize_flipp_file` for the
    full pipeline.

    Parameters
    ----------
    raw:
        A single product dict from the ``products`` list in a Flipp flyer file.
    store_chain:
        Brand folder slug, e.g. ``"loblaws"``.
    store_id:
        Store code string, e.g. ``"1000"``.
    flyer_id:
        Publication ID string, e.g. ``"7838648"``.
    fetched_on:
        ISO 8601 date the file was downloaded, e.g. ``"2026-04-03"``.
    province:
        Two-letter Canadian province code, e.g. ``"ON"``.
    flyer_valid_from:
        Flyer validity start date (ISO 8601).
    flyer_valid_to:
        Flyer validity end date (ISO 8601).
    """
    item_cats = raw.get("item_categories") or {}

    # Product-level validity dates fall back to flyer-level dates
    valid_from = _iso_date(raw.get("valid_from")) or flyer_valid_from
    valid_to = _iso_date(raw.get("valid_to")) or flyer_valid_to

    return FlyerItem(
        # ── Provenance ───────────────────────────────────────────────────────
        source_api="flipp",
        store_chain=store_chain,
        store_id=str(store_id) if store_id is not None else None,
        flyer_id=str(flyer_id) if flyer_id is not None else None,
        flyer_valid_from=valid_from,
        flyer_valid_to=valid_to,
        fetched_on=fetched_on,
        province=province,
        # ── Raw originals ────────────────────────────────────────────────────
        raw_name=raw.get("name") or None,
        raw_description=raw.get("description") or None,
        pre_price_text=raw.get("pre_price_text") or None,
        post_price_text=raw.get("post_price_text") or None,
        raw_categories=raw.get("categories") or None,
        # ── Language & product identity ───────────────────────────────────────
        language="en",
        name_en=raw.get("name") or None,
        description_en=raw.get("description") or None,
        brand=raw.get("brand") or None,
        sku=raw.get("sku") or None,
        product_url=raw.get("item_web_url") or None,
        image_url=raw.get("image_url") or None,
        # ── Prices ───────────────────────────────────────────────────────────
        sale_price=_parse_price(raw.get("price_text")),
        regular_price=_parse_price(raw.get("original_price")),
        # ── Promo (raw input — parsers applied downstream) ───────────────────
        promo_details=raw.get("sale_story") or None,
        # ── Categories ───────────────────────────────────────────────────────
        category_l1=_category_name(item_cats, "l1"),
        category_l2=_category_name(item_cats, "l2"),
        category_l3=_category_name(item_cats, "l3"),
        category_l4=_category_name(item_cats, "l4"),
    )


def normalize_flipp_file(
    flyer_data: dict,
    *,
    store_chain: str | None = None,
    store_id: str | None = None,
    province: str | None = None,
) -> list[FlyerItem]:
    """Normalise all products in a loaded Flipp flyer file dict.

    Filters applied:

    * Records where ``item_type == 5`` (banner / ad placeholders) are excluded.
    * Records with no ``name`` *and* no ``sku`` are excluded.

    Parameters
    ----------
    flyer_data:
        Parsed content of a ``flyers/<id>.json`` file.
    store_chain:
        Brand folder slug, e.g. ``"loblaws"``.
    store_id:
        Store code string from ``stores.json``.
    province:
        Two-letter province code from ``stores.json``.

    Returns
    -------
    list[FlyerItem]
        Zero or more validated :class:`~schema.FlyerItem` instances.
    """
    pub_meta = flyer_data.get("publication_meta") or {}

    flyer_id = str(
        flyer_data.get("publication_id") or pub_meta.get("id") or ""
    ) or None
    fetched_on = flyer_data.get("fetched_on") or None
    flyer_valid_from = _iso_date(pub_meta.get("valid_from"))
    flyer_valid_to = _iso_date(pub_meta.get("valid_to"))

    items: list[FlyerItem] = []
    for raw in flyer_data.get("products", []):
        # Filter: banner/ad placeholders
        if raw.get("item_type") == 5:
            continue
        # Filter: no name and no sku
        if not raw.get("name") and not raw.get("sku"):
            continue

        item = normalize_flipp_product(
            raw,
            store_chain=store_chain,
            store_id=store_id,
            flyer_id=flyer_id,
            fetched_on=fetched_on,
            province=province,
            flyer_valid_from=flyer_valid_from,
            flyer_valid_to=flyer_valid_to,
        )
        items.append(item)

    return items
