"""
Metro Digital Azure API normaliser — maps raw Metro product records to FlyerItem.

The public API is two functions:

* ``normalize_metro_product`` — maps one raw product dict to a ``FlyerItem``
  (filtering must be applied by the caller first).
* ``normalize_metro_file`` — full pipeline: loads a flyer dict, applies all
  filters, and returns a list of validated ``FlyerItem`` instances.

Usage::

    import json
    from pipeline.normalize_metro import normalize_metro_file

    with open("data/food_basics/flyers/82596.json") as f:
        flyer_data = json.load(f)

    items = normalize_metro_file(
        flyer_data,
        store_chain="food_basics",
        store_id="320",
        province="ON",
    )
"""

from __future__ import annotations

from typing import Any

from pipeline.schema import FlyerItem


# ── Private helpers ───────────────────────────────────────────────────────────


def _parse_price(value: Any) -> float | None:
    """Coerce a raw Metro price value to ``float``, returning ``None`` on failure.

    Handles French comma-decimal notation (e.g. ``"14,99"`` → ``14.99``) as
    well as leading ``$`` signs and plain numeric strings.
    """
    if value is None:
        return None
    try:
        # Normalise French comma decimal separator before any other processing
        cleaned = str(value).strip().lstrip("$").replace(",", ".")
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def _iso_date(value: str | None) -> str | None:
    """Return the ``YYYY-MM-DD`` prefix of an ISO 8601 datetime string."""
    if not value:
        return None
    return value[:10]


def _map_category(main_en: str | None, main_fr: str | None) -> str | None:
    """Return the best-available top-level category string.

    Prefers the English value; falls back to French when English is absent.
    """
    return (main_en or None) or (main_fr or None)


# ── Public API ────────────────────────────────────────────────────────────────


def normalize_metro_product(
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
    """Map a single raw Metro product dict to a :class:`~schema.FlyerItem`.

    Filtering (``actionType`` in ``{"Inblock", "URL"}``, ``sku == "Inblock"``)
    must be applied *before* calling this function.  See
    :func:`normalize_metro_file` for the full pipeline.

    Parameters
    ----------
    raw:
        A single product dict from the ``products`` list in a Metro flyer file.
    store_chain:
        Brand folder slug, e.g. ``"food_basics"``.
    store_id:
        Store identifier string, e.g. ``"320"``.
    flyer_id:
        Metro job number string, e.g. ``"82596"``.
    fetched_on:
        ISO 8601 date the file was downloaded, e.g. ``"2026-04-03"``.
    province:
        Two-letter Canadian province code, e.g. ``"ON"``.
    flyer_valid_from:
        Flyer validity start date (ISO 8601).
    flyer_valid_to:
        Flyer validity end date (ISO 8601).
    """
    product_en = raw.get("productEn") or None
    product_fr = raw.get("productFr") or None
    body_en = raw.get("bodyEn") or None
    body_fr = raw.get("bodyFr") or None

    # ── Language detection ────────────────────────────────────────────────────
    if product_en and product_fr:
        language: str | None = "bil"
    elif product_fr and not product_en:
        language = "fr"
    else:
        language = "en"

    # ── Name resolution ───────────────────────────────────────────────────────
    name_en = product_en or body_en or None
    name_fr = product_fr or body_fr or None

    # ── Validity dates — product-level take precedence over flyer-level ───────
    valid_from = _iso_date(raw.get("validFrom")) or flyer_valid_from
    valid_to = _iso_date(raw.get("validTo")) or flyer_valid_to

    # ── Category ──────────────────────────────────────────────────────────────
    category_l1 = _map_category(raw.get("mainCategoryEn"), raw.get("mainCategoryFr"))

    # ── Promo details ─────────────────────────────────────────────────────────
    promo_raw = raw.get("waysToSave_EN") or raw.get("savingsEn") or None

    return FlyerItem(
        # ── Provenance ───────────────────────────────────────────────────────
        source_api="metro",
        store_chain=store_chain,
        store_id=str(store_id) if store_id is not None else None,
        flyer_id=str(flyer_id) if flyer_id is not None else None,
        flyer_valid_from=valid_from,
        flyer_valid_to=valid_to,
        fetched_on=fetched_on,
        province=province,
        # ── Raw originals ────────────────────────────────────────────────────
        raw_name=product_en or product_fr or None,
        raw_description=body_en or body_fr or None,
        raw_body=raw.get("contents") or None,
        # ── Language & product identity ───────────────────────────────────────
        language=language,
        name_en=name_en,
        name_fr=name_fr,
        description_en=body_en,
        description_fr=body_fr,
        sku=raw.get("sku") or None,
        image_url=raw.get("productImage") or None,
        # ── Prices ───────────────────────────────────────────────────────────
        sale_price=_parse_price(raw.get("salePrice")),
        regular_price=_parse_price(raw.get("regularPrice")),
        alternate_price=_parse_price(raw.get("alternatePrice")),
        member_price=_parse_price(raw.get("memberPriceEn")),
        # ── Promo unit / price unit ───────────────────────────────────────────
        price_unit=raw.get("promoUnitEn") or None,
        # ── Tax ──────────────────────────────────────────────────────────────
        tax_indicator=raw.get("tx") or None,
        # ── Promo ────────────────────────────────────────────────────────────
        promo_details=promo_raw,
        # ── Categories ───────────────────────────────────────────────────────
        category_l1=category_l1,
    )


def normalize_metro_file(
    flyer_data: dict,
    *,
    store_chain: str | None = None,
    store_id: str | None = None,
    province: str | None = None,
) -> list[FlyerItem]:
    """Normalise all products in a loaded Metro flyer file dict.

    Filters applied:

    * Records where ``actionType`` is ``"Inblock"`` or ``"URL"`` are excluded.
    * Records where ``sku == "Inblock"`` are excluded.

    Parameters
    ----------
    flyer_data:
        Parsed content of a ``flyers/<job>.json`` file produced by the Metro
        Azure fetcher.
    store_chain:
        Brand folder slug, e.g. ``"food_basics"``.
    store_id:
        Store identifier string from ``stores.json``.
    province:
        Two-letter province code from ``stores.json``.

    Returns
    -------
    list[FlyerItem]
        Zero or more validated :class:`~schema.FlyerItem` instances.
    """
    flyer_id = str(flyer_data.get("job") or "") or None
    fetched_on = flyer_data.get("fetched_on") or None
    # Metro files don't carry flyer-level date ranges at the file level;
    # product-level validFrom/validTo are used instead.
    flyer_valid_from: str | None = None
    flyer_valid_to: str | None = None

    # Use store_id from the file if not supplied by caller
    file_store_id = flyer_data.get("store_id")
    resolved_store_id = store_id if store_id is not None else (
        str(file_store_id) if file_store_id is not None else None
    )

    items: list[FlyerItem] = []
    for raw in flyer_data.get("products", []):
        # Filter: Inblock / URL action types
        action_type = raw.get("actionType")
        if action_type in ("Inblock", "URL"):
            continue
        # Filter: sku == "Inblock"
        if raw.get("sku") == "Inblock":
            continue

        item = normalize_metro_product(
            raw,
            store_chain=store_chain,
            store_id=resolved_store_id,
            flyer_id=flyer_id,
            fetched_on=fetched_on,
            province=province,
            flyer_valid_from=flyer_valid_from,
            flyer_valid_to=flyer_valid_to,
        )
        items.append(item)

    return items
