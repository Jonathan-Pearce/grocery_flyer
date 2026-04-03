"""
Multi-product entry splitter for grocery flyer records.

Detects flyer items that bundle multiple distinct products into one record
and splits them into individual child records, linked back to the combined
parent.

Usage::

    from parsers.multi_product_parser import split_multi_product
    from schema import FlyerItem

    record = FlyerItem(
        price_observation_key="loblaws:1234:sku:2024-01-01",
        raw_name="CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G",
        sale_price=2.99,
    )
    results = split_multi_product(record)
    # results[0] is the parent with is_multi_product=True
    # results[1], results[2] are the individual child records
"""

from __future__ import annotations

import re

from parsers.name_parser import parse_name
from parsers.weight_parser import parse_weight
from schema import FlyerItem

# ── Separator patterns ────────────────────────────────────────────────────────
# Separators must be surrounded by whitespace so that price-unit strings such
# as "3,99/lb" (slash without spaces) and OCR artifacts such as "ORCRUSHED"
# (no spaces around the embedded "OR") are never mistaken for product
# separators.

_RE_SEPARATOR = re.compile(
    r"( OR | OU | / | AND | ET )",
    re.IGNORECASE,
)

# A product-name segment must contain at least one alphabetic word of three or
# more characters.  This filters out pure-numeric strings, single-letter
# tokens, and two-letter unit abbreviations such as "lb" or "kg".
_RE_PRODUCT_WORD = re.compile(r"[A-Za-z]{3,}")

# Weight fields defined on FlyerItem whose values are reset to schema defaults
# before re-extracting per-variant weights.
_WEIGHT_FIELD_DEFAULTS: dict[str, object] = {
    "weight_value": None,
    "weight_unit": None,
    "weight_is_range": False,
    "weight_min": None,
    "weight_max": None,
    "pack_count": None,
    "pack_unit_size": None,
    "pack_unit": None,
}


# ── Private helpers ───────────────────────────────────────────────────────────


def _is_product_name(text: str) -> bool:
    """Return ``True`` if *text* contains at least one product-name word.

    A product name must contain at least one alphabetic word of three or more
    characters, ruling out pure numbers, weight-only segments, and short unit
    abbreviations.
    """
    return bool(_RE_PRODUCT_WORD.search(text.strip()))


# ── Public API ────────────────────────────────────────────────────────────────


def detect_variants(raw_name: str) -> list[str] | None:
    """Split *raw_name* into product variant strings, or return ``None``.

    Parameters
    ----------
    raw_name:
        Raw product name string from the source flyer record.

    Returns
    -------
    list[str]
        Two or more trimmed variant name strings when *raw_name* contains a
        recognised separator (``OR``, ``OU``, ``/``, ``AND``, ``ET``) and
        every segment independently passes the product-name check.
    None
        When the input is not a multi-product entry.
    """
    parts = _RE_SEPARATOR.split(raw_name)
    # re.split() with a capturing group interleaves separators between the
    # surrounding segments: "A OR B" → ["A ", " OR ", " B"].
    # Taking every second element (indices 0, 2, 4, …) gives the candidates.
    candidates = [parts[i].strip() for i in range(0, len(parts), 2)]

    if len(candidates) < 2:
        return None

    if not all(_is_product_name(c) for c in candidates):
        return None

    return candidates


def split_multi_product(
    record: FlyerItem,
    skus: list[str] | None = None,
) -> list[FlyerItem]:
    """Split a combined multi-product flyer entry into parent and child records.

    Parameters
    ----------
    record:
        A :class:`~schema.FlyerItem` to inspect.  When ``raw_name`` contains a
        recognised product separator whose segments each look like product
        names, the record is treated as multi-product.
    skus:
        Optional list of SKU strings sourced from ``custom_id_field_1``–
        ``custom_id_field_6`` in the upstream payload.  Each SKU is assigned
        to the corresponding child record in order of appearance.

    Returns
    -------
    list[FlyerItem]
        * **Multi-product**: ``[parent, child_1, child_2, …]``.
          The returned *parent* has ``is_multi_product=True``.  Each child has
          ``is_multi_product=False``, ``parent_record_id`` equal to the
          parent's ``price_observation_key``, its own ``name_en``/``name_fr``
          parsed from the variant string, and weight fields re-extracted from
          the variant name.  All price, promo, and flyer fields are copied
          from the parent.  ``raw_name`` on each child retains the original
          combined string.
        * **Not multi-product**: ``[record]`` (the input, unchanged).
    """
    if not isinstance(record.raw_name, str) or not record.raw_name.strip():
        return [record]

    variants = detect_variants(record.raw_name)
    if variants is None:
        return [record]

    # Mark a copy of the source record as multi-product.
    parent = record.model_copy(update={"is_multi_product": True})
    results: list[FlyerItem] = [parent]

    parent_data = parent.model_dump()

    for idx, variant_name in enumerate(variants):
        # Derive name and language for this variant.
        name_fields = parse_name(variant_name)

        # Re-extract weight specifically for this variant string.
        weight_fields = parse_weight(raw_name=variant_name)
        weight_fields.pop("parse_warnings", None)

        # Start from a full copy of parent data, then apply child overrides.
        child_data: dict = {**parent_data}

        # Multi-product linkage
        child_data["is_multi_product"] = False
        child_data["parent_record_id"] = parent.price_observation_key

        # Keep the original combined raw_name for traceability.
        child_data["raw_name"] = record.raw_name

        # Reset name fields before applying variant-specific ones.
        child_data["name_en"] = None
        child_data["name_fr"] = None
        child_data["language"] = None
        child_data.update(name_fields)

        # Reset weight fields to schema defaults, then apply variant-specific
        # values so that each child reflects only its own size information.
        child_data.update(_WEIGHT_FIELD_DEFAULTS)
        child_data.update(weight_fields)

        # Assign a SKU from the caller-supplied list (custom_id_field_1-6).
        if skus and idx < len(skus):
            child_data["sku"] = skus[idx]

        results.append(FlyerItem(**child_data))

    return results
