"""
Weight and size extractor for grocery flyer product records.

Extracts structured weight/size information (value, unit, range, multi-pack)
from raw product name and description strings.

Usage::

    from parsers.weight_parser import parse_weight

    fields = parse_weight(raw_name="153 G")
    # {"weight_value": 153.0, "weight_unit": "g", "parse_warnings": []}

    fields = parse_weight(raw_name="6x355 mL")
    # {"pack_count": 6, "pack_unit_size": 355.0, "pack_unit": "mL", "parse_warnings": []}
"""

from __future__ import annotations

import re

# ── Unit normalisation map ────────────────────────────────────────────────────
# Maps lowercase unit strings to canonical output units.
# Accepted outputs: g, kg, mL, L, lb, oz, count

_UNIT_MAP: dict[str, str] = {
    "g": "g",
    "gr": "g",
    "gram": "g",
    "grams": "g",
    "kg": "kg",
    "kilogram": "kg",
    "kilograms": "kg",
    "ml": "mL",
    "l": "L",
    "lb": "lb",
    "lbs": "lb",
    "pound": "lb",
    "pounds": "lb",
    "oz": "oz",
    "ounce": "oz",
    "ounces": "oz",
    "count": "count",
    "ct": "count",
}

# Build unit regex alternation from the map keys, longest-first to ensure
# correct prefix matching (e.g. "kg" before "g", "lbs" before "lb").
_UNIT_KEYS = sorted(_UNIT_MAP.keys(), key=len, reverse=True)
_UNIT_PAT = r"(?:" + "|".join(re.escape(k) for k in _UNIT_KEYS) + r")"

# ── Compiled regex patterns ───────────────────────────────────────────────────

# Multi-pack with range: "24/341-355 ml"
# Pattern groups: (pack_count) / (min_size) - (max_size) (unit)
_RE_PACK_RANGE = re.compile(
    r"(\d+)\s*/\s*(\d+(?:\.\d+)?)\s*[-\u2013]\s*(\d+(?:\.\d+)?)\s*("
    + _UNIT_PAT
    + r")\b",
    re.IGNORECASE,
)

# Multi-pack × size: "6x355 mL"
_RE_PACK_SIZE = re.compile(
    r"(\d+)\s*[x\u00d7]\s*(\d+(?:\.\d+)?)\s*(" + _UNIT_PAT + r")\b",
    re.IGNORECASE,
)

# Simple range: "65 - 375 g" or "70-79 G"
_RE_RANGE = re.compile(
    r"(\d+(?:\.\d+)?)\s*[-\u2013]\s*(\d+(?:\.\d+)?)\s*(" + _UNIT_PAT + r")\b",
    re.IGNORECASE,
)

# Simple weight/volume: "153 G", "1.89 L"
_RE_SIMPLE = re.compile(
    r"(\d+(?:\.\d+)?)\s*(" + _UNIT_PAT + r")\b",
    re.IGNORECASE,
)

# Pack apostrophe-s count: "3's", "24's"
_RE_PACK_APOS = re.compile(r"(\d+)'s\b", re.IGNORECASE)

# Pack keyword: "6 pk", "6 pack", "6 packs"
_RE_PACK_PK = re.compile(r"(\d+)\s*(?:pks?|packs?)\b", re.IGNORECASE)

# Pack "un." / "unit" count: "2 un.", "2 units"
# Uses a lookahead to avoid matching numbers inside unrelated words.
_RE_PACK_UN = re.compile(
    r"(\d+)\s*(?:un\.?|units?)(?=[,\s]|$)",
    re.IGNORECASE,
)


# ── Private helpers ───────────────────────────────────────────────────────────


def _normalize_unit(raw: str) -> str | None:
    """Normalise a raw unit string to a canonical output unit."""
    return _UNIT_MAP.get(raw.strip().lower())


def _extract_weight(s: str) -> dict:
    """Extract weight/pack fields from a single string.

    Returns a dict (possibly empty) containing any combination of the
    weight/pack fields found in *s*.
    """
    result: dict = {}

    # Priority 1: multi-pack with range — "24/341-355 ml"
    m = _RE_PACK_RANGE.search(s)
    if m:
        unit = _normalize_unit(m.group(4))
        result["pack_count"] = int(m.group(1))
        result["pack_unit_size"] = float(m.group(2))
        if unit:
            result["pack_unit"] = unit
        result["weight_is_range"] = True
        return result

    # Priority 2: multi-pack × size — "6x355 mL"
    m = _RE_PACK_SIZE.search(s)
    if m:
        unit = _normalize_unit(m.group(3))
        result["pack_count"] = int(m.group(1))
        result["pack_unit_size"] = float(m.group(2))
        if unit:
            result["pack_unit"] = unit
        return result

    # Priority 3: pack "un." count — may coexist with a separate weight
    # e.g. "2 un., 700 g" → pack_count=2 plus weight below
    m = _RE_PACK_UN.search(s)
    if m:
        result["pack_count"] = int(m.group(1))

    # Priority 4: simple range — "65 - 375 g"
    m = _RE_RANGE.search(s)
    if m:
        unit = _normalize_unit(m.group(3))
        result["weight_min"] = float(m.group(1))
        result["weight_max"] = float(m.group(2))
        if unit:
            result["weight_unit"] = unit
        result["weight_is_range"] = True
        return result

    # Priority 5: simple weight — "153 G", "1.89 L"
    m = _RE_SIMPLE.search(s)
    if m:
        unit = _normalize_unit(m.group(2))
        result["weight_value"] = float(m.group(1))
        if unit:
            result["weight_unit"] = unit

    # Pack-count-only patterns (only when not already set above)
    if "pack_count" not in result:
        # Apostrophe-s: "3's"
        m = _RE_PACK_APOS.search(s)
        if m:
            result["pack_count"] = int(m.group(1))
        else:
            # Pack keyword: "6 pk"
            m = _RE_PACK_PK.search(s)
            if m:
                result["pack_count"] = int(m.group(1))

    return result


def _sanity_check(result: dict) -> None:
    """Append warnings for likely unit errors to *result['parse_warnings']*."""
    warnings: list = result.setdefault("parse_warnings", [])
    wv = result.get("weight_value")
    wu = result.get("weight_unit")

    if wu == "mL" and wv is not None and wv < 10:
        warnings.append(
            f"weight_value={wv} with unit='mL' is suspiciously small; "
            "possible unit error (should be 'L'?)"
        )

    if wu == "g" and wv is not None and wv > 5000:
        warnings.append(
            f"weight_value={wv} with unit='g' is suspiciously large; "
            "possible unit error (should be 'kg'?)"
        )


# ── Public API ────────────────────────────────────────────────────────────────


def parse_weight(
    raw_name: str | None = None,
    raw_description: str | None = None,
    raw_body: str | None = None,
) -> dict:
    """Extract weight/size information from raw product strings.

    Searches *raw_name*, *raw_description*, and *raw_body* in priority order,
    using the first source that yields any weight or pack information.

    Parameters
    ----------
    raw_name:
        Raw product name from the source API.
    raw_description:
        Raw product description from the source API.
    raw_body:
        Raw promotional body text from the source API.

    Returns
    -------
    dict
        A dict with weight/pack-related fields ready to merge into a
        :class:`~schema.FlyerItem`.  A ``parse_warnings`` key (list of str)
        is always present and contains human-readable warnings about
        suspicious values.  Keys that are not applicable are absent from
        the dict (all schema weight fields default to ``None``).
    """
    result: dict = {"parse_warnings": []}

    for source in (raw_name, raw_description, raw_body):
        if not isinstance(source, str) or not source.strip():
            continue
        extracted = _extract_weight(source)
        if extracted:
            result.update(extracted)
            break

    _sanity_check(result)
    return result
