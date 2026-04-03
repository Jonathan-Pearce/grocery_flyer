"""
Price string parser for grocery flyer records.

Parses all raw price string formats found across the Flipp FlyerKit API and
Metro Digital Azure API into clean numeric fields on the unified schema.

Usage::

    from parsers.price_parser import parse_price

    fields = parse_price("3.98")
    # {"sale_price": 3.98, "parse_warnings": []}

    fields = parse_price("8.00", pre_text="2/")
    # {"sale_price": 4.0, "multi_buy_qty": 2, "multi_buy_total": 8.0, ...}
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# Number of pounds per kilogram (multiply a kg price by this to get lb price,
# or multiply a lb price by this to get kg price)
_LB_TO_KG = 2.20462

# Tolerance for lb/kg cross-validation (2 %)
_LB_KG_TOLERANCE = 0.02

# Patterns for multi-buy prefix, e.g. "2/" or "3 for "
_RE_MULTI_BUY_PRE = re.compile(r"^\s*(\d+)\s*(?:/|for)\s*$", re.IGNORECASE)

# "Starting at" / floor price prefixes
_FLOOR_PREFIXES = frozenset(
    {"starting at", "from", "à partir de", "a partir de", "dès", "des"}
)

# Dual weight price: "3,99/lb - 8,80/kg" or "3.99/lb – 8.80/kg"
_RE_DUAL_WEIGHT = re.compile(
    r"^\s*([\d,\.]+)\s*/\s*(lb|lbs|pound|pounds)\s*[-–—]\s*([\d,\.]+)\s*/\s*(kg|kilogram|kilograms)\s*$",
    re.IGNORECASE,
)

# Single-unit price with a trailing unit: "3.99/lb", "4.99 / kg"
_RE_UNIT_PRICE = re.compile(
    r"^\s*\$?\s*([\d,\.]+)\s*/\s*(lb|lbs|kg|each|ea|100g|100\s*g)\s*$",
    re.IGNORECASE,
)

# Disclaimer limit: "LIMIT 4 OVER LIMIT PAY 10.49 EA"
_RE_LIMIT = re.compile(
    r"LIMIT\s+(\d+).*?PAY\s+([\d,\.]+)",
    re.IGNORECASE,
)

# Unit keywords that may appear in post_text
_UNIT_KEYWORDS: dict[str, str] = {
    "lb": "lb",
    "lbs": "lb",
    "pound": "lb",
    "pounds": "lb",
    "kg": "kg",
    "kilogram": "kg",
    "kilograms": "kg",
    "each": "each",
    "ea": "each",
    "100g": "100g",
}


def _to_float(text: str) -> float | None:
    """Convert a price string fragment to float, normalising commas.

    Handles both European-style decimal commas (``"14,99"`` → ``14.99``) and
    thousands-separator commas (``"1,299.99"`` → ``1299.99``).
    """
    if not text:
        return None
    text = text.strip().lstrip("$").strip()
    if not text:
        return None
    # Determine whether comma is a thousands separator or decimal marker.
    # If the string contains both a comma and a period, treat comma as thousands.
    # If the string contains only a comma, treat it as decimal marker.
    if "," in text and "." not in text:
        # Could be European decimal (e.g. "14,99") or thousands ("1,299")
        parts = text.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Looks like decimal comma: "14,99" → "14.99"
            text = f"{parts[0]}.{parts[1]}"
        else:
            # Thousands-separator comma: "1,299" → "1299"
            text = text.replace(",", "")
    else:
        # Remove thousands-separator commas, keep decimal point
        text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_unit(unit_str: str | None) -> str | None:
    """Normalise a unit string to a canonical short form."""
    if not unit_str:
        return None
    key = unit_str.strip().lower()
    return _UNIT_KEYWORDS.get(key)


def parse_price(
    price_text: str | None,
    pre_text: str | None = None,
    post_text: str | None = None,
    original_price: str | None = None,
    disclaimer_text: str | None = None,
) -> dict:
    """Parse raw price string inputs into clean numeric fields.

    Parameters
    ----------
    price_text:
        The main price string from the API (e.g. ``"3.98"``, ``"8.00"``,
        ``"3,99/lb - 8,80/kg"``).
    pre_text:
        Text preceding the price (e.g. ``"2/"`` or ``"starting at"``).
    post_text:
        Text following the price (e.g. ``"lb"`` or ``"each"``).
    original_price:
        Regular / original price string (e.g. ``"5.99"``).
    disclaimer_text:
        Full disclaimer string that may contain purchase limits
        (e.g. ``"LIMIT 4 OVER LIMIT PAY 10.49 EA"``).

    Returns
    -------
    dict
        A dict with price-related fields ready to merge into a
        :class:`~schema.FlyerItem`.  Keys that are not applicable are
        absent from the dict.  A ``parse_warnings`` key (list of str) is
        always present and contains human-readable warnings about
        unrecognised or inconsistent inputs.
    """
    result: dict = {"parse_warnings": []}

    # ── Cents sign ────────────────────────────────────────────────────────────
    # When the price sign is "¢" the numeric value must be divided by 100.
    is_cents = False
    if isinstance(price_text, str) and "¢" in price_text:
        is_cents = True
        price_text = price_text.replace("¢", "").strip()

    # ── Dual weight price: "3,99/lb - 8,80/kg" ───────────────────────────────
    if isinstance(price_text, str) and price_text.strip():
        m = _RE_DUAL_WEIGHT.match(price_text)
        if m:
            price_per_lb = _to_float(m.group(1))
            price_per_kg = _to_float(m.group(3))
            if price_per_lb is not None and price_per_kg is not None:
                result["regular_price"] = price_per_lb
                result["price_unit"] = "lb"
                result["price_per_lb"] = price_per_lb
                result["price_per_kg"] = price_per_kg
                # Cross-validate lb/kg within 2 % tolerance
                expected_kg = price_per_lb * _LB_TO_KG
                if expected_kg > 0 and abs(price_per_kg - expected_kg) / expected_kg > _LB_KG_TOLERANCE:
                    msg = (
                        f"lb/kg mismatch: {price_per_lb}/lb × 2.20462 = "
                        f"{expected_kg:.3f}/kg but got {price_per_kg}/kg"
                    )
                    result["parse_warnings"].append(msg)
                    logger.warning(msg)
            else:
                result["parse_warnings"].append(
                    f"Could not parse dual weight price: {price_text!r}"
                )
            return result

        # ── Single-unit price with trailing unit: "3.99/lb" ──────────────────
        m2 = _RE_UNIT_PRICE.match(price_text)
        if m2:
            val = _to_float(m2.group(1))
            unit = _parse_unit(m2.group(2))
            if val is not None:
                result["sale_price"] = val / 100 if is_cents else val
                if unit:
                    result["price_unit"] = unit
                if unit == "lb":
                    result["price_per_lb"] = result["sale_price"]
                    result["price_per_kg"] = round(
                        result["sale_price"] * _LB_TO_KG, 4
                    )
                elif unit == "kg":
                    result["price_per_kg"] = result["sale_price"]
                    result["price_per_lb"] = round(
                        result["sale_price"] / _LB_TO_KG, 4
                    )
                return result

    # ── Pre-text: multi-buy (e.g. "2/" or "3 for") ───────────────────────────
    if pre_text and pre_text.strip():
        m = _RE_MULTI_BUY_PRE.match(pre_text.strip())
        if m:
            qty = int(m.group(1))
            total = _to_float(price_text) if price_text else None
            if total is not None:
                sale_price = round(total / qty, 4)
                result["multi_buy_qty"] = qty
                result["multi_buy_total"] = total
                result["sale_price"] = sale_price / 100 if is_cents else sale_price
            else:
                result["parse_warnings"].append(
                    f"Could not parse multi-buy price: pre={pre_text!r}, "
                    f"price={price_text!r}"
                )
                result["sale_price"] = None
            # Fall through to post_text / disclaimer processing below
            _apply_post_text(result, post_text)
            _apply_disclaimer(result, disclaimer_text)
            return result

        # ── Pre-text: floor price (e.g. "starting at") ───────────────────────
        if pre_text.strip().lower() in _FLOOR_PREFIXES:
            result["price_is_floor"] = True
        else:
            # Unrecognised pre_text — note it in warnings but continue parsing
            result["parse_warnings"].append(
                f"Unrecognised pre_price_text: {pre_text!r}"
            )

    # ── Main price parsing ────────────────────────────────────────────────────
    if not isinstance(price_text, str) or not price_text.strip():
        result["sale_price"] = None
    else:
        val = _to_float(price_text)
        if val is None:
            result["parse_warnings"].append(
                f"Could not parse price_text: {price_text!r}"
            )
            result["sale_price"] = None
        else:
            result["sale_price"] = val / 100 if is_cents else val

    # ── Post-text unit (e.g. "lb") ────────────────────────────────────────────
    _apply_post_text(result, post_text)

    # ── Regular price ─────────────────────────────────────────────────────────
    if original_price is not None and str(original_price).strip():
        reg = _to_float(str(original_price))
        if reg is not None:
            result["regular_price"] = reg

    # ── Sale > regular price warning ──────────────────────────────────────────
    sale = result.get("sale_price")
    reg = result.get("regular_price")
    if sale is not None and reg is not None and sale > reg:
        msg = (
            f"sale_price ({sale}) is greater than regular_price ({reg})"
        )
        result["parse_warnings"].append(msg)
        logger.warning(msg)

    # ── Disclaimer ────────────────────────────────────────────────────────────
    _apply_disclaimer(result, disclaimer_text)

    return result


# ── Private helpers ───────────────────────────────────────────────────────────


def _apply_post_text(result: dict, post_text: str | None) -> None:
    """Populate ``price_unit`` from *post_text* if not already set."""
    if not post_text or not post_text.strip():
        return
    unit = _parse_unit(post_text.strip())
    if unit and "price_unit" not in result:
        result["price_unit"] = unit


def _apply_disclaimer(result: dict, disclaimer_text: str | None) -> None:
    """Parse purchase-limit information from *disclaimer_text*."""
    if not disclaimer_text or not disclaimer_text.strip():
        return
    m = _RE_LIMIT.search(disclaimer_text)
    if m:
        result["purchase_limit"] = int(m.group(1))
        over_limit = _to_float(m.group(2))
        if over_limit is not None:
            result["over_limit_price"] = over_limit
