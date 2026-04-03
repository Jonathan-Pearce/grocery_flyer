"""
Promo/deal classifier for grocery flyer records.

Parses and classifies promotional offer strings (Flipp ``sale_story``, Metro
``waysToSave_EN``, ``savingsEn``, ``savingsFr``) into structured promo fields
on the unified schema.

Usage::

    from parsers.promo_parser import parse_promo

    fields = parse_promo("SAVE 25%")
    # {"promo_type": "percentage_off", "promo_details": "SAVE 25%"}

    fields = parse_promo("100 Scene+ PTS when you buy 2")
    # {
    #     "promo_type": "loyalty_points",
    #     "loyalty_program": "Scene+",
    #     "loyalty_points": 100,
    #     "loyalty_trigger": "when you buy 2",
    #     "promo_details": "100 Scene+ PTS when you buy 2",
    # }
"""

from __future__ import annotations

import re

# ── Day names (English and French) ────────────────────────────────────────────

_DAY_NAMES: frozenset[str] = frozenset(
    {
        # English
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        # French
        "lundi",
        "mardi",
        "mercredi",
        "jeudi",
        "vendredi",
        "samedi",
        "dimanche",
    }
)

_RE_DAY = re.compile(
    r"\b(?:" + "|".join(sorted(_DAY_NAMES, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

# ── Loyalty: Scene+ ───────────────────────────────────────────────────────────
# Matches "100 Scene+ PTS when you buy 2" or "Scene+ 100 pts"

_RE_SCENE_POINTS = re.compile(
    r"(\d[\d,]*)\s*Scene\+\s*(?:PTS?|points?)"
    r"|Scene\+\s*(\d[\d,]*)\s*(?:PTS?|points?)",
    re.IGNORECASE,
)

# ── Loyalty: PC Optimum ───────────────────────────────────────────────────────
# Matches "PC Optimum 6,000 pts" or "6,000 PC Optimum pts"

_RE_PC_POINTS = re.compile(
    r"PC\s*Optimum\s*(\d[\d,]*)\s*(?:PTS?|points?)"
    r"|(\d[\d,]*)\s*PC\s*Optimum\s*(?:PTS?|points?)",
    re.IGNORECASE,
)

# Trigger clause: "when you buy 2", "when you spend $10", etc.
_RE_TRIGGER = re.compile(
    r"(when\s+you\s+(?:buy|spend)[^,\.;]*"
    r"|with\s+purchase[^,\.;]*"
    r"|on\s+purchase[^,\.;]*)",
    re.IGNORECASE,
)

# ── BOGO ──────────────────────────────────────────────────────────────────────
# "BOGO", "buy 1 get 1", "buy one get one free"

_RE_BOGO = re.compile(
    r"\bBOGO\b|buy\s+(?:one|\d+)\s+get\s+(?:one|\d+)(?:\s+free)?",
    re.IGNORECASE,
)

# ── Multi-buy ─────────────────────────────────────────────────────────────────
# "2 for $5", "buy 2 save $3"

_RE_MULTI_BUY = re.compile(
    r"\d+\s+(?:for|pour)\s+\$|buy\s+\d+\s+save",
    re.IGNORECASE,
)

# ── Percentage off ────────────────────────────────────────────────────────────
# "SAVE 25%", "15% off", "Économisez 42%"

_RE_PCT_OFF = re.compile(r"\d+\s*%", re.IGNORECASE)

# ── Dollar off ────────────────────────────────────────────────────────────────
# "SAVE $1.80", "SAVE .99", "SAVE UP TO $5"

_RE_DOLLAR_OFF = re.compile(
    r"\bsave\b(?:\s+up\s+to)?\s+(?:\$\d|\.\d)",
    re.IGNORECASE,
)

# ── Rollback ──────────────────────────────────────────────────────────────────

_RE_ROLLBACK = re.compile(r"\brollback\b", re.IGNORECASE)

# ── Clearance ─────────────────────────────────────────────────────────────────

_RE_CLEARANCE = re.compile(r"\b(?:clearance|liquidation)\b", re.IGNORECASE)


# ── Private helpers ───────────────────────────────────────────────────────────


def _parse_points(text: str) -> int | None:
    """Parse a points value string, handling comma thousands-separators."""
    cleaned = text.replace(",", "")
    try:
        return int(cleaned)
    except ValueError:
        return None


# ── Public API ────────────────────────────────────────────────────────────────


def parse_promo(
    promo_text: str | None,
    *,
    member_price: float | None = None,
    prefix_en: str | None = None,
    prefix_fr: str | None = None,
) -> dict:
    """Parse and classify a promotional offer string.

    Parameters
    ----------
    promo_text:
        Raw promotional string from the source API (e.g. Flipp ``sale_story``,
        Metro ``waysToSave_EN`` / ``savingsEn`` / ``savingsFr``).
    member_price:
        When populated from Metro ``memberPriceEn``, the presence of a value
        signals ``"member_price"`` type if no stronger match is found.
    prefix_en:
        English sale-price prefix field (e.g. ``salePricePrefixEn``).  Any
        day names detected here are stored in ``promo_details``.
    prefix_fr:
        French sale-price prefix field (e.g. ``salePricePrefixFr``).  Any
        day names detected here are stored in ``promo_details``.

    Returns
    -------
    dict
        A dict with promo-related fields ready to merge into a
        :class:`~schema.FlyerItem`.  Always contains ``promo_type`` and
        ``promo_details``.  Loyalty keys (``loyalty_program``,
        ``loyalty_points``, ``loyalty_trigger``) are only included when the
        promo type is ``"loyalty_points"``.

    Classification priority (first match wins):

    1. ``loyalty_points`` — Scene+ or PC Optimum point-earning offer
    2. ``bogo`` — buy-one-get-one or BOGO keyword
    3. ``multi_buy`` — "N for $X" or "buy N save $X"
    4. ``percentage_off`` — contains ``N%``
    5. ``dollar_off`` — "SAVE $X" or "SAVE .XX"
    6. ``rollback`` — "Rollback" keyword
    7. ``clearance`` — "clearance" or "liquidation" keyword
    8. ``member_price`` — *member_price* parameter is not ``None``
    9. ``no_promo`` — fallback; raw string preserved in ``promo_details``
    """
    result: dict = {}

    # ── Day-restriction detection ─────────────────────────────────────────────
    day_details: str | None = None
    for prefix in (prefix_en, prefix_fr):
        if prefix and _RE_DAY.search(prefix):
            day_details = prefix.strip()
            break

    # ── Normalise input ───────────────────────────────────────────────────────
    text = (promo_text or "").strip()
    raw_details = day_details if day_details else (text if text else None)

    # ── Loyalty: Scene+ ───────────────────────────────────────────────────────
    m = _RE_SCENE_POINTS.search(text)
    if m:
        raw_pts = m.group(1) or m.group(2)
        pts = _parse_points(raw_pts) if raw_pts else None
        tm = _RE_TRIGGER.search(text)
        result["promo_type"] = "loyalty_points"
        result["loyalty_program"] = "Scene+"
        if pts is not None:
            result["loyalty_points"] = pts
        if tm:
            result["loyalty_trigger"] = tm.group(1).strip()
        result["promo_details"] = raw_details
        return result

    # ── Loyalty: PC Optimum ───────────────────────────────────────────────────
    m = _RE_PC_POINTS.search(text)
    if m:
        raw_pts = m.group(1) or m.group(2)
        pts = _parse_points(raw_pts) if raw_pts else None
        tm = _RE_TRIGGER.search(text)
        result["promo_type"] = "loyalty_points"
        result["loyalty_program"] = "PC Optimum"
        if pts is not None:
            result["loyalty_points"] = pts
        if tm:
            result["loyalty_trigger"] = tm.group(1).strip()
        result["promo_details"] = raw_details
        return result

    # ── BOGO ──────────────────────────────────────────────────────────────────
    if _RE_BOGO.search(text):
        result["promo_type"] = "bogo"
        result["promo_details"] = raw_details
        return result

    # ── Multi-buy ─────────────────────────────────────────────────────────────
    if _RE_MULTI_BUY.search(text):
        result["promo_type"] = "multi_buy"
        result["promo_details"] = raw_details
        return result

    # ── Percentage off ────────────────────────────────────────────────────────
    if _RE_PCT_OFF.search(text):
        result["promo_type"] = "percentage_off"
        result["promo_details"] = raw_details
        return result

    # ── Dollar off ────────────────────────────────────────────────────────────
    if _RE_DOLLAR_OFF.search(text):
        result["promo_type"] = "dollar_off"
        result["promo_details"] = raw_details
        return result

    # ── Rollback ──────────────────────────────────────────────────────────────
    if _RE_ROLLBACK.search(text):
        result["promo_type"] = "rollback"
        result["promo_details"] = raw_details
        return result

    # ── Clearance ─────────────────────────────────────────────────────────────
    if _RE_CLEARANCE.search(text):
        result["promo_type"] = "clearance"
        result["promo_details"] = raw_details
        return result

    # ── Member price ──────────────────────────────────────────────────────────
    if member_price is not None:
        result["promo_type"] = "member_price"
        result["promo_details"] = raw_details
        return result

    # ── No promo (fallback) ───────────────────────────────────────────────────
    result["promo_type"] = "no_promo"
    result["promo_details"] = raw_details
    return result
