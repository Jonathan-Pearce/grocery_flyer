"""
Product name cleaner for grocery flyer records.

Transforms raw product name strings (potentially ALL CAPS, bilingual,
containing trademark symbols, or having OCR artifacts) into clean,
consistently formatted product names.

Usage::

    from parsers.name_parser import parse_name

    fields = parse_name("MAPLE LEAF BACON")
    # {"name_en": "Maple Leaf Bacon", "language": "en"}

    fields = parse_name("Oat Milk\\n\\nLait d'avoine")
    # {"name_en": "Oat Milk", "name_fr": "Lait d'avoine", "language": "bil"}
"""

from __future__ import annotations

import re

# ── Known all-caps abbreviations to preserve ─────────────────────────────────
# When converting ALL-CAPS text to title case, these tokens are restored to
# their all-uppercase form.

_CAPS_ABBREVIATIONS: frozenset[str] = frozenset(
    {"PC", "IGA", "HBA", "BBQ", "USA"}
)

# ── OCR artifact fixes ────────────────────────────────────────────────────────
# Maps raw OCR artifact substrings to their corrected replacements.  Applied
# before case normalisation so that the corrected text participates in
# title-casing when the input is ALL CAPS.

OCR_FIXES: dict[str, str] = {
    "ORCRUSHED": "Or Crushed",
}

# ── Trademark symbol pattern ──────────────────────────────────────────────────

_RE_TRADEMARK = re.compile(r"[™®©]")

# ── French function-word detector ─────────────────────────────────────────────
# Matches any of the listed words at a word boundary (case-insensitive).

_FRENCH_WORDS: frozenset[str] = frozenset(
    {"de", "du", "les", "ou", "et", "avec", "au"}
)
_RE_FRENCH = re.compile(
    r"\b(?:" + "|".join(sorted(_FRENCH_WORDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

# ── English function-word detector ───────────────────────────────────────────
# Used together with _RE_FRENCH to identify bilingual ("bil") text.

_ENGLISH_WORDS: frozenset[str] = frozenset(
    {"the", "and", "with", "for"}
)
_RE_ENGLISH = re.compile(
    r"\b(?:" + "|".join(sorted(_ENGLISH_WORDS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


# ── Private helpers ───────────────────────────────────────────────────────────


def _is_all_caps(text: str) -> bool:
    """Return ``True`` if every alphabetic character in *text* is uppercase."""
    alpha = [c for c in text if c.isalpha()]
    return bool(alpha) and all(c.isupper() for c in alpha)


def _restore_abbreviations(text: str) -> str:
    """Restore known ALL-CAPS abbreviations lowered by :meth:`str.title`."""
    words = text.split()
    out = []
    for word in words:
        # Strip non-alphabetic characters to get the bare token for lookup.
        core = re.sub(r"[^A-Za-z]", "", word)
        if core.upper() in _CAPS_ABBREVIATIONS:
            out.append(word.replace(core, core.upper()))
        else:
            out.append(word)
    return " ".join(out)


def _normalize_case(text: str, was_all_caps: bool) -> str:
    """Convert *text* to title case if *was_all_caps* is ``True``."""
    if was_all_caps:
        text = text.title()
        text = _restore_abbreviations(text)
    return text


def _strip_trademarks(text: str) -> str:
    """Remove trademark symbols (™, ®, ©) from *text*."""
    return _RE_TRADEMARK.sub("", text).strip()


def _apply_ocr_fixes(text: str) -> str:
    """Apply all known OCR artifact replacements from :data:`OCR_FIXES`."""
    for artifact, fix in OCR_FIXES.items():
        text = text.replace(artifact, fix)
    return text


def _strip_brand_prefix(name: str, brand: str | None) -> str:
    """Remove a leading brand string from *name* when present."""
    if not brand or not name:
        return name
    brand_clean = brand.strip()
    if not brand_clean:
        return name
    if name.lower().startswith(brand_clean.lower()):
        # Strip common word-separating characters that may appear between the
        # brand token and the rest of the product name: space, comma, slash,
        # and hyphen (e.g. "Danone - Yogurt" or "PC/President's Choice Milk").
        remainder = name[len(brand_clean):].lstrip(" ,/-")
        return remainder if remainder else name
    return name


def _clean_segment(text: str, brand: str | None = None) -> str:
    """Apply all cleaning transformations to a single text segment."""
    was_caps = _is_all_caps(text)
    text = _apply_ocr_fixes(text)
    text = _strip_trademarks(text)
    text = _normalize_case(text, was_caps)
    text = _strip_brand_prefix(text, brand)
    return text.strip()


def _detect_language(text: str) -> str:
    """Detect the dominant language of *text*.

    Returns
    -------
    ``"fr"``
        French function words detected, no English function words.
    ``"bil"``
        Both French and English function words detected.
    ``"en"``
        No French function words detected (default).
    """
    has_french = bool(_RE_FRENCH.search(text))
    has_english = bool(_RE_ENGLISH.search(text))
    if has_french and has_english:
        return "bil"
    if has_french:
        return "fr"
    return "en"


# ── Public API ────────────────────────────────────────────────────────────────


def parse_name(
    name: str | None,
    brand: str | None = None,
) -> dict:
    """Parse and clean a raw product name string.

    Parameters
    ----------
    name:
        Raw product name from the source API.  May be ALL CAPS, bilingual
        (two segments separated by ``"\\n\\n"``), contain trademark symbols,
        or include OCR artifacts.
    brand:
        Optional brand name from the Flipp ``brand`` field.  When provided
        and the brand string appears at the start of *name*, it is not
        duplicated in the cleaned output.

    Returns
    -------
    dict
        Fields ready to merge into :class:`~schema.FlyerItem`.
        Always contains ``language`` (``"en"``, ``"fr"``, or ``"bil"``).
        When the input contains ``"\\n\\n"``, both ``name_en`` and
        ``name_fr`` are populated and ``language`` is ``"bil"``.
        Otherwise, ``name_en`` is populated for English/default inputs and
        ``name_fr`` for French-only inputs.
    """
    result: dict = {}

    if not isinstance(name, str) or not name.strip():
        result["language"] = "en"
        return result

    # ── Bilingual split on double newline ─────────────────────────────────────
    if "\n\n" in name:
        parts = name.split("\n\n", 1)
        segment_a = parts[0].strip()
        segment_b = parts[1].strip()

        # Detect language of each part to assign en/fr correctly.
        # Canadian convention: EN first, FR second — swap only when segment_a
        # is clearly French and segment_b is not.
        lang_a = _detect_language(segment_a)
        if lang_a == "fr":
            en_seg, fr_seg = segment_b, segment_a
        else:
            en_seg, fr_seg = segment_a, segment_b

        result["name_en"] = _clean_segment(en_seg, brand)
        result["name_fr"] = _clean_segment(fr_seg, brand)
        result["language"] = "bil"
        return result

    # ── Single segment ────────────────────────────────────────────────────────
    cleaned = _clean_segment(name, brand)
    lang = _detect_language(name)
    result["language"] = lang

    if lang == "fr":
        result["name_fr"] = cleaned
    else:
        result["name_en"] = cleaned

    return result
