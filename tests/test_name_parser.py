"""Tests for parsers/name_parser.py — parse_name()."""

from __future__ import annotations

import glob
import json
import os

import pytest

from parsers.name_parser import OCR_FIXES, parse_name


# ── Case normalisation ────────────────────────────────────────────────────────


class TestCaseNormalisation:
    """ALL CAPS strings are converted to title case."""

    def test_all_caps_basic(self):
        r = parse_name("MAPLE LEAF BACON")
        assert r["name_en"] == "Maple Leaf Bacon"

    def test_already_title_case_unchanged(self):
        r = parse_name("Maple Leaf Bacon")
        assert r["name_en"] == "Maple Leaf Bacon"

    def test_mixed_case_unchanged(self):
        r = parse_name("PC Free Range Chicken")
        assert r["name_en"] == "PC Free Range Chicken"

    def test_single_word_all_caps(self):
        r = parse_name("BACON")
        assert r["name_en"] == "Bacon"

    def test_abbreviation_pc_preserved(self):
        r = parse_name("PC CHICKEN BREAST")
        assert r["name_en"] == "PC Chicken Breast"

    def test_abbreviation_iga_preserved(self):
        r = parse_name("IGA BRAND YOGURT")
        assert r["name_en"] == "IGA Brand Yogurt"

    def test_abbreviation_hba_preserved(self):
        r = parse_name("HBA PRODUCTS SALE")
        assert r["name_en"] == "HBA Products Sale"

    def test_abbreviation_bbq_preserved(self):
        r = parse_name("BBQ CHICKEN")
        assert r["name_en"] == "BBQ Chicken"

    def test_abbreviation_usa_preserved(self):
        r = parse_name("USA GRADE CHICKEN")
        assert r["name_en"] == "USA Grade Chicken"

    def test_no_caps_conversion_mixed(self):
        """A string that is not ALL CAPS is left as-is."""
        r = parse_name("Maple Leaf BACON")
        assert r["name_en"] == "Maple Leaf BACON"


# ── Trademark stripping ───────────────────────────────────────────────────────


class TestTrademarkStripping:
    """Trademark symbols are removed from cleaned names."""

    def test_trade_mark_symbol(self):
        r = parse_name("Tide™ Pods")
        assert "™" not in r.get("name_en", "")

    def test_registered_symbol(self):
        r = parse_name("Bounce® Dryer Sheets")
        assert "®" not in r.get("name_en", "")

    def test_copyright_symbol(self):
        r = parse_name("Brand© Item")
        assert "©" not in r.get("name_en", "")

    def test_trademark_in_all_caps(self):
        """Trademark stripped before/during all-caps normalisation."""
        r = parse_name("TIDE™ PODS")
        assert "™" not in r.get("name_en", "")
        assert r["name_en"] == "Tide Pods"

    def test_multiple_trademark_symbols(self):
        r = parse_name("Brand™ Product® Name©")
        name = r.get("name_en", "")
        assert "™" not in name
        assert "®" not in name
        assert "©" not in name


# ── OCR artifact repair ───────────────────────────────────────────────────────


class TestOCRArtifactRepair:
    """Known OCR artifacts are corrected."""

    def test_ocr_fixes_is_dict(self):
        """OCR_FIXES must be a public dict constant."""
        assert isinstance(OCR_FIXES, dict)

    def test_orcrushed_standalone(self):
        r = parse_name("ORCRUSHED")
        assert r.get("name_en") == "Or Crushed"

    def test_orcrushed_in_sentence(self):
        r = parse_name("DICED ORCRUSHED TOMATOES")
        assert r.get("name_en") == "Diced Or Crushed Tomatoes"

    def test_ocr_fix_applied_before_case_normalisation(self):
        """After OCR fix, title case still applies to surrounding ALL-CAPS text."""
        r = parse_name("WHOLE ORCRUSHED PLUM TOMATOES")
        assert r.get("name_en") == "Whole Or Crushed Plum Tomatoes"


# ── Brand separation ──────────────────────────────────────────────────────────


class TestBrandSeparation:
    """Brand prefix is not duplicated in the cleaned name."""

    def test_brand_at_start_stripped(self):
        r = parse_name("MAPLE LEAF BACON", brand="Maple Leaf")
        assert r["name_en"] == "Bacon"

    def test_brand_not_at_start_kept(self):
        r = parse_name("CHICKEN BREAST", brand="Maple Leaf")
        assert r["name_en"] == "Chicken Breast"

    def test_brand_none_no_stripping(self):
        r = parse_name("MAPLE LEAF BACON", brand=None)
        assert r["name_en"] == "Maple Leaf Bacon"

    def test_brand_case_insensitive_match(self):
        """Brand match is case-insensitive against the cleaned name."""
        r = parse_name("TIDE PODS", brand="Tide")
        assert r["name_en"] == "Pods"

    def test_brand_equals_full_name_keeps_name(self):
        """When stripping would produce an empty string, the full name is kept."""
        r = parse_name("TIDE", brand="Tide")
        assert r["name_en"] == "Tide"

    def test_brand_stripped_in_bilingual_both_parts(self):
        """Brand is stripped from both EN and FR segments of a bilingual name."""
        r = parse_name("Danone Yogurt\n\nDanone Yogourt", brand="Danone")
        assert r["name_en"] == "Yogurt"
        assert r["name_fr"] == "Yogourt"


# ── Language detection ────────────────────────────────────────────────────────


class TestLanguageDetection:
    """Language is detected from function words."""

    def test_english_default(self):
        r = parse_name("Maple Leaf Bacon")
        assert r["language"] == "en"
        assert "name_en" in r

    def test_french_with_de(self):
        r = parse_name("Jus de pomme")
        assert r["language"] == "fr"
        assert "name_fr" in r

    def test_french_with_du(self):
        r = parse_name("Pain du blé")
        assert r["language"] == "fr"

    def test_french_with_les(self):
        r = parse_name("Les légumes frais")
        assert r["language"] == "fr"

    def test_french_with_et(self):
        r = parse_name("Pommes et poires")
        assert r["language"] == "fr"

    def test_french_with_avec(self):
        r = parse_name("Poulet avec sauce")
        assert r["language"] == "fr"

    def test_french_with_au(self):
        r = parse_name("Soupe au poulet")
        assert r["language"] == "fr"

    def test_bilingual_with_both_patterns(self):
        """Both French and English function words → 'bil'."""
        r = parse_name("Apple Juice and Jus de pomme")
        assert r["language"] == "bil"

    def test_english_only_function_words(self):
        r = parse_name("Chicken with Garlic and Herbs")
        assert r["language"] == "en"

    def test_no_function_words_defaults_to_en(self):
        r = parse_name("Organic Oat Milk")
        assert r["language"] == "en"


# ── Bilingual field splitting ─────────────────────────────────────────────────


class TestBilingualSplit:
    """Double-newline fields are split into name_en and name_fr."""

    def test_basic_split(self):
        r = parse_name("Apple Juice\n\nJus de pomme")
        assert r["name_en"] == "Apple Juice"
        assert r["name_fr"] == "Jus de pomme"
        assert r["language"] == "bil"

    def test_language_is_bil_on_split(self):
        r = parse_name("Whole Milk\n\nLait entier")
        assert r["language"] == "bil"

    def test_fr_first_en_second_swapped(self):
        """When French is detected first, EN and FR are assigned correctly."""
        r = parse_name("Jus de pomme\n\nApple Juice")
        assert r["name_en"] == "Apple Juice"
        assert r["name_fr"] == "Jus de pomme"

    def test_all_caps_bilingual(self):
        """Both segments receive case normalisation."""
        r = parse_name("APPLE JUICE\n\nJUS DE POMME")
        assert r["name_en"] == "Apple Juice"
        assert r["name_fr"] == "Jus De Pomme"

    def test_split_strips_whitespace(self):
        r = parse_name("  Apple Juice  \n\n  Jus de pomme  ")
        assert r["name_en"] == "Apple Juice"
        assert r["name_fr"] == "Jus de pomme"

    def test_split_only_first_double_newline(self):
        """Only the first \\n\\n is used as the split point; remaining text stays in name_fr."""
        r = parse_name("Apple Juice\n\nJus de pomme\n\nExtra")
        assert r["name_en"] == "Apple Juice"
        assert r["name_fr"] == "Jus de pomme\n\nExtra"


# ── Edge cases and no-exception guarantee ────────────────────────────────────


class TestEdgeCases:
    """Edge inputs never raise and always return a language key."""

    def test_none_returns_language_en(self):
        r = parse_name(None)
        assert r["language"] == "en"

    def test_empty_string_returns_language_en(self):
        r = parse_name("")
        assert r["language"] == "en"

    def test_whitespace_only_returns_language_en(self):
        r = parse_name("   ")
        assert r["language"] == "en"

    def test_language_always_present(self):
        for text in [None, "", "  ", "BACON", "Jus de pomme", "A\n\nB"]:
            r = parse_name(text)
            assert "language" in r

    @pytest.mark.parametrize(
        "text",
        [
            "MAPLE LEAF BACON",
            "Maple Leaf Bacon",
            "Tide™ Pods",
            "TIDE™ PODS",
            "ORCRUSHED TOMATOES",
            "Jus de pomme",
            "Apple Juice\n\nJus de pomme",
            "",
            None,
            "   ",
            "IGA BRAND ITEMS",
            "PC® CHICKEN",
            "some unknown @@## item !!",
        ],
    )
    def test_no_exception_on_varied_inputs(self, text):
        """parse_name must not raise on any input."""
        r = parse_name(text)
        assert "language" in r


# ── Integration: no exceptions on real data files ─────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ALL_FLYER_FILES = glob.glob(
    os.path.join(DATA_DIR, "**", "flyers", "*.json"), recursive=True
)


@pytest.mark.skipif(
    not ALL_FLYER_FILES,
    reason="No flyer files found under data/",
)
class TestNoExceptionsOnRealData:
    def test_all_name_fields_parseable(self):
        """parse_name must not raise on any name string in data/."""
        for path in ALL_FLYER_FILES:
            with open(path) as f:
                flyer_data = json.load(f)
            for product in flyer_data.get("products", []):
                r = parse_name(
                    name=product.get("name"),
                    brand=product.get("brand"),
                )
                assert "language" in r, (
                    f"Missing language for product in {path}"
                )
            for page in flyer_data.get("pages", []):
                for item in page.get("items", []):
                    r = parse_name(name=item.get("name"))
                    assert "language" in r
