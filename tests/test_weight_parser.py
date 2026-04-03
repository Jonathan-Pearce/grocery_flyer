"""Tests for parsers/weight_parser.py — parse_weight()."""

from __future__ import annotations

import glob
import json
import os

import pytest

from parsers.weight_parser import parse_weight


# ── Parametrized tests for all patterns in the issue spec ────────────────────


class TestWeightPatterns:
    """Parametrized unit tests covering every pattern in the issue table."""

    @pytest.mark.parametrize(
        "raw_name, expected",
        [
            # Simple weight
            ("153 G", {"weight_value": 153.0, "weight_unit": "g"}),
            ("1.89 L", {"weight_value": 1.89, "weight_unit": "L"}),
            # Range
            (
                "65 - 375 g",
                {
                    "weight_min": 65.0,
                    "weight_max": 375.0,
                    "weight_unit": "g",
                    "weight_is_range": True,
                },
            ),
            (
                "70-79 G",
                {
                    "weight_min": 70.0,
                    "weight_max": 79.0,
                    "weight_unit": "g",
                    "weight_is_range": True,
                },
            ),
            # Multi-pack × size
            (
                "6x355 mL",
                {"pack_count": 6, "pack_unit_size": 355.0, "pack_unit": "mL"},
            ),
            # Multi-pack / range
            (
                "24/341-355 ml",
                {
                    "pack_count": 24,
                    "pack_unit_size": 341.0,
                    "pack_unit": "mL",
                    "weight_is_range": True,
                },
            ),
            # Pack apostrophe-s
            ("3's", {"pack_count": 3}),
            ("24's", {"pack_count": 24}),
            # Pack keyword
            ("6 pk", {"pack_count": 6}),
            # Un. + weight combo
            (
                "2 un., 700 g",
                {"pack_count": 2, "weight_value": 700.0, "weight_unit": "g"},
            ),
        ],
    )
    def test_pattern(self, raw_name: str, expected: dict) -> None:
        r = parse_weight(raw_name=raw_name)
        for key, val in expected.items():
            if isinstance(val, float):
                assert r.get(key) == pytest.approx(val), f"key={key!r}"
            else:
                assert r.get(key) == val, f"key={key!r}"


# ── Sanity checks ─────────────────────────────────────────────────────────────


class TestSanityChecks:
    """Suspicious values add entries to parse_warnings."""

    def test_ml_small_value_fires_warning(self) -> None:
        """1.89 mL has weight_value < 10 with unit mL → warning."""
        r = parse_weight(raw_name="1.89 mL")
        assert r["weight_value"] == pytest.approx(1.89)
        assert r["weight_unit"] == "mL"
        assert len(r["parse_warnings"]) > 0

    def test_g_large_value_fires_warning(self) -> None:
        """6000 g has weight_value > 5000 with unit g → warning."""
        r = parse_weight(raw_name="6000 g")
        assert r["weight_value"] == pytest.approx(6000.0)
        assert r["weight_unit"] == "g"
        assert len(r["parse_warnings"]) > 0

    def test_valid_ml_no_warning(self) -> None:
        """355 mL is a normal can size → no warning."""
        r = parse_weight(raw_name="355 mL")
        assert r["parse_warnings"] == []

    def test_valid_g_no_warning(self) -> None:
        """500 g is a normal package size → no warning."""
        r = parse_weight(raw_name="500 g")
        assert r["parse_warnings"] == []

    def test_boundary_ml_exactly_10_no_warning(self) -> None:
        """10 mL is at the boundary; only < 10 fires a warning."""
        r = parse_weight(raw_name="10 mL")
        assert r["parse_warnings"] == []

    def test_boundary_g_exactly_5000_no_warning(self) -> None:
        """5000 g is at the boundary; only > 5000 fires a warning."""
        r = parse_weight(raw_name="5000 g")
        assert r["parse_warnings"] == []


# ── Source priority ───────────────────────────────────────────────────────────


class TestSourcePriority:
    """raw_name is searched first, then raw_description, then raw_body."""

    def test_raw_name_takes_priority_over_description(self) -> None:
        r = parse_weight(raw_name="500 g", raw_description="1 kg", raw_body="2 kg")
        assert r["weight_value"] == pytest.approx(500.0)
        assert r["weight_unit"] == "g"

    def test_description_used_when_name_is_none(self) -> None:
        r = parse_weight(raw_name=None, raw_description="750 mL", raw_body="1 L")
        assert r["weight_value"] == pytest.approx(750.0)
        assert r["weight_unit"] == "mL"

    def test_body_used_when_name_and_description_are_none(self) -> None:
        r = parse_weight(raw_name=None, raw_description=None, raw_body="1.5 kg")
        assert r["weight_value"] == pytest.approx(1.5)
        assert r["weight_unit"] == "kg"

    def test_description_used_when_name_is_empty_string(self) -> None:
        r = parse_weight(raw_name="", raw_description="200 g")
        assert r["weight_value"] == pytest.approx(200.0)

    def test_name_with_no_weight_falls_through_to_description(self) -> None:
        r = parse_weight(
            raw_name="Organic Chicken Breast",
            raw_description="300 g",
        )
        assert r["weight_value"] == pytest.approx(300.0)
        assert r["weight_unit"] == "g"


# ── Unit normalisation ────────────────────────────────────────────────────────


class TestUnitNormalisation:
    """Input unit strings are normalised to canonical output forms."""

    @pytest.mark.parametrize(
        "raw_name, expected_unit",
        [
            ("153 G", "g"),
            ("153 g", "g"),
            ("1 KG", "kg"),
            ("1 kg", "kg"),
            ("355 ML", "mL"),
            ("355 ml", "mL"),
            ("355 mL", "mL"),
            ("1.89 L", "L"),
            ("1.89 l", "L"),
            ("500 LB", "lb"),
            ("500 lb", "lb"),
            ("16 OZ", "oz"),
            ("16 oz", "oz"),
        ],
    )
    def test_unit_normalisation(self, raw_name: str, expected_unit: str) -> None:
        r = parse_weight(raw_name=raw_name)
        assert r.get("weight_unit") == expected_unit


# ── Edge cases and no-exception guarantee ────────────────────────────────────


class TestEdgeCases:
    """Edge inputs never raise; parse_warnings is always present."""

    def test_all_none_inputs_returns_parse_warnings(self) -> None:
        r = parse_weight(raw_name=None, raw_description=None, raw_body=None)
        assert "parse_warnings" in r

    def test_all_none_inputs_has_no_weight_fields(self) -> None:
        r = parse_weight(raw_name=None, raw_description=None, raw_body=None)
        assert r.get("weight_value") is None
        assert r.get("weight_unit") is None
        assert r.get("weight_min") is None
        assert r.get("weight_max") is None
        assert r.get("pack_count") is None

    def test_empty_strings_returns_parse_warnings(self) -> None:
        r = parse_weight(raw_name="", raw_description="", raw_body="")
        assert "parse_warnings" in r
        assert r.get("weight_value") is None

    def test_no_weight_string_returns_none_fields(self) -> None:
        r = parse_weight(raw_name="Organic Free Range Chicken Breast")
        assert r.get("weight_value") is None
        assert r.get("weight_unit") is None
        assert r.get("pack_count") is None

    @pytest.mark.parametrize(
        "raw_name",
        [
            "153 G",
            "1.89 L",
            "65 - 375 g",
            "70-79 G",
            "6x355 mL",
            "24/341-355 ml",
            "3's",
            "24's",
            "6 pk",
            "2 un., 700 g",
            "Organic Chicken Breast",
            "some unknown @@## item !!",
            None,
            "",
            "   ",
        ],
    )
    def test_no_exception_on_varied_inputs(self, raw_name: str | None) -> None:
        r = parse_weight(raw_name=raw_name)
        assert "parse_warnings" in r


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
    def test_all_weight_fields_parseable(self) -> None:
        """parse_weight must not raise on any name/description string in data/."""
        for path in ALL_FLYER_FILES:
            with open(path) as f:
                flyer_data = json.load(f)
            for product in flyer_data.get("products", []):
                r = parse_weight(
                    raw_name=product.get("name"),
                    raw_description=product.get("description"),
                )
                assert "parse_warnings" in r, (
                    f"Missing parse_warnings for product in {path}"
                )
            for page in flyer_data.get("pages", []):
                for item in page.get("items", []):
                    r = parse_weight(raw_name=item.get("name"))
                    assert "parse_warnings" in r
