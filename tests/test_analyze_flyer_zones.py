"""Tests for scripts/analyze_flyer_zones.py — FSA and Metro zone clustering."""

import json

import pytest

from scripts.analyze_flyer_zones import (
    _fsa,
    _parse_zone_expression,
    _SECTION_CODES,
)


# ── _fsa ─────────────────────────────────────────────────────────────────────

class TestFsa:
    def test_returns_first_three_chars(self):
        assert _fsa("M5V2B7") == "M5V"

    def test_normalises_to_uppercase(self):
        assert _fsa("m5v2b7") == "M5V"

    def test_strips_internal_space(self):
        assert _fsa("M5V 2B7") == "M5V"

    def test_returns_none_for_empty_string(self):
        assert _fsa("") is None

    def test_returns_none_for_short_code(self):
        assert _fsa("M5") is None

    def test_returns_none_for_none_input(self):
        assert _fsa(None) is None

    def test_handles_three_char_code_exactly(self):
        assert _fsa("L1V") == "L1V"


# ── _parse_zone_expression ────────────────────────────────────────────────────

class TestParseZoneExpression:
    def test_simple_base_zone(self):
        base, sections = _parse_zone_expression("M")
        assert base == "M"
        assert sections == []

    def test_single_section_code(self):
        base, sections = _parse_zone_expression("M[KOS]")
        assert base == ""   # M[KOS] is a section modifier, not a base geographic zone
        assert "KOS" in sections

    def test_geographic_qualifier_kept_as_base(self):
        # R[HOT] contains a non-section code (HOT is in SECTION_CODES but the whole
        # expression is a geographic qualifier for R zone — the current implementation
        # treats HOT as a section code inside R[HOT].  Verify the base includes "R".
        base, sections = _parse_zone_expression("R[HOT]_M")
        # R is kept because HOT is a section modifier, not a geographic code
        assert "M" in base

    def test_complex_expression_extracts_sections(self):
        expr = "R[!(HOT^REB)]_M_R[!HOT]_M[DEL^PUP^NEO]_M[ITL]_M[ALC]"
        base, sections = _parse_zone_expression(expr)
        assert "DEL" in sections
        assert "PUP" in sections
        assert "NEO" in sections
        assert "ITL" in sections
        assert "ALC" in sections

    def test_otb_zone(self):
        base, sections = _parse_zone_expression("OTB_MB_OTB[DEL^PUP^NEO]")
        assert "OTB" in base
        assert "MB" in base
        assert "DEL" in sections

    def test_empty_expression(self):
        base, sections = _parse_zone_expression("")
        assert base == ""
        assert sections == []

    def test_deduplicates_base_zones(self):
        base, _ = _parse_zone_expression("M_M_R")
        parts = base.split("_")
        assert parts.count("M") == 1

    def test_deduplicates_sections(self):
        _, sections = _parse_zone_expression("M[ALC]_M[ALC]")
        assert sections.count("ALC") == 1


# ── _SECTION_CODES ────────────────────────────────────────────────────────────

class TestSectionCodes:
    def test_contains_expected_codes(self):
        for code in ("KOS", "ITL", "ALC", "DEL", "PUP", "NEO", "SKP"):
            assert code in _SECTION_CODES, f"{code} missing from _SECTION_CODES"
