"""Tests for scripts/enrich_postal_codes.py — postal code recovery helpers."""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.enrich_postal_codes import (
    _clean_store_name,
    _geocode_postal_code,
    _CANADIAN_PC_RE,
)


# ── _clean_store_name ─────────────────────────────────────────────────────────

class TestCleanStoreName:
    def test_strips_leading_hash_number(self):
        assert _clean_store_name("#052 North York (Bathurst)") == "North York (Bathurst)"

    def test_strips_hash_with_no_space(self):
        assert _clean_store_name("#767Metro Henderson") == "Metro Henderson"

    def test_leaves_name_without_hash_unchanged(self):
        assert _clean_store_name("Loblaws Yonge") == "Loblaws Yonge"

    def test_strips_multi_digit_number(self):
        assert _clean_store_name("#1234 Some Store") == "Some Store"

    def test_handles_empty_string(self):
        assert _clean_store_name("") == ""


# ── _CANADIAN_PC_RE ───────────────────────────────────────────────────────────

class TestCanadianPostalCodeRegex:
    def test_matches_standard_format_no_space(self):
        assert _CANADIAN_PC_RE.search("M5V2B7") is not None

    def test_matches_standard_format_with_space(self):
        assert _CANADIAN_PC_RE.search("M5V 2B7") is not None

    def test_does_not_match_incomplete_code(self):
        assert _CANADIAN_PC_RE.search("M5V") is None

    def test_extracts_postcode_from_longer_string(self):
        m = _CANADIAN_PC_RE.search("postcode: K1A 0A6")
        assert m is not None
        assert m.group().replace(" ", "").upper() == "K1A0A6"


# ── _geocode_postal_code ──────────────────────────────────────────────────────

def _nominatim_resp(postcode: str | None):
    mock = MagicMock()
    mock.status_code = 200
    if postcode:
        mock.json.return_value = [{"address": {"postcode": postcode}}]
    else:
        mock.json.return_value = []
    return mock


class TestGeocodePostalCode:
    def test_returns_normalised_postal_code_on_success(self):
        with patch("scripts.enrich_postal_codes.requests.get",
                   return_value=_nominatim_resp("M5V 2B7")):
            result = _geocode_postal_code("Metro Yonge", "ON")
        assert result == "M5V2B7"

    def test_returns_none_when_no_results(self):
        with patch("scripts.enrich_postal_codes.requests.get",
                   return_value=_nominatim_resp(None)):
            result = _geocode_postal_code("Unknown Store", "ON")
        assert result is None

    def test_returns_none_on_request_exception(self):
        import requests as req_lib
        with patch("scripts.enrich_postal_codes.requests.get",
                   side_effect=req_lib.RequestException("timeout")):
            result = _geocode_postal_code("Metro Yonge", "ON")
        assert result is None

    def test_returns_none_on_non_200_status(self):
        mock = MagicMock()
        mock.status_code = 500
        with patch("scripts.enrich_postal_codes.requests.get", return_value=mock):
            result = _geocode_postal_code("Metro Yonge", "ON")
        assert result is None

    def test_handles_province_none(self):
        with patch("scripts.enrich_postal_codes.requests.get",
                   return_value=_nominatim_resp("L1V 1V9")):
            result = _geocode_postal_code("Some Store", None)
        assert result == "L1V1V9"

    def test_normalises_postcode_to_uppercase_no_space(self):
        with patch("scripts.enrich_postal_codes.requests.get",
                   return_value=_nominatim_resp("k1a 0a6")):
            result = _geocode_postal_code("Parliament", "ON")
        assert result == "K1A0A6"
