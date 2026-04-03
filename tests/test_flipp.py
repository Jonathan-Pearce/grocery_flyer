"""Tests for flipp.py — HTTP helpers and portfolio config."""

import json
from unittest.mock import MagicMock, patch

from flipp import (
    LOBLAWS_PORTFOLIO,
    SOBEYS_PORTFOLIO,
    WALMART_PORTFOLIO,
    Brand,
    fetch_publication_products,
    fetch_store,
    fetch_store_publications,
    save_json,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _brand(flyer_type_filter=None):
    return Brand(
        name="Test",
        slug="testslug",
        folder="test",
        access_token="token123",
        code_range=range(1, 10),
        flyer_type_filter=flyer_type_filter,
    )


def _mock_resp(status_code=200, json_data=None, json_exc=None):
    mock = MagicMock()
    mock.status_code = status_code
    if json_exc:
        mock.json.side_effect = json_exc
    else:
        mock.json.return_value = json_data
    return mock


# ── fetch_store_publications ──────────────────────────────────────────────────

class TestFetchStorePublications:
    def test_returns_list_response_directly(self):
        pubs = [{"id": 1, "flyer_type": "flyer"}, {"id": 2, "flyer_type": "flyer"}]
        with patch("flipp.get", return_value=pubs):
            result = fetch_store_publications(_brand(), "100")
        assert result == pubs

    def test_extracts_flyers_key_from_dict(self):
        pubs = [{"id": 1}]
        with patch("flipp.get", return_value={"flyers": pubs}):
            result = fetch_store_publications(_brand(), "100")
        assert result == pubs

    def test_extracts_publications_key_from_dict(self):
        pubs = [{"id": 2}]
        with patch("flipp.get", return_value={"publications": pubs}):
            result = fetch_store_publications(_brand(), "100")
        assert result == pubs

    def test_returns_empty_list_on_none_response(self):
        with patch("flipp.get", return_value=None):
            result = fetch_store_publications(_brand(), "100")
        assert result == []

    def test_returns_empty_list_when_dict_has_no_known_key(self):
        with patch("flipp.get", return_value={"other": []}):
            result = fetch_store_publications(_brand(), "100")
        assert result == []

    def test_returns_empty_list_when_flyers_value_is_not_a_list(self):
        with patch("flipp.get", return_value={"flyers": "bad_value"}):
            result = fetch_store_publications(_brand(), "100")
        assert result == []

    def test_flyer_type_filter_keeps_matching_pubs(self):
        pubs = [
            {"id": 1, "flyer_type": "groceryflyer"},
            {"id": 2, "flyer_type": "other"},
        ]
        with patch("flipp.get", return_value=pubs):
            result = fetch_store_publications(_brand(flyer_type_filter="groceryflyer"), "100")
        assert len(result) == 1
        assert result[0]["id"] == 1

    def test_flyer_type_filter_not_applied_when_none(self):
        pubs = [
            {"id": 1, "flyer_type": "groceryflyer"},
            {"id": 2, "flyer_type": "other"},
        ]
        with patch("flipp.get", return_value=pubs):
            result = fetch_store_publications(_brand(), "100")
        assert len(result) == 2

    def test_flyer_type_filter_returns_empty_when_none_match(self):
        pubs = [{"id": 1, "flyer_type": "other"}]
        with patch("flipp.get", return_value=pubs):
            result = fetch_store_publications(_brand(flyer_type_filter="groceryflyer"), "100")
        assert result == []


# ── fetch_publication_products ────────────────────────────────────────────────

class TestFetchPublicationProducts:
    def test_returns_list_of_products(self):
        products = [{"name": "Milk"}, {"name": "Bread"}]
        with patch("flipp.get", return_value=products):
            result = fetch_publication_products("123", "token")
        assert result == products

    def test_returns_empty_list_when_response_is_none(self):
        with patch("flipp.get", return_value=None):
            result = fetch_publication_products("123", "token")
        assert result == []

    def test_returns_empty_list_when_response_is_dict(self):
        with patch("flipp.get", return_value={"products": []}):
            result = fetch_publication_products("123", "token")
        assert result == []


# ── fetch_store ───────────────────────────────────────────────────────────────

class TestFetchStore:
    def test_returns_store_data_on_success(self):
        store_data = {"name": "Loblaws Yonge", "city": "Toronto"}
        with patch("flipp.requests.get", return_value=_mock_resp(200, store_data)):
            result = fetch_store(_brand(), 1001)
        assert result == store_data

    def test_returns_none_on_non_200(self):
        with patch("flipp.requests.get", return_value=_mock_resp(404)):
            result = fetch_store(_brand(), 9999)
        assert result is None

    def test_returns_none_when_response_is_empty(self):
        with patch("flipp.requests.get", return_value=_mock_resp(200, {})):
            result = fetch_store(_brand(), 1001)
        assert result is None

    def test_returns_none_on_request_exception(self):
        import requests as req_lib
        with patch("flipp.requests.get", side_effect=req_lib.RequestException("timeout")):
            result = fetch_store(_brand(), 1001)
        assert result is None

    def test_returns_none_on_json_decode_error(self):
        with patch("flipp.requests.get", return_value=_mock_resp(200, json_exc=ValueError("bad json"))):
            result = fetch_store(_brand(), 1001)
        assert result is None


# ── save_json ─────────────────────────────────────────────────────────────────

class TestSaveJson:
    def test_writes_json_file_with_correct_content(self, tmp_path):
        path = str(tmp_path / "out.json")
        save_json(path, {"key": "value"}, log_fn=lambda _: None)
        with open(path) as f:
            assert json.load(f) == {"key": "value"}

    def test_creates_nested_parent_directories(self, tmp_path):
        path = str(tmp_path / "a" / "b" / "out.json")
        save_json(path, [1, 2, 3], log_fn=lambda _: None)
        with open(path) as f:
            assert json.load(f) == [1, 2, 3]

    def test_overwrites_existing_file(self, tmp_path):
        path = str(tmp_path / "out.json")
        save_json(path, {"v": 1}, log_fn=lambda _: None)
        save_json(path, {"v": 2}, log_fn=lambda _: None)
        with open(path) as f:
            assert json.load(f)["v"] == 2


# ── Portfolio config sanity checks ────────────────────────────────────────────

class TestPortfolioConfigs:
    def test_all_loblaws_brands_have_required_fields(self):
        for brand in LOBLAWS_PORTFOLIO:
            assert brand.name
            assert brand.folder
            assert brand.access_token

    def test_all_sobeys_brands_have_slug_and_required_fields(self):
        for brand in SOBEYS_PORTFOLIO:
            assert brand.name
            assert brand.folder
            assert brand.access_token
            assert brand.slug is not None

    def test_walmart_has_groceryflyer_type_filter(self):
        assert len(WALMART_PORTFOLIO) == 1
        assert WALMART_PORTFOLIO[0].flyer_type_filter == "groceryflyer"

    def test_no_duplicate_folders_across_all_portfolios(self):
        all_brands = LOBLAWS_PORTFOLIO + SOBEYS_PORTFOLIO + WALMART_PORTFOLIO
        folders = [b.folder for b in all_brands]
        assert len(folders) == len(set(folders)), "Duplicate folder names found in portfolios"
