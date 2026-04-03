"""Tests for azure.py — Metro Digital API helpers and portfolio config."""

from unittest.mock import MagicMock, patch

from fetchers.azure import (
    METRO_PORTFOLIO,
    MetroBrand,
    metro_fetch_products,
    metro_fetch_store,
    metro_fetch_store_flyers,
    metro_headers,
    metro_load_credentials,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _brand(banner_id="banner123", api_key="key123", locale="en", expected_banner=None):
    return MetroBrand(
        name="Test Metro",
        folder="test",
        app_config_url="https://example.com/config/app.json",
        banner_id=banner_id,
        api_key=api_key,
        id_range=range(1, 10),
        expected_banner=expected_banner,
        locale=locale,
    )


def _mock_resp(status_code=200, json_data=None, json_exc=None):
    mock = MagicMock()
    mock.status_code = status_code
    if json_exc:
        mock.json.side_effect = json_exc
    else:
        mock.json.return_value = json_data
    return mock


# ── metro_load_credentials ────────────────────────────────────────────────────

class TestMetroLoadCredentials:
    def test_returns_true_when_credentials_already_set(self):
        brand = _brand()
        assert metro_load_credentials(brand) is True

    def test_fetches_credentials_from_app_json_when_missing(self):
        brand = _brand(banner_id=None, api_key=None)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"banner_id": "fetched_banner", "apikey": "fetched_key"}
        with patch("fetchers.azure.requests.get", return_value=mock_resp):
            result = metro_load_credentials(brand)
        assert result is True
        assert brand.banner_id == "fetched_banner"
        assert brand.api_key == "fetched_key"

    def test_returns_false_when_fetch_raises_exception(self):
        import requests as req_lib
        brand = _brand(banner_id=None, api_key=None)
        with patch("fetchers.azure.requests.get", side_effect=req_lib.RequestException("timeout")):
            result = metro_load_credentials(brand)
        assert result is False

    def test_returns_false_when_app_json_missing_keys(self):
        brand = _brand(banner_id=None, api_key=None)
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        with patch("fetchers.azure.requests.get", return_value=mock_resp):
            result = metro_load_credentials(brand)
        assert result is False


# ── metro_headers ─────────────────────────────────────────────────────────────

class TestMetroHeaders:
    def test_returns_subscription_key_header(self):
        headers = metro_headers(_brand(api_key="mykey"))
        assert headers["Ocp-Apim-Subscription-Key"] == "mykey"

    def test_returns_banner_header(self):
        headers = metro_headers(_brand(banner_id="mybanner"))
        assert headers["Banner"] == "mybanner"


# ── metro_fetch_store ─────────────────────────────────────────────────────────

class TestMetroFetchStore:
    def test_returns_store_info_on_success(self):
        payload = {
            "banner": "Metro Ontario",
            "flyers": [{"storeName": "Metro Yonge", "title": "82846"}],
        }
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, payload)):
            result = metro_fetch_store(_brand(), 85, "2026-04-03")
        assert result == {"store_name": "Metro Yonge", "banner": "Metro Ontario"}

    def test_returns_none_on_non_200(self):
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(404)):
            result = metro_fetch_store(_brand(), 9999, "2026-04-03")
        assert result is None

    def test_returns_none_when_flyers_list_is_empty(self):
        payload = {"banner": "Metro Ontario", "flyers": []}
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, payload)):
            result = metro_fetch_store(_brand(), 1, "2026-04-03")
        assert result is None

    def test_returns_none_when_expected_banner_mismatches(self):
        payload = {
            "banner": "Food Basics",
            "flyers": [{"storeName": "Food Basics #5", "title": "99"}],
        }
        brand = _brand(expected_banner="Metro Ontario")
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, payload)):
            result = metro_fetch_store(brand, 1, "2026-04-03")
        assert result is None

    def test_returns_store_when_banner_matches_expected(self):
        payload = {
            "banner": "Metro Ontario",
            "flyers": [{"storeName": "Metro Bathurst", "title": "82846"}],
        }
        brand = _brand(expected_banner="Metro Ontario")
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, payload)):
            result = metro_fetch_store(brand, 85, "2026-04-03")
        assert result is not None
        assert result["store_name"] == "Metro Bathurst"

    def test_returns_none_on_request_exception(self):
        import requests as req_lib
        with patch("fetchers.azure.requests.get", side_effect=req_lib.RequestException("timeout")):
            result = metro_fetch_store(_brand(), 1, "2026-04-03")
        assert result is None

    def test_returns_none_on_json_decode_error(self):
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, json_exc=ValueError("bad"))):
            result = metro_fetch_store(_brand(), 1, "2026-04-03")
        assert result is None


# ── metro_fetch_store_flyers ──────────────────────────────────────────────────

class TestMetroFetchStoreFlyers:
    def test_returns_flyer_list_on_success(self):
        flyers = [{"title": "82846"}, {"title": "82847"}]
        payload = {"flyers": flyers}
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, payload)):
            result = metro_fetch_store_flyers(_brand(), 85, "2026-04-03")
        assert result == flyers

    def test_returns_empty_list_on_non_200(self):
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(404)):
            result = metro_fetch_store_flyers(_brand(), 1, "2026-04-03")
        assert result == []

    def test_returns_empty_list_on_request_exception(self):
        import requests as req_lib
        with patch("fetchers.azure.requests.get", side_effect=req_lib.RequestException("err")):
            result = metro_fetch_store_flyers(_brand(), 1, "2026-04-03")
        assert result == []

    def test_returns_empty_list_when_flyers_key_missing(self):
        with patch("fetchers.azure.requests.get", return_value=_mock_resp(200, {})):
            result = metro_fetch_store_flyers(_brand(), 1, "2026-04-03")
        assert result == []


# ── metro_fetch_products ──────────────────────────────────────────────────────

class TestMetroFetchProducts:
    def test_returns_flat_product_list_from_blocks(self):
        blocks = [
            {"products": [{"name": "Milk"}, {"name": "Eggs"}]},
            {"products": [{"name": "Bread"}]},
        ]
        with patch("fetchers.azure.requests.post", return_value=_mock_resp(200, blocks)):
            result = metro_fetch_products(_brand(), "82846", 85)
        assert result == [{"name": "Milk"}, {"name": "Eggs"}, {"name": "Bread"}]

    def test_returns_empty_list_when_blocks_have_no_products(self):
        blocks = [{"products": []}, {"products": []}]
        with patch("fetchers.azure.requests.post", return_value=_mock_resp(200, blocks)):
            result = metro_fetch_products(_brand(), "82846", 85)
        assert result == []

    def test_returns_empty_list_on_non_200(self):
        with patch("fetchers.azure.requests.post", return_value=_mock_resp(404)):
            result = metro_fetch_products(_brand(), "82846", 85)
        assert result == []

    def test_returns_empty_list_on_request_exception(self):
        import requests as req_lib
        with patch("fetchers.azure.requests.post", side_effect=req_lib.RequestException("err")):
            result = metro_fetch_products(_brand(), "82846", 85)
        assert result == []

    def test_uses_brand_locale_by_default(self):
        with patch("fetchers.azure.requests.post", return_value=_mock_resp(200, [])) as mock_post:
            metro_fetch_products(_brand(locale="fr"), "82846", 85)
        assert "/fr/" in mock_post.call_args[0][0]

    def test_locale_override_takes_precedence_over_brand_locale(self):
        with patch("fetchers.azure.requests.post", return_value=_mock_resp(200, [])) as mock_post:
            metro_fetch_products(_brand(locale="en"), "82846", 85, locale="fr")
        assert "/fr/" in mock_post.call_args[0][0]


# ── Portfolio config sanity checks ────────────────────────────────────────────

class TestMetroPortfolioConfigs:
    def test_all_brands_have_required_fields(self):
        for brand in METRO_PORTFOLIO:
            assert brand.name
            assert brand.folder
            assert brand.app_config_url
            assert brand.locale in ("en", "fr")

    def test_no_duplicate_folders(self):
        folders = [b.folder for b in METRO_PORTFOLIO]
        assert len(folders) == len(set(folders)), "Duplicate folder names found in METRO_PORTFOLIO"

    def test_super_c_and_adonis_use_french_locale(self):
        french_brands = {b.folder: b for b in METRO_PORTFOLIO}
        assert french_brands["super_c"].locale == "fr"
        assert french_brands["adonis"].locale == "fr"
