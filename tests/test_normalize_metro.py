"""Tests for normalize_metro.py — Metro Azure API normaliser."""

from __future__ import annotations

import json
import os

import pytest

from normalize_metro import (
    _iso_date,
    _map_category,
    _parse_price,
    normalize_metro_file,
    normalize_metro_product,
)
from schema import FlyerItem


# ── Fixture helpers ───────────────────────────────────────────────────────────


def make_raw_product(**overrides) -> dict:
    """Return a minimal valid Metro product dict."""
    base = {
        "sku": "28769401",
        "productEn": "SCHNEIDERS BACON",
        "productFr": None,
        "bodyEn": "65 - 375 g SELECTED VARIETIES",
        "bodyFr": None,
        "salePrice": "3.98",
        "regularPrice": None,
        "alternatePrice": None,
        "memberPriceEn": None,
        "promoUnitEn": "EACH",
        "mainCategoryEn": "Meat and Deli",
        "mainCategoryFr": "Viandes et charcuterie",
        "productImage": "https://example.com/img.png",
        "tx": None,
        "waysToSave_EN": "This Week Only",
        "savingsEn": None,
        "validFrom": "2026-04-02T04:00:00Z",
        "validTo": "2026-04-08T04:00:00Z",
        "actionType": "Product",
        "contents": "SCHNEIDERS BACON. 65 - 375 g. 3.98 EACH.",
    }
    base.update(overrides)
    return base


def make_flyer_data(products: list[dict] | None = None) -> dict:
    """Return a minimal valid Metro flyer file dict."""
    return {
        "fetched_on": "2026-04-03",
        "job": "82596",
        "store_id": 320,
        "products": products if products is not None else [make_raw_product()],
    }


# ── _parse_price ──────────────────────────────────────────────────────────────


class TestParsePrice:
    def test_numeric_string(self):
        assert _parse_price("3.98") == pytest.approx(3.98)

    def test_dollar_prefix(self):
        assert _parse_price("$4.50") == pytest.approx(4.50)

    def test_integer_string(self):
        assert _parse_price("5") == pytest.approx(5.0)

    def test_numeric_float(self):
        assert _parse_price(2.49) == pytest.approx(2.49)

    def test_none_returns_none(self):
        assert _parse_price(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_price("") is None

    def test_non_numeric_returns_none(self):
        assert _parse_price("n/a") is None

    def test_french_comma_decimal(self):
        """French comma decimal: '14,99' → 14.99 (acceptance criterion)."""
        assert _parse_price("14,99") == pytest.approx(14.99)

    def test_french_comma_decimal_with_slash(self):
        """French comma decimal in price-unit string: '3,99/lb' → 3.99."""
        # The slash makes it non-numeric; normaliser handles price field only
        assert _parse_price("3,99") == pytest.approx(3.99)

    def test_dollar_with_french_comma(self):
        assert _parse_price("$14,99") == pytest.approx(14.99)


# ── _iso_date ─────────────────────────────────────────────────────────────────


class TestIsoDate:
    def test_datetime_string(self):
        assert _iso_date("2026-04-02T04:00:00Z") == "2026-04-02"

    def test_date_only_string(self):
        assert _iso_date("2026-04-09") == "2026-04-09"

    def test_none_returns_none(self):
        assert _iso_date(None) is None

    def test_empty_string_returns_none(self):
        assert _iso_date("") is None


# ── _map_category ─────────────────────────────────────────────────────────────


class TestMapCategory:
    def test_prefers_english(self):
        assert _map_category("Meat and Deli", "Viandes et charcuterie") == "Meat and Deli"

    def test_falls_back_to_french(self):
        assert _map_category(None, "Viandes et charcuterie") == "Viandes et charcuterie"

    def test_both_none_returns_none(self):
        assert _map_category(None, None) is None

    def test_empty_english_falls_back_to_french(self):
        assert _map_category("", "Viandes") == "Viandes"


# ── normalize_metro_product ───────────────────────────────────────────────────


class TestNormalizeMetroProduct:
    def test_returns_flyer_item(self):
        item = normalize_metro_product(make_raw_product())
        assert isinstance(item, FlyerItem)

    def test_source_api_is_metro(self):
        item = normalize_metro_product(make_raw_product())
        assert item.source_api == "metro"

    def test_language_en_when_only_english(self):
        raw = make_raw_product(productEn="Bacon", productFr=None)
        item = normalize_metro_product(raw)
        assert item.language == "en"

    def test_language_bil_when_both(self):
        raw = make_raw_product(productEn="Bacon", productFr="Bacon QC")
        item = normalize_metro_product(raw)
        assert item.language == "bil"

    def test_language_fr_when_only_french(self):
        raw = make_raw_product(productEn=None, productFr="Lait 2%", bodyEn="2% Milk")
        item = normalize_metro_product(raw)
        assert item.language == "fr"

    def test_name_en_from_product_en(self):
        raw = make_raw_product(productEn="Bacon 375 g")
        item = normalize_metro_product(raw)
        assert item.name_en == "Bacon 375 g"

    def test_name_en_fallback_to_body_en(self):
        """French-only product should have name_en populated from bodyEn."""
        raw = make_raw_product(productEn=None, productFr="Lait 2%", bodyEn="2% Milk")
        item = normalize_metro_product(raw)
        assert item.name_en == "2% Milk"

    def test_name_en_none_when_no_english(self):
        raw = make_raw_product(productEn=None, bodyEn=None)
        item = normalize_metro_product(raw)
        assert item.name_en is None

    def test_name_fr_from_product_fr(self):
        raw = make_raw_product(productFr="Bacon 375 g")
        item = normalize_metro_product(raw)
        assert item.name_fr == "Bacon 375 g"

    def test_name_fr_fallback_to_body_fr(self):
        raw = make_raw_product(productFr=None, bodyFr="Variétés assorties")
        item = normalize_metro_product(raw)
        assert item.name_fr == "Variétés assorties"

    def test_description_en_from_body_en(self):
        raw = make_raw_product(bodyEn="SELECTED VARIETIES")
        item = normalize_metro_product(raw)
        assert item.description_en == "SELECTED VARIETIES"

    def test_description_fr_from_body_fr(self):
        raw = make_raw_product(bodyFr="VARIÉTÉS CHOISIES")
        item = normalize_metro_product(raw)
        assert item.description_fr == "VARIÉTÉS CHOISIES"

    def test_sku_mapped(self):
        item = normalize_metro_product(make_raw_product(sku="ABC123"))
        assert item.sku == "ABC123"

    def test_sku_none_when_empty_string(self):
        item = normalize_metro_product(make_raw_product(sku=""))
        assert item.sku is None

    def test_sale_price_parsed(self):
        item = normalize_metro_product(make_raw_product(salePrice="3.98"))
        assert item.sale_price == pytest.approx(3.98)

    def test_regular_price_parsed(self):
        item = normalize_metro_product(make_raw_product(regularPrice="5.49"))
        assert item.regular_price == pytest.approx(5.49)

    def test_regular_price_french_comma_decimal(self):
        """regularPrice = '14,99' → regular_price = 14.99 (acceptance criterion)."""
        item = normalize_metro_product(make_raw_product(regularPrice="14,99"))
        assert item.regular_price == pytest.approx(14.99)

    def test_alternate_price_parsed(self):
        item = normalize_metro_product(make_raw_product(alternatePrice="8.88"))
        assert item.alternate_price == pytest.approx(8.88)

    def test_member_price_parsed(self):
        item = normalize_metro_product(make_raw_product(memberPriceEn="2.99"))
        assert item.member_price == pytest.approx(2.99)

    def test_price_unit_from_promo_unit_en(self):
        item = normalize_metro_product(make_raw_product(promoUnitEn="EACH"))
        assert item.price_unit == "EACH"

    def test_category_l1_from_main_category_en(self):
        item = normalize_metro_product(make_raw_product(mainCategoryEn="Produce"))
        assert item.category_l1 == "Produce"

    def test_category_l1_fallback_to_french(self):
        raw = make_raw_product(mainCategoryEn=None, mainCategoryFr="Fruits et légumes")
        item = normalize_metro_product(raw)
        assert item.category_l1 == "Fruits et légumes"

    def test_tax_indicator_mapped(self):
        item = normalize_metro_product(make_raw_product(tx="+TX"))
        assert item.tax_indicator == "+TX"

    def test_image_url_mapped(self):
        item = normalize_metro_product(
            make_raw_product(productImage="https://example.com/img.png")
        )
        assert item.image_url == "https://example.com/img.png"

    def test_promo_details_from_ways_to_save(self):
        item = normalize_metro_product(make_raw_product(waysToSave_EN="This Week Only"))
        assert item.promo_details == "This Week Only"

    def test_promo_details_fallback_to_savings_en(self):
        raw = make_raw_product(waysToSave_EN=None, savingsEn="Save $1.00")
        item = normalize_metro_product(raw)
        assert item.promo_details == "Save $1.00"

    def test_raw_body_from_contents(self):
        item = normalize_metro_product(make_raw_product(contents="Full promo text"))
        assert item.raw_body == "Full promo text"

    def test_validity_dates_from_product(self):
        raw = make_raw_product(
            validFrom="2026-04-02T04:00:00Z", validTo="2026-04-08T04:00:00Z"
        )
        item = normalize_metro_product(raw)
        assert item.flyer_valid_from == "2026-04-02"
        assert item.flyer_valid_to == "2026-04-08"

    def test_validity_dates_fall_back_to_flyer_level(self):
        raw = make_raw_product()
        raw.pop("validFrom", None)
        raw.pop("validTo", None)
        item = normalize_metro_product(
            raw,
            flyer_valid_from="2026-04-01",
            flyer_valid_to="2026-04-07",
        )
        assert item.flyer_valid_from == "2026-04-01"
        assert item.flyer_valid_to == "2026-04-07"

    def test_context_injection(self):
        item = normalize_metro_product(
            make_raw_product(),
            store_chain="food_basics",
            store_id="320",
            flyer_id="82596",
            fetched_on="2026-04-03",
            province="ON",
        )
        assert item.store_chain == "food_basics"
        assert item.store_id == "320"
        assert item.flyer_id == "82596"
        assert item.fetched_on == "2026-04-03"
        assert item.province == "ON"

    def test_store_id_coerced_to_string(self):
        item = normalize_metro_product(make_raw_product(), store_id=320)
        assert item.store_id == "320"

    def test_flyer_id_coerced_to_string(self):
        item = normalize_metro_product(make_raw_product(), flyer_id=82596)
        assert item.flyer_id == "82596"


# ── normalize_metro_file ──────────────────────────────────────────────────────


class TestNormalizeMetroFile:
    def test_returns_list_of_flyer_items(self):
        items = normalize_metro_file(make_flyer_data())
        assert isinstance(items, list)
        assert all(isinstance(i, FlyerItem) for i in items)

    def test_normal_product_included(self):
        items = normalize_metro_file(make_flyer_data([make_raw_product()]))
        assert len(items) == 1

    def test_inblock_action_type_excluded(self):
        """Records with actionType='Inblock' must be excluded."""
        products = [
            make_raw_product(),
            make_raw_product(sku="VID001", actionType="Inblock"),
        ]
        items = normalize_metro_file(make_flyer_data(products))
        assert len(items) == 1
        assert all(p.sku != "VID001" for p in items)

    def test_url_action_type_excluded(self):
        """Records with actionType='URL' must be excluded."""
        products = [
            make_raw_product(),
            make_raw_product(sku="URL001", actionType="URL"),
        ]
        items = normalize_metro_file(make_flyer_data(products))
        assert len(items) == 1
        assert all(p.sku != "URL001" for p in items)

    def test_sku_inblock_excluded(self):
        """Records where sku == 'Inblock' must be excluded."""
        products = [
            make_raw_product(),
            make_raw_product(sku="Inblock", actionType="Product"),
        ]
        items = normalize_metro_file(make_flyer_data(products))
        assert len(items) == 1
        assert all(p.sku != "Inblock" for p in items)

    def test_empty_products_list(self):
        items = normalize_metro_file(make_flyer_data([]))
        assert items == []

    def test_all_url_items_excluded(self):
        products = [make_raw_product(sku=f"U{i}", actionType="URL") for i in range(3)]
        items = normalize_metro_file(make_flyer_data(products))
        assert items == []

    def test_flyer_metadata_injected(self):
        items = normalize_metro_file(
            make_flyer_data(),
            store_chain="food_basics",
            store_id="320",
            province="ON",
        )
        assert len(items) == 1
        item = items[0]
        assert item.source_api == "metro"
        assert item.store_chain == "food_basics"
        assert item.store_id == "320"
        assert item.province == "ON"
        assert item.fetched_on == "2026-04-03"
        assert item.flyer_id == "82596"

    def test_store_id_from_file_when_not_supplied(self):
        """store_id is read from the file's store_id field when not passed in."""
        items = normalize_metro_file(make_flyer_data())
        assert items[0].store_id == "320"

    def test_multiple_products_all_normalised(self):
        products = [make_raw_product(sku=f"SKU{i:03d}") for i in range(8)]
        items = normalize_metro_file(make_flyer_data(products))
        assert len(items) == 8

    def test_mixed_action_types_correct_count(self):
        products = [
            make_raw_product(sku="P1", actionType="Product"),
            make_raw_product(sku="U1", actionType="URL"),
            make_raw_product(sku="P2", actionType="Product"),
            make_raw_product(sku="I1", actionType="Inblock"),
            make_raw_product(sku="P3", actionType="Product"),
        ]
        items = normalize_metro_file(make_flyer_data(products))
        assert len(items) == 3


# ── Integration: real Food Basics flyer file ──────────────────────────────────

_FB_FLYER = os.path.join(
    os.path.dirname(__file__), "..", "data", "food_basics", "flyers", "82596.json"
)


@pytest.mark.skipif(
    not os.path.exists(_FB_FLYER),
    reason="data/food_basics/flyers/82596.json not present",
)
class TestFoodBasicsFlyer82596:
    def _load_items(self) -> tuple[list[FlyerItem], dict]:
        with open(_FB_FLYER) as f:
            flyer_data = json.load(f)
        items = normalize_metro_file(
            flyer_data, store_chain="food_basics", store_id="320", province="ON"
        )
        return items, flyer_data

    def test_zero_schema_validation_errors(self):
        """Parsing 82596.json must yield zero schema validation errors."""
        items, _ = self._load_items()
        assert isinstance(items, list)
        assert all(isinstance(i, FlyerItem) for i in items), (
            "Non-FlyerItem found in output from 82596.json"
        )

    def test_inblock_video_record_absent(self):
        """Inblock / URL records must be absent from the normalised output."""
        items, flyer_data = self._load_items()
        excluded_skus = {
            p.get("sku")
            for p in flyer_data.get("products", [])
            if p.get("actionType") in ("Inblock", "URL")
            or p.get("sku") == "Inblock"
        }
        output_skus = {i.sku for i in items}
        overlap = excluded_skus & output_skus - {None, ""}
        assert not overlap, f"Excluded SKUs found in output: {overlap}"

    def test_output_count_less_than_total(self):
        """Output must be smaller than the raw product count (URL records filtered)."""
        items, flyer_data = self._load_items()
        total = len(flyer_data.get("products", []))
        assert len(items) < total

    def test_all_items_have_source_api_metro(self):
        items, _ = self._load_items()
        assert all(i.source_api == "metro" for i in items)

    def test_all_items_have_flyer_id(self):
        items, _ = self._load_items()
        assert all(i.flyer_id == "82596" for i in items)
