"""Tests for normalize_flipp.py — Flipp API normaliser."""

from __future__ import annotations

import glob
import json
import os

import pytest

from pipeline.normalize_flipp import (
    _category_name,
    _iso_date,
    _parse_price,
    normalize_flipp_file,
    normalize_flipp_product,
)
from pipeline.schema import FlyerItem


# ── Fixture helpers ───────────────────────────────────────────────────────────

def make_raw_product(**overrides) -> dict:
    """Return a minimal valid Flipp product dict."""
    base = {
        "id": 1001,
        "name": "Test Product 500 g",
        "sku": "SKU001",
        "description": "SELECTED VARIETIES",
        "brand": "TestBrand",
        "price_text": "3.99",
        "original_price": None,
        "pre_price_text": "",
        "post_price_text": "each",
        "sale_story": None,
        "disclaimer_text": None,
        "categories": ["Dairy"],
        "item_categories": {
            "l1": {"category_name": "Food, Beverages & Tobacco", "google_category_id": 319},
            "l2": {"category_name": "Food Items", "google_category_id": 342},
            "l3": {"category_name": "Dairy Products", "google_category_id": 422},
            "l4": None,
        },
        "item_type": 1,
        "item_web_url": "https://www.loblaws.ca/p/SKU001",
        "image_url": "https://f.wishabi.net/images/1001.jpg",
        "valid_from": "2026-04-03",
        "valid_to": "2026-04-09",
    }
    base.update(overrides)
    return base


def make_flyer_data(products: list[dict] | None = None) -> dict:
    """Return a minimal valid Flipp flyer file dict."""
    return {
        "fetched_on": "2026-04-03",
        "publication_id": "7838648",
        "publication_meta": {
            "id": 7838648,
            "valid_from": "2026-04-03T00:00:00-04:00",
            "valid_to": "2026-04-09T23:59:59-04:00",
        },
        "products": products if products is not None else [make_raw_product()],
    }


# ── _parse_price ──────────────────────────────────────────────────────────────

class TestParsePrice:
    def test_numeric_string(self):
        assert _parse_price("3.99") == pytest.approx(3.99)

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

    def test_comma_in_price(self):
        assert _parse_price("1,299.99") == pytest.approx(1299.99)


# ── _iso_date ─────────────────────────────────────────────────────────────────

class TestIsoDate:
    def test_datetime_string(self):
        assert _iso_date("2026-04-03T00:00:00-04:00") == "2026-04-03"

    def test_date_only_string(self):
        assert _iso_date("2026-04-09") == "2026-04-09"

    def test_none_returns_none(self):
        assert _iso_date(None) is None

    def test_empty_string_returns_none(self):
        assert _iso_date("") is None


# ── _category_name ────────────────────────────────────────────────────────────

class TestCategoryName:
    def test_extracts_name(self):
        cats = {"l1": {"category_name": "Food, Beverages & Tobacco"}}
        assert _category_name(cats, "l1") == "Food, Beverages & Tobacco"

    def test_none_level_data(self):
        cats = {"l1": None}
        assert _category_name(cats, "l1") is None

    def test_missing_level_returns_none(self):
        assert _category_name({"l1": {"category_name": "Food"}}, "l2") is None

    def test_none_categories_returns_none(self):
        assert _category_name(None, "l1") is None

    def test_empty_category_name_returns_none(self):
        cats = {"l1": {"category_name": ""}}
        assert _category_name(cats, "l1") is None


# ── normalize_flipp_product ───────────────────────────────────────────────────

class TestNormalizeFlippProduct:
    def test_returns_flyer_item(self):
        item = normalize_flipp_product(make_raw_product())
        assert isinstance(item, FlyerItem)

    def test_source_api_is_flipp(self):
        item = normalize_flipp_product(make_raw_product())
        assert item.source_api == "flipp"

    def test_language_is_en(self):
        item = normalize_flipp_product(make_raw_product())
        assert item.language == "en"

    def test_raw_name_mapped(self):
        item = normalize_flipp_product(make_raw_product(name="Test Product"))
        assert item.raw_name == "Test Product"

    def test_name_en_equals_raw_name(self):
        item = normalize_flipp_product(make_raw_product(name="Test Product"))
        assert item.name_en == "Test Product"

    def test_description_en_mapped(self):
        item = normalize_flipp_product(make_raw_product(description="SELECTED"))
        assert item.description_en == "SELECTED"
        assert item.raw_description == "SELECTED"

    def test_brand_mapped(self):
        item = normalize_flipp_product(make_raw_product(brand="Kraft"))
        assert item.brand == "Kraft"

    def test_sku_mapped(self):
        item = normalize_flipp_product(make_raw_product(sku="ABC123"))
        assert item.sku == "ABC123"

    def test_sale_price_parsed(self):
        item = normalize_flipp_product(make_raw_product(price_text="3.99"))
        assert item.sale_price == pytest.approx(3.99)

    def test_regular_price_parsed(self):
        item = normalize_flipp_product(make_raw_product(original_price="5.49"))
        assert item.regular_price == pytest.approx(5.49)

    def test_regular_price_none_when_absent(self):
        item = normalize_flipp_product(make_raw_product(original_price=None))
        assert item.regular_price is None

    def test_pre_price_text_mapped(self):
        raw = make_raw_product(pre_price_text="from")
        item = normalize_flipp_product(raw)
        assert item.pre_price_text == "from"

    def test_post_price_text_mapped(self):
        item = normalize_flipp_product(make_raw_product(post_price_text="each"))
        assert item.post_price_text == "each"

    def test_empty_pre_price_text_becomes_none(self):
        item = normalize_flipp_product(make_raw_product(pre_price_text=""))
        assert item.pre_price_text is None

    def test_sale_story_stored_as_promo_details(self):
        item = normalize_flipp_product(make_raw_product(sale_story="Save $1.50"))
        assert item.promo_details == "Save $1.50"

    def test_product_url_mapped(self):
        item = normalize_flipp_product(
            make_raw_product(item_web_url="https://www.loblaws.ca/p/SKU001")
        )
        assert item.product_url == "https://www.loblaws.ca/p/SKU001"

    def test_image_url_mapped(self):
        item = normalize_flipp_product(
            make_raw_product(image_url="https://f.wishabi.net/img.jpg")
        )
        assert item.image_url == "https://f.wishabi.net/img.jpg"

    def test_categories_mapped(self):
        item = normalize_flipp_product(make_raw_product(categories=["Dairy", "Milk"]))
        assert item.raw_categories == ["Dairy", "Milk"]

    def test_category_levels_mapped(self):
        item = normalize_flipp_product(make_raw_product())
        assert item.category_l1 == "Food, Beverages & Tobacco"
        assert item.category_l2 == "Food Items"
        assert item.category_l3 == "Dairy Products"
        assert item.category_l4 is None

    def test_context_injection(self):
        item = normalize_flipp_product(
            make_raw_product(),
            store_chain="loblaws",
            store_id="1000",
            flyer_id="7838648",
            fetched_on="2026-04-03",
            province="ON",
        )
        assert item.store_chain == "loblaws"
        assert item.store_id == "1000"
        assert item.flyer_id == "7838648"
        assert item.fetched_on == "2026-04-03"
        assert item.province == "ON"

    def test_flyer_dates_from_context_when_product_has_none(self):
        raw = make_raw_product()
        raw.pop("valid_from", None)
        raw.pop("valid_to", None)
        item = normalize_flipp_product(
            raw,
            flyer_valid_from="2026-04-03",
            flyer_valid_to="2026-04-09",
        )
        assert item.flyer_valid_from == "2026-04-03"
        assert item.flyer_valid_to == "2026-04-09"

    def test_product_dates_take_precedence_over_flyer_dates(self):
        raw = make_raw_product(valid_from="2026-04-05", valid_to="2026-04-11")
        item = normalize_flipp_product(
            raw,
            flyer_valid_from="2026-04-03",
            flyer_valid_to="2026-04-09",
        )
        assert item.flyer_valid_from == "2026-04-05"
        assert item.flyer_valid_to == "2026-04-11"

    def test_store_id_coerced_to_string(self):
        item = normalize_flipp_product(make_raw_product(), store_id=1000)
        assert item.store_id == "1000"

    def test_flyer_id_coerced_to_string(self):
        item = normalize_flipp_product(make_raw_product(), flyer_id=7838648)
        assert item.flyer_id == "7838648"


# ── normalize_flipp_file ──────────────────────────────────────────────────────

class TestNormalizeFlippFile:
    def test_returns_list_of_flyer_items(self):
        items = normalize_flipp_file(make_flyer_data())
        assert isinstance(items, list)
        assert all(isinstance(i, FlyerItem) for i in items)

    def test_normal_product_included(self):
        items = normalize_flipp_file(make_flyer_data([make_raw_product()]))
        assert len(items) == 1

    def test_banner_item_type_5_excluded(self):
        """item_type == 5 records must be excluded from output."""
        products = [
            make_raw_product(item_type=1),
            make_raw_product(id=2, sku="BANNER", item_type=5),
        ]
        items = normalize_flipp_file(make_flyer_data(products))
        assert len(items) == 1
        assert all(p.sku != "BANNER" for p in items)

    def test_no_name_and_no_sku_excluded(self):
        """Records with both name=None and sku=None are excluded."""
        products = [
            make_raw_product(),
            {"id": 99, "name": None, "sku": None, "item_type": 1},
        ]
        items = normalize_flipp_file(make_flyer_data(products))
        assert len(items) == 1

    def test_name_only_record_included(self):
        """Records with a name but no sku are kept."""
        products = [make_raw_product(sku=None)]
        items = normalize_flipp_file(make_flyer_data(products))
        assert len(items) == 1

    def test_sku_only_record_included(self):
        """Records with a sku but no name are kept."""
        products = [make_raw_product(name=None)]
        items = normalize_flipp_file(make_flyer_data(products))
        assert len(items) == 1

    def test_empty_products_list(self):
        items = normalize_flipp_file(make_flyer_data([]))
        assert items == []

    def test_all_banner_items_excluded(self):
        products = [make_raw_product(id=i, item_type=5) for i in range(5)]
        items = normalize_flipp_file(make_flyer_data(products))
        assert items == []

    def test_flyer_metadata_injected(self):
        items = normalize_flipp_file(
            make_flyer_data(),
            store_chain="loblaws",
            store_id="1000",
            province="ON",
        )
        assert len(items) == 1
        item = items[0]
        assert item.source_api == "flipp"
        assert item.store_chain == "loblaws"
        assert item.store_id == "1000"
        assert item.province == "ON"
        assert item.fetched_on == "2026-04-03"
        assert item.flyer_id == "7838648"

    def test_flyer_valid_dates_from_publication_meta(self):
        items = normalize_flipp_file(make_flyer_data())
        assert items[0].flyer_valid_from == "2026-04-03"
        assert items[0].flyer_valid_to == "2026-04-09"

    def test_missing_publication_meta(self):
        # Strip publication_meta and product-level dates so both sources are absent
        raw = make_raw_product()
        raw.pop("valid_from", None)
        raw.pop("valid_to", None)
        data = {"fetched_on": "2026-04-03", "products": [raw]}
        items = normalize_flipp_file(data, store_chain="loblaws")
        assert len(items) == 1
        assert items[0].flyer_valid_from is None

    def test_multiple_products_all_normalised(self):
        products = [make_raw_product(id=i, sku=f"SKU{i:03d}") for i in range(10)]
        items = normalize_flipp_file(make_flyer_data(products))
        assert len(items) == 10

    def test_mixed_item_types_correct_count(self):
        products = [
            make_raw_product(id=1, item_type=1),
            make_raw_product(id=2, item_type=5),
            make_raw_product(id=3, item_type=1),
            make_raw_product(id=4, item_type=5),
            make_raw_product(id=5, item_type=1),
        ]
        items = normalize_flipp_file(make_flyer_data(products))
        assert len(items) == 3


# ── Integration: real loblaws flyer files ─────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "loblaws", "flyers")
LOBLAWS_FLYER_FILES = glob.glob(os.path.join(DATA_DIR, "*.json"))


@pytest.mark.skipif(
    not LOBLAWS_FLYER_FILES,
    reason="No loblaws flyer files found under data/loblaws/flyers/",
)
class TestLoblawsFlyers:
    def test_all_files_produce_valid_flyer_items(self):
        """Parsing every loblaws flyer file must yield zero schema validation errors."""
        for path in LOBLAWS_FLYER_FILES:
            with open(path) as f:
                flyer_data = json.load(f)
            # normalize_flipp_file raises on validation failure; any exception
            # here means a schema violation.
            items = normalize_flipp_file(
                flyer_data, store_chain="loblaws", store_id="1000", province="ON"
            )
            assert isinstance(items, list), f"Expected list from {path}"
            assert all(isinstance(i, FlyerItem) for i in items), (
                f"Non-FlyerItem in output from {path}"
            )

    def test_banner_items_absent_from_output(self):
        """No output item may originate from an item_type=5 raw record."""
        for path in LOBLAWS_FLYER_FILES:
            with open(path) as f:
                flyer_data = json.load(f)
            # Collect all raw banner SKUs/names for comparison
            banner_ids = {
                str(p["id"])
                for p in flyer_data.get("products", [])
                if p.get("item_type") == 5
            }
            items = normalize_flipp_file(flyer_data)
            # We can't directly match by id (it's not in FlyerItem), so verify
            # counts: output should be less than total by at least the banner count.
            total = len(flyer_data.get("products", []))
            assert len(items) <= total - len(banner_ids), (
                f"Banner items may not have been filtered in {path}"
            )
