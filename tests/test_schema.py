"""Tests for schema.py — FlyerItem Pydantic model."""

import pytest

from pipeline.schema import FlyerItem


# ── Instantiation ─────────────────────────────────────────────────────────────

class TestFlyerItemInstantiation:
    def test_minimal_instantiation_with_defaults(self):
        item = FlyerItem()
        assert item.source_api is None
        assert item.store_chain is None
        assert item.currency == "CAD"
        assert item.price_is_floor is False
        assert item.is_food is False
        assert item.is_human_food is False
        assert item.is_multi_product is False
        assert item.weight_is_range is False
        assert item.multi_product_variants == []

    def test_full_instantiation(self):
        item = FlyerItem(
            source_api="flipp",
            store_chain="loblaws",
            store_id="1234",
            flyer_id="567890",
            flyer_valid_from="2026-04-03",
            flyer_valid_to="2026-04-09",
            fetched_on="2026-04-03",
            province="ON",
            raw_name="Organic Milk 2L",
            raw_description="Homo milk, 2 L",
            raw_body="Great price on organic milk",
            pre_price_text="from",
            post_price_text="each",
            raw_categories=["Dairy", "Milk"],
            name_en="Organic Milk",
            name_fr="Lait biologique",
            description_en="Homogenized milk, 2 L",
            description_fr="Lait homogénéisé, 2 L",
            brand="Organic Meadow",
            sku="ABC123",
            language="bil",
            product_url="https://www.loblaws.ca/milk",
            image_url="https://images.loblaws.ca/milk.jpg",
            sale_price=4.99,
            regular_price=6.49,
            price_unit="each",
            price_per_kg=2.50,
            price_per_lb=1.13,
            alternate_price=3.99,
            alternate_unit="2 for",
            member_price=4.49,
            price_is_floor=True,
            multi_buy_qty=2,
            multi_buy_total=9.00,
            currency="CAD",
            purchase_limit=4,
            over_limit_price=6.49,
            tax_indicator="+TX",
            promo_type="dollar_off",
            promo_details="Save $1.50",
            loyalty_program="PC Optimum",
            loyalty_points=500,
            loyalty_trigger="when you buy 2",
            weight_value=2.0,
            weight_unit="L",
            weight_is_range=False,
            pack_count=1,
            pack_unit_size=2000.0,
            pack_unit="mL",
            category_l1="Food & Beverages",
            category_l2="Dairy",
            category_l3="Milk",
            category_l4=None,
            is_food=True,
            is_human_food=True,
            is_multi_product=False,
            parent_record_id=None,
            multi_product_variants=[],
            price_observation_key="loblaws:1234:ABC123:2026-04-03",
        )
        assert item.source_api == "flipp"
        assert item.store_chain == "loblaws"
        assert item.sale_price == 4.99
        assert item.language == "bil"
        assert item.promo_type == "dollar_off"
        assert item.loyalty_program == "PC Optimum"
        assert item.weight_unit == "L"


# ── Round-trip serialisation ──────────────────────────────────────────────────

class TestFlyerItemRoundTrip:
    def test_round_trip_minimal(self):
        """Instantiate → JSON → deserialise must be lossless."""
        item = FlyerItem(source_api="metro", store_chain="food_basics", store_id="99")
        json_str = item.model_dump_json()
        restored = FlyerItem.model_validate_json(json_str)
        assert restored == item

    def test_round_trip_full(self):
        """All populated fields survive JSON serialisation."""
        item = FlyerItem(
            source_api="flipp",
            store_chain="no_frills",
            store_id="42",
            flyer_id="111222",
            flyer_valid_from="2026-04-03",
            flyer_valid_to="2026-04-09",
            fetched_on="2026-04-03",
            province="ON",
            name_en="Bread",
            sale_price=2.49,
            promo_type="no_promo",
            weight_value=675.0,
            weight_unit="g",
            is_food=True,
            is_human_food=True,
            multi_product_variants=["White", "Whole Wheat"],
            price_observation_key="no_frills:42:None:2026-04-03",
        )
        restored = FlyerItem.model_validate_json(item.model_dump_json())
        assert restored.sale_price == 2.49
        assert restored.weight_unit == "g"
        assert restored.multi_product_variants == ["White", "Whole Wheat"]

    def test_round_trip_via_dict(self):
        """model_dump → model_validate must also be lossless."""
        item = FlyerItem(source_api="metro", store_chain="super_c", province="QC")
        restored = FlyerItem.model_validate(item.model_dump())
        assert restored == item

    def test_round_trip_preserves_none_fields(self):
        item = FlyerItem(source_api="flipp")
        data = item.model_dump()
        assert data["store_chain"] is None
        restored = FlyerItem.model_validate(data)
        assert restored.store_chain is None


# ── extra='ignore' behaviour ──────────────────────────────────────────────────

class TestExtraFieldsIgnored:
    def test_unknown_fields_are_silently_ignored(self):
        data = {
            "source_api": "flipp",
            "store_chain": "loblaws",
            "unknown_field_xyz": "should be ignored",
            "another_extra": 999,
        }
        item = FlyerItem.model_validate(data)
        assert item.store_chain == "loblaws"
        assert not hasattr(item, "unknown_field_xyz")


# ── Literal field validation ──────────────────────────────────────────────────

class TestLiteralFields:
    @pytest.mark.parametrize("api", ["flipp", "metro"])
    def test_valid_source_api_values(self, api):
        item = FlyerItem(source_api=api)
        assert item.source_api == api

    def test_invalid_source_api_raises(self):
        with pytest.raises(Exception):
            FlyerItem(source_api="unknown")

    @pytest.mark.parametrize("lang", ["en", "fr", "bil"])
    def test_valid_language_values(self, lang):
        item = FlyerItem(language=lang)
        assert item.language == lang

    @pytest.mark.parametrize("unit", ["g", "kg", "mL", "L", "lb", "oz", "count"])
    def test_valid_weight_unit_values(self, unit):
        item = FlyerItem(weight_unit=unit)
        assert item.weight_unit == unit

    @pytest.mark.parametrize("promo", [
        "rollback", "percentage_off", "dollar_off", "multi_buy",
        "bogo", "loyalty_points", "member_price", "clearance", "no_promo",
    ])
    def test_valid_promo_type_values(self, promo):
        item = FlyerItem(promo_type=promo)
        assert item.promo_type == promo

    @pytest.mark.parametrize("program", ["Scene+", "PC Optimum"])
    def test_valid_loyalty_program_values(self, program):
        item = FlyerItem(loyalty_program=program)
        assert item.loyalty_program == program


# ── Default values ────────────────────────────────────────────────────────────

class TestDefaults:
    def test_currency_defaults_to_cad(self):
        assert FlyerItem().currency == "CAD"

    def test_bool_fields_default_to_false(self):
        item = FlyerItem()
        assert item.price_is_floor is False
        assert item.weight_is_range is False
        assert item.is_food is False
        assert item.is_human_food is False
        assert item.is_multi_product is False

    def test_multi_product_variants_defaults_to_empty_list(self):
        assert FlyerItem().multi_product_variants == []

    def test_multi_product_variants_not_shared_between_instances(self):
        a = FlyerItem()
        b = FlyerItem()
        a.multi_product_variants.append("x")
        assert b.multi_product_variants == []
