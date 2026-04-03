"""Tests for parsers/price_parser.py — parse_price()."""

from __future__ import annotations

import glob
import json
import os

import pytest

from parsers.price_parser import parse_price


# ── Table-driven cases from the issue spec ────────────────────────────────────

class TestParsePrice:
    def test_plain_numeric(self):
        """Simple decimal string → sale_price."""
        r = parse_price("3.98")
        assert r["sale_price"] == pytest.approx(3.98)

    def test_comma_decimal(self):
        """European-style decimal comma → normalised sale_price."""
        r = parse_price("14,99")
        assert r["sale_price"] == pytest.approx(14.99)

    def test_multi_buy_pre(self):
        """'2/' prefix → multi_buy_qty, multi_buy_total, sale_price = half."""
        r = parse_price("8.00", pre_text="2/")
        assert r["multi_buy_qty"] == 2
        assert r["multi_buy_total"] == pytest.approx(8.00)
        assert r["sale_price"] == pytest.approx(4.00)

    def test_starting_at_prefix(self):
        """'starting at' prefix → price_is_floor=True, sale_price set."""
        r = parse_price("7.99", pre_text="starting at")
        assert r["sale_price"] == pytest.approx(7.99)
        assert r.get("price_is_floor") is True

    def test_empty_price_text(self):
        """Empty price string → sale_price=None."""
        r = parse_price("")
        assert r["sale_price"] is None

    def test_none_price_text(self):
        """None price string → sale_price=None."""
        r = parse_price(None)
        assert r["sale_price"] is None

    def test_cents_sign(self):
        """Price with '¢' sign → divided by 100."""
        r = parse_price("99¢")
        assert r["sale_price"] == pytest.approx(0.99)

    def test_dual_weight_price(self):
        """'3,99/lb - 8,80/kg' → regular_price, price_unit, price_per_lb, price_per_kg."""
        r = parse_price("3,99/lb - 8,80/kg")
        assert r["regular_price"] == pytest.approx(3.99)
        assert r["price_unit"] == "lb"
        assert r["price_per_lb"] == pytest.approx(3.99)
        assert r["price_per_kg"] == pytest.approx(8.80)

    def test_post_text_unit(self):
        """post_text='lb' → price_unit='lb'."""
        r = parse_price("3.99", post_text="lb")
        assert r["price_unit"] == "lb"
        assert r["sale_price"] == pytest.approx(3.99)

    def test_disclaimer_limit(self):
        """Disclaimer with LIMIT…PAY → purchase_limit, over_limit_price."""
        r = parse_price("9.99", disclaimer_text="LIMIT 4 OVER LIMIT PAY 10.49 EA")
        assert r["purchase_limit"] == 4
        assert r["over_limit_price"] == pytest.approx(10.49)


# ── Additional coverage ───────────────────────────────────────────────────────

class TestParsePriceExtra:
    def test_dollar_sign_stripped(self):
        r = parse_price("$4.50")
        assert r["sale_price"] == pytest.approx(4.50)

    def test_integer_string(self):
        r = parse_price("5")
        assert r["sale_price"] == pytest.approx(5.0)

    def test_thousands_comma(self):
        """Thousands-separator comma should not be treated as decimal."""
        r = parse_price("1,299.99")
        assert r["sale_price"] == pytest.approx(1299.99)

    def test_unrecognised_format_returns_none_with_warning(self):
        r = parse_price("call for price")
        assert r["sale_price"] is None
        assert len(r["parse_warnings"]) > 0

    def test_parse_warnings_always_present(self):
        r = parse_price("3.99")
        assert "parse_warnings" in r
        assert isinstance(r["parse_warnings"], list)

    def test_no_warnings_on_clean_input(self):
        r = parse_price("3.99")
        assert r["parse_warnings"] == []

    def test_multi_buy_three_for(self):
        """'3 for' prefix form."""
        r = parse_price("9.00", pre_text="3 for")
        assert r["multi_buy_qty"] == 3
        assert r["multi_buy_total"] == pytest.approx(9.00)
        assert r["sale_price"] == pytest.approx(3.00)

    def test_floor_prefix_from(self):
        r = parse_price("5.00", pre_text="from")
        assert r.get("price_is_floor") is True

    def test_original_price_parsed(self):
        r = parse_price("3.99", original_price="5.49")
        assert r["regular_price"] == pytest.approx(5.49)

    def test_sale_greater_than_regular_adds_warning(self):
        r = parse_price("7.99", original_price="5.49")
        assert any("sale_price" in w for w in r["parse_warnings"])

    def test_cents_in_pre_field(self):
        """'¢' can appear in the price_text when priceSign field is merged in."""
        r = parse_price("¢99")
        assert r["sale_price"] == pytest.approx(0.99)

    def test_post_text_each(self):
        r = parse_price("2.99", post_text="each")
        assert r["price_unit"] == "each"

    def test_post_text_kg(self):
        r = parse_price("4.99", post_text="kg")
        assert r["price_unit"] == "kg"

    def test_dual_weight_lb_kg_cross_validation_ok(self):
        """Valid lb/kg pair → no warnings."""
        # 3.99/lb × 2.20462 ≈ 8.796/kg; 8.80 is within 2 %
        r = parse_price("3.99/lb - 8.80/kg")
        assert r["price_per_lb"] == pytest.approx(3.99)
        assert r["price_per_kg"] == pytest.approx(8.80)
        assert r["parse_warnings"] == []

    def test_dual_weight_lb_kg_cross_validation_warning(self):
        """Wildly inconsistent lb/kg pair → warning added."""
        # 1.00/lb × 2.20462 = 2.20/kg; 5.00 is far outside 2 %
        r = parse_price("1.00/lb - 5.00/kg")
        assert any("mismatch" in w for w in r["parse_warnings"])

    def test_single_unit_price_lb(self):
        r = parse_price("3.99/lb")
        assert r["sale_price"] == pytest.approx(3.99)
        assert r["price_unit"] == "lb"
        assert r["price_per_lb"] == pytest.approx(3.99)
        assert "price_per_kg" in r

    def test_single_unit_price_kg(self):
        r = parse_price("8.80/kg")
        assert r["sale_price"] == pytest.approx(8.80)
        assert r["price_unit"] == "kg"
        assert r["price_per_kg"] == pytest.approx(8.80)
        assert "price_per_lb" in r

    def test_disclaimer_no_limit_no_fields_added(self):
        r = parse_price("3.99", disclaimer_text="See store for details")
        assert "purchase_limit" not in r
        assert "over_limit_price" not in r

    def test_none_original_price_no_regular_price(self):
        r = parse_price("3.99", original_price=None)
        assert "regular_price" not in r

    def test_multi_buy_bad_price_adds_warning(self):
        r = parse_price("N/A", pre_text="2/")
        assert r["sale_price"] is None
        assert len(r["parse_warnings"]) > 0

    def test_comma_ambiguous_one_digit_treated_as_decimal(self):
        """'1,2' is ambiguous; our heuristic treats it as 1.2 (decimal comma)."""
        r = parse_price("1,2")
        assert r["sale_price"] == pytest.approx(1.2)

    def test_zero_lb_price_no_exception(self):
        """Zero lb price must not cause a division by zero during cross-validation."""
        r = parse_price("0.00/lb - 0.00/kg")
        assert "parse_warnings" in r

    @pytest.mark.parametrize("text", [
        "3.98", "14,99", "$4.50", "5", "99¢",
        "3,99/lb - 8,80/kg", "3.99/lb", "8.80/kg",
        "", None, "call for price", "n/a",
    ])
    def test_no_exception_on_varied_inputs(self, text):
        """parse_price must not raise on any input."""
        r = parse_price(text)
        assert "parse_warnings" in r


# ── Integration: no exceptions on real data files ─────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ALL_FLYER_FILES = glob.glob(os.path.join(DATA_DIR, "**", "flyers", "*.json"), recursive=True)


@pytest.mark.skipif(
    not ALL_FLYER_FILES,
    reason="No flyer files found under data/",
)
class TestNoExceptionsOnRealData:
    def test_all_price_fields_parseable(self):
        """parse_price must not raise on any price string in data/."""
        for path in ALL_FLYER_FILES:
            with open(path) as f:
                flyer_data = json.load(f)
            for product in flyer_data.get("products", []):
                # Flipp-style fields
                r = parse_price(
                    price_text=product.get("price_text"),
                    pre_text=product.get("pre_price_text"),
                    post_text=product.get("post_price_text"),
                    original_price=product.get("original_price"),
                    disclaimer_text=product.get("disclaimer_text"),
                )
                assert "parse_warnings" in r, (
                    f"Missing parse_warnings for product in {path}"
                )
            # Metro-style pages (list of items with price fields)
            for page in flyer_data.get("pages", []):
                for item in page.get("items", []):
                    r = parse_price(
                        price_text=item.get("price"),
                        pre_text=item.get("pricePrefix"),
                        post_text=item.get("priceSuffix"),
                    )
                    assert "parse_warnings" in r
