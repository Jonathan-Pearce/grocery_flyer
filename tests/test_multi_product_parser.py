"""Tests for parsers/multi_product_parser.py."""

from __future__ import annotations

import pytest

from parsers.multi_product_parser import detect_variants, split_multi_product
from pipeline.schema import FlyerItem


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_record(**kwargs) -> FlyerItem:
    """Return a minimal FlyerItem for testing."""
    defaults = {
        "price_observation_key": "loblaws:1234:sku001:2024-01-01",
        "store_chain": "loblaws",
        "store_id": "1234",
        "flyer_id": "567890",
        "sale_price": 2.99,
    }
    defaults.update(kwargs)
    return FlyerItem(**defaults)


# ── detect_variants ───────────────────────────────────────────────────────────


class TestDetectVariantsSplit:
    """detect_variants returns a list when the input is a valid multi-product string."""

    @pytest.mark.parametrize(
        "raw_name, expected_count",
        [
            # Acceptance-criteria strings
            (
                "CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G",
                2,
            ),
            (
                "MAPLE LEAF READY CRISP BACON or FULLY COOKED BREAKFAST SAUSAGES, 65/300 g",
                2,
            ),
            # Other separator keywords
            ("BUTTER OU MARGARINE", 2),
            ("BUTTER / MARGARINE", 2),
            ("BUTTER AND MARGARINE", 2),
            ("BUTTER ET MARGARINE", 2),
            # Case variations
            ("Butter and Margarine", 2),
            ("Beurre et Margarine", 2),
            # Three-way split
            ("BREAD OR BUTTER OR CHEESE", 3),
        ],
    )
    def test_split(self, raw_name: str, expected_count: int) -> None:
        result = detect_variants(raw_name)
        assert result is not None, f"Expected split for {raw_name!r}"
        assert len(result) == expected_count

    def test_chestnuts_variant_names(self) -> None:
        """Verify the exact variant strings returned for the first acceptance example."""
        result = detect_variants(
            "CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G"
        )
        assert result == [
            "CHESTNUTS, 85 G",
            "CROWN SUPREME CORN KERNEL, 340-410 G",
        ]

    def test_bacon_sausage_variant_names(self) -> None:
        """Verify the exact variant strings returned for the second acceptance example."""
        result = detect_variants(
            "MAPLE LEAF READY CRISP BACON or FULLY COOKED BREAKFAST SAUSAGES, 65/300 g"
        )
        assert result == [
            "MAPLE LEAF READY CRISP BACON",
            "FULLY COOKED BREAKFAST SAUSAGES, 65/300 g",
        ]


class TestDetectVariantsNoSplit:
    """detect_variants returns None when the input should not be split."""

    @pytest.mark.parametrize(
        "raw_name",
        [
            # Acceptance criteria: slash is a unit/price separator, not a product separator
            "3,99/lb",
            # OCR artifact (no space around OR)
            "ORCRUSHED TOMATOES",
            # Single product with no separator
            "MAPLE LEAF BACON 375 G",
            # Only numbers and units on one side
            "99/lb",
            # Empty-ish input
            "",
            "   ",
        ],
    )
    def test_no_split(self, raw_name: str) -> None:
        result = detect_variants(raw_name)
        assert result is None, f"Expected no split for {raw_name!r}"


# ── split_multi_product ───────────────────────────────────────────────────────


class TestSplitMultiProductAcceptanceCriteria:
    """Verify the two acceptance-criteria examples from the issue."""

    def test_chestnuts_corn_kernel(self) -> None:
        """Two children with correct weights; parent preserved."""
        record = _make_record(
            raw_name="CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G",
        )
        results = split_multi_product(record)

        # Parent + 2 children
        assert len(results) == 3

        parent, child1, child2 = results

        # Parent is flagged as multi-product
        assert parent.is_multi_product is True
        assert parent.raw_name == record.raw_name

        # Child 1 — chestnuts, 85 g
        assert child1.is_multi_product is False
        assert child1.parent_record_id == record.price_observation_key
        assert child1.raw_name == record.raw_name  # original combined string
        assert child1.name_en is not None
        assert "Chestnuts" in child1.name_en or "CHESTNUTS" in (child1.name_en or "")
        assert child1.weight_value == pytest.approx(85.0)
        assert child1.weight_unit == "g"

        # Child 2 — corn kernel, 340-410 g range
        assert child2.is_multi_product is False
        assert child2.parent_record_id == record.price_observation_key
        assert child2.weight_is_range is True
        assert child2.weight_min == pytest.approx(340.0)
        assert child2.weight_max == pytest.approx(410.0)
        assert child2.weight_unit == "g"

    def test_bacon_sausages(self) -> None:
        """Two children; price copied from parent."""
        record = _make_record(
            raw_name=(
                "MAPLE LEAF READY CRISP BACON or "
                "FULLY COOKED BREAKFAST SAUSAGES, 65/300 g"
            ),
            sale_price=4.49,
            promo_type="no_promo",
        )
        results = split_multi_product(record)

        assert len(results) == 3
        parent, child1, child2 = results

        assert parent.is_multi_product is True

        # Price copied to both children
        assert child1.sale_price == pytest.approx(4.49)
        assert child2.sale_price == pytest.approx(4.49)

        # Children are not multi-product and link back to parent
        assert child1.is_multi_product is False
        assert child2.is_multi_product is False
        assert child1.parent_record_id == record.price_observation_key
        assert child2.parent_record_id == record.price_observation_key

    def test_price_per_unit_slash_not_split(self) -> None:
        """Acceptance criteria: "3,99/lb" must NOT be split."""
        record = _make_record(raw_name="3,99/lb")
        results = split_multi_product(record)
        assert len(results) == 1
        assert results[0].is_multi_product is False


class TestSplitMultiProductParentPreservation:
    """Parent record is always present in the output with is_multi_product=True."""

    def test_parent_is_first_element(self) -> None:
        record = _make_record(raw_name="BUTTER OR MARGARINE")
        results = split_multi_product(record)
        assert results[0].is_multi_product is True

    def test_parent_sale_price_preserved(self) -> None:
        record = _make_record(raw_name="BUTTER OR MARGARINE", sale_price=3.49)
        parent = split_multi_product(record)[0]
        assert parent.sale_price == pytest.approx(3.49)

    def test_parent_raw_name_preserved(self) -> None:
        raw = "BUTTER OR MARGARINE"
        record = _make_record(raw_name=raw)
        parent = split_multi_product(record)[0]
        assert parent.raw_name == raw


class TestSplitMultiProductPriceFieldsCopied:
    """All price, promo, and flyer fields are copied to child records."""

    def test_price_fields_copied(self) -> None:
        record = _make_record(
            raw_name="BREAD OR BUTTER",
            sale_price=1.99,
            regular_price=2.49,
            price_per_kg=4.39,
            multi_buy_qty=2,
            multi_buy_total=3.98,
        )
        _, child1, child2 = split_multi_product(record)
        for child in (child1, child2):
            assert child.sale_price == pytest.approx(1.99)
            assert child.regular_price == pytest.approx(2.49)
            assert child.price_per_kg == pytest.approx(4.39)
            assert child.multi_buy_qty == 2
            assert child.multi_buy_total == pytest.approx(3.98)

    def test_flyer_fields_copied(self) -> None:
        record = _make_record(
            raw_name="BREAD OR BUTTER",
            flyer_valid_from="2024-01-01",
            flyer_valid_to="2024-01-07",
            store_chain="sobeys",
            province="ON",
        )
        _, child1, child2 = split_multi_product(record)
        for child in (child1, child2):
            assert child.flyer_valid_from == "2024-01-01"
            assert child.flyer_valid_to == "2024-01-07"
            assert child.store_chain == "sobeys"
            assert child.province == "ON"

    def test_promo_fields_copied(self) -> None:
        record = _make_record(
            raw_name="BREAD OR BUTTER",
            promo_type="percentage_off",
            promo_details="20% off",
        )
        _, child1, child2 = split_multi_product(record)
        for child in (child1, child2):
            assert child.promo_type == "percentage_off"
            assert child.promo_details == "20% off"


class TestSplitMultiProductRawName:
    """Children keep the original combined raw_name."""

    def test_children_raw_name_is_combined(self) -> None:
        combined = "APPLE OR ORANGE"
        record = _make_record(raw_name=combined)
        _, child1, child2 = split_multi_product(record)
        assert child1.raw_name == combined
        assert child2.raw_name == combined


class TestSplitMultiProductSkuAssignment:
    """SKUs from the skus list are assigned to child records in order."""

    def test_skus_assigned_in_order(self) -> None:
        record = _make_record(raw_name="APPLE OR ORANGE OR BANANA")
        results = split_multi_product(record, skus=["sku-A", "sku-B", "sku-C"])
        _, child1, child2, child3 = results
        assert child1.sku == "sku-A"
        assert child2.sku == "sku-B"
        assert child3.sku == "sku-C"

    def test_fewer_skus_than_variants(self) -> None:
        """Only the first N children receive a SKU; the rest are unchanged."""
        record = _make_record(raw_name="APPLE OR ORANGE OR BANANA")
        results = split_multi_product(record, skus=["sku-A"])
        _, child1, child2, child3 = results
        assert child1.sku == "sku-A"
        # child2 and child3 retain the parent's sku (None here)
        assert child2.sku is None
        assert child3.sku is None

    def test_no_skus_does_not_raise(self) -> None:
        record = _make_record(raw_name="APPLE OR ORANGE")
        results = split_multi_product(record)
        assert len(results) == 3


class TestSplitMultiProductWeightExtraction:
    """Per-variant weights are re-extracted; parent weights are not inherited."""

    def test_weight_not_inherited_from_parent(self) -> None:
        """Child without a weight in its name should have no weight fields set."""
        record = _make_record(
            raw_name="MAPLE LEAF BACON OR COOKED HAM",
            weight_value=375.0,
            weight_unit="g",
        )
        _, child1, child2 = split_multi_product(record)
        # Neither variant name carries a weight
        assert child1.weight_value is None
        assert child2.weight_value is None

    def test_per_variant_weight_extracted(self) -> None:
        record = _make_record(
            raw_name="CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G",
        )
        _, child1, child2 = split_multi_product(record)
        assert child1.weight_value == pytest.approx(85.0)
        assert child1.weight_unit == "g"
        assert child2.weight_is_range is True
        assert child2.weight_min == pytest.approx(340.0)
        assert child2.weight_max == pytest.approx(410.0)


class TestSplitMultiProductNonMulti:
    """Non-multi-product records are returned unchanged."""

    @pytest.mark.parametrize(
        "raw_name",
        [
            "MAPLE LEAF BACON 375 G",
            "3,99/lb",
            "ORCRUSHED TOMATOES",
            None,
            "",
        ],
    )
    def test_single_product_returned_unchanged(self, raw_name: str | None) -> None:
        record = _make_record(raw_name=raw_name)
        results = split_multi_product(record)
        assert len(results) == 1
        assert results[0] is record


class TestNoExceptions:
    """split_multi_product and detect_variants never raise exceptions."""

    @pytest.mark.parametrize(
        "raw_name",
        [
            None,
            "",
            "   ",
            "A",
            "OR",
            " OR ",
            "A OR B",
            "A / B",
            "A AND B",
            "A ET B",
            "A OU B",
            "3,99/lb",
            "ORCRUSHED",
            "CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G",
            "MAPLE LEAF READY CRISP BACON or FULLY COOKED BREAKFAST SAUSAGES, 65/300 g",
            "A OR B OR C OR D",
        ],
    )
    def test_no_exception_detect_variants(self, raw_name: str | None) -> None:
        if raw_name is None:
            return  # detect_variants expects a str; None is handled by split_multi_product
        detect_variants(raw_name)

    @pytest.mark.parametrize(
        "raw_name",
        [
            None,
            "",
            "   ",
            "SINGLE PRODUCT",
            "3,99/lb",
            "CHESTNUTS, 85 G OR CROWN SUPREME CORN KERNEL, 340-410 G",
            "MAPLE LEAF READY CRISP BACON or FULLY COOKED BREAKFAST SAUSAGES, 65/300 g",
        ],
    )
    def test_no_exception_split(self, raw_name: str | None) -> None:
        record = _make_record(raw_name=raw_name)
        split_multi_product(record)
