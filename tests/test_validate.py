"""Tests for validate.py — QA report generator."""

from __future__ import annotations

import json

from pipeline.schema import FlyerItem
from pipeline.validate import (
    _load_records,
    _section1,
    _section2,
    _section3,
    _section4,
    _section5,
    build_report,
    main,
)


# ── Fixture helpers ───────────────────────────────────────────────────────────


def _make_item(**kwargs) -> FlyerItem:
    defaults = {
        "source_api": "flipp",
        "store_chain": "loblaws",
        "store_id": "1",
        "flyer_id": "1001",
        "flyer_valid_from": "2026-04-03",
        "fetched_on": "2026-04-03",
        "raw_name": "Test Product",
        "sale_price": 3.99,
        "promo_type": "no_promo",
        "category_l1": "Pantry",
        "is_food": True,
        "is_human_food": True,
        "price_observation_key": "loblaws:1:SKU001:2026-04-03",
    }
    defaults.update(kwargs)
    return FlyerItem(**defaults)


def _write_flyer_json(tmp_path, store_chain: str, flyer_id: str, items: list[FlyerItem]) -> None:
    store_dir = tmp_path / store_chain
    store_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "flyer_id": flyer_id,
        "store_chain": store_chain,
        "generated_at": "2026-04-03T00:00:00+00:00",
        "record_count": len(items),
        "records": [item.model_dump() for item in items],
    }
    (store_dir / f"{flyer_id}.json").write_text(json.dumps(payload), encoding="utf-8")


# ── _load_records ─────────────────────────────────────────────────────────────


class TestLoadRecords:
    def test_empty_dir_returns_empty(self, tmp_path):
        records, count = _load_records(str(tmp_path))
        assert records == []
        assert count == 0

    def test_missing_dir_returns_empty(self, tmp_path):
        records, count = _load_records(str(tmp_path / "nonexistent"))
        assert records == []
        assert count == 0

    def test_loads_records_from_json(self, tmp_path):
        item = _make_item()
        _write_flyer_json(tmp_path, "loblaws", "1001", [item])
        records, count = _load_records(str(tmp_path))
        assert count == 1
        assert len(records) == 1
        assert isinstance(records[0], FlyerItem)
        assert records[0].store_chain == "loblaws"

    def test_loads_multiple_stores_and_flyers(self, tmp_path):
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item(store_chain="loblaws")])
        _write_flyer_json(tmp_path, "sobeys", "2001", [_make_item(store_chain="sobeys")] * 3)
        records, count = _load_records(str(tmp_path))
        assert count == 2
        assert len(records) == 4

    def test_skips_validation_report_json(self, tmp_path):
        """validation_report.json lives at the top level, not in a store subdir."""
        (tmp_path / "validation_report.json").write_text("{}", encoding="utf-8")
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item()])
        records, count = _load_records(str(tmp_path))
        # Only the store subdir JSON file is read (validation_report.json is at
        # the top level, not inside a store subdirectory, so it is skipped).
        assert count == 1
        assert len(records) == 1

    def test_tolerates_malformed_json(self, tmp_path):
        store_dir = tmp_path / "loblaws"
        store_dir.mkdir()
        (store_dir / "bad.json").write_text("NOT JSON", encoding="utf-8")
        (store_dir / "good.json").write_text(
            json.dumps(
                {"records": [_make_item().model_dump()]}
            ),
            encoding="utf-8",
        )
        records, count = _load_records(str(tmp_path))
        assert len(records) == 1


# ── Section 1 — Record counts ─────────────────────────────────────────────────


class TestSection1:
    def test_total_records(self):
        items = [_make_item() for _ in range(5)]
        s = _section1(items)
        assert s["total_records"] == 5

    def test_by_store_counts(self):
        items = [
            _make_item(store_chain="loblaws"),
            _make_item(store_chain="loblaws"),
            _make_item(store_chain="sobeys"),
        ]
        s = _section1(items)
        assert s["by_store"]["loblaws"] == 2
        assert s["by_store"]["sobeys"] == 1

    def test_filtered_records_always_zero(self):
        # Filtered records are excluded before the pipeline writes output.
        s = _section1([_make_item()])
        assert s["filtered_records"] == 0
        assert s["filtered_pct"] == 0.0

    def test_multi_product_counts(self):
        parent = _make_item(is_multi_product=True)
        child = _make_item(parent_record_id="parent-123")
        regular = _make_item()
        s = _section1([parent, child, regular])
        assert s["multi_product_parents"] == 1
        assert s["multi_product_children"] == 1

    def test_parse_warnings_detected(self):
        # weight_value ≈ 1.89, unit mL → triggers "suspiciously small" warning
        item = _make_item(raw_name="Product 1.89 mL")
        s = _section1([item])
        assert s["records_with_parse_warnings"] >= 1
        assert len(s["parse_warning_messages"]) >= 1

    def test_no_warnings_for_normal_records(self):
        item = _make_item(raw_name="Bread 500 g")
        s = _section1([item])
        assert s["records_with_parse_warnings"] == 0
        assert s["parse_warning_messages"] == []

    def test_empty_records(self):
        s = _section1([])
        assert s["total_records"] == 0
        assert s["by_store"] == {}


# ── Section 2 — Price quality ─────────────────────────────────────────────────


class TestSection2:
    def test_no_sale_price_counted(self):
        items = [
            _make_item(sale_price=None),
            _make_item(sale_price=None, multi_buy_total=5.0),
            _make_item(sale_price=3.99),
        ]
        s = _section2(items)
        assert s["no_sale_price"] == 2
        assert s["no_sale_expected"] == 1   # has multi_buy_total
        assert s["no_sale_unexpected"] == 1

    def test_sale_gt_regular_anomaly_detected(self):
        item = _make_item(sale_price=5.99, regular_price=4.99)
        s = _section2([item])
        assert s["sale_gt_regular_anomalies"] == 1
        assert item.price_observation_key in s["sale_gt_regular_keys"]

    def test_sale_lt_regular_no_anomaly(self):
        item = _make_item(sale_price=3.99, regular_price=5.99)
        s = _section2([item])
        assert s["sale_gt_regular_anomalies"] == 0

    def test_lb_kg_cross_validation_failure(self):
        # price_per_lb = 1.00 → expected price_per_kg ≈ 2.20462
        # Set price_per_kg = 2.50 → deviation > 2 %
        item = _make_item(
            price_per_lb=1.00,
            price_per_kg=2.50,
            price_observation_key="loblaws:1:SKU001:2026-04-03",
        )
        s = _section2([item])
        assert s["lb_kg_cross_validation_failures"] == 1
        assert "loblaws:1:SKU001:2026-04-03" in s["lb_kg_failure_keys"]

    def test_lb_kg_cross_validation_passes_within_tolerance(self):
        # price_per_lb = 1.00 → price_per_kg = 2.20462 exactly
        item = _make_item(price_per_lb=1.00, price_per_kg=2.20462)
        s = _section2([item])
        assert s["lb_kg_cross_validation_failures"] == 0

    def test_lb_kg_skipped_when_fields_none(self):
        item = _make_item(price_per_lb=None, price_per_kg=None)
        s = _section2([item])
        assert s["lb_kg_cross_validation_failures"] == 0

    def test_price_is_floor_counted(self):
        items = [_make_item(price_is_floor=True), _make_item(price_is_floor=False)]
        s = _section2(items)
        assert s["price_is_floor_count"] == 1

    def test_multi_buy_qty_counted(self):
        items = [_make_item(multi_buy_qty=2), _make_item()]
        s = _section2(items)
        assert s["multi_buy_qty_count"] == 1

    def test_empty_records(self):
        s = _section2([])
        assert s["total_records"] == 0
        assert s["no_sale_price_pct"] == 0.0


# ── Section 3 — Weight quality ────────────────────────────────────────────────


class TestSection3:
    def test_sanity_warning_count(self):
        # 1.89 mL is suspiciously small → triggers warning
        item = _make_item(raw_name="Product 1.89 mL")
        s = _section3([item])
        assert s["sanity_warning_count"] >= 1

    def test_no_weight_counted(self):
        items = [
            _make_item(weight_value=None, pack_count=None),
            _make_item(weight_value=500.0, weight_unit="g"),
            _make_item(weight_value=None, pack_count=6),  # pack_count set → has weight
        ]
        s = _section3(items)
        assert s["no_weight_extracted"] == 1

    def test_no_weight_pct(self):
        items = [_make_item(weight_value=None, pack_count=None)] * 3 + [_make_item(weight_value=500.0, weight_unit="g")]
        s = _section3(items)
        assert s["no_weight_pct"] == 75.0

    def test_weight_unit_distribution(self):
        items = [
            _make_item(weight_unit="g"),
            _make_item(weight_unit="g"),
            _make_item(weight_unit="kg"),
        ]
        s = _section3(items)
        assert s["weight_unit_distribution"]["g"] == 2
        assert s["weight_unit_distribution"]["kg"] == 1

    def test_empty_records(self):
        s = _section3([])
        assert s["total_records"] == 0
        assert s["no_weight_pct"] == 0.0


# ── Section 4 — Category coverage ────────────────────────────────────────────


class TestSection4:
    def test_category_l1_pct(self):
        items = [
            _make_item(category_l1="Produce"),
            _make_item(category_l1="Pantry"),
            _make_item(category_l1=None),
            _make_item(category_l1=None),
        ]
        s = _section4(items)
        assert s["with_category_l1"] == 2
        assert s["category_l1_pct"] == 50.0

    def test_food_category_pct(self):
        items = [
            _make_item(is_human_food=True, category_l1="Produce"),
            _make_item(is_human_food=True, category_l1=None),
            _make_item(is_human_food=False, category_l1="Household"),
        ]
        s = _section4(items)
        assert s["food_total"] == 2
        assert s["food_with_category_l1"] == 1
        assert s["food_category_l1_pct"] == 50.0

    def test_unmapped_to_other(self):
        items = [
            _make_item(category_l1="Other", raw_categories=["Clearance"]),
            _make_item(category_l1="Other", raw_categories=["Clearance", "Sale"]),
            _make_item(category_l1="Produce"),
        ]
        s = _section4(items)
        assert s["unmapped_to_other"] == 2
        assert "Clearance" in s["unmapped_raw_category_strings"]
        assert s["unmapped_raw_category_strings"]["Clearance"] == 2

    def test_food_category_pct_zero_food_records(self):
        items = [_make_item(is_human_food=False)]
        s = _section4(items)
        assert s["food_category_l1_pct"] == 0.0

    def test_empty_records(self):
        s = _section4([])
        assert s["category_l1_pct"] == 0.0
        assert s["food_category_l1_pct"] == 0.0


# ── Section 5 — Multi-product ─────────────────────────────────────────────────


class TestSection5:
    def test_multi_product_counts(self):
        items = [
            _make_item(is_multi_product=True),
            _make_item(is_multi_product=True),
            _make_item(parent_record_id="abc"),
            _make_item(parent_record_id="abc"),
            _make_item(parent_record_id="abc"),
            _make_item(),
        ]
        s = _section5(items)
        assert s["source_multi_product_records"] == 2
        assert s["child_records_generated"] == 3

    def test_empty_records(self):
        s = _section5([])
        assert s["source_multi_product_records"] == 0
        assert s["child_records_generated"] == 0


# ── build_report ──────────────────────────────────────────────────────────────


class TestBuildReport:
    def test_returns_all_sections(self, tmp_path):
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item()])
        report = build_report(str(tmp_path))
        assert "section1_record_counts" in report
        assert "section2_price_quality" in report
        assert "section3_weight_quality" in report
        assert "section4_category_coverage" in report
        assert "section5_multi_product" in report

    def test_metadata_fields(self, tmp_path):
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item()])
        _write_flyer_json(tmp_path, "loblaws", "1002", [_make_item()])
        report = build_report(str(tmp_path))
        assert report["input_dir"] == str(tmp_path)
        assert report["total_files_read"] == 2

    def test_empty_dir(self, tmp_path):
        report = build_report(str(tmp_path))
        assert report["section1_record_counts"]["total_records"] == 0
        assert report["total_files_read"] == 0


# ── main (CLI) ────────────────────────────────────────────────────────────────


class TestMain:
    def test_missing_input_dir_returns_1(self, tmp_path):
        rc = main(["--input-dir", str(tmp_path / "nonexistent")])
        assert rc == 1

    def test_empty_dir_exits_zero(self, tmp_path):
        rc = main(["--input-dir", str(tmp_path)])
        assert rc == 0

    def test_with_records_exits_zero(self, tmp_path):
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item()])
        rc = main(["--input-dir", str(tmp_path)])
        assert rc == 0

    def test_json_flag_writes_report(self, tmp_path):
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item()])
        rc = main(["--input-dir", str(tmp_path), "--json"])
        assert rc == 0
        report_path = tmp_path / "validation_report.json"
        assert report_path.exists()
        with open(report_path, encoding="utf-8") as fh:
            written = json.load(fh)
        assert "section1_record_counts" in written
        assert "section2_price_quality" in written

    def test_output_contains_section_headers(self, tmp_path, capsys):
        _write_flyer_json(tmp_path, "loblaws", "1001", [_make_item()])
        main(["--input-dir", str(tmp_path)])
        captured = capsys.readouterr()
        assert "1. Record Counts" in captured.out
        assert "2. Price Quality" in captured.out
        assert "3. Weight Quality" in captured.out
        assert "4. Category Coverage" in captured.out
        assert "5. Multi-Product" in captured.out
