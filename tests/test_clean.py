"""Tests for clean.py — pipeline orchestrator and output writer."""

from __future__ import annotations

import json
import os

import pytest

from pipeline.clean import _apply_pipeline, _is_up_to_date, _write_flyer_json, _write_parquet, main
from pipeline.schema import FlyerItem


# ── Fixture helpers ───────────────────────────────────────────────────────────


def _make_flipp_flyer(publication_id: str = "1001") -> dict:
    return {
        "fetched_on": "2026-04-03",
        "publication_id": publication_id,
        "publication_meta": {
            "id": int(publication_id),
            "valid_from": "2026-04-03T00:00:00-04:00",
            "valid_to": "2026-04-09T23:59:59-04:00",
        },
        "products": [
            {
                "id": 1,
                "name": "MAPLE LEAF BACON",
                "sku": "SKU001",
                "price_text": "3.99",
                "item_type": 1,
                "item_categories": {
                    "l1": {"category_name": "Food, Beverages & Tobacco"},
                    "l2": {"category_name": "Meat"},
                },
            }
        ],
    }


def _make_metro_flyer(job: str = "82000", store_id: int = 100) -> dict:
    return {
        "fetched_on": "2026-04-03",
        "job": job,
        "store_id": store_id,
        "products": [
            {
                "sku": "99001",
                "productEn": "Metro Test Product",
                "salePrice": "5.00",
                "actionType": "Product",
                "mainCategoryEn": "Fruit and Vegetables",
            }
        ],
    }


def _write_json(path: str, data: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def _minimal_item(**kwargs) -> FlyerItem:
    defaults = {
        "source_api": "flipp",
        "store_chain": "loblaws",
        "flyer_id": "1001",
        "fetched_on": "2026-04-03",
        "raw_name": "Test Product",
        "sale_price": 3.99,
    }
    defaults.update(kwargs)
    return FlyerItem(**defaults)


# ── _apply_pipeline ───────────────────────────────────────────────────────────


class TestApplyPipeline:
    def test_returns_list_of_flyer_items(self):
        item = _minimal_item()
        result = _apply_pipeline(item)
        assert isinstance(result, list)
        assert len(result) >= 1
        assert all(isinstance(r, FlyerItem) for r in result)

    def test_cleans_name(self):
        item = _minimal_item(raw_name="MAPLE LEAF BACON")
        result = _apply_pipeline(item)
        assert result[0].name_en == "Maple Leaf Bacon"

    def test_sets_promo_type(self):
        item = _minimal_item(promo_details="SAVE 20%")
        result = _apply_pipeline(item)
        assert result[0].promo_type == "percentage_off"

    def test_extracts_weight(self):
        item = _minimal_item(raw_name="Product 500 g")
        result = _apply_pipeline(item)
        assert result[0].weight_value == 500.0
        assert result[0].weight_unit == "g"

    def test_multi_buy_from_pre_price_text(self):
        item = _minimal_item(sale_price=8.0, pre_price_text="2/")
        result = _apply_pipeline(item)
        assert result[0].multi_buy_qty == 2
        assert result[0].multi_buy_total == 8.0
        assert result[0].sale_price == pytest.approx(4.0)

    def test_maps_flipp_category(self):
        item = _minimal_item(
            source_api="flipp",
            category_l1="Food, Beverages & Tobacco",
            category_l2="Beverages",
        )
        result = _apply_pipeline(item)
        assert result[0].category_l1 == "Beverages"

    def test_maps_metro_category(self):
        item = _minimal_item(
            source_api="metro",
            category_l1="Fruit and Vegetables",
        )
        result = _apply_pipeline(item)
        assert result[0].category_l1 == "Produce"

    def test_sets_food_flags_for_food(self):
        item = _minimal_item(source_api="metro", category_l1="Fruit and Vegetables")
        result = _apply_pipeline(item)
        assert result[0].is_food is True
        assert result[0].is_human_food is True

    def test_sets_food_flags_false_for_household(self):
        item = _minimal_item(source_api="flipp", category_l1="Health & Beauty")
        result = _apply_pipeline(item)
        assert result[0].is_food is False
        assert result[0].is_human_food is False

    def test_splits_multi_product(self):
        item = _minimal_item(
            raw_name="CHESTNUTS, 85 G OR CROWN CORN, 340 G",
            price_observation_key="loblaws:1:sku:2026-04-03",
        )
        result = _apply_pipeline(item)
        assert len(result) >= 2
        assert result[0].is_multi_product is True

    def test_handles_none_sale_price(self):
        item = _minimal_item(sale_price=None)
        result = _apply_pipeline(item)
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_handles_none_raw_name(self):
        item = _minimal_item(raw_name=None)
        result = _apply_pipeline(item)
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_unknown_category_keeps_existing(self):
        item = _minimal_item(
            source_api="flipp",
            category_l1="Unknown Category XYZ",
            category_l2=None,
        )
        result = _apply_pipeline(item)
        # Unmapped category should be preserved (not overwritten with None)
        assert result[0].category_l1 == "Unknown Category XYZ"


# ── _is_up_to_date ────────────────────────────────────────────────────────────


class TestIsUpToDate:
    def test_returns_false_when_file_missing(self, tmp_path):
        path = str(tmp_path / "nonexistent.json")
        assert _is_up_to_date(path, "2026-04-03") is False

    def test_returns_false_when_fetched_on_none(self, tmp_path):
        path = str(tmp_path / "f.json")
        _write_json(path, {"generated_at": "2026-04-03T12:00:00+00:00"})
        assert _is_up_to_date(path, None) is False

    def test_returns_true_when_dates_match(self, tmp_path):
        path = str(tmp_path / "f.json")
        _write_json(path, {"generated_at": "2026-04-03T12:00:00+00:00"})
        assert _is_up_to_date(path, "2026-04-03") is True

    def test_returns_false_when_dates_differ(self, tmp_path):
        path = str(tmp_path / "f.json")
        _write_json(path, {"generated_at": "2026-04-02T12:00:00+00:00"})
        assert _is_up_to_date(path, "2026-04-03") is False

    def test_returns_false_on_invalid_json(self, tmp_path):
        path_obj = tmp_path / "broken.json"
        path_obj.write_text("not json")
        assert _is_up_to_date(str(path_obj), "2026-04-03") is False

    def test_returns_false_when_generated_at_missing(self, tmp_path):
        path = str(tmp_path / "f.json")
        _write_json(path, {"record_count": 5})
        assert _is_up_to_date(path, "2026-04-03") is False


# ── _write_flyer_json ─────────────────────────────────────────────────────────


class TestWriteFlyerJson:
    def test_writes_correct_structure(self, tmp_path):
        out = str(tmp_path / "food_basics" / "82596.json")
        records = [_minimal_item(store_chain="food_basics", flyer_id="82596")]
        _write_flyer_json(out, "82596", "food_basics", "2026-04-03", records)

        with open(out, encoding="utf-8") as fh:
            data = json.load(fh)

        assert data["flyer_id"] == "82596"
        assert data["store_chain"] == "food_basics"
        assert "generated_at" in data
        assert data["record_count"] == 1
        assert len(data["records"]) == 1

    def test_creates_parent_directories(self, tmp_path):
        out = str(tmp_path / "a" / "b" / "c" / "flyer.json")
        _write_flyer_json(out, "1", "store", "2026-04-03", [])
        assert os.path.exists(out)

    def test_generated_at_is_iso_string(self, tmp_path):
        out = str(tmp_path / "f.json")
        _write_flyer_json(out, "1", "store", "2026-04-03", [])
        with open(out, encoding="utf-8") as fh:
            data = json.load(fh)
        generated_at = data["generated_at"]
        # Should be parseable as an ISO datetime
        import datetime as dt
        dt.datetime.fromisoformat(generated_at)

    def test_record_count_reflects_record_list(self, tmp_path):
        out = str(tmp_path / "f.json")
        records = [_minimal_item() for _ in range(5)]
        _write_flyer_json(out, "1", "store", "2026-04-03", records)
        with open(out, encoding="utf-8") as fh:
            data = json.load(fh)
        assert data["record_count"] == 5
        assert len(data["records"]) == 5


# ── _write_parquet ────────────────────────────────────────────────────────────


class TestWriteParquet:
    def test_writes_readable_parquet(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        out = str(tmp_path / "all_flyers.parquet")
        records = [_minimal_item(flyer_id=str(i)) for i in range(3)]
        _write_parquet(out, records)

        table = pq.read_table(out)
        assert table.num_rows == 3

    def test_loadable_with_pandas(self, tmp_path):
        pytest.importorskip("pyarrow")
        pd = pytest.importorskip("pandas")

        out = str(tmp_path / "all_flyers.parquet")
        records = [_minimal_item(flyer_id=str(i)) for i in range(2)]
        _write_parquet(out, records)

        df = pd.read_parquet(out)
        assert len(df) == 2

    def test_no_op_when_empty(self, tmp_path):
        pytest.importorskip("pyarrow")
        out = str(tmp_path / "all_flyers.parquet")
        _write_parquet(out, [])
        assert not os.path.exists(out)

    def test_list_fields_serialised_as_json_strings(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        item = _minimal_item()
        item = item.model_copy(
            update={"multi_product_variants": ["A", "B"], "raw_categories": ["cat1"]}
        )
        out = str(tmp_path / "out.parquet")
        _write_parquet(out, [item])

        table = pq.read_table(out)
        row = table.to_pydict()
        # List fields are stored as JSON strings
        assert isinstance(row["multi_product_variants"][0], str)
        assert row["multi_product_variants"][0] == '["A", "B"]'


# ── main() CLI ────────────────────────────────────────────────────────────────


class TestMain:
    def _setup_data(self, tmp: str, chain: str, flyer_file: dict, pub_id: str) -> None:
        _write_json(
            os.path.join(tmp, chain, "stores.json"),
            {"1000": {"name": "Test", "province": "ON"}},
        )
        _write_json(
            os.path.join(tmp, chain, "store_flyers.json"),
            {"1000": [{"id": int(pub_id)}]},
        )
        _write_json(
            os.path.join(tmp, chain, "flyers", f"{pub_id}.json"),
            flyer_file,
        )

    def test_dry_run_prints_count(self, tmp_path, capsys):
        out_dir = str(tmp_path / "cleaned")
        ret = main(["--store", "loblaws", "--dry-run", "--output-dir", out_dir])
        assert ret == 0
        assert not os.path.exists(out_dir)

    def test_dry_run_exit_zero(self, tmp_path, capsys):
        data_dir = str(tmp_path / "data")
        out_dir = str(tmp_path / "out")
        # Empty data dir — dry-run should still return 0
        os.makedirs(data_dir)
        ret = main(["--dry-run", "--output-dir", out_dir])
        assert ret == 0

    def test_main_with_no_data_returns_zero(self, tmp_path):
        out_dir = str(tmp_path / "out")
        ret = main(["--output-dir", out_dir])
        assert ret == 0

    def test_dry_run_no_files_written(self, tmp_path):
        out_dir = str(tmp_path / "out")
        ret = main(["--dry-run", "--output-dir", out_dir])
        assert ret == 0
        assert not os.path.exists(out_dir)

    def test_force_flag_accepted(self, tmp_path):
        out_dir = str(tmp_path / "out")
        ret = main(["--force", "--dry-run", "--output-dir", out_dir])
        assert ret == 0

    def test_store_flag_accepted(self, tmp_path):
        out_dir = str(tmp_path / "out")
        ret = main(["--store", "food_basics", "--dry-run", "--output-dir", out_dir])
        assert ret == 0


# ── Integration: end-to-end via iter_flyers ───────────────────────────────────


class TestEndToEnd:
    """Integration tests that drive the full pipeline with a real temp data dir."""

    def _make_data_dir(
        self, base: str, chain: str, flyer: dict, flyer_id: str
    ) -> tuple[str, str]:
        data_dir = os.path.join(base, "data")
        _write_json(
            os.path.join(data_dir, chain, "stores.json"),
            {"1000": {"name": "Test", "province": "ON"}},
        )
        _write_json(
            os.path.join(data_dir, chain, "store_flyers.json"),
            {"1000": [{"id": int(flyer_id) if flyer_id.isdigit() else flyer_id}]},
        )
        _write_json(
            os.path.join(data_dir, chain, "flyers", f"{flyer_id}.json"),
            flyer,
        )
        return data_dir

    def test_flipp_flyer_produces_json(self, tmp_path):
        data_dir = self._make_data_dir(
            str(tmp_path), "loblaws", _make_flipp_flyer("1001"), "1001"
        )
        out_dir = str(tmp_path / "cleaned")

        from pipeline.clean import _apply_pipeline
        from pipeline.load_raw import iter_flyers

        # Run through the full pipeline manually
        for store_chain, flyer_id, fetched_on, items in iter_flyers(
            data_dir=data_dir, store="loblaws"
        ):
            processed = []
            for item in items:
                processed.extend(_apply_pipeline(item))
            out_path = os.path.join(out_dir, store_chain, f"{flyer_id}.json")
            from pipeline.clean import _write_flyer_json
            _write_flyer_json(out_path, flyer_id, store_chain, fetched_on, processed)

        out_path = os.path.join(out_dir, "loblaws", "1001.json")
        assert os.path.exists(out_path)

        with open(out_path, encoding="utf-8") as fh:
            data = json.load(fh)

        assert data["flyer_id"] == "1001"
        assert data["store_chain"] == "loblaws"
        assert data["record_count"] >= 1
        assert len(data["records"]) >= 1

    def test_metro_flyer_produces_json(self, tmp_path):
        data_dir = self._make_data_dir(
            str(tmp_path), "food_basics", _make_metro_flyer("82000", 100), "82000"
        )
        out_dir = str(tmp_path / "cleaned")

        from pipeline.load_raw import iter_flyers

        for store_chain, flyer_id, fetched_on, items in iter_flyers(
            data_dir=data_dir, store="food_basics"
        ):
            processed = []
            for item in items:
                processed.extend(_apply_pipeline(item))
            out_path = os.path.join(out_dir, store_chain, f"{flyer_id}.json")
            from pipeline.clean import _write_flyer_json
            _write_flyer_json(out_path, flyer_id, store_chain, fetched_on, processed)

        out_path = os.path.join(out_dir, "food_basics", "82000.json")
        assert os.path.exists(out_path)

        with open(out_path, encoding="utf-8") as fh:
            data = json.load(fh)

        assert data["store_chain"] == "food_basics"
        assert data["record_count"] == 1
        assert data["records"][0]["category_l1"] == "Produce"
        assert data["records"][0]["is_food"] is True

    def test_idempotency_skips_up_to_date_file(self, tmp_path):
        data_dir = self._make_data_dir(
            str(tmp_path), "loblaws", _make_flipp_flyer("1001"), "1001"
        )
        out_dir = str(tmp_path / "cleaned")

        from pipeline.load_raw import iter_flyers

        write_count = [0]

        def run():
            for store_chain, flyer_id, fetched_on, items in iter_flyers(
                data_dir=data_dir
            ):
                processed = []
                for item in items:
                    processed.extend(_apply_pipeline(item))
                out_path = os.path.join(out_dir, store_chain, f"{flyer_id}.json")
                from pipeline.clean import _is_up_to_date, _write_flyer_json
                if not _is_up_to_date(out_path, fetched_on):
                    _write_flyer_json(out_path, flyer_id, store_chain, fetched_on, processed)
                    write_count[0] += 1

        run()
        assert write_count[0] == 1

        write_count[0] = 0
        run()
        assert write_count[0] == 0  # second run should skip

    def test_parquet_is_written_and_loadable(self, tmp_path):
        pytest.importorskip("pyarrow")
        pd = pytest.importorskip("pandas")

        data_dir = self._make_data_dir(
            str(tmp_path), "loblaws", _make_flipp_flyer("1001"), "1001"
        )
        out_dir = str(tmp_path / "cleaned")

        from pipeline.load_raw import iter_flyers

        all_records = []
        for store_chain, flyer_id, fetched_on, items in iter_flyers(data_dir=data_dir):
            for item in items:
                all_records.extend(_apply_pipeline(item))

        parquet_path = os.path.join(out_dir, "all_flyers.parquet")
        _write_parquet(parquet_path, all_records)

        df = pd.read_parquet(parquet_path)
        assert len(df) >= 1
        assert "store_chain" in df.columns
        assert "flyer_id" in df.columns
