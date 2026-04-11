"""Tests for pipeline/price_history.py."""

from __future__ import annotations

import datetime
import os

import pytest

from pipeline.price_history import (
    _compute_regular_price,
    _median,
    _week_start,
    build_price_history,
)


# ── _week_start ───────────────────────────────────────────────────────────────


class TestWeekStart:
    def test_known_thursday(self):
        # 2026-04-02 is a Thursday; Monday is 2026-03-30
        result = _week_start("2026-04-02")
        assert result == datetime.date(2026, 3, 30)

    def test_monday_unchanged(self):
        result = _week_start("2026-03-30")
        assert result == datetime.date(2026, 3, 30)

    def test_sunday_maps_to_previous_monday(self):
        # 2026-04-05 (Sunday) → 2026-03-30 (Monday)
        result = _week_start("2026-04-05")
        assert result == datetime.date(2026, 3, 30)

    def test_none_returns_none(self):
        assert _week_start(None) is None

    def test_invalid_string_returns_none(self):
        assert _week_start("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert _week_start("") is None

    def test_timestamp_prefix_accepted(self):
        # ISO timestamps have a 10-char date prefix
        result = _week_start("2026-04-02T10:30:00+00:00")
        assert result == datetime.date(2026, 3, 30)


# ── _median ───────────────────────────────────────────────────────────────────


class TestMedian:
    def test_empty_returns_none(self):
        assert _median([]) is None

    def test_single(self):
        assert _median([5.0]) == 5.0

    def test_odd_count(self):
        assert _median([1.0, 3.0, 2.0]) == 2.0

    def test_even_count(self):
        assert _median([1.0, 2.0, 3.0, 4.0]) == 2.5


# ── _compute_regular_price ────────────────────────────────────────────────────


class TestComputeRegularPrice:
    def test_priority1_observed(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[4.99, 5.49],
            chain_no_promo_sale=[3.99],
            cross_chain_no_promo=[6.0],
            category_sale_prices=[2.0, 3.0],
        )
        assert source == "observed"
        assert conf == 1.0
        assert price == 5.49  # max of observed regular prices

    def test_priority2_own_history_4plus_rows(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[],
            chain_no_promo_sale=[3.99, 3.99, 4.29, 3.99],
            cross_chain_no_promo=[6.0],
            category_sale_prices=[2.0],
        )
        assert source == "own_history"
        assert conf == 0.8
        assert price == 4.29

    def test_priority3_own_history_sparse(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[],
            chain_no_promo_sale=[3.99, 4.29],
            cross_chain_no_promo=[6.0],
            category_sale_prices=[2.0],
        )
        assert source == "own_history_sparse"
        assert conf == 0.5
        assert price == 4.29

    def test_priority3_single_no_promo_sale(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[],
            chain_no_promo_sale=[4.99],
            cross_chain_no_promo=[],
            category_sale_prices=[],
        )
        assert source == "own_history_sparse"
        assert conf == 0.5

    def test_priority4_cross_chain(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[],
            chain_no_promo_sale=[],
            cross_chain_no_promo=[5.0, 4.5],
            category_sale_prices=[2.0],
        )
        assert source == "cross_chain"
        assert conf == 0.4
        assert price == 5.0

    def test_priority5_category_median(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[],
            chain_no_promo_sale=[],
            cross_chain_no_promo=[],
            category_sale_prices=[2.0, 4.0, 3.0],
        )
        assert source == "category_median"
        assert conf == 0.2
        assert price == 3.0

    def test_priority6_none(self):
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[],
            chain_no_promo_sale=[],
            cross_chain_no_promo=[],
            category_sale_prices=[],
        )
        assert source == "none"
        assert conf == 0.0
        assert price is None

    def test_priority1_beats_all_others(self):
        # Even if everything else is available, priority 1 wins
        price, source, conf = _compute_regular_price(
            chain_no_promo_regular=[5.0],
            chain_no_promo_sale=[4.99, 4.99, 4.99, 4.99],
            cross_chain_no_promo=[6.0],
            category_sale_prices=[2.0, 3.0],
        )
        assert source == "observed"

    def test_own_history_threshold_exactly_four(self):
        # Exactly 4 rows → own_history (not sparse)
        _, source, conf = _compute_regular_price([], [1.0, 2.0, 3.0, 4.0], [], [])
        assert source == "own_history"
        assert conf == 0.8

    def test_own_history_threshold_exactly_three(self):
        # Exactly 3 rows → own_history_sparse
        _, source, conf = _compute_regular_price([], [1.0, 2.0, 3.0], [], [])
        assert source == "own_history_sparse"
        assert conf == 0.5


# ── build_price_history ───────────────────────────────────────────────────────


def _write_parquet(path: str, rows: list[dict]) -> None:
    """Write a list of dicts to a Parquet file, creating parent directories."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _write_products(path: str, rows: list[dict]) -> None:
    """Write a products.parquet dimension file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    schema = pa.schema(
        [
            ("canonical_product_id", pa.string()),
            ("canonical_name", pa.string()),
            ("canonical_brand", pa.string()),
            ("category_l1", pa.string()),
            ("category_l2", pa.string()),
            ("category_l3", pa.string()),
            ("is_food", pa.bool_()),
            ("is_human_food", pa.bool_()),
            ("weight_value", pa.float64()),
            ("weight_unit", pa.string()),
            ("match_tier", pa.string()),
            ("observation_count", pa.int64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


def _make_obs_row(
    store_chain: str = "loblaws",
    store_id: str = "1001",
    sku: str | None = "A001",
    name_en: str = "Whole Milk",
    brand: str | None = None,
    category_l3: str | None = None,
    sale_price: float | None = 3.99,
    regular_price: float | None = None,
    promo_type: str | None = "no_promo",
    flyer_valid_from: str = "2026-03-30",
) -> dict:
    return {
        "store_chain": store_chain,
        "store_id": store_id,
        "sku": sku,
        "name_en": name_en,
        "brand": brand,
        "category_l3": category_l3,
        "sale_price": sale_price,
        "regular_price": regular_price,
        "promo_type": promo_type,
        "flyer_valid_from": flyer_valid_from,
        "flyer_id": "9999",
        "source_api": "flipp",
    }


class TestBuildPriceHistory:
    def test_basic_output_shape(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir, "9999.parquet"),
            [_make_obs_row()],
        )
        out_path = str(tmp_path / "price_history.parquet")
        products_path = str(tmp_path / "products.parquet")

        n = build_price_history(
            str(tmp_path / "observations"),
            products_path,
            out_path,
        )

        assert n == 1
        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        table = dataset.to_table()
        assert "canonical_product_id" in table.schema.names
        assert "regular_price_source" in table.schema.names
        assert "price_basis_conf" in table.schema.names
        assert "sale_freq_chain" in table.schema.names
        assert "weeks_observed" in table.schema.names

    def test_idempotent(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir, "9999.parquet"),
            [_make_obs_row()],
        )
        out_path = str(tmp_path / "price_history.parquet")
        products_path = str(tmp_path / "products.parquet")

        n1 = build_price_history(
            str(tmp_path / "observations"),
            products_path,
            out_path,
        )
        n2 = build_price_history(
            str(tmp_path / "observations"),
            products_path,
            out_path,
        )

        assert n1 == n2

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        table = dataset.to_table()
        assert len(table) == n1

    def test_regular_price_cascade_priority1(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir, "9999.parquet"),
            [_make_obs_row(regular_price=5.99, promo_type="no_promo")],
        )
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        assert len(rows) == 1
        assert rows[0]["regular_price_source"] == "observed"
        assert rows[0]["price_basis_conf"] == 1.0

    def test_regular_price_own_history_4_rows(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # Four no_promo rows with sale_price (no regular_price) across 4 weeks
        obs_rows = [
            _make_obs_row(promo_type="no_promo", sale_price=3.99, flyer_valid_from=f"2026-0{m}-02")
            for m in range(1, 5)
        ]
        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), obs_rows)
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        sources = {r["regular_price_source"] for r in rows}
        assert "own_history" in sources

    def test_regular_price_own_history_sparse(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # Only 2 no_promo rows
        obs_rows = [
            _make_obs_row(promo_type="no_promo", sale_price=3.99, flyer_valid_from="2026-01-06"),
            _make_obs_row(promo_type="no_promo", sale_price=4.29, flyer_valid_from="2026-02-02"),
        ]
        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), obs_rows)
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        sources = {r["regular_price_source"] for r in rows}
        assert "own_history_sparse" in sources

    def test_regular_price_cross_chain(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # Same product at loblaws (promo only) and sobeys (no_promo).
        # We intentionally omit 'sku' so the resolver uses the probable tier,
        # matching on name fingerprint + brand + category_l3, which produces
        # the same canonical_product_id at both chains.
        shared_fields = {
            "name_en": "Whole Milk 1L",
            "brand": "Beatrice",
            "category_l3": "Milk",
            "sku": None,
        }
        obs_dir_loblaws = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        obs_dir_sobeys = str(
            tmp_path / "observations" / "store_chain=sobeys" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir_loblaws, "9001.parquet"),
            [
                _make_obs_row(
                    store_chain="loblaws",
                    promo_type="percentage_off",
                    sale_price=2.99,
                    **shared_fields,
                )
            ],
        )
        _write_parquet(
            os.path.join(obs_dir_sobeys, "9002.parquet"),
            [
                _make_obs_row(
                    store_chain="sobeys",
                    promo_type="no_promo",
                    sale_price=4.49,
                    **shared_fields,
                )
            ],
        )
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()

        # The loblaws row should have cross_chain source since it has no
        # no_promo observations of its own but the same product at sobeys does.
        sources = {r["regular_price_source"] for r in rows}
        assert "cross_chain" in sources

    def test_sale_freq_chain_in_range(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_rows = [
            _make_obs_row(promo_type="percentage_off", flyer_valid_from="2026-01-06"),
            _make_obs_row(promo_type="no_promo", flyer_valid_from="2026-01-13"),
            _make_obs_row(promo_type="percentage_off", flyer_valid_from="2026-01-20"),
            _make_obs_row(promo_type="no_promo", flyer_valid_from="2026-01-27"),
        ]
        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), obs_rows)
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        for row in rows:
            freq = row["sale_freq_chain"]
            assert 0.0 <= freq <= 1.0, f"sale_freq_chain={freq} out of [0, 1]"

    def test_sale_freq_chain_zero_when_all_no_promo(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_rows = [
            _make_obs_row(promo_type="no_promo", flyer_valid_from="2026-01-06"),
            _make_obs_row(promo_type="no_promo", flyer_valid_from="2026-01-13"),
        ]
        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), obs_rows)
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        for row in rows:
            assert row["sale_freq_chain"] == 0.0

    def test_cycle_low_le_cycle_high(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_rows = [
            _make_obs_row(sale_price=1.99, flyer_valid_from="2026-01-06"),
            _make_obs_row(sale_price=3.99, flyer_valid_from="2026-01-13"),
            _make_obs_row(sale_price=2.49, flyer_valid_from="2026-01-20"),
        ]
        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), obs_rows)
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        for row in rows:
            low = row["cycle_low_52w"]
            high = row["cycle_high_52w"]
            if low is not None and high is not None:
                assert low <= high, f"cycle_low_52w={low} > cycle_high_52w={high}"

    def test_no_history_gives_none_conf(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # A product with only promotional rows (no no_promo), no cross-chain,
        # no products.parquet → should fall to "none" / 0.0
        obs_rows = [
            _make_obs_row(
                promo_type="percentage_off",
                sale_price=2.00,
                flyer_valid_from="2026-03-30",
                # Give it a unique SKU so no cross-chain match possible
                sku="UNIQUE_XYZ_001",
            )
        ]
        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), obs_rows)
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        assert len(rows) == 1
        row = rows[0]
        assert row["regular_price_source"] == "none"
        assert row["price_basis_conf"] == 0.0

    def test_weeks_observed_counts_all_chains(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # Same product (matched via probable tier — no sku, same name/brand/category)
        # seen at loblaws in week 1 and sobeys in week 5.
        shared_fields = {
            "name_en": "Cheddar Cheese",
            "brand": "Black Diamond",
            "category_l3": "Cheese",
            "sku": None,
        }
        obs_dir_loblaws = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=1"
        )
        obs_dir_sobeys = str(
            tmp_path / "observations" / "store_chain=sobeys" / "year=2026" / "week=5"
        )
        _write_parquet(
            os.path.join(obs_dir_loblaws, "9001.parquet"),
            [_make_obs_row(store_chain="loblaws", flyer_valid_from="2025-12-29", **shared_fields)],
        )
        _write_parquet(
            os.path.join(obs_dir_sobeys, "9002.parquet"),
            [_make_obs_row(store_chain="sobeys", flyer_valid_from="2026-01-26", **shared_fields)],
        )
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        # Both rows refer to the same canonical product seen in 2 distinct weeks
        # (one at each chain), so weeks_observed should be 2 for every row.
        for row in rows:
            assert row["weeks_observed"] == 2

    def test_chain_count(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # Same product (probable tier — no sku) at three different chains.
        shared_fields = {
            "name_en": "Orange Juice",
            "brand": "Tropicana",
            "category_l3": "Juice",
            "sku": None,
        }
        for chain, flyer_id in [("loblaws", "1001"), ("sobeys", "1002"), ("metro_on", "1003")]:
            obs_dir = str(
                tmp_path / "observations" / f"store_chain={chain}" / "year=2026" / "week=14"
            )
            _write_parquet(
                os.path.join(obs_dir, f"{flyer_id}.parquet"),
                [_make_obs_row(store_chain=chain, flyer_valid_from="2026-04-01", **shared_fields)],
            )
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        for row in rows:
            assert row["chain_count"] == 3

    def test_category_sibling_count_uses_products_dim(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds
        from pipeline.product_resolver import _resolve_record

        # Two observations with the same category_l3; both have a sku so they
        # each get their own strict canonical_product_id.
        row_a = _make_obs_row(sku="MILK001", name_en="Milk 2%", sale_price=3.50)
        row_b = _make_obs_row(sku="MILK002", name_en="Milk Skim", sale_price=3.25)

        cid_a, _, _ = _resolve_record(row_a)
        cid_b, _, _ = _resolve_record(row_b)

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(os.path.join(obs_dir, "9999.parquet"), [row_a, row_b])

        products_path = str(tmp_path / "products.parquet")
        _write_products(
            products_path,
            [
                {
                    "canonical_product_id": cid_a,
                    "canonical_name": "Milk 2%",
                    "canonical_brand": None,
                    "category_l1": "Food",
                    "category_l2": "Dairy",
                    "category_l3": "Milk",
                    "is_food": True,
                    "is_human_food": True,
                    "weight_value": None,
                    "weight_unit": None,
                    "match_tier": "strict",
                    "observation_count": 1,
                },
                {
                    "canonical_product_id": cid_b,
                    "canonical_name": "Milk Skim",
                    "canonical_brand": None,
                    "category_l1": "Food",
                    "category_l2": "Dairy",
                    "category_l3": "Milk",
                    "is_food": True,
                    "is_human_food": True,
                    "weight_value": None,
                    "weight_unit": None,
                    "match_tier": "strict",
                    "observation_count": 1,
                },
            ],
        )

        out_path = str(tmp_path / "price_history.parquet")
        build_price_history(
            str(tmp_path / "observations"),
            products_path,
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()
        # Both products are in category "Milk" and each has a sale_price,
        # so category_sibling_count should be 2 for each row.
        for row in rows:
            assert row["category_sibling_count"] == 2

    def test_empty_observations_dir(self, tmp_path):
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "observations")
        os.makedirs(obs_dir, exist_ok=True)
        out_path = str(tmp_path / "price_history.parquet")

        n = build_price_history(
            obs_dir,
            str(tmp_path / "products.parquet"),
            out_path,
        )
        assert n == 0

    def test_rows_missing_flyer_valid_from_are_skipped(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        row_with_date = _make_obs_row(flyer_valid_from="2026-03-30")
        row_no_date = {**_make_obs_row(), "flyer_valid_from": None, "sku": "B999"}

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir, "9999.parquet"),
            [row_with_date, row_no_date],
        )
        out_path = str(tmp_path / "price_history.parquet")

        n = build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        # Only the row with a valid date should produce a feature row
        assert n == 1

    def test_52_week_window_excludes_older_data(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        # Two observations: one in trailing 52 weeks, one older (> 52 weeks ago)
        target_week = datetime.date(2026, 3, 30)
        in_window_date = "2025-12-01"  # ~17 weeks before target
        out_of_window_date = "2024-03-01"  # > 52 weeks before target

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir, "9999.parquet"),
            [
                _make_obs_row(
                    sale_price=2.99,
                    promo_type="percentage_off",
                    flyer_valid_from=in_window_date,
                ),
                _make_obs_row(
                    sale_price=9.99,
                    promo_type="percentage_off",
                    flyer_valid_from=out_of_window_date,
                ),
            ],
        )
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        rows = dataset.to_table().to_pylist()

        # Compute the Monday of the in-window observation's week using the
        # same helper as the implementation under test.
        in_window_monday = _week_start(in_window_date)

        # For every output row that falls within the trailing 52-week window of
        # target_week, cycle_high_52w must not include the out-of-window price.
        for row in rows:
            ws = datetime.date.fromisoformat(row["week_start"])
            if ws in (target_week, in_window_monday):
                if row["cycle_high_52w"] is not None:
                    assert row["cycle_high_52w"] <= 3.0 + 0.01, (
                        f"cycle_high_52w {row['cycle_high_52w']} should not include "
                        "out-of-window data"
                    )

    def test_output_schema_columns(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        obs_dir = str(
            tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14"
        )
        _write_parquet(
            os.path.join(obs_dir, "9999.parquet"),
            [_make_obs_row()],
        )
        out_path = str(tmp_path / "price_history.parquet")

        build_price_history(
            str(tmp_path / "observations"),
            str(tmp_path / "products.parquet"),
            out_path,
        )

        dataset = ds.dataset(out_path, format="parquet", partitioning="hive")
        schema_names = set(dataset.schema.names)
        expected = {
            "canonical_product_id",
            "week_start",
            "regular_price_estimated",
            "regular_price_source",
            "price_basis_conf",
            "sale_freq_chain",
            "sale_freq_market",
            "cycle_low_52w",
            "cycle_high_52w",
            "weeks_observed",
            "chain_count",
            "category_sibling_count",
        }
        missing = expected - schema_names
        assert not missing, f"Missing columns: {missing}"
