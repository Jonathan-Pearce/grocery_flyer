"""Tests for pipeline/product_resolver.py."""

from __future__ import annotations

import os

import pytest

from pipeline.product_resolver import (
    _canonical_id,
    _name_fingerprint,
    _resolve_record,
    resolve_products,
)


# ── _name_fingerprint ─────────────────────────────────────────────────────────


class TestNameFingerprint:
    def test_none_inputs_return_none(self):
        assert _name_fingerprint(None) is None
        assert _name_fingerprint(None, None) is None

    def test_empty_string_returns_none(self):
        assert _name_fingerprint("") is None
        assert _name_fingerprint("   ") is None

    def test_returns_12_hex_chars(self):
        result = _name_fingerprint("Whole Milk 1L")
        assert result is not None
        assert len(result) == 12
        assert all(c in "0123456789abcdef" for c in result)

    def test_case_insensitive(self):
        assert _name_fingerprint("Whole Milk") == _name_fingerprint("WHOLE MILK")
        assert _name_fingerprint("Whole Milk") == _name_fingerprint("whole milk")

    def test_size_tokens_stripped(self):
        # "500g" should be stripped so "Bread 500g" ≈ "Bread"
        fp1 = _name_fingerprint("Bread 500g")
        fp2 = _name_fingerprint("Bread")
        assert fp1 == fp2

    def test_various_size_units_stripped(self):
        fp_base = _name_fingerprint("Orange Juice")
        for token in ("1L", "1l", "500mL", "500ml", "2kg", "1.5kg", "16oz", "2lb"):
            fp = _name_fingerprint(f"Orange Juice {token}")
            assert fp == fp_base, f"Token '{token}' was not stripped"

    def test_multipack_stripped(self):
        fp1 = _name_fingerprint("Chips 2pk")
        fp2 = _name_fingerprint("Chips")
        assert fp1 == fp2

    def test_pkg_stripped(self):
        fp1 = _name_fingerprint("Bread pkg")
        fp2 = _name_fingerprint("Bread")
        assert fp1 == fp2

    def test_token_order_independent(self):
        # Tokens are sorted, so order of words doesn't matter
        assert _name_fingerprint("Milk Whole") == _name_fingerprint("Whole Milk")

    def test_punctuation_stripped(self):
        assert _name_fingerprint("Milk, Whole") == _name_fingerprint("Milk Whole")

    def test_fallback_to_name_fr(self):
        # Should use name_fr when name_en is None
        result = _name_fingerprint(None, "Lait Entier")
        assert result is not None
        assert len(result) == 12

    def test_name_en_preferred_over_name_fr(self):
        fp_en = _name_fingerprint("Whole Milk", "Lait Entier")
        fp_en_only = _name_fingerprint("Whole Milk", None)
        assert fp_en == fp_en_only

    def test_deterministic(self):
        # Same input → same output on every call
        name = "Cheddar Cheese 400g"
        assert _name_fingerprint(name) == _name_fingerprint(name)


# ── _canonical_id ─────────────────────────────────────────────────────────────


class TestCanonicalId:
    def test_returns_12_hex_chars(self):
        result = _canonical_id("strict", "loblaws", "12345")
        assert len(result) == 12
        assert all(c in "0123456789abcdef" for c in result)

    def test_none_treated_as_empty(self):
        result = _canonical_id("strict", None, "abc")
        assert result is not None

    def test_different_inputs_different_ids(self):
        a = _canonical_id("strict", "loblaws", "123")
        b = _canonical_id("strict", "sobeys", "123")
        assert a != b

    def test_same_inputs_same_id(self):
        assert _canonical_id("strict", "loblaws", "123") == _canonical_id("strict", "loblaws", "123")


# ── _resolve_record ───────────────────────────────────────────────────────────


class TestResolveRecord:
    # ── Tier 1: strict ────────────────────────────────────────────────────────

    def test_strict_when_chain_and_sku_present(self):
        record = {"store_chain": "loblaws", "sku": "12345"}
        cid, tier, conf = _resolve_record(record)
        assert tier == "strict"
        assert conf == 1.0
        assert len(cid) == 12

    def test_strict_id_is_deterministic(self):
        record = {"store_chain": "loblaws", "sku": "12345"}
        cid1, _, _ = _resolve_record(record)
        cid2, _, _ = _resolve_record(record)
        assert cid1 == cid2

    def test_strict_different_chains_different_ids(self):
        r1 = {"store_chain": "loblaws", "sku": "12345"}
        r2 = {"store_chain": "sobeys", "sku": "12345"}
        cid1, _, _ = _resolve_record(r1)
        cid2, _, _ = _resolve_record(r2)
        assert cid1 != cid2

    def test_strict_no_collision_across_chain_sku_pairs(self):
        # Acceptance criterion: strict-matched products never collide across
        # different (store_chain, sku) pairs.
        pairs = [
            ("loblaws", "001"),
            ("loblaws", "002"),
            ("sobeys", "001"),
            ("no_frills", "999"),
        ]
        ids = [_resolve_record({"store_chain": c, "sku": s})[0] for c, s in pairs]
        assert len(set(ids)) == len(pairs)

    def test_strict_skips_when_sku_empty_string(self):
        record = {"store_chain": "loblaws", "sku": "", "name_en": "Milk", "brand": "Beatrice", "weight_unit": "L", "category_l3": "Dairy"}
        _, tier, _ = _resolve_record(record)
        assert tier != "strict"

    def test_strict_skips_when_chain_none(self):
        record = {"store_chain": None, "sku": "12345", "name_en": "Milk", "brand": "Beatrice", "weight_unit": "L", "category_l3": "Dairy"}
        _, tier, _ = _resolve_record(record)
        assert tier != "strict"

    # ── Tier 2: probable ──────────────────────────────────────────────────────

    def test_probable_with_all_three_signals(self):
        record = {
            "store_chain": None,
            "sku": None,
            "name_en": "Cheddar Cheese",
            "brand": "Armstrong",
            "weight_unit": "g",
            "category_l3": "Cheese",
        }
        _, tier, conf = _resolve_record(record)
        assert tier == "probable"
        assert conf == 0.6

    def test_probable_with_exactly_two_signals(self):
        record = {
            "store_chain": None,
            "sku": None,
            "name_en": "Cheddar Cheese",
            "brand": "Armstrong",
            "weight_unit": "g",
            "category_l3": None,
        }
        _, tier, conf = _resolve_record(record)
        assert tier == "probable"
        assert conf == 0.6

    def test_probable_requires_at_least_two_signals(self):
        # Only one signal → must fall through to category
        record = {
            "store_chain": None,
            "sku": None,
            "name_en": "Cheddar Cheese",
            "brand": "Armstrong",
            "weight_unit": None,
            "category_l3": None,
        }
        _, tier, _ = _resolve_record(record)
        assert tier == "category"

    def test_probable_requires_fingerprint(self):
        # No name → can't compute fingerprint → must fall through
        record = {
            "store_chain": None,
            "sku": None,
            "name_en": None,
            "name_fr": None,
            "brand": "Armstrong",
            "weight_unit": "g",
            "category_l3": "Cheese",
        }
        _, tier, _ = _resolve_record(record)
        assert tier == "category"

    def test_probable_same_fingerprint_same_signals_same_id(self):
        base = {
            "store_chain": None,
            "sku": None,
            "name_en": "Cheddar Cheese",
            "brand": "Armstrong",
            "weight_unit": "g",
            "category_l3": "Cheese",
        }
        cid1, _, _ = _resolve_record(base)
        cid2, _, _ = _resolve_record(base)
        assert cid1 == cid2

    def test_probable_different_brand_different_id(self):
        r1 = {
            "name_en": "Cheddar Cheese",
            "brand": "Armstrong",
            "weight_unit": "g",
            "category_l3": "Cheese",
        }
        r2 = {
            "name_en": "Cheddar Cheese",
            "brand": "Kraft",
            "weight_unit": "g",
            "category_l3": "Cheese",
        }
        cid1, _, _ = _resolve_record(r1)
        cid2, _, _ = _resolve_record(r2)
        assert cid1 != cid2

    # ── Tier 3: category fallback ─────────────────────────────────────────────

    def test_category_fallback_used_as_last_resort(self):
        record = {"name_en": "Mystery Item"}
        _, tier, conf = _resolve_record(record)
        assert tier == "category"
        assert conf == 0.2

    def test_category_uses_category_l3_first(self):
        r = {"category_l3": "Cheese", "category_l1": "Food"}
        cid, tier, _ = _resolve_record(r)
        assert tier == "category"
        expected_cid = _canonical_id("category", "Cheese")
        assert cid == expected_cid

    def test_category_falls_back_to_l1(self):
        r = {"category_l3": None, "category_l1": "Food"}
        cid, tier, _ = _resolve_record(r)
        assert tier == "category"
        expected_cid = _canonical_id("category", "Food")
        assert cid == expected_cid

    def test_category_unknown_when_no_categories(self):
        r = {}
        cid, tier, _ = _resolve_record(r)
        assert tier == "category"
        expected_cid = _canonical_id("category", "unknown")
        assert cid == expected_cid


# ── resolve_products ──────────────────────────────────────────────────────────


def _write_parquet(path: str, rows: list[dict]) -> None:
    """Write a list of dicts to a Parquet file, creating parent dirs."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


class TestResolveProducts:
    def test_returns_empty_mapping_when_no_observations(self, tmp_path):
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "observations")
        os.makedirs(obs_dir)
        out_path = str(tmp_path / "db" / "dimensions" / "products.parquet")

        result = resolve_products(obs_dir, out_path)

        assert result == {}
        assert os.path.exists(out_path)

    def test_products_parquet_written(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        obs_dir = str(tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {
                    "store_chain": "loblaws",
                    "sku": "A001",
                    "name_en": "Whole Milk 1L",
                    "is_food": True,
                    "is_human_food": True,
                }
            ],
        )

        resolve_products(str(tmp_path / "observations"), out_path)

        assert os.path.exists(out_path)
        table = pq.read_table(out_path)
        assert "canonical_product_id" in table.schema.names

    def test_every_observation_gets_one_id(self, tmp_path):
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {"store_chain": "loblaws", "sku": "A001", "name_en": "Milk"},
                {"store_chain": "loblaws", "sku": "A002", "name_en": "Bread"},
                {"name_en": "Eggs", "brand": "Farm Fresh", "weight_unit": "count", "category_l3": "Eggs"},
            ],
        )

        result = resolve_products(str(tmp_path / "observations"), out_path)

        # 3 observations → 3 observation keys
        assert len(result) == 3
        # Each maps to a non-empty string
        for obs_key, cid in result.items():
            assert isinstance(cid, str) and cid

    def test_strict_no_collision_across_chain_sku(self, tmp_path):
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "observations" / "store_chain=x" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {"store_chain": "loblaws", "sku": "AAA", "price_observation_key": "loblaws:1:AAA:2026-04-01"},
                {"store_chain": "sobeys", "sku": "AAA", "price_observation_key": "sobeys:1:AAA:2026-04-01"},
            ],
        )

        result = resolve_products(str(tmp_path / "observations"), out_path)

        cid_loblaws = result["loblaws:1:AAA:2026-04-01"]
        cid_sobeys = result["sobeys:1:AAA:2026-04-01"]
        assert cid_loblaws != cid_sobeys

    def test_idempotent(self, tmp_path):
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {"store_chain": "loblaws", "sku": "A001", "name_en": "Milk"},
            ],
        )

        result1 = resolve_products(str(tmp_path / "observations"), out_path)
        result2 = resolve_products(str(tmp_path / "observations"), out_path)
        assert result1 == result2

    def test_probable_matches_grouped(self, tmp_path):
        """Two observations with same fingerprint + 2 matching signals → same ID."""
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        obs_dir = str(tmp_path / "observations" / "store_chain=x" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {
                    "name_en": "Cheddar Cheese",
                    "brand": "Armstrong",
                    "weight_unit": "g",
                    "category_l3": "Cheese",
                    "price_observation_key": "k1",
                },
                {
                    "name_en": "Cheddar Cheese",
                    "brand": "Armstrong",
                    "weight_unit": "g",
                    "category_l3": "Cheese",
                    "price_observation_key": "k2",
                },
            ],
        )

        result = resolve_products(str(tmp_path / "observations"), out_path)

        assert result["k1"] == result["k2"]

        # products.parquet should have exactly one row for these two observations
        table = pq.read_table(out_path)
        rows = table.to_pylist()
        matching = [r for r in rows if r["match_tier"] == "probable"]
        assert len(matching) == 1
        assert matching[0]["observation_count"] == 2

    def test_products_parquet_schema(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        obs_dir = str(tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [{"store_chain": "loblaws", "sku": "A1", "name_en": "Milk", "is_food": True}],
        )

        resolve_products(str(tmp_path / "observations"), out_path)

        table = pq.read_table(out_path)
        expected_cols = {
            "canonical_product_id",
            "canonical_name",
            "canonical_brand",
            "category_l1",
            "category_l2",
            "category_l3",
            "is_food",
            "is_human_food",
            "weight_value",
            "weight_unit",
            "match_tier",
            "observation_count",
        }
        assert expected_cols.issubset(set(table.schema.names))

    def test_canonical_name_is_most_common(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        obs_dir = str(tmp_path / "observations" / "store_chain=x" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {"store_chain": "loblaws", "sku": "A1", "name_en": "Whole Milk"},
                {"store_chain": "loblaws", "sku": "A1", "name_en": "Whole Milk"},
                {"store_chain": "loblaws", "sku": "A1", "name_en": "Milk Whole"},
            ],
        )

        resolve_products(str(tmp_path / "observations"), out_path)

        table = pq.read_table(out_path)
        rows = table.to_pylist()
        assert len(rows) == 1
        assert rows[0]["canonical_name"] == "Whole Milk"
        assert rows[0]["observation_count"] == 3

    def test_observation_count_correct(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        obs_dir = str(tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {"store_chain": "loblaws", "sku": "A1", "name_en": "Milk"},
                {"store_chain": "loblaws", "sku": "A1", "name_en": "Milk"},
                {"store_chain": "loblaws", "sku": "A1", "name_en": "Milk"},
            ],
        )

        resolve_products(str(tmp_path / "observations"), out_path)

        table = pq.read_table(out_path)
        rows = table.to_pylist()
        assert rows[0]["observation_count"] == 3

    def test_nonexistent_observations_dir(self, tmp_path):
        """Should not raise; returns empty mapping and writes empty table."""
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "does_not_exist")
        out_path = str(tmp_path / "products.parquet")

        result = resolve_products(obs_dir, out_path)
        assert result == {}
        assert os.path.exists(out_path)

    def test_price_observation_key_used_when_present(self, tmp_path):
        pytest.importorskip("pyarrow")

        obs_dir = str(tmp_path / "observations" / "store_chain=loblaws" / "year=2026" / "week=14")
        out_path = str(tmp_path / "products.parquet")

        _write_parquet(
            os.path.join(obs_dir, "1001.parquet"),
            [
                {
                    "store_chain": "loblaws",
                    "sku": "A1",
                    "price_observation_key": "loblaws:1000:A1:2026-04-01",
                }
            ],
        )

        result = resolve_products(str(tmp_path / "observations"), out_path)

        assert "loblaws:1000:A1:2026-04-01" in result
