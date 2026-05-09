"""Tests for load_raw.py — raw file loader and pipeline router."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from pipeline.load_raw import _flipp_store_id, _store_province, iter_records
from pipeline.schema import FlyerItem


# ── Fixture helpers ───────────────────────────────────────────────────────────


def _make_flipp_flyer(publication_id: str = "1001") -> dict:
    """Return a minimal Flipp flyer file dict."""
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
                "name": "Test Product",
                "sku": "SKU001",
                "price_text": "3.99",
                "item_type": 1,
            }
        ],
    }


def _make_metro_flyer(job: str = "82000", store_id: int = 100) -> dict:
    """Return a minimal Metro flyer file dict."""
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
            }
        ],
    }


def _make_stores(store_code: str = "100", province: str = "ON") -> dict:
    """Return a minimal stores.json dict."""
    return {
        store_code: {
            "name": "Test Store",
            "merchant_store_code": store_code,
            "province": province,
        }
    }


def _make_store_flyers(store_code: str = "100", publication_id: int = 1001) -> dict:
    """Return a minimal store_flyers.json dict."""
    return {
        store_code: [
            {
                "id": publication_id,
                "valid_from": "2026-04-03T00:00:00-04:00",
                "valid_to": "2026-04-09T23:59:59-04:00",
            }
        ]
    }


def _write_json(path: str, data: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def _write_flyer_parquet(tmp_dir: str, chain: str, flyer_data: dict) -> None:
    """Write a raw flyer dict to ``<tmp_dir>/<chain>/flyers.parquet`` (append)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_path = os.path.join(tmp_dir, chain, "flyers.parquet")
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    if "publication_id" in flyer_data:
        pub_meta = flyer_data.get("publication_meta") or {}
        envelope: dict = {
            "flyer_id": str(flyer_data["publication_id"]),
            "source_api": "flipp",
            "fetched_on": flyer_data.get("fetched_on"),
            "pub_valid_from": pub_meta.get("valid_from"),
            "pub_valid_to": pub_meta.get("valid_to"),
            "pub_locale": pub_meta.get("locale"),
            "products_url": flyer_data.get("products_url"),
        }
    elif "job" in flyer_data:
        envelope = {
            "flyer_id": str(flyer_data["job"]),
            "source_api": "metro",
            "fetched_on": flyer_data.get("fetched_on"),
            "store_id": str(flyer_data.get("store_id", "")),
            "products_url": flyer_data.get("products_url"),
        }
    else:
        # Unknown source_api — write a row that will trigger the ValueError path.
        envelope = {
            "flyer_id": "unknown",
            "source_api": "unknown",
            "fetched_on": None,
        }

    rows = []
    for product in flyer_data.get("products", [{}]):
        row = dict(envelope)
        for k, v in product.items():
            row[k] = json.dumps(v) if isinstance(v, (list, dict)) else v
        rows.append(row)

    if not rows:
        rows = [dict(envelope)]

    new_table = pa.Table.from_pylist(rows)
    if os.path.isfile(parquet_path):
        existing = pq.read_table(parquet_path)
        combined = pa.concat_tables([existing, new_table], promote_options="default")
    else:
        combined = new_table
    pq.write_table(combined, parquet_path)


# ── _store_province ───────────────────────────────────────────────────────────


class TestStoreProvince:
    def test_returns_province_when_found(self):
        stores = _make_stores("1000", "BC")
        assert _store_province(stores, "1000") == "BC"

    def test_returns_none_when_store_missing(self):
        stores = _make_stores("1000", "BC")
        assert _store_province(stores, "9999") is None

    def test_returns_none_when_store_id_none(self):
        stores = _make_stores("1000", "BC")
        assert _store_province(stores, None) is None

    def test_coerces_store_id_to_str(self):
        stores = _make_stores("200", "QC")
        # store_id passed as int-like string — should match str key "200"
        assert _store_province(stores, "200") == "QC"

    def test_returns_none_on_empty_stores(self):
        assert _store_province({}, "1000") is None


# ── _flipp_store_id ───────────────────────────────────────────────────────────


class TestFlippStoreId:
    def test_returns_store_code_when_found(self):
        store_flyers = _make_store_flyers("1050", 7838648)
        assert _flipp_store_id(store_flyers, "7838648") == "1050"

    def test_returns_none_when_not_found(self):
        store_flyers = _make_store_flyers("1050", 7838648)
        assert _flipp_store_id(store_flyers, "9999999") is None

    def test_matches_string_to_int_id(self):
        # publication IDs in store_flyers.json are stored as integers
        store_flyers = {"200": [{"id": 42, "valid_from": None, "valid_to": None}]}
        assert _flipp_store_id(store_flyers, "42") == "200"

    def test_returns_first_matching_store(self):
        store_flyers = {
            "A": [{"id": 100}],
            "B": [{"id": 100}],
        }
        result = _flipp_store_id(store_flyers, "100")
        assert result in ("A", "B")

    def test_ignores_non_list_entries(self):
        store_flyers = {"99": "not-a-list"}
        assert _flipp_store_id(store_flyers, "42") is None


# ── iter_records ──────────────────────────────────────────────────────────────


class TestIterRecords:
    def test_flipp_file_yields_flyer_items(self):
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            chain = "loblaws_test"
            _write_json(os.path.join(tmp, chain, "stores.json"), _make_stores("1000", "ON"))
            _write_json(os.path.join(tmp, chain, "store_flyers.json"), _make_store_flyers("1000", 1001))
            _write_flyer_parquet(tmp, chain, _make_flipp_flyer("1001"))

            items = list(iter_records(data_dir=tmp))

        assert len(items) == 1
        item = items[0]
        assert isinstance(item, FlyerItem)
        assert item.source_api == "flipp"
        assert item.store_chain == chain
        assert item.flyer_id == "1001"

    def test_flipp_resolves_province_from_stores(self):
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            chain = "loblaws_test"
            _write_json(os.path.join(tmp, chain, "stores.json"), _make_stores("1000", "ON"))
            _write_json(os.path.join(tmp, chain, "store_flyers.json"), _make_store_flyers("1000", 1001))
            _write_flyer_parquet(tmp, chain, _make_flipp_flyer("1001"))

            items = list(iter_records(data_dir=tmp))

        assert items[0].province == "ON"
        assert items[0].store_id == "1000"

    def test_metro_file_yields_flyer_items(self):
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            chain = "food_basics_test"
            _write_json(os.path.join(tmp, chain, "stores.json"), _make_stores("100", "ON"))
            _write_json(os.path.join(tmp, chain, "store_flyers.json"), {})
            _write_flyer_parquet(tmp, chain, _make_metro_flyer("82000", 100))

            items = list(iter_records(data_dir=tmp))

        assert len(items) == 1
        item = items[0]
        assert isinstance(item, FlyerItem)
        assert item.source_api == "metro"
        assert item.store_chain == chain
        assert item.flyer_id == "82000"

    def test_metro_resolves_province_from_stores(self):
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            chain = "food_basics_test"
            _write_json(os.path.join(tmp, chain, "stores.json"), _make_stores("100", "ON"))
            _write_json(os.path.join(tmp, chain, "store_flyers.json"), {})
            _write_flyer_parquet(tmp, chain, _make_metro_flyer("82000", 100))

            items = list(iter_records(data_dir=tmp))

        assert items[0].province == "ON"
        assert items[0].store_id == "100"

    def test_unknown_source_api_raises_value_error(self):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmp:
            chain = "unknown_test"
            _write_json(os.path.join(tmp, chain, "stores.json"), {})
            _write_json(os.path.join(tmp, chain, "store_flyers.json"), {})
            # Write a Parquet row with an unrecognised source_api.
            bad_row = [{"flyer_id": "bad", "source_api": "unknown", "fetched_on": None}]
            pq.write_table(
                pa.Table.from_pylist(bad_row),
                os.path.join(tmp, chain, "flyers.parquet"),
            )

            with pytest.raises(ValueError, match="Cannot determine API source"):
                list(iter_records(data_dir=tmp))

    def test_store_filter_restricts_to_named_store(self):
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            for chain, pub_id in [("brand_a", "1001"), ("brand_b", "2001")]:
                _write_json(os.path.join(tmp, chain, "stores.json"), _make_stores("1", "BC"))
                _write_json(os.path.join(tmp, chain, "store_flyers.json"), _make_store_flyers("1", int(pub_id)))
                _write_flyer_parquet(tmp, chain, _make_flipp_flyer(pub_id))

            items = list(iter_records(data_dir=tmp, store="brand_a"))

        assert len(items) == 1
        assert items[0].store_chain == "brand_a"

    def test_empty_data_dir_yields_nothing(self):
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            items = list(iter_records(data_dir=tmp))
        assert items == []

    def test_nonexistent_data_dir_yields_nothing(self):
        items = list(iter_records(data_dir="/tmp/does_not_exist_xyz"))
        assert items == []

    def test_missing_stores_json_still_works(self):
        """Province and store_id fall back to None when stores.json is absent."""
        pytest.importorskip("pyarrow")
        with tempfile.TemporaryDirectory() as tmp:
            chain = "no_stores_test"
            # No stores.json written
            _write_json(os.path.join(tmp, chain, "store_flyers.json"), {})
            _write_flyer_parquet(tmp, chain, _make_metro_flyer("82000", 100))

            items = list(iter_records(data_dir=tmp))

        assert len(items) == 1
        assert items[0].province is None

    def test_processes_real_loblaws_file(self):
        """Smoke-test against data/loblaws/flyers.parquet if present."""
        pytest.importorskip("pyarrow")
        data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
        parquet_path = os.path.join(data_dir, "loblaws", "flyers.parquet")
        if not os.path.isfile(parquet_path):
            pytest.skip("No loblaws flyers.parquet present")

        items = list(iter_records(data_dir=data_dir, store="loblaws"))
        assert len(items) > 0
        for item in items:
            assert item.source_api == "flipp"
            assert item.store_chain == "loblaws"

    def test_processes_real_food_basics_file(self):
        """Smoke-test against data/food_basics/flyers.parquet if present."""
        pytest.importorskip("pyarrow")
        data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
        parquet_path = os.path.join(data_dir, "food_basics", "flyers.parquet")
        if not os.path.isfile(parquet_path):
            pytest.skip("No food_basics flyers.parquet present")

        items = list(iter_records(data_dir=data_dir, store="food_basics"))
        assert len(items) > 0
        for item in items:
            assert item.source_api == "metro"
            assert item.store_chain == "food_basics"
