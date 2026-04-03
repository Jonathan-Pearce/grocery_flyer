"""Tests for pipeline/build_db.py."""

from __future__ import annotations

import json
import os

import pytest

from pipeline.build_db import _partition_dir, build_dimensions, build_observations


# ── _partition_dir ────────────────────────────────────────────────────────────


class TestPartitionDir:
    def test_known_date(self):
        # 2026-04-02 is ISO week 14 of 2026
        result = _partition_dir("db", "loblaws", "2026-04-02")
        assert result == os.path.join(
            "db", "observations", "store_chain=loblaws", "year=2026", "week=14"
        )

    def test_none_does_not_raise(self):
        # Should fall back to today without raising
        result = _partition_dir("db", "loblaws", None)
        assert "store_chain=loblaws" in result
        assert "year=" in result
        assert "week=" in result

    def test_invalid_date_string_falls_back(self):
        result = _partition_dir("db", "food_basics", "not-a-date")
        assert "store_chain=food_basics" in result

    def test_custom_db_dir(self):
        result = _partition_dir("/tmp/mydb", "metro", "2026-01-05")
        assert result.startswith(os.path.join("/tmp/mydb", "observations"))

    def test_week_boundary(self):
        # 2026-01-01 is ISO week 1 of 2026
        result = _partition_dir("db", "sobeys", "2026-01-01")
        assert "year=2026" in result
        assert "week=1" in result

    def test_store_chain_embedded(self):
        result = _partition_dir("db", "no_frills", "2026-04-02")
        assert "store_chain=no_frills" in result


# ── build_observations ────────────────────────────────────────────────────────


def _write_json(path: str, data: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def _make_envelope(
    flyer_id: str = "1001",
    store_chain: str = "loblaws",
    flyer_valid_from: str | None = "2026-04-02",
    record_count: int = 2,
) -> dict:
    records = [
        {
            "source_api": "flipp",
            "store_chain": store_chain,
            "store_id": "1000",
            "flyer_id": flyer_id,
            "flyer_valid_from": flyer_valid_from,
            "flyer_valid_to": "2026-04-08",
            "fetched_on": "2026-04-02",
            "raw_name": f"Product {i}",
            "sale_price": 3.99,
            "multi_product_variants": [],
            "raw_categories": ["Grocery"],
        }
        for i in range(record_count)
    ]
    return {
        "flyer_id": flyer_id,
        "store_chain": store_chain,
        "generated_at": "2026-04-03T00:00:00+00:00",
        "record_count": record_count,
        "records": records,
    }


class TestBuildObservations:
    def test_creates_parquet_file(self, tmp_path):
        pytest.importorskip("pyarrow")
        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_json(
            os.path.join(cleaned, "loblaws", "1001.json"),
            _make_envelope("1001", "loblaws"),
        )

        build_observations(db, cleaned)

        part = _partition_dir(db, "loblaws", "2026-04-02")
        assert os.path.exists(os.path.join(part, "1001.parquet"))

    def test_parquet_is_readable(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_json(
            os.path.join(cleaned, "loblaws", "1001.json"),
            _make_envelope("1001", "loblaws", record_count=3),
        )

        build_observations(db, cleaned)

        part = _partition_dir(db, "loblaws", "2026-04-02")
        table = pq.ParquetFile(os.path.join(part, "1001.parquet")).read()
        assert table.num_rows == 3

    def test_idempotent_without_force(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_json(
            os.path.join(cleaned, "loblaws", "1001.json"),
            _make_envelope("1001", "loblaws"),
        )

        build_observations(db, cleaned)
        capsys.readouterr()  # discard first run output

        build_observations(db, cleaned)
        out = capsys.readouterr().out
        assert "0 written" in out
        assert "1 skipped" in out

    def test_force_overwrites(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_json(
            os.path.join(cleaned, "loblaws", "1001.json"),
            _make_envelope("1001", "loblaws"),
        )

        build_observations(db, cleaned)
        capsys.readouterr()

        build_observations(db, cleaned, force=True)
        out = capsys.readouterr().out
        assert "1 written" in out
        assert "0 skipped" in out

    def test_store_filter(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_json(
            os.path.join(cleaned, "loblaws", "1001.json"),
            _make_envelope("1001", "loblaws"),
        )
        _write_json(
            os.path.join(cleaned, "metro", "2001.json"),
            _make_envelope("2001", "metro"),
        )

        build_observations(db, cleaned, store="loblaws")
        out = capsys.readouterr().out

        # Only loblaws should appear in the output
        assert "loblaws" in out
        assert "metro" not in out

        # metro parquet should not exist
        part = _partition_dir(db, "metro", "2026-04-02")
        assert not os.path.exists(os.path.join(part, "2001.parquet"))

    def test_per_brand_summary_printed(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_json(
            os.path.join(cleaned, "loblaws", "1001.json"),
            _make_envelope("1001", "loblaws"),
        )
        _write_json(
            os.path.join(cleaned, "loblaws", "1002.json"),
            _make_envelope("1002", "loblaws"),
        )

        build_observations(db, cleaned)
        out = capsys.readouterr().out
        assert "loblaws: 2 written" in out

    def test_list_fields_serialised_as_strings(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        envelope = _make_envelope("1001", "loblaws", record_count=1)
        envelope["records"][0]["multi_product_variants"] = ["A", "B"]
        envelope["records"][0]["raw_categories"] = ["Grocery", "Dairy"]
        _write_json(os.path.join(cleaned, "loblaws", "1001.json"), envelope)

        build_observations(db, cleaned)

        part = _partition_dir(db, "loblaws", "2026-04-02")
        table = pq.ParquetFile(os.path.join(part, "1001.parquet")).read()
        row = table.to_pydict()
        assert isinstance(row["multi_product_variants"][0], str)
        assert row["multi_product_variants"][0] == '["A", "B"]'

    def test_none_flyer_valid_from_uses_fetched_on(self, tmp_path):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        envelope = _make_envelope("1001", "loblaws", flyer_valid_from=None)
        # fetched_on is set to "2026-04-02" in _make_envelope
        _write_json(os.path.join(cleaned, "loblaws", "1001.json"), envelope)

        build_observations(db, cleaned)

        # Partition should be based on fetched_on date "2026-04-02"
        part = _partition_dir(db, "loblaws", "2026-04-02")
        assert os.path.exists(os.path.join(part, "1001.parquet"))

    def test_empty_cleaned_dir_does_not_raise(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned_missing")

        # Should not raise even when cleaned_dir doesn't exist
        build_observations(db, cleaned)

    def test_multiple_brands(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        for chain, fid in [("loblaws", "1001"), ("food_basics", "2001")]:
            _write_json(
                os.path.join(cleaned, chain, f"{fid}.json"),
                _make_envelope(fid, chain),
            )

        build_observations(db, cleaned)
        out = capsys.readouterr().out
        assert "loblaws" in out
        assert "food_basics" in out


# ── build_dimensions ──────────────────────────────────────────────────────────


def _write_stores_json(data_dir: str, chain: str, stores: dict) -> None:
    path = os.path.join(data_dir, chain, "stores.json")
    _write_json(path, stores)


def _write_store_flyers_json(data_dir: str, chain: str, store_flyers: dict) -> None:
    path = os.path.join(data_dir, chain, "store_flyers.json")
    _write_json(path, store_flyers)


_METRO_STORES = {
    "21937": {"store_name": "Sauvé", "banner": "Adonis"},
    "21938": {"store_name": "Laval", "banner": "Adonis"},
}

_FLIPP_STORES = {
    "1000": {
        "name": "Loblaws - Queen Street West",
        "postal_code": "M5V2B7",
        "province": "ON",
        "city": "Toronto",
    },
    "1001": {
        "name": "Loblaws - Yonge",
        "postal_code": "M4W2L2",
        "province": "ON",
        "city": "Toronto",
    },
}

_METRO_STORE_FLYERS = {
    "21937": [
        {
            "title": "83006",
            "startDate": "2026-04-02T00:00:00Z",
            "endDate": "2026-04-08T23:59:00Z",
            "language": "bil",
            "province": "QC",
        }
    ],
    "21938": [
        {
            "title": "83006",  # same flyer, should be deduplicated
            "startDate": "2026-04-02T00:00:00Z",
            "endDate": "2026-04-08T23:59:00Z",
            "language": "bil",
            "province": "QC",
        }
    ],
}

_FLIPP_STORE_FLYERS = {
    "1000": [
        {
            "id": 7865059,
            "valid_from": "2026-04-02T00:00:00-04:00",
            "valid_to": "2026-04-08T23:59:59-04:00",
            "locale": "en",
            "postal_code": "M5V2B7",
        }
    ],
    "1001": [],
}


class TestBuildDimensions:
    def test_creates_parquet_files(self, tmp_path):
        pytest.importorskip("pyarrow")
        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "adonis", _METRO_STORES)
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "adonis", _METRO_STORE_FLYERS)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        build_dimensions(db, data)

        assert os.path.exists(os.path.join(db, "dimensions", "stores.parquet"))
        assert os.path.exists(os.path.join(db, "dimensions", "flyers.parquet"))

    def test_stores_row_count(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "adonis", _METRO_STORES)
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "adonis", {})
        _write_store_flyers_json(data, "loblaws", {})

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "stores.parquet"))
        # 2 adonis + 2 loblaws stores
        assert table.num_rows == 4

    def test_stores_metro_fields(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "adonis", _METRO_STORES)
        _write_store_flyers_json(data, "adonis", {})

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "stores.parquet"))
        d = table.to_pydict()
        assert "adonis" in d["store_chain"]
        assert "Sauvé" in d["store_name"]
        assert "Adonis" in d["banner"]
        # Metro stores have no province/city/postal_code
        idx = d["store_chain"].index("adonis")
        assert d["province"][idx] is None
        assert d["city"][idx] is None
        assert d["postal_code"][idx] is None

    def test_stores_flipp_fields(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", {})

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "stores.parquet"))
        d = table.to_pydict()
        assert "loblaws" in d["store_chain"]
        assert "Loblaws - Queen Street West" in d["store_name"]
        assert "ON" in d["province"]
        assert "Toronto" in d["city"]
        assert "M5V2B7" in d["postal_code"]
        # Flipp stores have no banner
        idx = d["store_chain"].index("loblaws")
        assert d["banner"][idx] is None

    def test_stores_columns(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", {})

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "stores.parquet"))
        assert set(table.schema.names) == {
            "store_chain", "store_id", "store_name", "banner",
            "province", "city", "postal_code",
        }

    def test_flyers_deduplication(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "adonis", _METRO_STORES)
        _write_store_flyers_json(data, "adonis", _METRO_STORE_FLYERS)

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "flyers.parquet"))
        # Both stores share flyer "83006" — should appear only once
        d = table.to_pydict()
        assert d["flyer_id"].count("83006") == 1

    def test_flyers_metro_fields(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "adonis", _METRO_STORES)
        _write_store_flyers_json(data, "adonis", _METRO_STORE_FLYERS)

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "flyers.parquet"))
        d = table.to_pydict()
        assert "83006" in d["flyer_id"]
        idx = d["flyer_id"].index("83006")
        assert d["valid_from"][idx] == "2026-04-02T00:00:00Z"
        assert d["valid_to"][idx] == "2026-04-08T23:59:00Z"
        assert d["language"][idx] == "bil"
        assert d["province"][idx] == "QC"

    def test_flyers_flipp_fields(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "flyers.parquet"))
        d = table.to_pydict()
        assert "7865059" in d["flyer_id"]
        idx = d["flyer_id"].index("7865059")
        assert d["valid_from"][idx] == "2026-04-02T00:00:00-04:00"
        assert d["language"][idx] == "en"
        assert d["province"][idx] is None

    def test_flyers_columns(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        build_dimensions(db, data)

        table = pq.read_table(os.path.join(db, "dimensions", "flyers.parquet"))
        assert set(table.schema.names) == {
            "flyer_id", "store_chain", "store_id",
            "valid_from", "valid_to", "language", "province",
        }

    def test_multiple_brands(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "adonis", _METRO_STORES)
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "adonis", _METRO_STORE_FLYERS)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        build_dimensions(db, data)

        stores_table = pq.read_table(os.path.join(db, "dimensions", "stores.parquet"))
        chains = set(stores_table.to_pydict()["store_chain"])
        assert "adonis" in chains
        assert "loblaws" in chains

        flyers_table = pq.read_table(os.path.join(db, "dimensions", "flyers.parquet"))
        flyer_chains = set(flyers_table.to_pydict()["store_chain"])
        assert "adonis" in flyer_chains
        assert "loblaws" in flyer_chains

    def test_overwrite_on_rerun(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        build_dimensions(db, data)
        first_mtime = os.path.getmtime(
            os.path.join(db, "dimensions", "stores.parquet")
        )

        import time
        time.sleep(0.05)

        build_dimensions(db, data)
        second_mtime = os.path.getmtime(
            os.path.join(db, "dimensions", "stores.parquet")
        )
        assert second_mtime >= first_mtime

    def test_missing_data_dir_does_not_raise(self, tmp_path):
        pytest.importorskip("pyarrow")

        db = str(tmp_path / "db")
        data = str(tmp_path / "data_missing")

        build_dimensions(db, data)

        import pyarrow.parquet as pq

        stores_table = pq.read_table(os.path.join(db, "dimensions", "stores.parquet"))
        assert stores_table.num_rows == 0

    def test_empty_store_flyers_does_not_raise(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        data = str(tmp_path / "data")
        db = str(tmp_path / "db")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", {"1000": [], "1001": []})

        build_dimensions(db, data)

        flyers_table = pq.read_table(os.path.join(db, "dimensions", "flyers.parquet"))
        assert flyers_table.num_rows == 0
