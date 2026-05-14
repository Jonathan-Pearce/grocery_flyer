"""Tests for pipeline/build_db.py."""

from __future__ import annotations

import json
import os
import time

import pytest

from pipeline.build_db import _partition_dir, build_dimensions, build_observations, build_products, build_price_history, build_scores, main


pytestmark = pytest.mark.critical


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


# NOTE: _write_json retained for non-stores/store_flyers use elsewhere in this file
def _write_json(path: str, data: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def _make_record_rows(
    flyer_id: str = "1001",
    store_chain: str = "loblaws",
    flyer_valid_from: str | None = "2026-04-02",
    record_count: int = 2,
) -> list[dict]:
    """Return Parquet-ready rows (list fields JSON-encoded) for *record_count* records."""
    import json as _json
    return [
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
            "multi_product_variants": _json.dumps([]),
            "raw_categories": _json.dumps(["Grocery"]),
        }
        for i in range(record_count)
    ]


def _write_cleaned_parquet(
    cleaned_dir: str,
    chain: str,
    rows: list[dict],
) -> None:
    """Append *rows* to ``<cleaned_dir>/<chain>.parquet``, creating it if absent."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_path = os.path.join(cleaned_dir, f"{chain}.parquet")
    os.makedirs(cleaned_dir, exist_ok=True)
    new_table = pa.Table.from_pylist(rows)
    if os.path.isfile(parquet_path):
        existing = pq.read_table(parquet_path)
        combined = pa.concat_tables([existing, new_table], promote_options="default")
    else:
        combined = new_table
    pq.write_table(combined, parquet_path)


class TestBuildObservations:
    def test_creates_parquet_file(self, tmp_path):
        pytest.importorskip("pyarrow")
        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))

        build_observations(db, cleaned)

        part = _partition_dir(db, "loblaws", "2026-04-02")
        assert os.path.exists(os.path.join(part, "1001.parquet"))

    def test_parquet_is_readable(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws", record_count=3))

        build_observations(db, cleaned)

        part = _partition_dir(db, "loblaws", "2026-04-02")
        table = pq.ParquetFile(os.path.join(part, "1001.parquet")).read()
        assert table.num_rows == 3

    def test_idempotent_without_force(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))

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

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))

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

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_cleaned_parquet(cleaned, "metro", _make_record_rows("2001", "metro"))

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

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1002", "loblaws"))

        build_observations(db, cleaned)
        out = capsys.readouterr().out
        assert "loblaws: 2 written" in out

    def test_list_fields_serialised_as_strings(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        # The cleaned Parquet already has list fields as JSON strings (written by clean.py)
        row = _make_record_rows("1001", "loblaws", record_count=1)[0]
        row["multi_product_variants"] = '["A", "B"]'
        row["raw_categories"] = '["Grocery", "Dairy"]'
        _write_cleaned_parquet(cleaned, "loblaws", [row])

        build_observations(db, cleaned)

        part = _partition_dir(db, "loblaws", "2026-04-02")
        table = pq.ParquetFile(os.path.join(part, "1001.parquet")).read()
        row_out = table.to_pydict()
        assert isinstance(row_out["multi_product_variants"][0], str)
        assert row_out["multi_product_variants"][0] == '["A", "B"]'

    def test_none_flyer_valid_from_uses_fetched_on(self, tmp_path):
        pytest.importorskip("pyarrow")

        cleaned = str(tmp_path / "cleaned")
        db = str(tmp_path / "db")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws", flyer_valid_from=None))

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
            _write_cleaned_parquet(cleaned, chain, _make_record_rows(fid, chain))

        build_observations(db, cleaned)
        out = capsys.readouterr().out
        assert "loblaws" in out
        assert "food_basics" in out


# ── build_dimensions ──────────────────────────────────────────────────────────


def _write_stores_parquet(data_dir: str, chain: str, stores: dict) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = os.path.join(data_dir, chain, "stores.parquet")
    rows = [{"store_code": str(c), "province": v.get("province"), "store_name": v.get("store_name") or v.get("name"), "raw_json": json.dumps(v)} for c, v in stores.items()]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = pa.schema([("store_code", pa.string()), ("province", pa.string()), ("store_name", pa.string()), ("raw_json", pa.string())])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema) if rows else pa.table({"store_code": pa.array([], pa.string()), "province": pa.array([], pa.string()), "store_name": pa.array([], pa.string()), "raw_json": pa.array([], pa.string())}), path)


def _write_store_flyers_parquet(data_dir: str, chain: str, store_flyers: dict) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = os.path.join(data_dir, chain, "store_flyers.parquet")
    rows = []
    for code, pubs in store_flyers.items():
        for pub in (pubs or []):
            rows.append({"store_code": str(code), "flyer_id": str(pub.get("title") or pub.get("id") or ""), "raw_json": json.dumps(pub)})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = pa.schema([("store_code", pa.string()), ("flyer_id", pa.string()), ("raw_json", pa.string())])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema) if rows else pa.table({"store_code": pa.array([], pa.string()), "flyer_id": pa.array([], pa.string()), "raw_json": pa.array([], pa.string())}), path)


# Aliases kept so existing call sites don't need mass-renaming
_write_stores_json = _write_stores_parquet
_write_store_flyers_json = _write_store_flyers_parquet

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


# ── main (CLI) ────────────────────────────────────────────────────────────────


class TestMain:
    def test_returns_zero_on_success(self, tmp_path):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        rc = main([
            "--db-dir", db,
            "--cleaned-dir", cleaned,
            "--data-dir", data,
        ])
        assert rc == 0

    def test_creates_observations_and_dimensions(self, tmp_path):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data])

        assert os.path.isdir(os.path.join(db, "observations"))
        assert os.path.isdir(os.path.join(db, "dimensions"))
        assert os.path.exists(os.path.join(db, "dimensions", "stores.parquet"))
        assert os.path.exists(os.path.join(db, "dimensions", "flyers.parquet"))

    def test_dimensions_only_skips_observations(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        rc = main([
            "--db-dir", db,
            "--cleaned-dir", cleaned,
            "--data-dir", data,
            "--dimensions-only",
        ])
        assert rc == 0
        # Dimension tables must exist
        assert os.path.exists(os.path.join(db, "dimensions", "stores.parquet"))
        # Observations directory must NOT have been created
        assert not os.path.isdir(os.path.join(db, "observations"))

    def test_summary_line_printed(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data])
        out = capsys.readouterr().out
        assert "Done." in out
        assert "flyers written" in out
        assert "Dimensions rebuilt." in out

    def test_dimensions_only_summary_line(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data, "--dimensions-only"])
        out = capsys.readouterr().out
        assert out.strip() == "Done. Dimensions rebuilt."

    def test_idempotent_second_run_zero_written(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data])
        capsys.readouterr()

        main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data])
        out = capsys.readouterr().out
        assert "Done. 0 flyers written" in out

    def test_store_flag_filters_brand(self, tmp_path, capsys):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_cleaned_parquet(cleaned, "food_basics", _make_record_rows("2001", "food_basics"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        main([
            "--db-dir", db,
            "--cleaned-dir", cleaned,
            "--data-dir", data,
            "--store", "loblaws",
        ])
        out = capsys.readouterr().out
        assert "loblaws" in out
        assert "food_basics" not in out

    def test_returns_one_on_error(self, tmp_path, monkeypatch, capsys):
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        def _raise(*args, **kwargs):
            raise RuntimeError("simulated failure")

        monkeypatch.setattr("pipeline.build_db.build_observations", _raise)

        rc = main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data])
        assert rc == 1
        err = capsys.readouterr().err
        assert "RuntimeError" in err

    def test_help_exits_zero(self):
        import subprocess
        result = subprocess.run(
            ["python", "-m", "pipeline.build_db", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--dimensions-only" in result.stdout
        assert "--data-dir" in result.stdout
        assert "--store" in result.stdout
        assert "--score" in result.stdout

    def test_score_flag_runs_scoring_pipeline(self, tmp_path, monkeypatch):
        """--score calls build_products, build_price_history, build_scores."""
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        calls = []

        monkeypatch.setattr(
            "pipeline.build_db.build_products",
            lambda **kw: calls.append(("build_products", kw)),
        )
        monkeypatch.setattr(
            "pipeline.build_db.build_price_history",
            lambda **kw: calls.append(("build_price_history", kw)),
        )
        monkeypatch.setattr(
            "pipeline.build_db.build_scores",
            lambda **kw: calls.append(("build_scores", kw)),
        )

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        rc = main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data, "--score"])
        assert rc == 0
        step_names = [c[0] for c in calls]
        assert step_names == ["build_products", "build_price_history", "build_scores"]

    def test_no_score_flag_skips_scoring_pipeline(self, tmp_path, monkeypatch):
        """Without --score, scoring functions are never called."""
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        calls = []

        monkeypatch.setattr(
            "pipeline.build_db.build_products",
            lambda **kw: calls.append("build_products"),
        )
        monkeypatch.setattr(
            "pipeline.build_db.build_price_history",
            lambda **kw: calls.append("build_price_history"),
        )
        monkeypatch.setattr(
            "pipeline.build_db.build_scores",
            lambda **kw: calls.append("build_scores"),
        )

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        rc = main(["--db-dir", db, "--cleaned-dir", cleaned, "--data-dir", data])
        assert rc == 0
        assert calls == []

    def test_dimensions_only_unaffected_by_score(self, tmp_path, monkeypatch):
        """--dimensions-only skips scoring even if --score is also passed."""
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        calls = []

        monkeypatch.setattr(
            "pipeline.build_db.build_products",
            lambda **kw: calls.append("build_products"),
        )

        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        rc = main([
            "--db-dir", db,
            "--cleaned-dir", cleaned,
            "--data-dir", data,
            "--dimensions-only",
            "--score",
        ])
        assert rc == 0
        assert calls == []

    def test_score_with_store_flag(self, tmp_path, monkeypatch):
        """--score can be combined with --store."""
        pytest.importorskip("pyarrow")
        db = str(tmp_path / "db")
        cleaned = str(tmp_path / "cleaned")
        data = str(tmp_path / "data")

        calls = []

        monkeypatch.setattr(
            "pipeline.build_db.build_products",
            lambda **kw: calls.append(("build_products", kw)),
        )
        monkeypatch.setattr(
            "pipeline.build_db.build_price_history",
            lambda **kw: calls.append(("build_price_history", kw)),
        )
        monkeypatch.setattr(
            "pipeline.build_db.build_scores",
            lambda **kw: calls.append(("build_scores", kw)),
        )

        _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))
        _write_stores_json(data, "loblaws", _FLIPP_STORES)
        _write_store_flyers_json(data, "loblaws", _FLIPP_STORE_FLYERS)

        rc = main([
            "--db-dir", db,
            "--cleaned-dir", cleaned,
            "--data-dir", data,
            "--store", "loblaws",
            "--score",
        ])
        assert rc == 0
        step_names = [c[0] for c in calls]
        assert step_names == ["build_products", "build_price_history", "build_scores"]


# ── Required named tests (issue acceptance criteria) ──────────────────────────

_ADONIS_ENVELOPE = {
    "flyer_id": "83006",
    "store_chain": "adonis",
    "generated_at": "2026-04-03T00:00:00+00:00",
    "record_count": 1,
    "records": [
        {
            "source_api": "metro",
            "store_chain": "adonis",
            "store_id": "21937",
            "flyer_id": "83006",
            "flyer_valid_from": "2026-04-02",
            "flyer_valid_to": "2026-04-08",
            "fetched_on": "2026-04-03",
            "name_en": "test item",
            "sale_price": 1.99,
            "multi_product_variants": [],
            "raw_categories": None,
        }
    ],
}


def test_partition_dir_basic():
    result = _partition_dir("db", "loblaws", "2026-04-02")
    assert result == os.path.join(
        "db", "observations", "store_chain=loblaws", "year=2026", "week=14"
    )


def test_partition_dir_none_date():
    result = _partition_dir("db", "loblaws", None)
    assert "store_chain=loblaws" in result
    assert "year=" in result
    assert "week=" in result


def test_build_observations_creates_files(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    cleaned = str(tmp_path / "cleaned")
    db = str(tmp_path / "db")
    _write_cleaned_parquet(cleaned, "adonis", _make_record_rows("83006", "adonis", record_count=1))

    written, skipped = build_observations(db_dir=db, cleaned_dir=cleaned)
    assert written == 1
    assert skipped == 0

    part_dir = _partition_dir(db, "adonis", "2026-04-02")
    out_path = os.path.join(part_dir, "83006.parquet")
    assert os.path.exists(out_path)
    table = pq.ParquetFile(out_path).read()
    assert table.num_rows == 1


def test_build_observations_idempotent(tmp_path):
    pytest.importorskip("pyarrow")

    cleaned = str(tmp_path / "cleaned")
    db = str(tmp_path / "db")
    _write_cleaned_parquet(cleaned, "adonis", _make_record_rows("83006", "adonis", record_count=1))

    written1, _ = build_observations(db_dir=db, cleaned_dir=cleaned)
    assert written1 == 1

    written2, skipped2 = build_observations(db_dir=db, cleaned_dir=cleaned)
    assert written2 == 0
    assert skipped2 == 1


def test_build_observations_force(tmp_path):
    pytest.importorskip("pyarrow")

    cleaned = str(tmp_path / "cleaned")
    db = str(tmp_path / "db")
    _write_cleaned_parquet(cleaned, "adonis", _make_record_rows("83006", "adonis", record_count=1))

    build_observations(db_dir=db, cleaned_dir=cleaned)

    part_dir = _partition_dir(db, "adonis", "2026-04-02")
    out_path = os.path.join(part_dir, "83006.parquet")
    mtime_before = os.path.getmtime(out_path)

    time.sleep(0.05)

    written, skipped = build_observations(db_dir=db, cleaned_dir=cleaned, force=True)
    assert written == 1
    assert skipped == 0
    assert os.path.getmtime(out_path) > mtime_before


def test_build_observations_store_filter(tmp_path):
    pytest.importorskip("pyarrow")

    cleaned = str(tmp_path / "cleaned")
    db = str(tmp_path / "db")

    _write_cleaned_parquet(cleaned, "adonis", _make_record_rows("83006", "adonis", record_count=1))
    _write_cleaned_parquet(cleaned, "loblaws", _make_record_rows("1001", "loblaws"))

    written, skipped = build_observations(db_dir=db, cleaned_dir=cleaned, store="adonis")
    assert written == 1

    part_dir = _partition_dir(db, "adonis", "2026-04-02")
    assert os.path.exists(os.path.join(part_dir, "83006.parquet"))
    # loblaws should not have been processed
    loblaws_part = _partition_dir(db, "loblaws", "2026-04-02")
    assert not os.path.exists(os.path.join(loblaws_part, "1001.parquet"))


def test_build_dimensions_stores(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    data = str(tmp_path / "data")
    db = str(tmp_path / "db")
    _write_stores_parquet(data, "adonis",
        {"21937": {"store_name": "Adonis MTL", "city": "Montreal", "province": "QC"}})
    _write_store_flyers_parquet(data, "adonis", {})

    build_dimensions(db_dir=db, data_dir=data)

    stores_path = os.path.join(db, "dimensions", "stores.parquet")
    assert os.path.exists(stores_path)
    table = pq.read_table(stores_path)
    col_names = table.schema.names
    assert "store_chain" in col_names
    assert "store_id" in col_names
    assert "store_name" in col_names


def test_build_dimensions_flyers(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    data = str(tmp_path / "data")
    db = str(tmp_path / "db")
    _write_stores_parquet(data, "adonis", {})
    _write_store_flyers_parquet(data, "adonis",
        {"21937": [{"title": "83006", "startDate": "2026-04-02", "endDate": "2026-04-08"}]})

    build_dimensions(db_dir=db, data_dir=data)

    flyers_path = os.path.join(db, "dimensions", "flyers.parquet")
    assert os.path.exists(flyers_path)
    table = pq.read_table(flyers_path)
    col_names = table.schema.names
    assert "flyer_id" in col_names
    assert "store_chain" in col_names
    assert "store_id" in col_names


# ── build_products / build_price_history / build_scores ───────────────────────


def _write_obs_parquet(db_dir: str, store_chain: str, flyer_id: str, rows: list[dict]) -> None:
    """Write a minimal observations Parquet file into the expected partition path."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    part_dir = _partition_dir(db_dir, store_chain, rows[0].get("flyer_valid_from"))
    os.makedirs(part_dir, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, os.path.join(part_dir, f"{flyer_id}.parquet"))


def test_build_products_writes_parquet(tmp_path, monkeypatch):
    """build_products delegates to resolve_products and writes products.parquet."""
    pytest.importorskip("pyarrow")

    db = str(tmp_path / "db")
    obs_dir = os.path.join(db, "observations")
    mapping = {"obs_key_1": "cpid_abc"}

    calls = []

    def _fake_resolve(observations_dir, out_path):
        calls.append((observations_dir, out_path))
        # Create the output file so callers can verify it exists
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.table({"canonical_product_id": ["cpid_abc"]}), out_path)
        return mapping

    monkeypatch.setattr("pipeline.product_resolver.resolve_products", _fake_resolve)

    build_products(db_dir=db, observations_dir=obs_dir)

    assert calls[0] == (obs_dir, os.path.join(db, "dimensions", "products.parquet"))
    assert os.path.exists(os.path.join(db, "dimensions", "products.parquet"))


def test_build_products_delegates_to_resolve_products(tmp_path, monkeypatch):
    """build_products passes correct paths to resolve_products."""
    pytest.importorskip("pyarrow")
    import pyarrow as pa
    import pyarrow.parquet as pq

    db = str(tmp_path / "db")
    obs_dir = os.path.join(db, "observations")

    captured = {}

    def _fake_resolve(observations_dir, out_path):
        captured["observations_dir"] = observations_dir
        captured["out_path"] = out_path
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        pq.write_table(pa.table({"canonical_product_id": pa.array([], type=pa.string())}), out_path)
        return {}

    monkeypatch.setattr("pipeline.product_resolver.resolve_products", _fake_resolve)

    build_products(db_dir=db, observations_dir=obs_dir)

    assert captured["observations_dir"] == obs_dir
    assert captured["out_path"] == os.path.join(db, "dimensions", "products.parquet")


def test_build_price_history_delegates(tmp_path, monkeypatch):
    """build_price_history passes correct paths to the underlying function."""
    pytest.importorskip("pyarrow")

    db = str(tmp_path / "db")
    captured = {}

    def _fake_bph(observations_dir, products_path, out_path):
        captured["observations_dir"] = observations_dir
        captured["products_path"] = products_path
        captured["out_path"] = out_path
        return 0

    monkeypatch.setattr("pipeline.price_history.build_price_history", _fake_bph)

    build_price_history(db_dir=db)

    assert captured["observations_dir"] == os.path.join(db, "observations")
    assert captured["products_path"] == os.path.join(db, "dimensions", "products.parquet")
    assert captured["out_path"] == os.path.join(db, "features", "price_history.parquet")


def test_build_scores_delegates(tmp_path, monkeypatch):
    """build_scores passes correct paths to score_deals."""
    pytest.importorskip("pyarrow")

    db = str(tmp_path / "db")
    captured = {}

    def _fake_score(observations_dir, price_history_path, config_path, out_dir, today):
        captured["observations_dir"] = observations_dir
        captured["price_history_path"] = price_history_path
        captured["config_path"] = config_path
        captured["out_dir"] = out_dir
        captured["today"] = today
        return 0

    monkeypatch.setattr("pipeline.deal_scorer.score_deals", _fake_score)

    build_scores(db_dir=db)

    assert captured["observations_dir"] == os.path.join(db, "observations")
    assert captured["price_history_path"] == os.path.join(db, "features", "price_history.parquet")
    assert captured["config_path"] == os.path.join("config", "scoring.yaml")
    assert captured["out_dir"] == os.path.join(db, "scores")
    assert captured["today"] is None


def test_build_scores_passes_today(tmp_path, monkeypatch):
    """build_scores converts a today string to a date object."""
    import datetime

    pytest.importorskip("pyarrow")

    db = str(tmp_path / "db")
    captured = {}

    def _fake_score(observations_dir, price_history_path, config_path, out_dir, today):
        captured["today"] = today
        return 0

    monkeypatch.setattr("pipeline.deal_scorer.score_deals", _fake_score)

    build_scores(db_dir=db, today="2026-04-11")

    assert captured["today"] == datetime.date(2026, 4, 11)
