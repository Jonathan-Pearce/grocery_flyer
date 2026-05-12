"""Schema regression tests for scripts/export_frontend_data.py."""

from __future__ import annotations

import gzip
import json
from math import nan

import pytest

pyarrow = pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from scripts import export_frontend_data as efd  # noqa: E402


def _write_parquet(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(path))


def test_export_scores_schema_and_sorting(tmp_path, monkeypatch):
    scores_path = tmp_path / "active_scores.parquet"
    out_path = tmp_path / "active_scores.json.gz"

    rows = [
        {
            "flyer_id": "F1",
            "sku": "SKU001",
            "store_chain": "loblaws",
            "store_id": "100",
            "name_en": "Item A",
            "sale_price": 4.99,
            "regular_price": 6.99,
            "price_unit": "ea",
            "promo_type": "percentage_off",
            "flyer_valid_from": "2026-05-01T00:00:00",
            "flyer_valid_to": "2026-05-07T23:59:59",
            "deal_score": 70,
            "score_discount_depth": 20,
            "score_deal_rarity": 10,
            "score_essentiality": 15,
            "score_cycle_position": 10,
            "score_authenticity": 10,
            "score_loyalty_bonus": 2,
            "confidence": 0.8,
            "confidence_label": "High",
            "category_l1": "Produce",
            "category_l2": "Fruit",
            "image_url": "https://example.com/a.jpg",
        },
        {
            "flyer_id": "F2",
            "sku": "SKU002",
            "store_chain": "metro",
            "store_id": "200",
            "name_en": "Item B",
            "sale_price": 3.99,
            "regular_price": 5.99,
            "price_unit": "ea",
            "promo_type": "dollar_off",
            "flyer_valid_from": "2026-05-01",
            "flyer_valid_to": "2026-05-07",
            "deal_score": 88,
            "score_discount_depth": 23,
            "score_deal_rarity": 16,
            "score_essentiality": 17,
            "score_cycle_position": 12,
            "score_authenticity": 14,
            "score_loyalty_bonus": 3,
            "confidence": 0.7,
            "confidence_label": "Medium",
            "category_l1": "Dairy & Eggs",
            "category_l2": "Milk",
            "image_url": "https://example.com/b.jpg",
        },
        {
            "flyer_id": "F3",
            "sku": "SKU003",
            "store_chain": "sobeys",
            "store_id": "300",
            "name_en": "Item C",
            "sale_price": 2.99,
            "flyer_valid_from": "2026-05-01",
            "flyer_valid_to": "2026-05-07",
            "deal_score": None,
        },
    ]
    _write_parquet(scores_path, rows)

    monkeypatch.setattr(efd, "SCORES_PATH", scores_path)
    monkeypatch.setattr(efd, "OUT_PATH", out_path)

    efd._export_scores()

    with gzip.open(out_path, "rt", encoding="utf-8") as fh:
        payload = json.load(fh)

    assert len(payload) == 2
    assert payload[0]["deal_score"] == 88
    assert payload[1]["deal_score"] == 70
    assert payload[0]["flyer_valid_from"] == "2026-05-01"
    assert payload[0]["flyer_valid_to"] == "2026-05-07"
    assert {"flyer_id", "store_chain", "deal_score"}.issubset(payload[0].keys())


def test_export_rankings_schema(tmp_path, monkeypatch):
    rankings_dir = tmp_path / "rankings"
    chain_path = rankings_dir / "current_chain_rankings.parquet"
    flyer_path = rankings_dir / "current_flyer_rankings.parquet"
    history_path = rankings_dir / "weekly_history.parquet"
    rankings_out = tmp_path / "rankings.json"
    history_out = tmp_path / "rankings_history.json"

    _write_parquet(
        chain_path,
        [
            {
                "week_label": "2026-W20",
                "store_chain": "loblaws",
                "flyer_count": 3,
                "item_count": 120,
                "hot_count": 22,
                "hot_ratio": 0.1833,
                "avg_flyer_grade": 78.5,
                "letter_grade": "A",
                "rank": 1,
            }
        ],
    )
    _write_parquet(
        flyer_path,
        [
            {
                "flyer_id": "F1",
                "store_chain": "loblaws",
                "flyer_valid_from": "2026-05-01",
                "flyer_valid_to": "2026-05-07",
                "item_count": 50,
                "hot_count": 10,
                "good_count": 20,
                "avg_score": 74.2,
                "top10_avg": 88.1,
                "hot_ratio": 0.2,
                "flyer_grade": 80.3,
                "letter_grade": "A",
                "week_label": "2026-W20",
            }
        ],
    )
    _write_parquet(
        history_path,
        [
            {
                "week_label": "2026-W20",
                "store_chain": "loblaws",
                "flyer_count": 3,
                "item_count": 120,
                "hot_count": 22,
                "hot_ratio": 0.1833,
                "avg_flyer_grade": 78.5,
                "letter_grade": "A",
                "rank": 1,
            },
            {
                "week_label": "2026-W19",
                "store_chain": "loblaws",
                "flyer_count": 3,
                "item_count": 118,
                "hot_count": 20,
                "hot_ratio": 0.1694,
                "avg_flyer_grade": 75.1,
                "letter_grade": "A",
                "rank": 1,
            },
        ],
    )

    monkeypatch.setattr(efd, "RANKINGS_DIR", rankings_dir)
    monkeypatch.setattr(efd, "CHAIN_RANKINGS_PATH", chain_path)
    monkeypatch.setattr(efd, "FLYER_RANKINGS_PATH", flyer_path)
    monkeypatch.setattr(efd, "HISTORY_PATH", history_path)
    monkeypatch.setattr(efd, "RANKINGS_OUT_PATH", rankings_out)
    monkeypatch.setattr(efd, "RANKINGS_HISTORY_OUT_PATH", history_out)

    efd.export_rankings()

    with open(rankings_out, encoding="utf-8") as fh:
        payload = json.load(fh)
    assert set(payload.keys()) == {"chains", "flyers"}
    assert payload["chains"][0]["store_chain"] == "loblaws"
    assert payload["flyers"][0]["flyer_id"] == "F1"

    with open(history_out, encoding="utf-8") as fh:
        history = json.load(fh)
    assert history[0]["week_label"] == "2026-W20"
    assert history[1]["week_label"] == "2026-W19"
    assert "chains" in history[0]


def test_export_stores_geo_schema_and_nan_cleanup(tmp_path, monkeypatch):
    geo_path = tmp_path / "stores_geo.parquet"
    geo_out = tmp_path / "stores_geo.json"

    _write_parquet(
        geo_path,
        [
            {
                "chain": "loblaws",
                "store_code": "100",
                "store_name": "Store A",
                "address": "1 Main St",
                "city": "Toronto",
                "province": "ON",
                "postal_code": "M5V2T6",
                "lat": 43.64,
                "lon": -79.39,
                "geo_source": "raw_json",
            },
            {
                "chain": "metro",
                "store_code": "200",
                "store_name": "Store B",
                "address": "2 Main St",
                "city": "Montreal",
                "province": "QC",
                "postal_code": "H2X1Y4",
                "lat": nan,
                "lon": nan,
                "geo_source": "none",
            },
        ],
    )

    monkeypatch.setattr(efd, "GEO_PATH", geo_path)
    monkeypatch.setattr(efd, "GEO_OUT_PATH", geo_out)

    efd.export_stores_geo()

    with open(geo_out, encoding="utf-8") as fh:
        payload = json.load(fh)

    assert len(payload) == 2
    assert payload[0]["chain"] == "loblaws"
    assert payload[1]["lat"] is None
    assert payload[1]["lon"] is None
    assert {"chain", "store_code", "geo_source", "lat", "lon"}.issubset(payload[0].keys())
