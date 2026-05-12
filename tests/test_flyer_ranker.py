"""Tests for pipeline/flyer_ranker.py."""

from __future__ import annotations

import os
import tempfile

import pytest

pyarrow = pytest.importorskip("pyarrow")

from pipeline.flyer_ranker import (
    letter_grade,
    flyer_grade,
    rank_flyers,
    _aggregate_flyer_rows,
    _aggregate_chain_rows,
    _append_history,
    _read_parquet,
    _write_parquet,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_scored_row(
    flyer_id: str = "FLY001",
    store_chain: str = "loblaws",
    deal_score: int = 70,
    flyer_valid_from: str = "2026-05-05",
    flyer_valid_to: str = "2026-05-11",
    sku: str = "SKU001",
) -> dict:
    return {
        "flyer_id": flyer_id,
        "sku": sku,
        "store_chain": store_chain,
        "store_id": "1001",
        "name_en": "Test Product",
        "sale_price": 4.99,
        "deal_score": deal_score,
        "flyer_valid_from": flyer_valid_from,
        "flyer_valid_to": flyer_valid_to,
    }


# ── letter_grade ──────────────────────────────────────────────────────────────

class TestLetterGrade:
    def test_a_at_75(self):
        assert letter_grade(75.0) == "A"

    def test_a_above_75(self):
        assert letter_grade(90.0) == "A"

    def test_b_at_60(self):
        assert letter_grade(60.0) == "B"

    def test_b_below_75(self):
        assert letter_grade(74.9) == "B"

    def test_c_at_45(self):
        assert letter_grade(45.0) == "C"

    def test_d_at_30(self):
        assert letter_grade(30.0) == "D"

    def test_f_below_30(self):
        assert letter_grade(29.9) == "F"

    def test_f_at_zero(self):
        assert letter_grade(0.0) == "F"


# ── flyer_grade ───────────────────────────────────────────────────────────────

class TestFlyerGrade:
    def test_empty_scores(self):
        assert flyer_grade([]) == 0.0

    def test_all_hot_scores_high_grade(self):
        scores = [85, 90, 82, 88]
        grade = flyer_grade(scores)
        # avg=86.25, hot_ratio=1.0, top10_avg=86.25
        # 0.40*86.25 + 0.30*100 + 0.30*86.25 = 34.5+30+25.875 = 90.375
        assert grade > 85

    def test_all_low_scores_low_grade(self):
        scores = [10, 15, 20, 5]
        grade = flyer_grade(scores)
        assert grade < 30

    def test_mixed_scores(self):
        scores = [80, 70, 60, 50, 40]
        grade = flyer_grade(scores)
        assert 40 < grade < 80

    def test_single_score(self):
        grade = flyer_grade([100])
        assert grade > 0

    def test_more_than_10_items(self):
        # top10_avg should use only the top 10
        scores = [10] * 5 + [90] * 15
        grade = flyer_grade(scores)
        # top10_avg = 90, but avg_score is pulled down by the 10s
        assert grade > 50  # should still be decent

    def test_grade_is_float(self):
        assert isinstance(flyer_grade([70, 80]), float)


# ── _aggregate_flyer_rows ─────────────────────────────────────────────────────

class TestAggregateFlyerRows:
    def test_empty(self):
        assert _aggregate_flyer_rows([]) == []

    def test_single_flyer(self):
        rows = [
            _make_scored_row(deal_score=80),
            _make_scored_row(deal_score=90, sku="SKU002"),
            _make_scored_row(deal_score=60, sku="SKU003"),
        ]
        result = _aggregate_flyer_rows(rows)
        assert len(result) == 1
        r = result[0]
        assert r["flyer_id"] == "FLY001"
        assert r["store_chain"] == "loblaws"
        assert r["item_count"] == 3
        assert r["hot_count"] == 2  # 80 and 90
        assert r["good_count"] == 2  # 80 and 90 only (60 < 65)

    def test_multiple_flyers(self):
        rows = [
            _make_scored_row(flyer_id="F1", store_chain="loblaws", deal_score=85),
            _make_scored_row(flyer_id="F1", store_chain="loblaws", deal_score=75, sku="S2"),
            _make_scored_row(flyer_id="F2", store_chain="nofrills", deal_score=50),
        ]
        result = _aggregate_flyer_rows(rows)
        assert len(result) == 2
        ids = {r["flyer_id"] for r in result}
        assert ids == {"F1", "F2"}

    def test_rows_without_deal_score_skipped(self):
        rows = [
            _make_scored_row(deal_score=70),
            {"flyer_id": "FLY001", "store_chain": "loblaws", "deal_score": None},
        ]
        result = _aggregate_flyer_rows(rows)
        assert result[0]["item_count"] == 1

    def test_row_without_flyer_id_skipped(self):
        rows = [
            {"store_chain": "loblaws", "deal_score": 70},
            _make_scored_row(deal_score=80),
        ]
        result = _aggregate_flyer_rows(rows)
        assert len(result) == 1

    def test_letter_grade_present(self):
        rows = [_make_scored_row(deal_score=80)]
        result = _aggregate_flyer_rows(rows)
        assert "letter_grade" in result[0]
        assert result[0]["letter_grade"] in {"A", "B", "C", "D", "F"}


# ── _aggregate_chain_rows ────────────────────────────────────────────────────

class TestAggregateChainRows:
    def _make_flyer_row(self, flyer_id, store_chain, flyer_grade, item_count=10, hot_count=3):
        return {
            "flyer_id": flyer_id,
            "store_chain": store_chain,
            "flyer_grade": flyer_grade,
            "item_count": item_count,
            "hot_count": hot_count,
            "good_count": 5,
            "avg_score": flyer_grade,
            "top10_avg": flyer_grade,
            "hot_ratio": round(hot_count / item_count, 4),
            "letter_grade": letter_grade(flyer_grade),
            "flyer_valid_from": "2026-05-05",
            "flyer_valid_to": "2026-05-11",
            "week_label": "2026-W20",
        }

    def test_empty(self):
        assert _aggregate_chain_rows([], "2026-W20") == []

    def test_single_chain(self):
        flyer_rows = [self._make_flyer_row("F1", "loblaws", 70.0)]
        result = _aggregate_chain_rows(flyer_rows, "2026-W20")
        assert len(result) == 1
        assert result[0]["store_chain"] == "loblaws"
        assert result[0]["rank"] == 1
        assert result[0]["week_label"] == "2026-W20"

    def test_ranking_order(self):
        flyer_rows = [
            self._make_flyer_row("F1", "nofrills", 50.0),
            self._make_flyer_row("F2", "loblaws", 80.0),
            self._make_flyer_row("F3", "metro", 65.0),
        ]
        result = _aggregate_chain_rows(flyer_rows, "2026-W20")
        assert result[0]["store_chain"] == "loblaws"
        assert result[1]["store_chain"] == "metro"
        assert result[2]["store_chain"] == "nofrills"
        assert [r["rank"] for r in result] == [1, 2, 3]

    def test_multiple_flyers_per_chain(self):
        flyer_rows = [
            self._make_flyer_row("F1", "loblaws", 80.0),
            self._make_flyer_row("F2", "loblaws", 60.0),
        ]
        result = _aggregate_chain_rows(flyer_rows, "2026-W20")
        assert len(result) == 1
        assert result[0]["avg_flyer_grade"] == 70.0
        assert result[0]["flyer_count"] == 2


# ── rank_flyers (integration) ─────────────────────────────────────────────────

class TestRankFlyers:
    def _write_scored_rows(self, rows: list[dict], path: str) -> None:
        _write_parquet(rows, path)

    def test_rank_flyers_creates_files(self):
        rows = [
            _make_scored_row(flyer_id="F1", store_chain="loblaws", deal_score=80),
            _make_scored_row(flyer_id="F1", store_chain="loblaws", deal_score=70, sku="S2"),
            _make_scored_row(flyer_id="F2", store_chain="nofrills", deal_score=60),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_path = os.path.join(tmpdir, "active_scores.parquet")
            out_dir = os.path.join(tmpdir, "rankings")
            self._write_scored_rows(rows, scores_path)

            flyer_rows, chain_rows = rank_flyers(scores_path, out_dir, "2026-W20")

            assert len(flyer_rows) == 2
            assert len(chain_rows) == 2
            assert os.path.exists(os.path.join(out_dir, "current_flyer_rankings.parquet"))
            assert os.path.exists(os.path.join(out_dir, "current_chain_rankings.parquet"))
            assert os.path.exists(os.path.join(out_dir, "weekly_history.parquet"))

    def test_history_append_dedup(self):
        rows = [
            _make_scored_row(flyer_id="F1", store_chain="loblaws", deal_score=80),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_path = os.path.join(tmpdir, "active_scores.parquet")
            out_dir = os.path.join(tmpdir, "rankings")
            self._write_scored_rows(rows, scores_path)

            # First run
            rank_flyers(scores_path, out_dir, "2026-W20")
            hist1 = _read_parquet(os.path.join(out_dir, "weekly_history.parquet"))
            assert len(hist1) == 1

            # Second run with same week — should not duplicate
            rank_flyers(scores_path, out_dir, "2026-W20")
            hist2 = _read_parquet(os.path.join(out_dir, "weekly_history.parquet"))
            assert len(hist2) == 1

    def test_history_different_weeks(self):
        rows = [
            _make_scored_row(flyer_id="F1", store_chain="loblaws", deal_score=80),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_path = os.path.join(tmpdir, "active_scores.parquet")
            out_dir = os.path.join(tmpdir, "rankings")
            self._write_scored_rows(rows, scores_path)

            rank_flyers(scores_path, out_dir, "2026-W19")
            rank_flyers(scores_path, out_dir, "2026-W20")
            hist = _read_parquet(os.path.join(out_dir, "weekly_history.parquet"))
            weeks = {r["week_label"] for r in hist}
            assert weeks == {"2026-W19", "2026-W20"}

    def test_empty_scores(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_path = os.path.join(tmpdir, "active_scores.parquet")
            out_dir = os.path.join(tmpdir, "rankings")
            _write_parquet([], scores_path)

            flyer_rows, chain_rows = rank_flyers(scores_path, out_dir, "2026-W20")
            assert flyer_rows == []
            assert chain_rows == []

    def test_missing_scores_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_path = os.path.join(tmpdir, "nonexistent.parquet")
            out_dir = os.path.join(tmpdir, "rankings")

            flyer_rows, chain_rows = rank_flyers(scores_path, out_dir, "2026-W20")
            assert flyer_rows == []
            assert chain_rows == []

    def test_default_week_label(self):
        """rank_flyers with week_label=None should use a valid ISO week string."""
        import datetime
        rows = [_make_scored_row(deal_score=70)]
        with tempfile.TemporaryDirectory() as tmpdir:
            scores_path = os.path.join(tmpdir, "active_scores.parquet")
            out_dir = os.path.join(tmpdir, "rankings")
            self._write_scored_rows(rows, scores_path)

            flyer_rows, chain_rows = rank_flyers(scores_path, out_dir)
            # Week label should look like "YYYY-Www"
            today = datetime.date.today()
            year, week, _ = today.isocalendar()
            expected = f"{year}-W{week:02d}"
            assert chain_rows[0]["week_label"] == expected
