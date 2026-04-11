"""Tests for pipeline/deal_scorer.py."""

from __future__ import annotations

import datetime
import os

import pytest

# Lazy-import helpers so we can skip if pyyaml/pyarrow not installed

yaml = pytest.importorskip("yaml")
pyarrow = pytest.importorskip("pyarrow")

from pipeline.deal_scorer import (  # noqa: E402
    _bracket_pts,
    _bracket_pts_max,
    _calc_confidence,
    _confidence_label,
    _load_config,
    _parse_date,
    _score_authenticity,
    _score_cycle_position,
    _score_deal_rarity,
    _score_discount_depth,
    _score_essentiality,
    _score_loyalty_bonus,
    score_deals,
)


# ── Fixture: scoring config ───────────────────────────────────────────────────

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "config", "scoring.yaml"
)


@pytest.fixture(scope="module")
def cfg():
    return _load_config(CONFIG_PATH)


# ── _parse_date ───────────────────────────────────────────────────────────────


class TestParseDate:
    def test_iso_string(self):
        assert _parse_date("2026-04-11") == datetime.date(2026, 4, 11)

    def test_datetime_object(self):
        dt = datetime.datetime(2026, 4, 11, 12, 0, 0)
        assert _parse_date(dt) == datetime.date(2026, 4, 11)

    def test_date_object(self):
        d = datetime.date(2026, 4, 11)
        assert _parse_date(d) == d

    def test_none_returns_none(self):
        assert _parse_date(None) is None

    def test_invalid_returns_none(self):
        assert _parse_date("not-a-date") is None


# ── _bracket_pts ─────────────────────────────────────────────────────────────


class TestBracketPts:
    BRACKETS = [
        {"min_pct": 40, "pts": 25},
        {"min_pct": 30, "pts": 20},
        {"min_pct": 10, "pts": 8},
        {"min_pct": 0, "pts": 0},
    ]

    def test_exact_threshold(self):
        assert _bracket_pts(40, self.BRACKETS, "min_pct", "pts") == 25

    def test_between_brackets(self):
        assert _bracket_pts(35, self.BRACKETS, "min_pct", "pts") == 20

    def test_below_all(self):
        # 0 should match the last bracket (min_pct: 0)
        assert _bracket_pts(0, self.BRACKETS, "min_pct", "pts") == 0

    def test_none_returns_zero(self):
        assert _bracket_pts(None, self.BRACKETS, "min_pct", "pts") == 0


# ── _bracket_pts_max ─────────────────────────────────────────────────────────


class TestBracketPtsMax:
    BRACKETS = [
        {"max_freq": 0.10, "pts": 20},
        {"max_freq": 0.25, "pts": 15},
        {"max_freq": 0.40, "pts": 10},
        {"max_freq": 1.01, "pts": 2},
    ]

    def test_low_freq(self):
        assert _bracket_pts_max(0.05, self.BRACKETS, "max_freq", "pts") == 20

    def test_exact_boundary_exclusive(self):
        # 0.10 is NOT < 0.10, so moves to next bracket
        assert _bracket_pts_max(0.10, self.BRACKETS, "max_freq", "pts") == 15

    def test_high_freq(self):
        assert _bracket_pts_max(0.95, self.BRACKETS, "max_freq", "pts") == 2

    def test_none_returns_zero(self):
        assert _bracket_pts_max(None, self.BRACKETS, "max_freq", "pts") == 0


# ── Component 1: Discount Depth ───────────────────────────────────────────────


class TestScoreDiscountDepth:
    def test_40pct_off_gives_max_pts(self, cfg):
        # 40% off a $10 item = $4 saved, bonus from dollar bracket
        score = _score_discount_depth(6.0, 10.0, None, None, cfg)
        assert score == cfg["discount_depth"]["max_pts"]

    def test_zero_discount_gives_zero_base(self, cfg):
        # same price → 0% off → pts=0, but dollar bonus may add something
        score = _score_discount_depth(10.0, 10.0, None, None, cfg)
        assert score == 0

    def test_cold_start_no_regular_price(self, cfg):
        score = _score_discount_depth(3.99, None, None, None, cfg)
        assert score == cfg["discount_depth"]["cold_start_pts"]

    def test_multi_buy_normalisation(self, cfg):
        # 2/$5 = $2.50/unit. Regular $4.00 → 37.5% off
        score_multi = _score_discount_depth(None, 4.0, 2, 5.0, cfg)
        # 37.5% falls in the 30–39% bracket
        score_direct = _score_discount_depth(2.5, 4.0, None, None, cfg)
        assert score_multi == score_direct

    def test_score_capped_at_max(self, cfg):
        # Extreme discount — result must not exceed max_pts
        score = _score_discount_depth(0.01, 100.0, None, None, cfg)
        assert score <= cfg["discount_depth"]["max_pts"]

    def test_score_never_negative(self, cfg):
        score = _score_discount_depth(999.0, 1.0, None, None, cfg)
        assert score >= 0


# ── Component 2: Deal Rarity ──────────────────────────────────────────────────


class TestScoreDealRarity:
    def test_rare_deal_high_score(self, cfg):
        # 5% frequency → rare
        score = _score_deal_rarity(0.05, False, cfg)
        assert score >= 15

    def test_common_deal_low_score(self, cfg):
        # 80% frequency → common
        score = _score_deal_rarity(0.80, False, cfg)
        assert score <= 5

    def test_cold_start(self, cfg):
        score = _score_deal_rarity(None, False, cfg)
        assert score == cfg["deal_rarity"]["cold_start_pts"]

    def test_cross_chain_bonus_applied(self, cfg):
        score_no_excl = _score_deal_rarity(0.05, False, cfg)
        score_excl = _score_deal_rarity(0.05, True, cfg)
        assert score_excl == min(
            cfg["deal_rarity"]["max_pts"],
            score_no_excl + cfg["deal_rarity"]["cross_chain_exclusive_bonus"],
        )

    def test_score_capped_at_max(self, cfg):
        score = _score_deal_rarity(0.01, True, cfg)
        assert score <= cfg["deal_rarity"]["max_pts"]


# ── Component 3: Item Essentiality ────────────────────────────────────────────


class TestScoreEssentiality:
    def test_tier1_category(self, cfg):
        score = _score_essentiality("Produce", None, None, cfg)
        assert score == cfg["essentiality"]["max_pts"]

    def test_tier5_category(self, cfg):
        score = _score_essentiality("Apparel & General Merchandise", None, None, cfg)
        assert score == 0

    def test_staple_keyword_override(self, cfg):
        # "milk" should override to tier 1 regardless of category
        score = _score_essentiality("Other", "2% Milk 4L", None, cfg)
        assert score == cfg["essentiality"]["max_pts"]

    def test_staple_keyword_french(self, cfg):
        score = _score_essentiality("Other", None, "Pain de blé entier", cfg)
        assert score == cfg["essentiality"]["max_pts"]

    def test_unknown_category_defaults_to_tier5(self, cfg):
        score = _score_essentiality("UnknownCat", None, None, cfg)
        assert score == 0

    def test_none_category(self, cfg):
        score = _score_essentiality(None, None, None, cfg)
        assert score >= 0


# ── Component 4: Price Cycle Position ─────────────────────────────────────────


class TestScoreCyclePosition:
    def test_at_yearly_low(self, cfg):
        # price == cycle_low → percentile = 0 → best score
        score = _score_cycle_position(2.0, 2.0, 5.0, cfg)
        assert score == cfg["cycle_position"]["max_pts"]

    def test_at_yearly_high(self, cfg):
        # price == cycle_high → percentile = 1 → worst score
        score = _score_cycle_position(5.0, 2.0, 5.0, cfg)
        assert score == 0

    def test_cold_start_no_cycle(self, cfg):
        score = _score_cycle_position(3.0, None, None, cfg)
        assert score == cfg["cycle_position"]["cold_start_pts"]

    def test_flat_cycle_uses_cold_start(self, cfg):
        # cycle_high == cycle_low → division by zero → cold start
        score = _score_cycle_position(3.0, 3.0, 3.0, cfg)
        assert score == cfg["cycle_position"]["cold_start_pts"]

    def test_midpoint(self, cfg):
        # price at midpoint → percentile = 0.5 → mid-range score
        score = _score_cycle_position(3.5, 2.0, 5.0, cfg)
        assert 0 <= score <= cfg["cycle_position"]["max_pts"]


# ── Component 5: Deal Authenticity ────────────────────────────────────────────


class TestScoreAuthenticity:
    TODAY = datetime.date(2026, 4, 11)

    def test_fresh_bogo_with_inflation(self, cfg):
        # bogo + 25% inflation + week 1 flyer → high authenticity
        score = _score_authenticity(
            sale_price=3.0,
            regular_price_estimated=4.0,
            promo_type="bogo",
            purchase_limit=None,
            flyer_valid_from=datetime.date(2026, 4, 8),  # 3 days ago → week 1
            today=self.TODAY,
            cfg=cfg,
        )
        assert score > 8

    def test_no_promo_no_inflation(self, cfg):
        score = _score_authenticity(
            sale_price=5.0,
            regular_price_estimated=5.0,
            promo_type="no_promo",
            purchase_limit=None,
            flyer_valid_from=None,
            today=self.TODAY,
            cfg=cfg,
        )
        assert score == 0

    def test_purchase_limit_1_penalty(self, cfg):
        score_no_limit = _score_authenticity(
            sale_price=3.0,
            regular_price_estimated=5.0,
            promo_type="percentage_off",
            purchase_limit=None,
            flyer_valid_from=datetime.date(2026, 4, 10),
            today=self.TODAY,
            cfg=cfg,
        )
        score_limit_1 = _score_authenticity(
            sale_price=3.0,
            regular_price_estimated=5.0,
            promo_type="percentage_off",
            purchase_limit=1,
            flyer_valid_from=datetime.date(2026, 4, 10),
            today=self.TODAY,
            cfg=cfg,
        )
        assert score_limit_1 == max(
            0, score_no_limit + cfg["authenticity"]["purchase_limit_1_penalty"]
        )

    def test_stale_flyer_penalty(self, cfg):
        # Flyer started 21 days ago → week 3+
        score_fresh = _score_authenticity(
            sale_price=3.0,
            regular_price_estimated=5.0,
            promo_type="percentage_off",
            purchase_limit=None,
            flyer_valid_from=datetime.date(2026, 4, 10),  # 1 day ago
            today=self.TODAY,
            cfg=cfg,
        )
        score_stale = _score_authenticity(
            sale_price=3.0,
            regular_price_estimated=5.0,
            promo_type="percentage_off",
            purchase_limit=None,
            flyer_valid_from=datetime.date(2026, 3, 21),  # 21 days ago
            today=self.TODAY,
            cfg=cfg,
        )
        assert score_fresh > score_stale

    def test_score_never_negative(self, cfg):
        score = _score_authenticity(5.0, 3.0, "no_promo", 1, None, self.TODAY, cfg)
        assert score >= 0

    def test_score_capped_at_max(self, cfg):
        score = _score_authenticity(
            sale_price=1.0,
            regular_price_estimated=100.0,
            promo_type="bogo",
            purchase_limit=None,
            flyer_valid_from=datetime.date(2026, 4, 11),
            today=self.TODAY,
            cfg=cfg,
        )
        assert score <= cfg["authenticity"]["max_pts"]


# ── Component 6: Loyalty & Stacking ──────────────────────────────────────────


class TestScoreLoyaltyBonus:
    def test_no_loyalty_no_bonus(self, cfg):
        score = _score_loyalty_bonus(None, None, 5.0, None, cfg)
        assert score == 0

    def test_member_price_stack(self, cfg):
        # member_price < sale_price → stacking bonus
        score = _score_loyalty_bonus(None, None, 5.0, 4.50, cfg)
        assert score == cfg["loyalty_bonus"]["member_price_stack_bonus"]

    def test_loyalty_points_value(self, cfg):
        # PC Optimum: 10000 pts = $1. 20000 pts → $2 CAD
        score = _score_loyalty_bonus("PC Optimum", 20000, 5.0, None, cfg)
        assert score > 0

    def test_score_capped_at_max(self, cfg):
        score = _score_loyalty_bonus("PC Optimum", 100000, 5.0, 3.0, cfg)
        assert score <= cfg["loyalty_bonus"]["max_pts"]


# ── Confidence ────────────────────────────────────────────────────────────────


class TestCalcConfidence:
    def test_fully_observed_gives_high_confidence(self, cfg):
        conf, *_ = _calc_confidence(52, 1.0, "strict", 5, 100, cfg)
        assert conf >= 0.75

    def test_cold_start_gives_low_confidence(self, cfg):
        conf, *_ = _calc_confidence(0, 0.0, "category", 0, 0, cfg)
        assert conf < 0.45

    def test_confidence_in_range(self, cfg):
        for weeks in (0, 4, 12, 52):
            conf, *_ = _calc_confidence(weeks, 0.5, "probable", 2, 10, cfg)
            assert 0.0 <= conf <= 1.0

    def test_confidence_label_high(self, cfg):
        label = _confidence_label(0.80, cfg)
        assert label == cfg["confidence"]["label_high"]

    def test_confidence_label_medium(self, cfg):
        label = _confidence_label(0.60, cfg)
        assert label == cfg["confidence"]["label_medium"]

    def test_confidence_label_low(self, cfg):
        label = _confidence_label(0.30, cfg)
        assert label == cfg["confidence"]["label_low"]


# ── Integration: score_deals ──────────────────────────────────────────────────


def _write_obs_parquet(path: str, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _make_obs(
    store_chain: str = "loblaws",
    store_id: str = "1001",
    flyer_id: str = "FLY001",
    sku: str = "SKU001",
    name_en: str = "Whole Milk 4L",
    sale_price: float = 4.99,
    regular_price: float | None = 6.49,
    promo_type: str = "percentage_off",
    category_l1: str = "Dairy & Eggs",
    flyer_valid_from: str = "2026-04-07",
    flyer_valid_to: str = "2026-04-13",
) -> dict:
    return {
        "store_chain": store_chain,
        "store_id": store_id,
        "flyer_id": flyer_id,
        "sku": sku,
        "name_en": name_en,
        "sale_price": sale_price,
        "regular_price": regular_price,
        "promo_type": promo_type,
        "category_l1": category_l1,
        "flyer_valid_from": flyer_valid_from,
        "flyer_valid_to": flyer_valid_to,
        "source_api": "flipp",
        "purchase_limit": None,
        "loyalty_program": None,
        "loyalty_points": None,
        "member_price": None,
        "multi_buy_qty": None,
        "multi_buy_total": None,
        "brand": None,
        "weight_unit": None,
        "category_l3": None,
    }


class TestScoreDeals:
    TODAY = datetime.date(2026, 4, 11)

    def test_active_rows_filtered(self, tmp_path):
        # Only rows with flyer_valid_from <= TODAY <= flyer_valid_to should appear
        obs_dir = str(tmp_path / "observations")
        active_obs = _make_obs()  # 2026-04-07 to 2026-04-13 — active on TODAY
        future_obs = _make_obs(
            sku="SKU002",
            flyer_id="FLY002",
            flyer_valid_from="2026-04-20",
            flyer_valid_to="2026-04-26",
        )
        past_obs = _make_obs(
            sku="SKU003",
            flyer_id="FLY003",
            flyer_valid_from="2026-03-01",
            flyer_valid_to="2026-03-07",
        )
        _write_obs_parquet(
            os.path.join(obs_dir, "obs.parquet"),
            [active_obs, future_obs, past_obs],
        )

        out_dir = str(tmp_path / "scores")
        n = score_deals(
            observations_dir=obs_dir,
            price_history_path=str(tmp_path / "nonexistent_ph"),
            config_path=CONFIG_PATH,
            out_dir=out_dir,
            today=self.TODAY,
        )
        assert n == 1

    def test_deal_score_in_range(self, tmp_path):
        obs_dir = str(tmp_path / "observations")
        _write_obs_parquet(
            os.path.join(obs_dir, "obs.parquet"),
            [_make_obs()],
        )
        out_dir = str(tmp_path / "scores")
        score_deals(
            observations_dir=obs_dir,
            price_history_path=str(tmp_path / "nonexistent_ph"),
            config_path=CONFIG_PATH,
            out_dir=out_dir,
            today=self.TODAY,
        )

        import pyarrow.parquet as pq

        table = pq.read_table(os.path.join(out_dir, "active_scores.parquet"))
        rows = table.to_pylist()
        assert len(rows) == 1
        assert 0 <= rows[0]["deal_score"] <= 100
        assert 0.0 <= rows[0]["confidence"] <= 1.0

    def test_idempotent_active_scores(self, tmp_path):
        obs_dir = str(tmp_path / "observations")
        _write_obs_parquet(
            os.path.join(obs_dir, "obs.parquet"),
            [_make_obs()],
        )
        out_dir = str(tmp_path / "scores")
        kwargs = dict(
            observations_dir=obs_dir,
            price_history_path=str(tmp_path / "nonexistent_ph"),
            config_path=CONFIG_PATH,
            out_dir=out_dir,
            today=self.TODAY,
        )

        score_deals(**kwargs)
        score_deals(**kwargs)

        import pyarrow.parquet as pq

        table = pq.read_table(os.path.join(out_dir, "active_scores.parquet"))
        # Should still have exactly 1 row (overwrite, not append)
        assert len(table) == 1

    def test_archive_no_duplicates(self, tmp_path):
        obs_dir = str(tmp_path / "observations")
        _write_obs_parquet(
            os.path.join(obs_dir, "obs.parquet"),
            [_make_obs()],
        )
        out_dir = str(tmp_path / "scores")
        kwargs = dict(
            observations_dir=obs_dir,
            price_history_path=str(tmp_path / "nonexistent_ph"),
            config_path=CONFIG_PATH,
            out_dir=out_dir,
            today=self.TODAY,
        )

        score_deals(**kwargs)
        score_deals(**kwargs)

        import pyarrow.parquet as pq

        table = pq.read_table(os.path.join(out_dir, "archived_scores.parquet"))
        # Should have exactly 1 row — deduplication should prevent doubling
        assert len(table) == 1

    def test_output_columns_present(self, tmp_path):
        obs_dir = str(tmp_path / "observations")
        _write_obs_parquet(
            os.path.join(obs_dir, "obs.parquet"),
            [_make_obs()],
        )
        out_dir = str(tmp_path / "scores")
        score_deals(
            observations_dir=obs_dir,
            price_history_path=str(tmp_path / "nonexistent_ph"),
            config_path=CONFIG_PATH,
            out_dir=out_dir,
            today=self.TODAY,
        )

        import pyarrow.parquet as pq

        schema_names = pq.read_table(
            os.path.join(out_dir, "active_scores.parquet")
        ).schema.names
        expected = [
            "deal_score",
            "score_discount_depth",
            "score_deal_rarity",
            "score_essentiality",
            "score_cycle_position",
            "score_authenticity",
            "score_loyalty_bonus",
            "confidence",
            "confidence_history_depth",
            "confidence_price_basis",
            "confidence_match_tier",
            "confidence_chain_coverage",
            "confidence_category_coverage",
            "confidence_label",
            "match_tier",
            "regular_price_estimated",
            "regular_price_source",
            "scored_on",
        ]
        for col in expected:
            assert col in schema_names, f"Missing column: {col}"

    def test_cold_start_neutral_not_zero(self, tmp_path):
        """Cold-start rows must use neutral values, not zeros."""
        obs_dir = str(tmp_path / "observations")
        # Observation with no sku/brand (→ category-tier match) and no price history
        obs = _make_obs(sku=None)
        obs["sku"] = None
        _write_obs_parquet(
            os.path.join(obs_dir, "obs.parquet"),
            [obs],
        )
        out_dir = str(tmp_path / "scores")
        score_deals(
            observations_dir=obs_dir,
            price_history_path=str(tmp_path / "nonexistent_ph"),
            config_path=CONFIG_PATH,
            out_dir=out_dir,
            today=self.TODAY,
        )

        import pyarrow.parquet as pq

        row = pq.read_table(os.path.join(out_dir, "active_scores.parquet")).to_pylist()[0]
        # cycle_position should be cold_start_pts (7), not 0
        assert row["score_cycle_position"] == 7
        # deal_rarity should be cold_start_pts (10), not 0
        assert row["score_deal_rarity"] == 10
