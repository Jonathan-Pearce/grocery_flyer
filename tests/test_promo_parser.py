"""Tests for parsers/promo_parser.py — parse_promo()."""

from __future__ import annotations

import pytest

from parsers.promo_parser import parse_promo


# ── Table-driven cases from the issue spec ────────────────────────────────────


class TestPromoTypes:
    """One test per promo type from the issue spec table."""

    def test_rollback(self):
        r = parse_promo("Rollback")
        assert r["promo_type"] == "rollback"

    def test_percentage_off_save_caps(self):
        r = parse_promo("SAVE 25%")
        assert r["promo_type"] == "percentage_off"

    def test_percentage_off_lowercase(self):
        r = parse_promo("15% off")
        assert r["promo_type"] == "percentage_off"

    def test_percentage_off_uppercase(self):
        r = parse_promo("15% OFF")
        assert r["promo_type"] == "percentage_off"

    def test_percentage_off_french(self):
        r = parse_promo("Économisez 42%")
        assert r["promo_type"] == "percentage_off"

    def test_dollar_off_with_dollar_sign(self):
        r = parse_promo("SAVE $1.80")
        assert r["promo_type"] == "dollar_off"

    def test_dollar_off_no_dollar_sign(self):
        r = parse_promo("SAVE .99")
        assert r["promo_type"] == "dollar_off"

    def test_dollar_off_up_to(self):
        r = parse_promo("SAVE UP TO $5")
        assert r["promo_type"] == "dollar_off"

    def test_multi_buy_for(self):
        r = parse_promo("2 for $5")
        assert r["promo_type"] == "multi_buy"

    def test_multi_buy_buy_save(self):
        r = parse_promo("buy 2 save $3")
        assert r["promo_type"] == "multi_buy"

    def test_bogo_keyword(self):
        r = parse_promo("BOGO")
        assert r["promo_type"] == "bogo"

    def test_bogo_buy_get(self):
        r = parse_promo("buy 1 get 1")
        assert r["promo_type"] == "bogo"

    def test_loyalty_scene_plus(self):
        r = parse_promo("100 Scene+ PTS when you buy 2")
        assert r["promo_type"] == "loyalty_points"

    def test_loyalty_pc_optimum(self):
        r = parse_promo("PC Optimum 6,000 pts")
        assert r["promo_type"] == "loyalty_points"

    def test_member_price(self):
        r = parse_promo(None, member_price=4.99)
        assert r["promo_type"] == "member_price"

    def test_clearance_english(self):
        r = parse_promo("clearance")
        assert r["promo_type"] == "clearance"

    def test_clearance_french(self):
        r = parse_promo("liquidation")
        assert r["promo_type"] == "clearance"

    def test_no_promo_empty(self):
        r = parse_promo("")
        assert r["promo_type"] == "no_promo"

    def test_no_promo_none(self):
        r = parse_promo(None)
        assert r["promo_type"] == "no_promo"

    def test_no_promo_weekly_specials(self):
        r = parse_promo("Weekly specials")
        assert r["promo_type"] == "no_promo"


# ── Loyalty extraction ────────────────────────────────────────────────────────


class TestLoyaltyExtraction:
    """Scene+ and PC Optimum loyalty values correctly extracted."""

    def test_scene_plus_program(self):
        r = parse_promo("100 Scene+ PTS when you buy 2")
        assert r["loyalty_program"] == "Scene+"

    def test_scene_plus_points(self):
        r = parse_promo("100 Scene+ PTS when you buy 2")
        assert r["loyalty_points"] == 100

    def test_scene_plus_trigger(self):
        r = parse_promo("100 Scene+ PTS when you buy 2")
        assert r["loyalty_trigger"] == "when you buy 2"

    def test_pc_optimum_program(self):
        r = parse_promo("PC Optimum 6,000 pts")
        assert r["loyalty_program"] == "PC Optimum"

    def test_pc_optimum_points_comma_thousands(self):
        """Comma thousands-separator must be stripped before int conversion."""
        r = parse_promo("PC Optimum 6,000 pts")
        assert r["loyalty_points"] == 6000

    def test_pc_optimum_no_trigger_when_absent(self):
        r = parse_promo("PC Optimum 6,000 pts")
        assert "loyalty_trigger" not in r

    def test_loyalty_keys_absent_for_non_loyalty(self):
        r = parse_promo("SAVE 25%")
        assert "loyalty_program" not in r
        assert "loyalty_points" not in r
        assert "loyalty_trigger" not in r

    def test_scene_plus_case_insensitive(self):
        r = parse_promo("500 scene+ pts")
        assert r["promo_type"] == "loyalty_points"
        assert r["loyalty_program"] == "Scene+"
        assert r["loyalty_points"] == 500

    def test_pc_optimum_points_plain_integer(self):
        r = parse_promo("PC Optimum 500 pts")
        assert r["loyalty_points"] == 500

    def test_scene_plus_reversed_order(self):
        """Scene+ before the point value: 'Scene+ 200 PTS'."""
        r = parse_promo("Scene+ 200 PTS")
        assert r["promo_type"] == "loyalty_points"
        assert r["loyalty_program"] == "Scene+"
        assert r["loyalty_points"] == 200


# ── Day-restriction extraction ────────────────────────────────────────────────


class TestDayRestriction:
    """Day-restricted deals populated in promo_details."""

    def test_french_day_names_in_prefix_en(self):
        r = parse_promo("SAVE 25%", prefix_en="jeudi et vendredi")
        assert r["promo_type"] == "percentage_off"
        assert r["promo_details"] == "jeudi et vendredi"

    def test_english_day_names_in_prefix_fr(self):
        r = parse_promo("SAVE $1.80", prefix_fr="Thursday and Friday")
        assert r["promo_type"] == "dollar_off"
        assert r["promo_details"] == "Thursday and Friday"

    def test_prefix_en_takes_precedence_over_prefix_fr(self):
        r = parse_promo("clearance", prefix_en="samedi", prefix_fr="vendredi")
        assert r["promo_details"] == "samedi"

    def test_no_day_names_promo_details_is_raw_text(self):
        r = parse_promo("SAVE 25%")
        assert r["promo_details"] == "SAVE 25%"

    def test_day_detection_case_insensitive(self):
        r = parse_promo("SAVE 25%", prefix_en="Monday only")
        assert r["promo_details"] == "Monday only"

    def test_prefix_without_day_names_does_not_override_details(self):
        r = parse_promo("SAVE 25%", prefix_en="Sale price")
        assert r["promo_details"] == "SAVE 25%"

    def test_day_restriction_with_no_promo(self):
        r = parse_promo(None, prefix_en="dimanche")
        assert r["promo_type"] == "no_promo"
        assert r["promo_details"] == "dimanche"


# ── Unknown / no-promo strings ────────────────────────────────────────────────


class TestUnknownStrings:
    """Unknown strings classify as no_promo with raw string preserved."""

    def test_unknown_raw_preserved(self):
        r = parse_promo("Weekly specials")
        assert r["promo_type"] == "no_promo"
        assert r["promo_details"] == "Weekly specials"

    def test_none_promo_details_is_none(self):
        r = parse_promo(None)
        assert r["promo_type"] == "no_promo"
        assert r["promo_details"] is None

    def test_empty_promo_details_is_none(self):
        r = parse_promo("")
        assert r["promo_type"] == "no_promo"
        assert r["promo_details"] is None

    def test_whitespace_only_treated_as_empty(self):
        r = parse_promo("   ")
        assert r["promo_type"] == "no_promo"
        assert r["promo_details"] is None

    def test_promo_type_always_present(self):
        for text in ["", None, "Weekly specials", "SAVE 25%", "BOGO"]:
            r = parse_promo(text)
            assert "promo_type" in r

    def test_promo_details_always_present(self):
        for text in ["", None, "Weekly specials", "SAVE 25%"]:
            r = parse_promo(text)
            assert "promo_details" in r


# ── Priority ordering ─────────────────────────────────────────────────────────


class TestPriorityOrdering:
    """Verify that higher-priority types override lower-priority ones."""

    def test_percentage_off_beats_dollar_off(self):
        """'SAVE 25%' has % so it's percentage_off, not dollar_off."""
        r = parse_promo("SAVE 25%")
        assert r["promo_type"] == "percentage_off"

    def test_bogo_beats_multi_buy(self):
        """'buy 1 get 1' is bogo, not multi_buy."""
        r = parse_promo("buy 1 get 1")
        assert r["promo_type"] == "bogo"

    def test_loyalty_beats_percentage_off(self):
        """A loyalty offer with % still resolves as loyalty_points."""
        r = parse_promo("Earn 500 Scene+ PTS on 25% off items")
        assert r["promo_type"] == "loyalty_points"

    def test_member_price_fallback_when_no_text(self):
        """member_price fires only when no text promo is present."""
        r = parse_promo(None, member_price=3.99)
        assert r["promo_type"] == "member_price"

    def test_member_price_overridden_by_text_promo(self):
        """If text identifies a promo, member_price stays as a price field."""
        r = parse_promo("SAVE 25%", member_price=3.99)
        assert r["promo_type"] == "percentage_off"

    def test_bogo_keyword_lowercase(self):
        r = parse_promo("bogo")
        assert r["promo_type"] == "bogo"

    def test_rollback_mixed_case(self):
        r = parse_promo("ROLLBACK")
        assert r["promo_type"] == "rollback"

    def test_clearance_mixed_case(self):
        r = parse_promo("CLEARANCE")
        assert r["promo_type"] == "clearance"

    def test_liquidation_mixed_case(self):
        r = parse_promo("LIQUIDATION")
        assert r["promo_type"] == "clearance"

    def test_buy_one_get_one_free(self):
        r = parse_promo("buy one get one free")
        assert r["promo_type"] == "bogo"


# ── No-exception guarantee ────────────────────────────────────────────────────


class TestNoExceptions:
    @pytest.mark.parametrize(
        "text",
        [
            "Rollback",
            "SAVE 25%",
            "15% off",
            "Économisez 42%",
            "SAVE $1.80",
            "SAVE .99",
            "SAVE UP TO $5",
            "2 for $5",
            "buy 2 save $3",
            "buy 1 get 1",
            "BOGO",
            "100 Scene+ PTS when you buy 2",
            "PC Optimum 6,000 pts",
            "clearance",
            "liquidation",
            "Weekly specials",
            "",
            None,
            "   ",
            "some unknown promo text @@##",
        ],
    )
    def test_no_exception_on_varied_inputs(self, text):
        """parse_promo must not raise on any input."""
        r = parse_promo(text)
        assert "promo_type" in r
        assert "promo_details" in r
