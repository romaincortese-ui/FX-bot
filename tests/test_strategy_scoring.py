from fxbot.strategies.scoring import StrategyScoringContext
from fxbot.strategies.scoring import _apply_calibration
from fxbot.strategies.scoring import _apply_target_adjustments
from fxbot.strategies.scoring import _macro_bias_conflicts_direction
from fxbot.strategies.scoring import _strategy_blocked_by_news_pause


def test_macro_bias_conflict_blocks_opposite_direction():
    assert _macro_bias_conflicts_direction("LONG", "SHORT_ONLY") is True
    assert _macro_bias_conflicts_direction("SHORT", "LONG_ONLY") is True
    assert _macro_bias_conflicts_direction("LONG", "LONG_ONLY") is False


def test_target_adjustments_expand_with_aligned_macro_and_shrink_in_high_vix():
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(20.0, 10.0, "LONG", "LONG_ONLY", 24.0)

    assert round(tp_pips, 2) == 20.8
    assert round(sl_pips, 2) == 7.2
    assert macro_confidence == 1.0


def test_news_pause_blocks_non_carry_strategies_only():
    assert _strategy_blocked_by_news_pause("TREND", True) is True
    assert _strategy_blocked_by_news_pause("CARRY", True) is False
    assert _strategy_blocked_by_news_pause("BREAKOUT", False) is False


def test_apply_calibration_adjusts_selection_score_and_threshold():
    rejected = []
    ctx = StrategyScoringContext(
        get_spread_pips=lambda instrument: 1.0,
        fetch_candles=lambda instrument, granularity, count: None,
        reject=lambda strategy, instrument, reason: rejected.append((strategy, instrument, reason)),
        mark_pair_failure=lambda *args, **kwargs: None,
        determine_direction=lambda *args, **kwargs: "LONG",
        get_post_news_events=lambda instrument, now=None: [],
        apply_macro_directional_bias=None,
        macro_filters={},
        macro_news=[],
        is_pair_paused_by_news=lambda instrument, now=None: False,
        market_regime_mult=1.0,
        adaptive_offsets={},
        dxy_ema_gap=None,
        dxy_gate_threshold=0.0,
        vix_level=None,
        vix_low_threshold=15.0,
        get_trade_calibration_adjustment=lambda strategy, instrument, session_name: {
            "threshold_offset": 4.0,
            "risk_mult": 0.5,
            "block_reason": None,
            "source": "pair",
        },
    )

    calibrated = _apply_calibration("TREND", "USD_JPY", {"name": "TOKYO"}, 60.0, 45.0, ctx)

    assert calibrated is not None
    selection_score, adjusted_threshold, adjustment = calibrated
    assert selection_score == 26.0
    assert adjusted_threshold == 49.0
    assert adjustment["source"] == "pair"
    assert rejected == []


def test_apply_calibration_rejects_blocked_setup():
    rejected = []
    ctx = StrategyScoringContext(
        get_spread_pips=lambda instrument: 1.0,
        fetch_candles=lambda instrument, granularity, count: None,
        reject=lambda strategy, instrument, reason: rejected.append((strategy, instrument, reason)),
        mark_pair_failure=lambda *args, **kwargs: None,
        determine_direction=lambda *args, **kwargs: "LONG",
        get_post_news_events=lambda instrument, now=None: [],
        apply_macro_directional_bias=None,
        macro_filters={},
        macro_news=[],
        is_pair_paused_by_news=lambda instrument, now=None: False,
        market_regime_mult=1.0,
        adaptive_offsets={},
        dxy_ema_gap=None,
        dxy_gate_threshold=0.0,
        vix_level=None,
        vix_low_threshold=15.0,
        get_trade_calibration_adjustment=lambda strategy, instrument, session_name: {
            "threshold_offset": 0.0,
            "risk_mult": 1.0,
            "block_reason": "calibration block",
            "source": "session",
        },
    )

    calibrated = _apply_calibration("TREND", "USD_JPY", {"name": "TOKYO"}, 60.0, 45.0, ctx)

    assert calibrated is None
    assert rejected == [("TREND", "USD_JPY", "calibration block")]