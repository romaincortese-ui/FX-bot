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