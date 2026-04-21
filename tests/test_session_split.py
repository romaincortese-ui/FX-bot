from datetime import datetime, timezone

from fxbot.session_split import (
    classify_session,
    compute_london_opening_range,
    plan_london_breakout,
    session_strategy_bias,
)


def _utc(h: int, m: int = 0) -> datetime:
    return datetime(2026, 4, 21, h, m, tzinfo=timezone.utc)


def test_classify_tokyo_wraps_midnight():
    assert classify_session(_utc(1)) == "TOKYO"
    assert classify_session(_utc(23, 30)) == "TOKYO"
    assert classify_session(_utc(6, 59)) == "TOKYO"


def test_classify_london_open_and_overlap():
    assert classify_session(_utc(7, 30)) == "LONDON_OPEN"
    assert classify_session(_utc(10)) == "LONDON"
    assert classify_session(_utc(13)) == "EU_NY_OVERLAP"


def test_classify_ny_and_late_ny():
    assert classify_session(_utc(17)) == "NY"
    assert classify_session(_utc(21)) == "LATE_NY"


def test_session_bias_tokyo_prefers_mean_revert():
    b = session_strategy_bias("TOKYO")
    assert b["SCALPER"] > 1.0
    assert b["TREND"] < 1.0


def test_session_bias_london_open_boosts_breakout():
    b = session_strategy_bias("LONDON_OPEN")
    assert b.get("LONDON_BREAKOUT", 0) > 1.0


def test_session_bias_overlap_favours_trend():
    b = session_strategy_bias("EU_NY_OVERLAP")
    assert b["TREND"] > 1.0


def test_session_bias_unknown_returns_neutral():
    b = session_strategy_bias("ZZZ")
    assert all(v == 1.0 for v in b.values())


def _bar(hour: int, minute: int, high: float, low: float) -> dict:
    return {
        "timestamp": datetime(2026, 4, 21, hour, minute, tzinfo=timezone.utc),
        "high": high,
        "low": low,
    }


def test_opening_range_filters_window():
    bars = [
        _bar(6, 55, 1.1010, 1.1000),   # before window
        _bar(7, 5, 1.1020, 1.1005),
        _bar(7, 35, 1.1025, 1.1008),
        _bar(8, 5, 1.1030, 1.1002),    # after window
    ]
    rng = compute_london_opening_range(m5_bars=bars)
    assert rng == (1.1025, 1.1005)


def test_opening_range_empty_returns_none():
    assert compute_london_opening_range(m5_bars=[]) is None


def test_plan_london_breakout_basic():
    plan = plan_london_breakout(
        instrument="EUR_USD",
        range_high=1.1020,
        range_low=1.1000,
        pip_size=0.0001,
        break_buffer_pips=2.0,
        target_rr=2.0,
    )
    assert plan is not None
    assert plan.break_long_level > 1.1020
    assert plan.break_short_level < 1.1000
    assert abs(plan.range_pips - 20.0) < 1e-6
    # Risk-reward enforced.
    long_risk = plan.break_long_level - plan.stop_long_level
    long_reward = plan.target_long_level - plan.break_long_level
    assert abs(long_reward / long_risk - 2.0) < 1e-6


def test_plan_london_breakout_degenerate_range():
    plan = plan_london_breakout(
        instrument="EUR_USD", range_high=1.1000, range_low=1.1000
    )
    assert plan is None


def test_plan_london_breakout_with_atr_stop():
    plan = plan_london_breakout(
        instrument="EUR_USD",
        range_high=1.1020,
        range_low=1.1000,
        pip_size=0.0001,
        stop_atr=0.0015,
        stop_atr_mult=1.0,
    )
    assert plan is not None
    # ATR stop (15 pips) is wider than range (20 pips from break level?).
    # Range stop = range_low = 1.1000; ATR stop = break_long - 15 pips.
    # We take the wider — i.e. lower stop level for long.
    assert plan.stop_long_level <= 1.1000
