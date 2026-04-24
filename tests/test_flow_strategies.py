from datetime import datetime, timezone

from fxbot.flow_strategies import (
    active_flow_window,
    instrument_is_flow_eligible,
    is_london_fix_window,
    is_month_end_window,
    is_tokyo_fix_window,
)


def test_london_fix_window_triggers_at_16_utc():
    # Wed 2025-01-15 15:58 UTC — inside the 15:55–16:05 window.
    ts = datetime(2025, 1, 15, 15, 58, tzinfo=timezone.utc)
    w = is_london_fix_window(ts)
    assert w.in_window
    assert w.event == "LONDON_FIX"
    assert w.risk_multiplier == 1.25


def test_london_fix_outside_window():
    ts = datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)
    assert not is_london_fix_window(ts).in_window


def test_london_fix_weekend_closed():
    ts = datetime(2025, 1, 18, 16, 0, tzinfo=timezone.utc)  # Saturday
    assert not is_london_fix_window(ts).in_window


def test_tokyo_fix_window():
    ts = datetime(2025, 1, 15, 0, 55, tzinfo=timezone.utc)
    w = is_tokyo_fix_window(ts)
    assert w.in_window
    assert w.event == "TOKYO_FIX"


def test_month_end_window_last_business_day():
    # 2025-01-31 is a Friday — last business day of January. 15:30 UTC
    ts = datetime(2025, 1, 31, 15, 30, tzinfo=timezone.utc)
    w = is_month_end_window(ts)
    assert w.in_window
    assert w.event == "MONTH_END"
    assert w.risk_multiplier == 1.35


def test_month_end_not_triggered_mid_month():
    ts = datetime(2025, 1, 15, 15, 30, tzinfo=timezone.utc)
    assert not is_month_end_window(ts).in_window


def test_active_flow_window_prefers_month_end():
    # 2025-01-31 15:58 UTC overlaps both month-end and London fix.
    ts = datetime(2025, 1, 31, 15, 58, tzinfo=timezone.utc)
    w = active_flow_window(ts)
    assert w.in_window
    assert w.event == "MONTH_END"


def test_active_flow_window_none():
    ts = datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)
    assert not active_flow_window(ts).in_window


def test_instrument_eligibility():
    assert instrument_is_flow_eligible("EUR_USD", "LONDON_FIX")
    assert not instrument_is_flow_eligible("AUD_NZD", "LONDON_FIX")
    assert instrument_is_flow_eligible("USD_JPY", "TOKYO_FIX")
    assert instrument_is_flow_eligible("EUR_USD", "MONTH_END")
    assert not instrument_is_flow_eligible("", "LONDON_FIX")
