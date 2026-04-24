from datetime import datetime, timezone

from fxbot.flow_strategies import (
    active_flow_window,
    instrument_is_flow_eligible,
    is_end_of_quarter_window,
)


def test_end_of_quarter_inside_window():
    # 2024-03-28 was a Thursday — last business day of March (29 was Good Friday).
    ts = datetime(2024, 3, 28, 14, 30, tzinfo=timezone.utc)
    w = is_end_of_quarter_window(ts)
    assert w.in_window
    assert w.event == "END_OF_QUARTER"
    assert w.risk_multiplier == 1.40


def test_end_of_quarter_outside_target_months():
    # February 28, 2025 is the last business day of Feb but not a quarter-end month.
    ts = datetime(2025, 2, 28, 14, 30, tzinfo=timezone.utc)
    w = is_end_of_quarter_window(ts)
    assert not w.in_window


def test_end_of_quarter_outside_utc_hours():
    ts = datetime(2024, 3, 28, 10, 0, tzinfo=timezone.utc)
    w = is_end_of_quarter_window(ts)
    assert not w.in_window


def test_active_flow_window_prefers_quarter_end_over_month_end():
    # Day is both last-BD-of-March AND inside 15:00–16:00 UTC (both windows match).
    ts = datetime(2024, 3, 28, 15, 30, tzinfo=timezone.utc)
    w = active_flow_window(ts)
    assert w.event == "END_OF_QUARTER"


def test_instrument_eligibility_end_of_quarter():
    assert instrument_is_flow_eligible("EUR_USD", "END_OF_QUARTER")
    assert instrument_is_flow_eligible("EUR_CHF", "END_OF_QUARTER")
    assert not instrument_is_flow_eligible("AUD_NZD", "END_OF_QUARTER")
