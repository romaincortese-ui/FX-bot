from datetime import datetime, timedelta, timezone

from fxbot.decision_day import (
    decision_day_follow_through,
    is_central_bank_decision,
)


def _event(title: str, currency: str, pause_end: datetime) -> dict:
    return {
        "title": title,
        "currency": currency,
        "impact": "High",
        "pause_end": pause_end.isoformat(),
    }


def test_is_central_bank_decision_identifies_fomc():
    assert is_central_bank_decision({"title": "FOMC Statement", "currency": "USD"})


def test_is_central_bank_decision_rejects_cpi():
    assert not is_central_bank_decision({"title": "Core CPI m/m", "currency": "USD"})


def test_is_central_bank_decision_wrong_currency():
    # "FOMC" keyword is USD-only; attach to EUR → not a decision.
    assert not is_central_bank_decision({"title": "FOMC Statement", "currency": "EUR"})


def test_follow_through_inside_window():
    release = datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)
    now = release + timedelta(minutes=5)
    evt = _event("FOMC Statement", "USD", release)
    sig = decision_day_follow_through(
        instrument="EUR_USD", events=[evt], now=now
    )
    assert sig.in_window
    assert sig.event_currency == "USD"
    assert sig.risk_multiplier == 1.20


def test_follow_through_before_delay():
    # 60s after release is still inside the 90s confirmation gate.
    release = datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)
    now = release + timedelta(seconds=60)
    evt = _event("FOMC Statement", "USD", release)
    sig = decision_day_follow_through(
        instrument="EUR_USD", events=[evt], now=now
    )
    assert not sig.in_window


def test_follow_through_after_window():
    release = datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)
    now = release + timedelta(minutes=20)
    evt = _event("FOMC Statement", "USD", release)
    sig = decision_day_follow_through(
        instrument="EUR_USD", events=[evt], now=now
    )
    assert not sig.in_window


def test_follow_through_ignores_unrelated_instrument():
    release = datetime(2026, 5, 1, 18, 0, tzinfo=timezone.utc)
    now = release + timedelta(minutes=5)
    evt = _event("FOMC Statement", "USD", release)
    # AUD_NZD has no USD leg.
    sig = decision_day_follow_through(
        instrument="AUD_NZD", events=[evt], now=now
    )
    assert not sig.in_window


def test_follow_through_ignores_cpi_release():
    release = datetime(2026, 5, 1, 12, 30, tzinfo=timezone.utc)
    now = release + timedelta(minutes=5)
    evt = _event("Core CPI m/m", "USD", release)
    sig = decision_day_follow_through(
        instrument="EUR_USD", events=[evt], now=now
    )
    assert not sig.in_window
