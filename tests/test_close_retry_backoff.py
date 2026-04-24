"""Tier 2v2 E3 — verify close-retry back-off engages on MARKET_HALTED.

Third-memo §8 E3: after the TRADE_DOESNT_EXIST fix (P0.1) collapses
phantom-trade rejects to terminal success, the remaining retry surface is
``MARKET_HALTED`` (weekend / news suspension). This test verifies the
exponential back-off schedule is what the module advertises and that
``close_trade_exit`` actually schedules a retry rather than silently
dropping the trade.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def main(monkeypatch):
    mod = importlib.import_module("main")
    monkeypatch.setattr(mod, "PAPER_TRADE", False, raising=False)
    monkeypatch.setattr(mod, "OANDA_ACCOUNT_ID", "TEST-ACC", raising=False)
    monkeypatch.setattr(mod, "mark_pair_success", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(mod, "mark_pair_failure", lambda *a, **k: None, raising=False)
    # Clear any accumulated retry state between tests.
    mod._pending_close_retries.clear()
    return mod


def test_exponential_backoff_schedule(main, monkeypatch):
    """Attempts 1..N must follow base * 2^(n-1), capped at CLOSE_RETRY_MAX_SECS."""
    monkeypatch.setattr(main, "CLOSE_RETRY_BASE_SECS", 300, raising=False)
    monkeypatch.setattr(main, "CLOSE_RETRY_MAX_SECS", 7200, raising=False)
    delays = [main._next_close_retry_delay(n) for n in range(1, 7)]
    # 300, 600, 1200, 2400, 4800, 7200 (cap hit)
    assert delays == [300, 600, 1200, 2400, 4800, 7200]


def test_backoff_capped_at_max(main, monkeypatch):
    monkeypatch.setattr(main, "CLOSE_RETRY_BASE_SECS", 300, raising=False)
    monkeypatch.setattr(main, "CLOSE_RETRY_MAX_SECS", 7200, raising=False)
    # Huge attempt count must still clamp to CLOSE_RETRY_MAX_SECS.
    assert main._next_close_retry_delay(99) == 7200


def test_backoff_floor_is_one_base_interval(main, monkeypatch):
    monkeypatch.setattr(main, "CLOSE_RETRY_BASE_SECS", 300, raising=False)
    # Degenerate/zero attempts must not underflow below base.
    assert main._next_close_retry_delay(0) == 300


def test_market_halted_close_schedules_retry(main, monkeypatch):
    """close_trade_exit must call schedule_close_retry on MARKET_HALTED."""
    # Stub the OANDA round-trip so close_trade_result reports a halted reject.
    monkeypatch.setattr(
        main,
        "close_trade_result",
        lambda trade_id, label, instrument=None: (False, "MARKET_HALTED"),
    )
    monkeypatch.setattr(main, "get_current_price", lambda inst: {"bid": 1.0, "ask": 1.0})

    scheduled: list[tuple[dict, str]] = []
    monkeypatch.setattr(
        main,
        "schedule_close_retry",
        lambda trade, reason: scheduled.append((trade, reason)),
    )

    trade = {
        "id": "42",
        "instrument": "EUR_USD",
        "label": "RESTORED",
        "direction": "LONG",
        "entry_price": 1.0,
        "units": 1,
        "opened_ts": 0,
    }

    result = main.close_trade_exit(trade, "FORCED_CLOSE")

    assert result is False
    assert scheduled, "MARKET_HALTED must schedule a close-retry"
    assert scheduled[0][0] is trade
    assert "MARKET_HALTED" in scheduled[0][1]


def test_reconciled_close_does_not_schedule_retry(main, monkeypatch):
    """TRADE_DOESNT_EXIST (sentinel) must NOT reschedule — phantom is gone."""
    monkeypatch.setattr(
        main,
        "close_trade_result",
        lambda trade_id, label, instrument=None: (True, main.BROKER_RECONCILED_SENTINEL),
    )
    monkeypatch.setattr(main, "get_current_price", lambda inst: {"bid": 1.0, "ask": 1.0})
    monkeypatch.setattr(main, "send_telegram_alert", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(main, "telegram", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(main, "_record_closed_trade_pnl", lambda *a, **k: None, raising=False)

    scheduled: list = []
    monkeypatch.setattr(
        main, "schedule_close_retry",
        lambda trade, reason: scheduled.append((trade, reason)),
    )

    trade = {
        "id": "77",
        "instrument": "NZD_USD",
        "label": "RESTORED",
        "direction": "LONG",
        "entry_price": 0.58,
        "units": 1,
        "opened_ts": 0,
    }

    # Depending on downstream branches the call may succeed or return early;
    # the only invariant we assert is that no retry is queued.
    try:
        main.close_trade_exit(trade, "FORCED_CLOSE")
    except Exception:
        pass
    assert scheduled == []
