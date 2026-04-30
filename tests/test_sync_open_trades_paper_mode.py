"""Regression — capital-floor paper trades must survive OANDA sync.

Reproduces the bug where a TREND SHORT EUR_GBP entry showed up in the
Telegram announcement but ``/status`` reported 0 open trades. Root
cause: ``sync_open_trades_with_oanda`` only short-circuited on the
env-level ``PAPER_TRADE`` flag and therefore wiped paper trades added
while the Tier 2v2 capital-floor gate was forcing paper mode on an
otherwise-live OANDA account.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def main(monkeypatch):
    mod = importlib.import_module("main")
    monkeypatch.setattr(mod, "PAPER_TRADE", False, raising=False)
    monkeypatch.setattr(mod, "OANDA_API_KEY", "x", raising=False)
    monkeypatch.setattr(mod, "OANDA_ACCOUNT_ID", "TEST-ACC", raising=False)
    # Ensure we start with a clean slate.
    with mod._open_trades_lock:
        mod.open_trades.clear()
    mod.trade_history.clear()
    mod._pending_close_retries.clear()
    monkeypatch.setattr(mod, "save_state", lambda: None, raising=False)
    return mod


def test_sync_skips_when_capital_floor_forces_paper(main, monkeypatch):
    """Paper trades injected by the floor gate must NOT be wiped by sync."""
    monkeypatch.setattr(main, "_effective_paper_trade", lambda: True, raising=False)
    fetched: list = []
    monkeypatch.setattr(
        main,
        "fetch_open_trades_from_oanda",
        lambda: fetched.append("called") or [],
        raising=False,
    )

    paper_trade = {
        "id": "PAPER_1",
        "label": "TREND",
        "instrument": "EUR_GBP",
        "direction": "SHORT",
        "entry_price": 0.86704,
        "units": -2947,
    }
    with main._open_trades_lock:
        main.open_trades.append(paper_trade)

    changed = main.sync_open_trades_with_oanda(reason="test")

    assert changed is False
    assert fetched == []  # must not even call OANDA when paper-mode is active
    assert len(main.open_trades) == 1
    assert main.open_trades[0]["id"] == "PAPER_1"


def test_sync_still_runs_when_floor_clear(main, monkeypatch):
    """When the capital floor is clear, sync must still reconcile with OANDA."""
    monkeypatch.setattr(main, "_effective_paper_trade", lambda: False, raising=False)
    monkeypatch.setattr(main, "fetch_open_trades_from_oanda", lambda: [], raising=False)
    monkeypatch.setattr(main, "save_state", lambda: None, raising=False)

    # Seed a stale paper trade — a clean float should wipe it, because the
    # account is now genuinely live and OANDA has no matching position.
    with main._open_trades_lock:
        main.open_trades.append({"id": "PAPER_OLD", "label": "TREND", "instrument": "EUR_GBP"})

    main.sync_open_trades_with_oanda(reason="test")

    # The sync proceeded; the paper-only trade is gone.
    assert all(t.get("id") != "PAPER_OLD" for t in main.open_trades)


def test_sync_does_not_clear_local_state_when_broker_fetch_fails(main, monkeypatch):
    """An OANDA outage must not look like an empty broker account."""
    monkeypatch.setattr(main, "_effective_paper_trade", lambda: False, raising=False)
    monkeypatch.setattr(main, "fetch_open_trades_from_oanda", lambda: None, raising=False)

    with main._open_trades_lock:
        main.open_trades.append({"id": "26", "label": "TREND", "instrument": "USD_CHF"})

    changed = main.sync_open_trades_with_oanda(reason="test")

    assert changed is False
    assert [t.get("id") for t in main.open_trades] == ["26"]


def test_sync_alerts_and_records_broker_side_stop_loss(main, monkeypatch):
    """A trade missing from OANDA is dropped locally with a Telegram close alert."""
    monkeypatch.setattr(main, "_effective_paper_trade", lambda: False, raising=False)
    monkeypatch.setattr(main, "fetch_open_trades_from_oanda", lambda: [], raising=False)
    monkeypatch.setattr(
        main,
        "fetch_trade_details_from_oanda",
        lambda trade_id: {
            "id": trade_id,
            "state": "CLOSED",
            "averageClosePrice": "0.78925",
            "realizedPL": "-2.53",
            "closeTime": "2026-04-29T17:44:50Z",
        },
        raising=False,
    )
    sent: list[str] = []
    posterior_updates: list[tuple[str, bool]] = []
    monkeypatch.setattr(main, "telegram", lambda msg, *a, **k: sent.append(msg), raising=False)
    monkeypatch.setattr(main, "get_account_currency", lambda: "GBP", raising=False)
    monkeypatch.setattr(main, "_tier2_refresh_drawdown_state", lambda: None, raising=False)
    monkeypatch.setattr(
        main,
        "_tier2_update_posteriors",
        lambda label, win: posterior_updates.append((label, win)),
        raising=False,
    )

    trade = {
        "id": "26",
        "label": "TREND",
        "instrument": "USD_CHF",
        "direction": "LONG",
        "entry_price": 0.79073,
        "tp_price": 0.79419,
        "sl_price": 0.78925,
        "units": 1908,
        "opened_at": "2026-04-29T17:00:08Z",
        "opened_ts": 0,
        "score": 55.35,
    }
    with main._open_trades_lock:
        main.open_trades.append(trade)
    main._pending_close_retries["26"] = {"attempts": 1}

    changed = main.sync_open_trades_with_oanda(reason="test")

    assert changed is True
    assert main.open_trades == []
    assert "26" not in main._pending_close_retries
    assert main.trade_history[-1]["trade_id"] == "26"
    assert main.trade_history[-1]["reason"] == "STOP_LOSS"
    assert main.trade_history[-1]["pnl"] == -2.53
    assert main.trade_history[-1]["pnl_pips"] == -14.8
    assert posterior_updates == [("TREND", False)]
    assert any("Closed at broker" in msg and "STOP LOSS" in msg for msg in sent)


def test_close_all_counts_broker_side_reconciliation(main, monkeypatch):
    """A /close-triggered sync should not report 'no positions' after reconciling one."""
    monkeypatch.setattr(main, "_effective_paper_trade", lambda: False, raising=False)
    monkeypatch.setattr(main, "fetch_open_trades_from_oanda", lambda: [], raising=False)
    monkeypatch.setattr(
        main,
        "fetch_trade_details_from_oanda",
        lambda trade_id: {
            "id": trade_id,
            "state": "CLOSED",
            "averageClosePrice": "159.956",
            "realizedPL": "-2.45",
            "closeTime": "2026-04-30T07:20:00Z",
        },
        raising=False,
    )
    sent: list[str] = []
    monkeypatch.setattr(main, "telegram", lambda msg, *a, **k: sent.append(msg), raising=False)
    monkeypatch.setattr(main, "get_account_currency", lambda: "GBP", raising=False)
    monkeypatch.setattr(main, "_tier2_refresh_drawdown_state", lambda: None, raising=False)
    monkeypatch.setattr(main, "_tier2_update_posteriors", lambda *a, **k: None, raising=False)

    with main._open_trades_lock:
        main.open_trades.append(
            {
                "id": "36",
                "label": "CARRY",
                "instrument": "USD_JPY",
                "direction": "LONG",
                "entry_price": 160.349,
                "tp_price": 161.294,
                "sl_price": 159.956,
                "units": 1419,
                "opened_at": "2026-04-29T22:24:24Z",
                "opened_ts": 0,
                "score": 57.2,
            }
        )

    closed_count, failed_count = main.close_all_open_positions(reason="MANUAL_CLOSE")

    assert (closed_count, failed_count) == (1, 0)
    assert main.last_open_trades_reconciled_count == 1
    assert main.open_trades == []
    assert main.trade_history[-1]["trade_id"] == "36"
    assert main.trade_history[-1]["reason"] == "STOP_LOSS"
    assert any("CARRY Closed at broker" in msg and "-2.45" in msg for msg in sent)


def test_heartbeat_syncs_before_rendering_open_count(main, monkeypatch):
    """Heartbeat should not render stale local positions after broker sync."""
    with main._open_trades_lock:
        main.open_trades.append({"id": "26", "label": "TREND", "instrument": "USD_CHF", "direction": "LONG"})
    calls: list[str] = []

    def fake_sync(reason: str = "manual"):
        calls.append(reason)
        with main._open_trades_lock:
            main.open_trades.clear()
        return True

    sent: list[str] = []
    monkeypatch.setattr(main, "sync_open_trades_with_oanda", fake_sync, raising=False)
    monkeypatch.setattr(main, "publish_bot_runtime_status", lambda *a, **k: True, raising=False)
    monkeypatch.setattr(
        main,
        "get_current_session",
        lambda: {"name": "TEST", "aggression": "normal", "pairs_allowed": []},
        raising=False,
    )
    monkeypatch.setattr(main, "get_paused_pairs_by_news", lambda pairs: [], raising=False)
    monkeypatch.setattr(main, "get_account_currency", lambda: "GBP", raising=False)
    monkeypatch.setattr(main, "_calibration_summary", lambda: {"active": False}, raising=False)
    monkeypatch.setattr(main, "telegram", lambda msg, *a, **k: sent.append(msg), raising=False)
    monkeypatch.setattr(main, "HEARTBEAT_INTERVAL", 0, raising=False)
    monkeypatch.setattr(main, "last_heartbeat_at", 0, raising=False)

    main.send_heartbeat(95.59)

    assert calls == ["heartbeat"]
    assert "Open: 0 trades" in sent[-1]
