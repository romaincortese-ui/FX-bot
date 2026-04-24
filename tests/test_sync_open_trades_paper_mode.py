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
