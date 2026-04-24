"""Memo 4 §8 F5 — entry-spread audit.

Asserts that ``place_order`` emits the ``[ENTRY_SPREAD]`` audit line and
(in strict mode, the default) refuses to submit an order whose observed
bid/ask spread exceeds the per-strategy ``<STRATEGY>_MAX_SPREAD_PIPS``
cap. Closes the "spread sample ≠ entry spread" gap flagged in memo 4
§3.3 where USD_CHF at 6.7 p was filled by TREND despite a 2 p cap.
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
    monkeypatch.setattr(mod, "_effective_paper_trade", lambda: False, raising=False)
    monkeypatch.setattr(mod, "get_current_price", lambda inst: {"bid": 1.0, "ask": 1.0}, raising=False)
    return mod


def test_entry_spread_audit_blocks_trend_above_cap(main, monkeypatch, caplog):
    """TREND must not fill USD_CHF at 6.7 p when TREND_MAX_SPREAD_PIPS=2.0."""
    monkeypatch.setattr(main, "TREND_MAX_SPREAD_PIPS", 2.0, raising=False)
    monkeypatch.delenv("ENTRY_SPREAD_AUDIT_STRICT", raising=False)

    submitted: list[dict] = []
    monkeypatch.setattr(main, "oanda_post", lambda *a, **k: submitted.append(k) or {"orderFillTransaction": {}}, raising=False)

    # USD_CHF pip size = 0.0001; a 6.7p spread ⇒ ask - bid = 0.00067.
    with caplog.at_level("INFO"):
        result = main.place_order(
            instrument="USD_CHF",
            units=1000,
            direction="LONG",
            label="TREND",
            strategy="TREND",
            bid=0.9000,
            ask=0.90067,
        )

    assert result.get("blocked") is True
    assert result.get("reason") == "entry_spread_above_cap"
    assert submitted == []  # never reached OANDA
    assert any("[ENTRY_SPREAD]" in rec.message for rec in caplog.records)
    assert any("[ENTRY_SPREAD_BLOCKED]" in rec.message for rec in caplog.records)


def test_entry_spread_audit_allows_within_cap(main, monkeypatch, caplog):
    """A spread inside the cap must produce the audit line but not block."""
    monkeypatch.setattr(main, "TREND_MAX_SPREAD_PIPS", 2.0, raising=False)
    monkeypatch.delenv("ENTRY_SPREAD_AUDIT_STRICT", raising=False)

    submitted: list[dict] = []
    def _fake_post(path, body, **kw):
        submitted.append(body)
        return {"orderFillTransaction": {"id": "F1", "price": "0.9001"}}
    monkeypatch.setattr(main, "oanda_post", _fake_post, raising=False)

    with caplog.at_level("INFO"):
        result = main.place_order(
            instrument="USD_CHF",
            units=1000,
            direction="LONG",
            label="TREND",
            strategy="TREND",
            bid=0.9000,
            ask=0.90015,  # 1.5 p — inside the 2.0 p cap
        )

    assert not result.get("blocked")
    assert submitted  # reached OANDA
    audit_lines = [rec.message for rec in caplog.records if "[ENTRY_SPREAD]" in rec.message]
    assert audit_lines, "expected at least one [ENTRY_SPREAD] audit line"
    assert any("spread_pips=1.50" in line for line in audit_lines)
    assert any("cap_pips=2.00" in line for line in audit_lines)


def test_entry_spread_audit_strict_can_be_bypassed(main, monkeypatch):
    """Setting ENTRY_SPREAD_AUDIT_STRICT=0 must disable the hard block."""
    monkeypatch.setattr(main, "SCALPER_MAX_SPREAD_PIPS", 1.0, raising=False)
    monkeypatch.setenv("ENTRY_SPREAD_AUDIT_STRICT", "0")

    submitted: list[dict] = []
    def _fake_post(path, body, **kw):
        submitted.append(body)
        return {"orderFillTransaction": {"id": "F1", "price": "1.0001"}}
    monkeypatch.setattr(main, "oanda_post", _fake_post, raising=False)

    result = main.place_order(
        instrument="EUR_USD",
        units=1000,
        direction="LONG",
        label="SCALPER",
        strategy="SCALPER",
        bid=1.0000,
        ask=1.00030,  # 3.0 p — well above the 1.0 p cap
    )

    # Strict mode disabled — trade is NOT blocked by the audit gate.
    assert not result.get("blocked")
    assert submitted, "order must reach OANDA when strict audit is disabled"
