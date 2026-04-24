"""Unit tests for broker-reject reconciliation (third-memo §2.1 / Tier 0 P0.1).

These verify that close_trade_result() collapses the TRADE_DOESNT_EXIST class of
OANDA rejections into a (True, BROKER_RECONCILED_SENTINEL) terminal success so
the outer close-retry loop drops the phantom trade instead of entering the
retry storm observed in the 17h40 W2 production log.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def main(monkeypatch):
    mod = importlib.import_module("main")
    monkeypatch.setattr(mod, "PAPER_TRADE", False, raising=False)
    monkeypatch.setattr(mod, "OANDA_ACCOUNT_ID", "TEST-ACC", raising=False)
    # Neutralise pair-health side-effects so the test asserts behaviour, not telemetry.
    monkeypatch.setattr(mod, "mark_pair_success", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(mod, "mark_pair_failure", lambda *a, **k: None, raising=False)
    return mod


@pytest.mark.parametrize(
    "reject_code",
    [
        "TRADE_DOESNT_EXIST",
        "POSITION_DOESNT_EXIST",
        "ORDER_DOESNT_EXIST",
        "CLOSEOUT_POSITION_DOESNT_EXIST",
    ],
)
def test_reconcilable_rejects_report_success_with_sentinel(main, monkeypatch, reject_code):
    """Phantom-close rejects become (True, SENTINEL) so callers drop state."""
    def fake_put(path, body):
        return {
            "orderRejectTransaction": {"rejectReason": reject_code},
            "status_code": 404,
            "error": reject_code,
        }

    monkeypatch.setattr(main, "oanda_put", fake_put)

    ok, err = main.close_trade_result("99999", label="TEST", instrument="EUR_USD")

    assert ok is True
    assert err == main.BROKER_RECONCILED_SENTINEL


@pytest.mark.parametrize(
    "reject_code",
    [
        "MARKET_HALTED",
        "INSUFFICIENT_MARGIN",
        "TAKE_PROFIT_ORDER_NOT_FOUND",
    ],
)
def test_non_reconcilable_rejects_still_fail(main, monkeypatch, reject_code):
    """Genuine broker errors must still propagate as (False, message)."""
    def fake_put(path, body):
        return {
            "orderRejectTransaction": {"rejectReason": reject_code},
            "status_code": 400,
            "error": reject_code,
        }

    monkeypatch.setattr(main, "oanda_put", fake_put)

    ok, err = main.close_trade_result("123", label="TEST", instrument="EUR_USD")

    assert ok is False
    assert err is not None
    assert err != main.BROKER_RECONCILED_SENTINEL
    assert reject_code in err


def test_close_trade_helper_reports_success_on_reconcile(main, monkeypatch):
    """close_trade() wrapper should surface the reconciled case as success."""
    def fake_put(path, body):
        return {
            "orderRejectTransaction": {"rejectReason": "TRADE_DOESNT_EXIST"},
            "status_code": 404,
            "error": "TRADE_DOESNT_EXIST",
        }

    monkeypatch.setattr(main, "oanda_put", fake_put)

    assert main.close_trade("42", label="TEST", instrument="EUR_USD") is True


def test_is_broker_reconcile_reject_detects_embedded_code(main):
    assert main._is_broker_reconcile_reject(
        "Order rejected: TRADE_DOESNT_EXIST for id 99999"
    )
    assert main._is_broker_reconcile_reject(
        "closeout_position_doesnt_exist (lowercase variant)"
    )
    assert not main._is_broker_reconcile_reject("MARKET_HALTED")
    assert not main._is_broker_reconcile_reject(None)
    assert not main._is_broker_reconcile_reject("")
