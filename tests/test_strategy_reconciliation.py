"""Tests for Tier 2 §16 — strategy reconciliation."""
from __future__ import annotations

from datetime import datetime, timezone

from fxbot.strategy_reconciliation import StrategyReconciliation


def test_same_direction_is_allowed():
    r = StrategyReconciliation()
    ts = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    d1 = r.check(strategy="SCALPER", instrument="EUR_USD", direction="LONG", score=70.0, now_utc=ts)
    d2 = r.check(strategy="TREND", instrument="EUR_USD", direction="LONG", score=60.0, now_utc=ts)
    assert d1.allowed
    assert d2.allowed


def test_opposite_direction_same_bar_blocked():
    r = StrategyReconciliation()
    ts = datetime(2024, 1, 1, 12, 2, tzinfo=timezone.utc)
    r.check(strategy="REVERSAL", instrument="EUR_USD", direction="SHORT", score=70.0, now_utc=ts)
    d = r.check(strategy="SCALPER", instrument="EUR_USD", direction="LONG", score=70.0, now_utc=ts)
    assert not d.allowed
    assert "REVERSAL_SHORT" in d.reason


def test_opposite_direction_different_bar_allowed():
    r = StrategyReconciliation()
    t1 = datetime(2024, 1, 1, 12, 2, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 1, 12, 17, tzinfo=timezone.utc)  # next 15m bar
    r.check(strategy="REVERSAL", instrument="EUR_USD", direction="SHORT", score=70.0, now_utc=t1)
    d = r.check(strategy="SCALPER", instrument="EUR_USD", direction="LONG", score=70.0, now_utc=t2)
    assert d.allowed


def test_opposite_direction_weak_prior_allowed():
    r = StrategyReconciliation()
    ts = datetime(2024, 1, 1, 12, 2, tzinfo=timezone.utc)
    r.check(strategy="REVERSAL", instrument="EUR_USD", direction="SHORT", score=40.0, now_utc=ts)
    # candidate score 100 — prior (40) < 0.75*100 so no veto
    d = r.check(strategy="SCALPER", instrument="EUR_USD", direction="LONG", score=100.0, now_utc=ts)
    assert d.allowed


def test_same_strategy_not_self_blocks():
    r = StrategyReconciliation()
    ts = datetime(2024, 1, 1, 12, 2, tzinfo=timezone.utc)
    r.check(strategy="SCALPER", instrument="EUR_USD", direction="SHORT", score=70.0, now_utc=ts)
    d = r.check(strategy="SCALPER", instrument="EUR_USD", direction="LONG", score=70.0, now_utc=ts)
    assert d.allowed  # reconciliation targets cross-strategy conflicts only
