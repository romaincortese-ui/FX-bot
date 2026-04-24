"""Tier 2v2 E2 — capital floor / forced-paper-trade tests."""
from __future__ import annotations

from fxbot.capital_floor import (
    CapitalFloorDecision,
    capital_floor_status_fields,
    evaluate_capital_floor,
)


def test_below_floor_forces_paper_trade():
    d = evaluate_capital_floor(account_balance=194.75, min_balance=10_000.0)
    assert d.below_floor is True
    assert d.force_paper_trade is True
    assert "capital_floor_breached" in d.reason


def test_above_floor_does_not_force_paper_trade():
    d = evaluate_capital_floor(account_balance=12_500.0, min_balance=10_000.0)
    assert d.below_floor is False
    assert d.force_paper_trade is False
    assert d.reason == "above_capital_floor"


def test_explicit_paper_trade_always_wins():
    d = evaluate_capital_floor(
        account_balance=999_999.0, min_balance=10_000.0, paper_trade=True
    )
    assert d.force_paper_trade is True
    assert d.reason == "paper_trade_explicit"


def test_disabled_flag_is_hard_bypass():
    d = evaluate_capital_floor(
        account_balance=0.0, min_balance=10_000.0, enabled=False
    )
    assert d.below_floor is False
    assert d.force_paper_trade is False
    assert d.reason == "capital_floor_disabled"


def test_zero_or_negative_floor_disables_silently():
    for floor in (0.0, -1.0):
        d = evaluate_capital_floor(account_balance=50.0, min_balance=floor)
        assert d.below_floor is False
        assert d.force_paper_trade is False
        assert d.reason == "no_floor_configured"


def test_equal_to_floor_is_not_below():
    d = evaluate_capital_floor(account_balance=10_000.0, min_balance=10_000.0)
    assert d.below_floor is False
    assert d.force_paper_trade is False


def test_status_fields_shape():
    d = evaluate_capital_floor(account_balance=200.0, min_balance=10_000.0)
    fields = capital_floor_status_fields(d)
    assert fields["capital_floor_enabled"] is True
    assert fields["capital_floor_below"] is True
    assert fields["capital_floor_force_paper"] is True
    assert fields["capital_floor_balance"] == 200.0
    assert fields["capital_floor_min_balance"] == 10_000.0
    assert isinstance(fields["capital_floor_reason"], str)


def test_decision_is_frozen_dataclass():
    d = evaluate_capital_floor(account_balance=1.0, min_balance=10.0)
    assert isinstance(d, CapitalFloorDecision)
    try:
        d.below_floor = False  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("CapitalFloorDecision must be frozen")
