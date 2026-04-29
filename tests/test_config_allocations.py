"""Regression tests for fxbot.config.validate_main_config.

Crash 29-Apr-2026 (logs.1777477331485.json): the validator required
`FX_BUDGET_ALLOCATION + GOLD_BUDGET_ALLOCATION == 1.0`. After the
account-separation refactor each bot operates against its own OANDA
sub-account, so both env vars correctly default to `1.0`, summing to 2.0
and triggering a crash-loop. The cross-bot sleeve invariant has been
removed; per-bot caps remain enforced via field-level `le=1.0`.
"""
from __future__ import annotations

import pytest

from fxbot.config import MainRuntimeConfig, validate_main_config


def _baseline_payload() -> dict:
    return {
        "FX_BUDGET_ALLOCATION": 1.0,
        "GOLD_BUDGET_ALLOCATION": 1.0,
        "SCALPER_ALLOCATION_PCT": 0.25,
        "TREND_ALLOCATION_PCT": 0.25,
        "REVERSAL_ALLOCATION_PCT": 0.25,
        "BREAKOUT_ALLOCATION_PCT": 0.25,
        "MAX_RISK_PER_TRADE": 0.015,
        "MAX_RISK_PER_PAIR": 0.03,
        "MAX_TOTAL_EXPOSURE": 0.06,
        "MAX_CORRELATED_TRADES": 2,
        "MAX_OPEN_TRADES": 3,
        "LEVERAGE": 30.0,
        "DAILY_LOSS_LIMIT_PCT": 0.03,
        "STREAK_LOSS_MAX": 4,
        "SESSION_LOSS_PAUSE_PCT": 0.02,
        "SESSION_LOSS_PAUSE_MINS": 60,
        "PAIR_HEALTH_BLOCK_BASE_SECS": 300,
        "PAIR_HEALTH_BLOCK_MAX_SECS": 7200,
        "PAIR_HEALTH_PROBE_INTERVAL_SECS": 60,
        "SCAN_INTERVAL_BASE": 30,
        "SCAN_INTERVAL_ACTIVE": 10,
    }


def test_independent_per_bot_allocations_accepted():
    """Both bots at 1.0 (separate sub-accounts) must validate cleanly."""
    payload = _baseline_payload()
    validate_main_config(payload)  # must not raise


def test_zero_fx_allocation_accepted():
    payload = _baseline_payload()
    payload["FX_BUDGET_ALLOCATION"] = 0.0
    validate_main_config(payload)


def test_strategy_allocation_invariant_still_enforced():
    payload = _baseline_payload()
    payload["TREND_ALLOCATION_PCT"] = 0.50  # total would be 1.25
    with pytest.raises(ValueError, match="Core strategy allocations must sum to 1.0"):
        validate_main_config(payload)


def test_per_bot_allocation_above_one_rejected():
    payload = _baseline_payload()
    payload["FX_BUDGET_ALLOCATION"] = 1.5
    with pytest.raises(ValueError):
        validate_main_config(payload)
