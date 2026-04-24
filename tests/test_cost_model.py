"""Tests for the net-of-cost R:R gate (Tier 1 §7 item 9)."""
from fxbot.cost_model import compute_net_rr, net_rr_passes, DEFAULT_MIN_NET_RR


def test_gross_2_to_1_fails_after_costs():
    # SCALPER-typical numbers: 8 pip SL, 16 pip TP, 1.2 pip round-trip spread.
    # Gross R:R = 2.0. Net R:R = (16 - 1.2 - 1.2 - 0.3) / 8 = 1.66 → FAIL at 1.8.
    b = compute_net_rr(
        sl_pips=8.0, tp_pips=16.0,
        entry_spread_pips=1.2,
        slippage_pips=0.3,
    )
    assert b.net_rr < DEFAULT_MIN_NET_RR
    assert not b.passed


def test_gross_3_to_1_passes_after_costs():
    b = compute_net_rr(
        sl_pips=10.0, tp_pips=30.0,
        entry_spread_pips=0.8,
        slippage_pips=0.3,
    )
    assert b.net_rr >= DEFAULT_MIN_NET_RR
    assert b.passed


def test_zero_sl_pips_returns_rr_zero_and_fails():
    b = compute_net_rr(sl_pips=0.0, tp_pips=10.0, entry_spread_pips=1.0)
    assert b.net_rr == 0.0
    assert not b.passed


def test_min_rr_zero_passes_when_net_tp_nonnegative():
    # With min_net_rr=0 the gate accepts any setup whose net reward is ≥ 0.
    # A setup whose net reward is negative (costs exceed TP) still fails,
    # which is the correct behaviour — the call-site disables the gate by
    # not calling compute_net_rr at all.
    pos = compute_net_rr(
        sl_pips=10.0, tp_pips=12.0,
        entry_spread_pips=0.5,
        slippage_pips=0.1,
        min_net_rr=0.0,
    )
    assert pos.passed

    neg = compute_net_rr(
        sl_pips=8.0, tp_pips=10.0,
        entry_spread_pips=5.0,
        slippage_pips=1.0,
        min_net_rr=0.0,
    )
    assert not neg.passed


def test_boolean_wrapper_matches_full_breakdown():
    assert net_rr_passes(sl_pips=10, tp_pips=30, entry_spread_pips=0.8) is True
    assert net_rr_passes(sl_pips=10, tp_pips=12, entry_spread_pips=1.0) is False


def test_exit_spread_defaults_to_entry_spread():
    b = compute_net_rr(sl_pips=10, tp_pips=25, entry_spread_pips=1.5)
    assert b.exit_spread_pips == 1.5


def test_financing_credit_helps_rr():
    # Negative financing = carry credit; net TP goes UP.
    b_cost = compute_net_rr(sl_pips=10, tp_pips=20, entry_spread_pips=1.0, financing_pips=2.0)
    b_credit = compute_net_rr(sl_pips=10, tp_pips=20, entry_spread_pips=1.0, financing_pips=-2.0)
    assert b_credit.net_rr > b_cost.net_rr
