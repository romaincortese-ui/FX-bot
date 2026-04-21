from datetime import datetime, timezone

import pytest

from fxbot.execution import (
    plan_limit_entry,
    plan_staged_exit,
    should_flatten_for_weekend,
    should_use_limit_stop,
)


def test_limit_entry_long_at_mid():
    p = plan_limit_entry(direction="LONG", bid=1.1000, ask=1.1002)
    assert p.side == "LONG"
    assert abs(p.limit_price - 1.1001) < 1e-9
    assert p.wait_seconds == 2


def test_limit_entry_passive_long():
    p = plan_limit_entry(direction="LONG", bid=1.1000, ask=1.1002, mid_offset_frac=0.0)
    assert abs(p.limit_price - 1.1000) < 1e-9


def test_limit_entry_aggressive_long_crosses():
    p = plan_limit_entry(direction="LONG", bid=1.1000, ask=1.1002, mid_offset_frac=1.0)
    assert abs(p.limit_price - 1.1002) < 1e-9


def test_limit_entry_short_at_mid():
    p = plan_limit_entry(direction="SHORT", bid=1.1000, ask=1.1002)
    assert abs(p.limit_price - 1.1001) < 1e-9


def test_invalid_quote_raises():
    with pytest.raises(ValueError):
        plan_limit_entry(direction="LONG", bid=1.1002, ask=1.1000)
    with pytest.raises(ValueError):
        plan_limit_entry(direction="SIDEWAYS", bid=1.10, ask=1.11)


def test_staged_exit_default_fractions():
    plan = plan_staged_exit()
    assert len(plan.legs) == 3
    assert plan.legs[0].fraction == 0.40
    assert plan.legs[0].target_atr_mult == 2.0
    assert plan.legs[1].fraction == 0.30
    assert plan.legs[1].target_atr_mult == 3.5
    assert plan.legs[2].target_atr_mult is None
    assert plan.legs[2].kind == "trailing"
    assert abs(plan.total_fraction - 1.0) < 1e-9


def test_staged_exit_rejects_over_allocated():
    with pytest.raises(ValueError):
        plan_staged_exit(tp1_fraction=0.5, tp2_fraction=0.5, trailing_fraction=0.5)


def test_weekend_flatten_fires_friday_21_utc():
    fri_21 = datetime(2026, 4, 24, 21, 30, tzinfo=timezone.utc)  # Fri 21:30Z
    assert should_flatten_for_weekend(now_utc=fri_21, strategy="SCALPER") is True


def test_weekend_flatten_not_fires_thursday():
    thu = datetime(2026, 4, 23, 22, 0, tzinfo=timezone.utc)
    assert should_flatten_for_weekend(now_utc=thu, strategy="SCALPER") is False


def test_weekend_flatten_skips_carry_by_default():
    fri_21 = datetime(2026, 4, 24, 21, 30, tzinfo=timezone.utc)
    assert should_flatten_for_weekend(now_utc=fri_21, strategy="CARRY") is False


def test_weekend_flatten_can_force_carry():
    fri_21 = datetime(2026, 4, 24, 21, 30, tzinfo=timezone.utc)
    assert should_flatten_for_weekend(
        now_utc=fri_21, strategy="CARRY", carry_exempt=False
    ) is True


def test_limit_stop_for_cross_pairs():
    assert should_use_limit_stop("GBP_JPY") is True
    assert should_use_limit_stop("EUR_GBP") is True
    assert should_use_limit_stop("EUR_USD") is False
    assert should_use_limit_stop("USD_JPY") is False
    assert should_use_limit_stop("") is False
