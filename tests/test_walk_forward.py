from datetime import datetime, timedelta, timezone

from fxbot.walk_forward import (
    evaluate_walk_forward,
    should_recalibrate_now,
)


def _m(pf, ret, n):
    return {"profit_factor": pf, "return_per_trade_pct": ret, "trade_count": n}


def test_healthy_walk_forward_accepts():
    d = evaluate_walk_forward(
        in_sample=_m(pf=1.6, ret=0.40, n=200),
        out_of_sample=_m(pf=1.3, ret=0.30, n=60),
    )
    assert d.accept is True
    assert d.reason == "passed"


def test_low_oos_pf_rejects():
    d = evaluate_walk_forward(
        in_sample=_m(pf=1.6, ret=0.40, n=200),
        out_of_sample=_m(pf=1.05, ret=0.30, n=60),
    )
    assert d.accept is False
    assert "oos_pf_" in d.reason


def test_thin_oos_sample_rejects():
    d = evaluate_walk_forward(
        in_sample=_m(pf=1.6, ret=0.40, n=200),
        out_of_sample=_m(pf=1.5, ret=0.35, n=10),
    )
    assert d.accept is False
    assert "oos_trade_count_10" in d.reason


def test_degradation_below_half_rejects():
    d = evaluate_walk_forward(
        in_sample=_m(pf=1.6, ret=0.50, n=200),
        out_of_sample=_m(pf=1.2, ret=0.15, n=60),     # ratio 0.3
    )
    assert d.accept is False
    assert "degradation_ratio_" in d.reason


def test_is_negative_is_never_accepted():
    d = evaluate_walk_forward(
        in_sample=_m(pf=0.8, ret=-0.10, n=200),
        out_of_sample=_m(pf=1.3, ret=0.20, n=60),
    )
    # IS return is negative → degradation collapses to 0.0 → reject.
    assert d.accept is False


def test_should_recalibrate_true_when_never_shipped():
    assert should_recalibrate_now(last_shipped_at=None) is True


def test_should_recalibrate_false_within_interval():
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(days=3)
    assert should_recalibrate_now(last_shipped_at=last, now=now) is False


def test_should_recalibrate_true_after_interval():
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(days=8)
    assert should_recalibrate_now(last_shipped_at=last, now=now) is True


def test_naive_datetime_is_treated_as_utc():
    now = datetime(2026, 4, 21, 12, 0)          # naive
    last = datetime(2026, 4, 10, 12, 0)         # naive, 11d earlier
    assert should_recalibrate_now(last_shipped_at=last, now=now) is True
