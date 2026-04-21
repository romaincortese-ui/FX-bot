import math

from fxbot.vol_sizing import (
    compute_vol_adjusted_risk_pct,
    realised_daily_vol_annualised,
)


def _flat_series(n: int, ret_per_day: float, start: float = 100.0) -> list[float]:
    closes = [start]
    for _ in range(n):
        closes.append(closes[-1] * (1 + ret_per_day))
    return closes


def test_insufficient_history_returns_passthrough():
    closes = [100.0, 101.0, 102.0]  # only 3 points → lookback 20 unreachable
    result = compute_vol_adjusted_risk_pct(
        base_risk_pct=0.015,
        daily_closes=closes,
        target_annualised_vol=0.08,
        lookback_days=20,
    )
    assert result.reason == "insufficient_history"
    assert math.isclose(result.adjusted_risk_pct, 0.015)


def test_perfectly_flat_series_treated_as_insufficient():
    closes = _flat_series(30, 0.0)
    result = compute_vol_adjusted_risk_pct(
        base_risk_pct=0.015,
        daily_closes=closes,
        target_annualised_vol=0.08,
        lookback_days=20,
    )
    # Zero variance → None realised → passthrough.
    assert result.reason == "insufficient_history"


def test_high_realised_vol_scales_down_risk():
    # Alternating +/- 2% daily returns → very high realised vol.
    closes = [100.0]
    for i in range(30):
        closes.append(closes[-1] * (1.02 if i % 2 == 0 else 0.98))
    result = compute_vol_adjusted_risk_pct(
        base_risk_pct=0.015,
        daily_closes=closes,
        target_annualised_vol=0.08,
        lookback_days=20,
    )
    assert result.reason in {"vol_target_applied", "floored_to_min"}
    assert result.adjusted_risk_pct < 0.015


def test_low_realised_vol_scales_up_but_capped():
    # Tiny drift ~= 0.01%/day → very low vol.
    closes = _flat_series(40, 0.0001)
    # Add a hint of noise so variance > 0.
    closes = [c * (1 + (0.00005 if i % 3 == 0 else -0.00005)) for i, c in enumerate(closes)]
    result = compute_vol_adjusted_risk_pct(
        base_risk_pct=0.015,
        daily_closes=closes,
        target_annualised_vol=0.08,
        lookback_days=20,
        max_risk_pct=0.020,
    )
    assert result.adjusted_risk_pct <= 0.020 + 1e-12
    assert result.adjusted_risk_pct >= 0.015 - 1e-12


def test_explicit_bounds_respected():
    closes = [100.0]
    for i in range(30):
        closes.append(closes[-1] * (1.05 if i % 2 == 0 else 0.95))
    result = compute_vol_adjusted_risk_pct(
        base_risk_pct=0.015,
        daily_closes=closes,
        target_annualised_vol=0.08,
        lookback_days=20,
        min_risk_pct=0.005,
        max_risk_pct=0.030,
    )
    assert 0.005 - 1e-12 <= result.adjusted_risk_pct <= 0.030 + 1e-12


def test_realised_vol_positive_for_noisy_series():
    closes = [100.0]
    for i in range(40):
        closes.append(closes[-1] * (1.01 if i % 2 == 0 else 0.99))
    vol = realised_daily_vol_annualised(closes, lookback_days=20)
    assert vol is not None and vol > 0
