from fxbot.regime import (
    Regime,
    classify_regime,
    compute_dxy_20d_slope_pct,
    compute_spy_ema_ratio,
    compute_vix_percentile_60d,
    is_strategy_enabled,
)


def _series(start: float, step: float, n: int) -> list[float]:
    return [start + step * i for i in range(n)]


def test_empty_inputs_default_to_chop():
    r = classify_regime()
    assert r.regime is Regime.CHOP


def test_strong_dxy_rally_is_usd_trend():
    dxy = _series(100.0, 0.15, 40)  # +0.15/day over 40d → ~6% total
    r = classify_regime(dxy_closes=dxy)
    assert r.regime is Regime.USD_TREND
    assert r.dxy_20d_slope_pct is not None
    assert r.dxy_20d_slope_pct > 1.5


def test_strong_dxy_sell_off_is_usd_trend():
    dxy = _series(110.0, -0.15, 40)
    r = classify_regime(dxy_closes=dxy)
    assert r.regime is Regime.USD_TREND
    assert r.dxy_20d_slope_pct < -1.5


def test_risk_off_requires_high_vix_and_weak_spy():
    vix = _series(15.0, 0.0, 60) + [35.0]   # latest VIX spike
    spy = _series(500.0, -1.0, 30)          # falling SPY
    r = classify_regime(vix_history=vix, spy_closes=spy)
    assert r.regime is Regime.RISK_OFF


def test_risk_on_requires_low_vix_and_rising_spy():
    vix = _series(25.0, 0.0, 60) + [10.0]   # VIX drops to floor
    spy = _series(400.0, 2.0, 30)
    r = classify_regime(vix_history=vix, spy_closes=spy)
    assert r.regime is Regime.RISK_ON


def test_mixed_signals_are_chop():
    vix = _series(18.0, 0.0, 60) + [18.0]   # middling VIX
    spy = _series(450.0, 0.1, 30)           # tiny drift
    r = classify_regime(vix_history=vix, spy_closes=spy)
    assert r.regime is Regime.CHOP


def test_dxy_trend_takes_precedence_over_risk_off():
    dxy = _series(100.0, 0.15, 40)
    vix = _series(15.0, 0.0, 60) + [35.0]
    spy = _series(500.0, -1.0, 30)
    r = classify_regime(dxy_closes=dxy, vix_history=vix, spy_closes=spy)
    assert r.regime is Regime.USD_TREND


def test_strategy_gates():
    assert is_strategy_enabled("SCALPER", Regime.CHOP) is True
    assert is_strategy_enabled("SCALPER", Regime.USD_TREND) is False
    assert is_strategy_enabled("TREND", Regime.CHOP) is False
    assert is_strategy_enabled("TREND", Regime.USD_TREND) is True
    assert is_strategy_enabled("CARRY", Regime.RISK_OFF) is False
    assert is_strategy_enabled("REVERSAL", Regime.USD_TREND) is False


def test_vix_percentile_monotone():
    vix = list(range(1, 61))   # 1..60
    assert compute_vix_percentile_60d(vix) is not None
    # Latest = 60, strictly greater than all previous → percentile near 100.
    pct = compute_vix_percentile_60d(vix)
    assert pct > 95.0


def test_helpers_handle_short_histories():
    assert compute_dxy_20d_slope_pct([1, 2, 3]) is None
    assert compute_vix_percentile_60d([1, 2]) is None
    assert compute_spy_ema_ratio([1, 2]) is None
