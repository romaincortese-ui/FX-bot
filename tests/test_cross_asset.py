from fxbot.cross_asset import (
    build_cross_asset_overlay,
    compute_eur_bias_score,
    compute_risk_on_score,
    compute_usd_bias_score,
    cross_asset_pair_bias,
)


def _ramp(start: float, step: float, n: int) -> list[float]:
    return [start + step * i for i in range(n)]


def test_risk_on_none_without_inputs():
    assert compute_risk_on_score() is None


def test_risk_on_rising_spy():
    spy = _ramp(400, 1.0, 40)
    s = compute_risk_on_score(spy_closes=spy)
    assert s is not None and s > 0.0


def test_risk_on_spiking_vix_is_negative():
    vix = [15.0] * 60 + [30.0]
    s = compute_risk_on_score(vix_history=vix)
    assert s is not None and s < 0.0


def test_usd_bias_rising_dxy_is_positive():
    dxy = _ramp(100, 0.1, 40)
    s = compute_usd_bias_score(dxy_closes=dxy)
    assert s is not None and s > 0.0


def test_usd_bias_rising_yield_is_positive():
    # Flat DXY, no curve, yield rising.
    y = [4.00, 4.05, 4.10, 4.15, 4.25, 4.35]
    s = compute_usd_bias_score(us10y_yield_history=y)
    assert s is not None and s > 0.0


def test_eur_bias_rising_spread_is_positive():
    spread = [10, 15, 20, 25, 35, 40]
    s = compute_eur_bias_score(german_us_2y_spread_bp_history=spread)
    assert s is not None and s > 0.0


def test_overlay_pair_bias_eur_usd():
    overlay = build_cross_asset_overlay(
        dxy_closes=_ramp(100, 0.1, 40),  # strong USD → USD+
    )
    bias = cross_asset_pair_bias("EUR_USD", overlay, score_points=12.0)
    # USD is quote on EUR_USD → pair should be SHORT (negative).
    assert bias < 0


def test_overlay_pair_bias_usd_jpy():
    overlay = build_cross_asset_overlay(
        dxy_closes=_ramp(100, 0.1, 40),
    )
    bias = cross_asset_pair_bias("USD_JPY", overlay, score_points=12.0)
    assert bias > 0


def test_overlay_pair_bias_risk_fx():
    overlay = build_cross_asset_overlay(
        spy_closes=_ramp(400, 1.0, 40),  # risk-on
    )
    aud = cross_asset_pair_bias("AUD_USD", overlay)
    jpy = cross_asset_pair_bias("USD_JPY", overlay)
    # Risk-on → AUD long vs USD, USD long vs JPY (safe haven).
    assert aud > 0
    assert jpy > 0


def test_overlay_pair_bias_no_inputs_is_zero():
    overlay = build_cross_asset_overlay()
    assert cross_asset_pair_bias("EUR_USD", overlay) == 0.0


def test_pair_bias_bounded():
    overlay = build_cross_asset_overlay(
        dxy_closes=_ramp(100, 5.0, 40),      # huge USD trend
        spy_closes=_ramp(400, 10.0, 40),     # huge risk-on
    )
    b = cross_asset_pair_bias("EUR_USD", overlay, score_points=12.0)
    assert -12.0 <= b <= 12.0
