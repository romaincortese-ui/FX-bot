from fxbot.options_iv import (
    classify_iv_regime,
    classify_risk_reversal,
    strategy_weight_for_iv_regime,
)


def test_small_rr_is_none():
    s = classify_risk_reversal(instrument="EUR_USD", rr_25d_vols=0.3)
    assert s.bias_direction == "NONE"


def test_positive_rr_biases_long():
    s = classify_risk_reversal(instrument="EUR_USD", rr_25d_vols=1.5)
    assert s.bias_direction == "LONG"
    assert s.score_multiplier > 0


def test_negative_rr_biases_short():
    s = classify_risk_reversal(instrument="EUR_USD", rr_25d_vols=-2.0)
    assert s.bias_direction == "SHORT"


def test_iv_regime_insufficient_history():
    r = classify_iv_regime(
        instrument="EUR_USD", atm_iv_history=[5.0, 6.0], current_atm_iv=5.5
    )
    assert r.regime == "NEUTRAL"
    assert r.percentile is None


def test_iv_regime_high_is_mean_revert():
    hist = [5.0 + 0.01 * i for i in range(100)]  # 5..~6
    r = classify_iv_regime(
        instrument="EUR_USD", atm_iv_history=hist, current_atm_iv=10.0
    )
    assert r.regime == "MEAN_REVERT"


def test_iv_regime_low_is_breakout_soft():
    hist = [5.0 + 0.01 * i for i in range(100)]
    r = classify_iv_regime(
        instrument="EUR_USD", atm_iv_history=hist, current_atm_iv=4.0
    )
    assert r.regime == "BREAKOUT_SOFT"


def test_iv_regime_middle_is_trend():
    hist = [5.0 + 0.01 * i for i in range(100)]
    r = classify_iv_regime(
        instrument="EUR_USD", atm_iv_history=hist, current_atm_iv=5.5
    )
    assert r.regime == "TREND"


def test_strategy_weights_for_mean_revert():
    r = classify_iv_regime(
        instrument="EUR_USD",
        atm_iv_history=[5.0 + 0.01 * i for i in range(100)],
        current_atm_iv=10.0,
    )
    assert strategy_weight_for_iv_regime("SCALPER", r) == 1.2
    assert strategy_weight_for_iv_regime("TREND", r) < 1.0


def test_strategy_weights_for_breakout_soft():
    r = classify_iv_regime(
        instrument="EUR_USD",
        atm_iv_history=[5.0 + 0.01 * i for i in range(100)],
        current_atm_iv=4.0,
    )
    assert strategy_weight_for_iv_regime("TREND", r) == 0.6
    assert strategy_weight_for_iv_regime("SCALPER", r) == 1.0
