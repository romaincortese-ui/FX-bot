import pytest

from fxbot.carry_basket import (
    CurrencyRate,
    build_carry_basket,
    compute_exposure_multiplier,
    drawdown_kill,
    should_rebalance,
)


def _universe() -> list[CurrencyRate]:
    return [
        CurrencyRate("USD", 5.25, 8.0),
        CurrencyRate("EUR", 3.75, 7.0),
        CurrencyRate("GBP", 5.00, 8.5),
        CurrencyRate("JPY", 0.10, 6.5),
        CurrencyRate("CHF", 1.50, 7.0),
        CurrencyRate("AUD", 4.35, 10.0),
        CurrencyRate("NZD", 5.50, 11.0),
        CurrencyRate("CAD", 4.75, 7.5),
    ]


def test_exposure_multiplier_below_threshold():
    assert compute_exposure_multiplier(usdjpy_1w_iv_pct=8.0) == 1.0


def test_exposure_multiplier_above_zero_threshold():
    assert compute_exposure_multiplier(usdjpy_1w_iv_pct=14.0) == 0.0


def test_exposure_multiplier_ramps():
    m = compute_exposure_multiplier(usdjpy_1w_iv_pct=12.0)
    assert 0.0 < m < 1.0


def test_exposure_multiplier_missing_defaults_to_one():
    assert compute_exposure_multiplier(usdjpy_1w_iv_pct=None) == 1.0


def test_build_basket_structure():
    b = build_carry_basket(rates=_universe(), top_n=3, bottom_n=3)
    assert len(b.legs) == 6
    longs = [l for l in b.legs if l.direction == "LONG"]
    shorts = [l for l in b.legs if l.direction == "SHORT"]
    assert len(longs) == 3
    assert len(shorts) == 3
    total_w = sum(l.weight for l in b.legs)
    assert abs(total_w - 1.0) < 1e-9


def test_build_basket_picks_highest_and_lowest():
    b = build_carry_basket(rates=_universe(), top_n=2, bottom_n=2)
    long_ccys = {l.currency for l in b.legs if l.direction == "LONG"}
    short_ccys = {l.currency for l in b.legs if l.direction == "SHORT"}
    assert "NZD" in long_ccys  # highest yielder 5.50
    assert "USD" in long_ccys  # 5.25
    assert "JPY" in short_ccys  # lowest yielder
    assert "CHF" in short_ccys  # second-lowest


def test_basket_expected_carry_positive():
    b = build_carry_basket(rates=_universe())
    assert b.expected_annual_carry_pct > 0


def test_basket_exposure_scaling_on_high_iv():
    b = build_carry_basket(rates=_universe(), usdjpy_1w_iv_pct=12.0)
    assert 0.0 < b.exposure_multiplier < 1.0


def test_insufficient_universe_returns_empty():
    b = build_carry_basket(rates=_universe()[:4], top_n=3, bottom_n=3)
    assert b.legs == ()
    assert b.exposure_multiplier == 0.0


def test_should_rebalance_none_is_true():
    assert should_rebalance(last_rebalance_days_ago=None) is True


def test_should_rebalance_weekly():
    assert should_rebalance(last_rebalance_days_ago=3) is False
    assert should_rebalance(last_rebalance_days_ago=8) is True


def test_drawdown_kill_triggers():
    # 5% expected annual carry, 30 days held → accrued ≈ 0.41%.
    # kill if DD > 1.5 × 0.41 ≈ 0.62%.
    assert drawdown_kill(
        basket_drawdown_pct=1.0, expected_annual_carry_pct=5.0, holding_days=30
    ) is True


def test_drawdown_kill_does_not_trigger_small_dd():
    assert drawdown_kill(
        basket_drawdown_pct=0.2, expected_annual_carry_pct=5.0, holding_days=30
    ) is False


def test_drawdown_kill_no_carry_no_kill():
    assert drawdown_kill(
        basket_drawdown_pct=10.0, expected_annual_carry_pct=0.0, holding_days=30
    ) is False
