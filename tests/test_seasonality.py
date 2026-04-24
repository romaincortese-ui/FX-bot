from datetime import datetime, timezone

from fxbot.seasonality import get_seasonal_bias, seasonal_risk_multiplier


def test_eurusd_pre_london_is_mean_revert():
    ts = datetime(2025, 3, 5, 8, 30, tzinfo=timezone.utc)
    bias = get_seasonal_bias("EUR_USD", ts)
    assert bias.tendency == "MEAN_REVERT"
    assert bias.strategy_multiplier["REVERSAL"] > 1.0


def test_eurusd_us_open_is_trend():
    ts = datetime(2025, 3, 5, 13, 45, tzinfo=timezone.utc)
    bias = get_seasonal_bias("EUR_USD", ts)
    assert bias.tendency == "TREND"
    assert bias.strategy_multiplier["TREND"] > 1.0


def test_eurusd_late_night_is_chop():
    ts = datetime(2025, 3, 5, 21, 30, tzinfo=timezone.utc)
    bias = get_seasonal_bias("EUR_USD", ts)
    assert bias.tendency == "CHOP"
    assert bias.strategy_multiplier["TREND"] < 1.0


def test_unknown_pair_is_neutral():
    ts = datetime(2025, 3, 5, 13, 0, tzinfo=timezone.utc)
    bias = get_seasonal_bias("AUD_CAD", ts)
    assert bias.tendency == "NEUTRAL"
    assert bias.strategy_multiplier == {}


def test_risk_multiplier_cap_and_floor():
    ts = datetime(2025, 3, 5, 13, 0, tzinfo=timezone.utc)
    m = seasonal_risk_multiplier("TREND", "EUR_USD", ts)
    assert 0.5 <= m <= 1.35


def test_risk_multiplier_defaults_to_one_for_unknown():
    ts = datetime(2025, 3, 5, 2, 0, tzinfo=timezone.utc)
    assert seasonal_risk_multiplier("TREND", "EUR_USD", ts) == 1.0
