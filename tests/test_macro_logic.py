from fxbot.macro_logic import build_rate_bias, merge_biases


def test_build_rate_bias_prefers_higher_relative_rate():
    rates = {"US_2Y": 4.0, "UK_2Y": 4.5, "EU_2Y": 3.2, "JP_2Y": 0.5}
    biases = build_rate_bias(rates, rate_spread_threshold=0.25)
    assert biases["GBP_USD"] == "LONG_ONLY"
    assert biases["USD_JPY"] == "LONG_ONLY"


def test_merge_biases_uses_later_group_priority():
    merged = merge_biases({"EUR_USD": "LONG_ONLY"}, {"EUR_USD": "SHORT_ONLY", "GBP_USD": "LONG_ONLY"})
    assert merged == {"EUR_USD": "SHORT_ONLY", "GBP_USD": "LONG_ONLY"}
