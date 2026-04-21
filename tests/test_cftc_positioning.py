from fxbot.cftc_positioning import (
    compute_positioning_signal,
    pair_positioning_bias,
    should_fade_entry,
)


def _history(value: int, n: int = 104) -> list[int]:
    return [value + i for i in range(n)]


def test_insufficient_history_returns_none():
    sig = compute_positioning_signal(currency="EUR", net_position=100, history=[1, 2, 3])
    assert sig.extreme == "NONE"
    assert sig.percentile is None


def test_extreme_long_triggers_fade():
    hist = list(range(-50, 54))  # 104 weeks
    sig = compute_positioning_signal(currency="EUR", net_position=200, history=hist)
    assert sig.extreme == "LONG"
    assert sig.score_multiplier > 0
    assert should_fade_entry(sig, "LONG") is True
    assert should_fade_entry(sig, "SHORT") is False


def test_extreme_short_triggers_fade():
    hist = list(range(-50, 54))
    sig = compute_positioning_signal(currency="EUR", net_position=-200, history=hist)
    assert sig.extreme == "SHORT"
    assert should_fade_entry(sig, "SHORT") is True


def test_mid_range_is_none():
    hist = list(range(-50, 54))
    sig = compute_positioning_signal(currency="EUR", net_position=0, history=hist)
    assert sig.extreme == "NONE"
    assert sig.score_multiplier == 0.0


def test_pair_bias_base_crowded_long():
    hist = list(range(-50, 54))
    eur = compute_positioning_signal(currency="EUR", net_position=500, history=hist)
    usd = compute_positioning_signal(currency="USD", net_position=0, history=hist)
    assert pair_positioning_bias("EUR_USD", eur, usd) == "LONG"


def test_pair_bias_quote_crowded_long():
    hist = list(range(-50, 54))
    usd = compute_positioning_signal(currency="USD", net_position=500, history=hist)
    eur = compute_positioning_signal(currency="EUR", net_position=0, history=hist)
    # EUR_USD with quote (USD) long-crowded → fade pair short.
    assert pair_positioning_bias("EUR_USD", eur, usd) == "SHORT"


def test_pair_bias_unknown_when_empty():
    assert pair_positioning_bias("", None, None) == "NONE"


def test_should_fade_entry_handles_unknown_side():
    hist = list(range(-50, 54))
    sig = compute_positioning_signal(currency="EUR", net_position=200, history=hist)
    assert should_fade_entry(sig, "INVALID") is False
