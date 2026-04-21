from fxbot.news_surprise import (
    classify_surprise_bias,
    compute_surprise_z,
    pair_bias_from_surprise,
    surprise_score_multiplier,
)


def test_compute_surprise_z_basic():
    z = compute_surprise_z(actual=250_000.0, consensus=200_000.0, historical_std=50_000.0)
    assert z == 1.0


def test_compute_surprise_z_zero_std_returns_none():
    assert compute_surprise_z(actual=1, consensus=1, historical_std=0) is None


def test_below_threshold_does_not_fire():
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=205_000,
        consensus=200_000,
        historical_std=50_000,
    )
    assert sig.bias_direction == "NONE"
    assert sig.score_multiplier == 0.0


def test_beat_fires_long_by_default():
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=300_000,
        consensus=200_000,
        historical_std=50_000,
    )
    assert sig.bias_direction == "LONG"
    assert sig.bias_currency == "USD"
    assert sig.score_multiplier > 0


def test_miss_fires_short():
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=100_000,
        consensus=200_000,
        historical_std=50_000,
    )
    assert sig.bias_direction == "SHORT"
    assert sig.surprise_z == -2.0


def test_direction_on_beat_inverted():
    # For an event where a beat implies the currency should fall
    # (pass direction_on_beat="SHORT"):
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=300_000,
        consensus=200_000,
        historical_std=50_000,
        direction_on_beat="SHORT",
    )
    assert sig.bias_direction == "SHORT"


def test_pair_bias_base_leg():
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=300_000,
        consensus=200_000,
        historical_std=50_000,
    )
    assert pair_bias_from_surprise("USD_JPY", sig) == "LONG"
    # EUR_USD: USD is quote → inverted → SHORT.
    assert pair_bias_from_surprise("EUR_USD", sig) == "SHORT"
    # Unrelated pair.
    assert pair_bias_from_surprise("EUR_GBP", sig) == "NONE"


def test_surprise_score_multiplier_saturates():
    assert surprise_score_multiplier(0.3) == 0.0
    assert surprise_score_multiplier(0.5) == 0.0   # boundary
    assert surprise_score_multiplier(1.0) > 0.4
    assert surprise_score_multiplier(5.0) < 1.5
    assert surprise_score_multiplier(None) == 0.0


def test_no_std_returns_no_signal():
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=100,
        consensus=90,
        historical_std=0,
    )
    assert sig.bias_direction == "NONE"
    assert sig.reason == "no_historical_std"


def test_decay_seconds_preserved():
    sig = classify_surprise_bias(
        event_currency="USD",
        actual=300_000,
        consensus=200_000,
        historical_std=50_000,
        decay_seconds=7200,
    )
    assert sig.decay_seconds == 7200
