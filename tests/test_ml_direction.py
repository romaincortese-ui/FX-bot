import math
import random

from fxbot.ml_direction import (
    GradientBoostedStumps,
    extract_features,
    label_direction,
    walk_forward_accuracy,
)


def _synthetic_series(n: int = 80, seed: int = 1) -> tuple[list[float], list[float], list[float]]:
    rng = random.Random(seed)
    closes = [1.1000]
    highs = [1.1005]
    lows = [1.0995]
    for _ in range(n - 1):
        drift = 0.0001 * rng.uniform(-1, 1)
        c = closes[-1] + drift
        closes.append(c)
        highs.append(c + 0.0003 * rng.random())
        lows.append(c - 0.0003 * rng.random())
    return closes, highs, lows


def test_extract_features_returns_none_on_short_window():
    assert extract_features(closes=[1.0, 1.01], highs=[1.0, 1.01], lows=[1.0, 1.01]) is None


def test_extract_features_shape():
    closes, highs, lows = _synthetic_series(80)
    feats = extract_features(closes=closes, highs=highs, lows=lows)
    assert feats is not None
    expected = {"ema_ratio", "rsi_norm", "macd_norm", "ret_5_norm", "range_norm"}
    assert set(feats.keys()) == expected
    for v in feats.values():
        assert -1.0 <= v <= 1.0 or math.isfinite(v)


def test_label_direction_up():
    closes = [1.0 + 0.001 * i for i in range(20)]
    assert label_direction(closes=closes, atr=0.005, horizon_bars=12, atr_mult=0.8) == 1


def test_label_direction_down():
    closes = [1.0 - 0.001 * i for i in range(20)]
    assert label_direction(closes=closes, atr=0.005, horizon_bars=12, atr_mult=0.8) == -1


def test_label_direction_flat():
    closes = [1.0 + 0.00001 * i for i in range(20)]
    assert label_direction(closes=closes, atr=0.005, horizon_bars=12, atr_mult=0.8) == 0


def test_label_direction_bad_inputs():
    assert label_direction(closes=[1.0], atr=0.005) == 0
    assert label_direction(closes=[1.0] * 20, atr=0.0) == 0


def _linear_dataset(n: int = 120) -> list[tuple[dict[str, float], int]]:
    """Toy dataset: label = 1 if feat_a + feat_b > 0 else 0."""
    rng = random.Random(7)
    samples = []
    for _ in range(n):
        a = rng.uniform(-1, 1)
        b = rng.uniform(-1, 1)
        label = 1 if a + b + rng.gauss(0, 0.1) > 0 else 0
        samples.append(({"feat_a": a, "feat_b": b}, label))
    return samples


def test_gbstumps_fits_and_predicts():
    data = _linear_dataset(200)
    X = [x for x, _ in data]
    y = [yy for _, yy in data]
    model = GradientBoostedStumps(n_estimators=30, learning_rate=0.2).fit(X, y)
    # Training accuracy should be clearly above chance.
    correct = sum(1 for x, yy in data if model.predict(x) == yy)
    assert correct / len(data) > 0.75


def test_gbstumps_predict_proba_range():
    data = _linear_dataset(80)
    X = [x for x, _ in data]
    y = [yy for _, yy in data]
    model = GradientBoostedStumps(n_estimators=20, learning_rate=0.2).fit(X, y)
    for x, _ in data:
        p = model.predict_proba(x)
        assert 0.0 <= p <= 1.0


def test_fit_rejects_empty():
    model = GradientBoostedStumps(n_estimators=5)
    try:
        model.fit([], [])
    except ValueError:
        return
    raise AssertionError("expected ValueError for empty input")


def test_walk_forward_accuracy_above_chance():
    data = _linear_dataset(200)
    acc = walk_forward_accuracy(
        samples=data, train_size=80, test_size=40, n_estimators=20, learning_rate=0.2
    )
    assert acc is not None
    assert acc > 0.65


def test_walk_forward_accuracy_too_few_samples():
    data = _linear_dataset(20)
    assert walk_forward_accuracy(samples=data, train_size=80, test_size=40) is None
