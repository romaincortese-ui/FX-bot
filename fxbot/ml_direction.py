"""ML direction model (FX-bot Q3 §4.5).

A dependency-free, interpretable alternative to XGBoost for scoring the
short-horizon direction of an FX pair. Per the memo: a shallow tree
ensemble is easier to deploy, audit, and walk-forward validate than a
deep net. This module implements gradient-boosted decision stumps
(depth-1 trees) trained on logistic loss, in pure Python + stdlib only.

Scope of this module:

* ``extract_features`` — compact feature extractor from an M5 window
  (EMA9/21 ratio, RSI, MACD hist, ATR-normalised range, recent return).
* ``label_direction`` — target sign((next-60m return) / ATR) with a
  0.8x ATR dead-band per the memo.
* ``GradientBoostedStumps`` — tiny classifier with ``fit`` and
  ``predict_proba`` methods.
* ``walk_forward_accuracy`` — rolling 30-day train / 30-day test
  accuracy evaluator.

Nothing here is plumbed into ``main.py``; the caller plugs the trained
classifier's probability into the strategy score after the existing
continuous-confidence pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from math import exp, log, tanh


# -- Feature extraction --------------------------------------------------


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = float(values[0])
    for v in values[1:]:
        ema = float(v) * k + ema * (1 - k)
    return ema


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_g = gains / period
    avg_l = losses / period
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        g = diff if diff > 0 else 0.0
        l = -diff if diff < 0 else 0.0
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def _macd_hist(closes: list[float]) -> float | None:
    fast = _ema(closes, 12)
    slow = _ema(closes, 26)
    if fast is None or slow is None:
        return None
    # Signal line on the last 9 (fast-slow) values — approximate.
    # For a single-point feature we use fast-slow directly.
    return fast - slow


def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def extract_features(
    *,
    closes: list[float],
    highs: list[float],
    lows: list[float],
) -> dict[str, float] | None:
    """Produce a compact feature dict from a recent M5 window.

    Returns None if the window is too short. All features are scale-
    normalised so they are comparable across pairs.
    """
    if len(closes) < 30 or len(highs) != len(closes) or len(lows) != len(closes):
        return None
    ema_fast = _ema(closes, 9)
    ema_slow = _ema(closes, 21)
    rsi = _rsi(closes, 14)
    macd_h = _macd_hist(closes)
    atr = _atr(highs, lows, closes, 14)
    last = closes[-1]
    if None in (ema_fast, ema_slow, rsi, macd_h, atr) or atr == 0:
        return None
    ret_5 = (last - closes[-5]) / closes[-5] if closes[-5] else 0.0
    return {
        "ema_ratio": tanh((ema_fast - ema_slow) / atr),
        "rsi_norm": max(-1.0, min(1.0, (rsi - 50.0) / 30.0)),
        "macd_norm": tanh(macd_h / atr),
        "ret_5_norm": tanh(ret_5 / 0.001),
        "range_norm": tanh((highs[-1] - lows[-1]) / atr),
    }


# -- Target labeling -----------------------------------------------------


def label_direction(
    *,
    closes: list[float],
    atr: float,
    horizon_bars: int = 12,
    atr_mult: float = 0.8,
) -> int:
    """Label the sign of the next ``horizon_bars`` return vs 0.8 × ATR.

    Returns +1 if next return > 0.8 ATR, -1 if < -0.8 ATR, else 0
    (flat — dropped from training).
    """
    if len(closes) < horizon_bars + 1 or atr <= 0:
        return 0
    start = closes[-horizon_bars - 1]
    end = closes[-1]
    move = end - start
    threshold = float(atr_mult) * float(atr)
    if move > threshold:
        return 1
    if move < -threshold:
        return -1
    return 0


# -- Gradient-boosted stumps --------------------------------------------


@dataclass
class _Stump:
    feature: str
    threshold: float
    left_value: float   # margin contribution when x[f] <= threshold
    right_value: float  # margin contribution when x[f] >  threshold


def _sigmoid(x: float) -> float:
    if x >= 0:
        return 1.0 / (1.0 + exp(-x))
    e = exp(x)
    return e / (1.0 + e)


@dataclass
class GradientBoostedStumps:
    """Tiny pure-python gradient-boosted depth-1 tree classifier on
    logistic loss. Labels ``y`` in {0, 1}.

    Not a performance-tuned implementation — 50 trees × handful of
    features × a few hundred samples trains in < 1s. It is intended as
    an interpretable reference model for walk-forward validation.
    """
    n_estimators: int = 50
    learning_rate: float = 0.1
    trees: list[_Stump] = field(default_factory=list)
    base_margin: float = 0.0
    feature_names: tuple[str, ...] = ()

    def _best_stump(
        self,
        X: list[dict[str, float]],
        residuals: list[float],
    ) -> _Stump | None:
        best_gain = -1.0
        best: _Stump | None = None
        for feat in self.feature_names:
            values = sorted({row[feat] for row in X})
            for i in range(len(values) - 1):
                thr = 0.5 * (values[i] + values[i + 1])
                left_sum = 0.0
                left_n = 0
                right_sum = 0.0
                right_n = 0
                for row, r in zip(X, residuals):
                    if row[feat] <= thr:
                        left_sum += r
                        left_n += 1
                    else:
                        right_sum += r
                        right_n += 1
                if left_n == 0 or right_n == 0:
                    continue
                # Gain = reduction in squared residual using mean splits.
                mean_left = left_sum / left_n
                mean_right = right_sum / right_n
                gain = left_n * mean_left ** 2 + right_n * mean_right ** 2
                if gain > best_gain:
                    best_gain = gain
                    best = _Stump(
                        feature=feat,
                        threshold=thr,
                        left_value=mean_left,
                        right_value=mean_right,
                    )
        return best

    def fit(
        self,
        X: list[dict[str, float]],
        y: list[int],
    ) -> "GradientBoostedStumps":
        if not X or not y or len(X) != len(y):
            raise ValueError("X/y length mismatch or empty")
        self.feature_names = tuple(sorted(X[0].keys()))
        # Initial log-odds base margin.
        p = max(1e-6, min(1 - 1e-6, sum(y) / len(y)))
        self.base_margin = log(p / (1.0 - p))
        margins = [self.base_margin] * len(X)
        self.trees = []
        for _ in range(int(self.n_estimators)):
            residuals = [y[i] - _sigmoid(margins[i]) for i in range(len(X))]
            stump = self._best_stump(X, residuals)
            if stump is None:
                break
            stump.left_value *= float(self.learning_rate)
            stump.right_value *= float(self.learning_rate)
            self.trees.append(stump)
            for i, row in enumerate(X):
                if row[stump.feature] <= stump.threshold:
                    margins[i] += stump.left_value
                else:
                    margins[i] += stump.right_value
        return self

    def predict_margin(self, row: dict[str, float]) -> float:
        m = self.base_margin
        for stump in self.trees:
            v = row.get(stump.feature, 0.0)
            m += stump.left_value if v <= stump.threshold else stump.right_value
        return m

    def predict_proba(self, row: dict[str, float]) -> float:
        return _sigmoid(self.predict_margin(row))

    def predict(self, row: dict[str, float]) -> int:
        return 1 if self.predict_proba(row) >= 0.5 else 0


# -- Walk-forward evaluator ----------------------------------------------


def walk_forward_accuracy(
    *,
    samples: list[tuple[dict[str, float], int]],
    train_size: int,
    test_size: int,
    n_estimators: int = 50,
    learning_rate: float = 0.1,
) -> float | None:
    """Return out-of-sample accuracy across a rolling walk-forward.

    ``samples`` is a chronologically ordered list of (features, label)
    with labels in {0, 1}. Requires at least ``train_size + test_size``
    samples.
    """
    n = len(samples)
    if n < int(train_size) + int(test_size):
        return None
    correct = 0
    total = 0
    start = 0
    while start + train_size + test_size <= n:
        train = samples[start : start + train_size]
        test = samples[start + train_size : start + train_size + test_size]
        model = GradientBoostedStumps(
            n_estimators=n_estimators, learning_rate=learning_rate
        )
        model.fit([x for x, _ in train], [y for _, y in train])
        for x, y in test:
            pred = model.predict(x)
            if pred == y:
                correct += 1
            total += 1
        start += test_size
    if total == 0:
        return None
    return correct / total
