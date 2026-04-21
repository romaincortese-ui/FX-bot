import numpy as np
import pandas as pd

from fxbot.direction_score import (
    DirectionScore,
    compute_direction_score,
    should_fire,
)


def _synthetic_df(trend: float, n: int = 200, base: float = 1.1000) -> pd.DataFrame:
    """Build a deterministic OHLC frame with a constant per-bar drift.

    ``trend`` is the per-bar close delta in price units.
    """
    rng = np.random.default_rng(42)
    closes = base + np.cumsum(np.full(n, trend) + rng.normal(0, abs(trend) * 0.2 + 1e-5, n))
    highs = closes + abs(trend) * 2.0
    lows = closes - abs(trend) * 2.0
    opens = np.concatenate([[closes[0]], closes[:-1]])
    return pd.DataFrame({"open": opens, "high": highs, "low": lows, "close": closes})


def test_empty_inputs_returns_zero_confidence():
    score = compute_direction_score("EUR_USD")
    assert isinstance(score, DirectionScore)
    assert score.confidence == 0.0
    assert score.contributing_weight == 0.0
    assert score.components == {}


def test_short_frames_are_skipped():
    df = pd.DataFrame({"open": [1.0, 1.0], "high": [1.01, 1.01], "low": [0.99, 0.99], "close": [1.0, 1.0]})
    score = compute_direction_score("EUR_USD", df_m5=df, df_h1=df, df_h4=df)
    # All frames are too short for any indicator.
    assert score.confidence == 0.0
    assert score.contributing_weight == 0.0


def test_strong_uptrend_is_long_with_high_confidence():
    df = _synthetic_df(trend=+0.0005)
    score = compute_direction_score("EUR_USD", df_m5=df, df_h1=df, df_h4=df)
    assert score.direction == "LONG"
    assert score.confidence > 0.4
    assert score.aggregate > 0.0


def test_strong_downtrend_is_short():
    df = _synthetic_df(trend=-0.0005)
    score = compute_direction_score("EUR_USD", df_m5=df, df_h1=df, df_h4=df)
    assert score.direction == "SHORT"
    assert score.aggregate < 0.0


def test_dxy_sign_flip_by_pair_leg():
    df_up = _synthetic_df(trend=+0.0005)
    # EUR/USD: DXY strong (+gap) should push SHORT.
    eur_usd = compute_direction_score(
        "EUR_USD",
        df_m5=df_up,
        df_h1=df_up,
        df_h4=df_up,
        dxy_ema_gap=0.01,
        dxy_gate_threshold=0.005,
    )
    # USD/JPY: DXY strong should push LONG.
    usd_jpy = compute_direction_score(
        "USD_JPY",
        df_m5=df_up,
        df_h1=df_up,
        df_h4=df_up,
        dxy_ema_gap=0.01,
        dxy_gate_threshold=0.005,
    )
    assert "dxy" in eur_usd.components
    assert "dxy" in usd_jpy.components
    assert eur_usd.components["dxy"] < 0
    assert usd_jpy.components["dxy"] > 0


def test_dxy_ignored_for_non_usd_pair():
    df = _synthetic_df(trend=+0.0005)
    score = compute_direction_score(
        "EUR_GBP",
        df_m5=df,
        df_h1=df,
        df_h4=df,
        dxy_ema_gap=0.01,
        dxy_gate_threshold=0.005,
    )
    assert "dxy" not in score.components


def test_should_fire_threshold():
    hi = DirectionScore(direction="LONG", confidence=0.5, aggregate=0.5, components={}, contributing_weight=3.0)
    lo = DirectionScore(direction="LONG", confidence=0.1, aggregate=0.1, components={}, contributing_weight=3.0)
    assert should_fire(hi, min_confidence=0.45) is True
    assert should_fire(lo, min_confidence=0.45) is False
