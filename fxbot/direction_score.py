"""Continuous direction-score module (FX-bot Sprint 1 §2.1).

Replaces the integer ``+1/-1`` vote sum in ``fxbot/strategies/direction.py``
with per-indicator confidences in ``[-1, +1]`` and a weighted aggregate.

Outputs a ``DirectionScore`` with:

* ``direction`` — ``"LONG"`` or ``"SHORT"`` (sign of aggregate),
* ``confidence`` — absolute aggregate in ``[0, 1]`` (``|weighted_sum| /
  max_possible_weight``),
* ``components`` — per-indicator breakdown for observability.

The main loop / scoring pipeline should then gate with
``confidence >= min_confidence`` (default 0.45) before firing any trade.

Component weights mirror the review memo:

- H4 EMA alignment → 3.0 (highest-TF trend)
- H1 EMA alignment → 2.0
- H1 MACD hist    → 2.0
- M5 EMA alignment → 1.0
- M5 RSI centred  → 1.0
- H1 RSI centred  → 1.0
- M5 MACD hist    → 1.0
- DXY alignment   → 2.0 (USD pairs only; sign flipped for quote-USD)

Every component returns ``None`` when the frame is too short; missing
components are skipped and weights are renormalised across the remaining
contributors.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import tanh
from typing import Optional

import pandas as pd

from fxbot.indicators import calc_ema, calc_macd, calc_rsi


@dataclass(frozen=True, slots=True)
class DirectionScore:
    direction: str                # "LONG" | "SHORT"
    confidence: float             # [0, 1]
    aggregate: float              # signed weighted score in [-1, +1]
    components: dict[str, float]  # per-indicator confidences in [-1, +1]
    contributing_weight: float    # sum of weights of contributing indicators


# Maximum weight if every component contributes.
_WEIGHTS: dict[str, float] = {
    "ema_h4": 3.0,
    "ema_h1": 2.0,
    "macd_h1": 2.0,
    "dxy": 2.0,
    "ema_m5": 1.0,
    "rsi_m5": 1.0,
    "rsi_h1": 1.0,
    "macd_m5": 1.0,
}


def _safe_atr_proxy(df: pd.DataFrame, lookback: int = 14) -> float | None:
    """Very small ATR proxy: rolling mean of (high - low) over ``lookback``.

    Used as the denominator in ``tanh((fast - slow) / atr)``. Returns
    ``None`` when the frame is too short or ATR is non-positive.
    """
    if df is None or len(df) < lookback + 1:
        return None
    if not {"high", "low"}.issubset(df.columns):
        return None
    try:
        atr = float((df["high"] - df["low"]).rolling(lookback).mean().iloc[-1])
    except Exception:
        return None
    if atr <= 0:
        return None
    return atr


def _ema_alignment(df: pd.DataFrame, fast: int, slow: int) -> float | None:
    if df is None or len(df) < slow + 5:
        return None
    close = df["close"]
    ema_fast = calc_ema(close, fast)
    ema_slow = calc_ema(close, slow)
    try:
        diff = float(ema_fast.iloc[-1]) - float(ema_slow.iloc[-1])
    except Exception:
        return None
    atr = _safe_atr_proxy(df)
    if atr is None:
        return None
    # tanh gives a smooth bounded confidence; scale by 0.5 so a 0.5*ATR
    # separation scores ~0.46 (not saturated).
    return max(-1.0, min(1.0, tanh(diff / (atr * 0.5))))


def _rsi_centred(df: pd.DataFrame) -> float | None:
    if df is None or len(df) < 20:
        return None
    try:
        rsi = float(calc_rsi(df["close"]))
    except Exception:
        return None
    # (rsi - 50) / 30 clamped; RSI 80 → 1.0, RSI 50 → 0.0, RSI 20 → -1.0.
    return max(-1.0, min(1.0, (rsi - 50.0) / 30.0))


def _macd_hist_conf(df: pd.DataFrame) -> float | None:
    if df is None or len(df) < 40:
        return None
    try:
        macd = calc_macd(df)
    except Exception:
        return None
    hist = macd.get("histogram") if isinstance(macd, dict) else None
    if hist is None:
        return None
    atr = _safe_atr_proxy(df)
    if atr is None:
        return None
    return max(-1.0, min(1.0, tanh(float(hist) / (atr * 0.5))))


def _dxy_component(
    instrument: str,
    dxy_ema_gap: float | None,
    dxy_gate_threshold: float,
) -> float | None:
    if dxy_ema_gap is None or "USD" not in instrument:
        return None
    try:
        base, quote = instrument.split("_")
    except ValueError:
        return None
    threshold = max(1e-9, float(dxy_gate_threshold))
    # Smooth tanh around the gate threshold.
    raw = tanh(float(dxy_ema_gap) / (threshold * 2.0))
    if quote == "USD":
        # For EUR/USD etc., DXY strong (raw > 0) means USD bid → short pair.
        return -raw
    if base == "USD":
        # For USD/JPY etc., DXY strong → long pair.
        return raw
    return None


def compute_direction_score(
    instrument: str,
    df_m5: Optional[pd.DataFrame] = None,
    df_h1: Optional[pd.DataFrame] = None,
    df_h4: Optional[pd.DataFrame] = None,
    *,
    dxy_ema_gap: float | None = None,
    dxy_gate_threshold: float = 0.005,
    ema_m5_fast: int = 9,
    ema_m5_slow: int = 21,
    ema_htf_fast: int = 20,
    ema_htf_slow: int = 50,
) -> DirectionScore:
    """Compute a continuous direction score.

    All indicator evaluations are wrapped in ``try/except``; missing or
    too-short frames are simply skipped and the weights renormalise across
    the remaining contributors. A zero-aggregate result falls back to
    ``"LONG"`` direction with ``confidence=0.0`` — callers should gate on
    confidence rather than the direction label in that case.
    """
    components: dict[str, float] = {}

    for key, value in (
        ("ema_m5", _ema_alignment(df_m5, ema_m5_fast, ema_m5_slow) if df_m5 is not None else None),
        ("ema_h1", _ema_alignment(df_h1, ema_htf_fast, ema_htf_slow) if df_h1 is not None else None),
        ("ema_h4", _ema_alignment(df_h4, ema_htf_fast, ema_htf_slow) if df_h4 is not None else None),
        ("rsi_m5", _rsi_centred(df_m5) if df_m5 is not None else None),
        ("rsi_h1", _rsi_centred(df_h1) if df_h1 is not None else None),
        ("macd_m5", _macd_hist_conf(df_m5) if df_m5 is not None else None),
        ("macd_h1", _macd_hist_conf(df_h1) if df_h1 is not None else None),
        ("dxy", _dxy_component(instrument, dxy_ema_gap, dxy_gate_threshold)),
    ):
        if value is not None:
            components[key] = value

    weighted_sum = 0.0
    contributing_weight = 0.0
    for key, value in components.items():
        weight = _WEIGHTS.get(key, 1.0)
        weighted_sum += value * weight
        contributing_weight += weight

    if contributing_weight <= 0:
        return DirectionScore(
            direction="LONG",
            confidence=0.0,
            aggregate=0.0,
            components={},
            contributing_weight=0.0,
        )

    aggregate = max(-1.0, min(1.0, weighted_sum / contributing_weight))
    confidence = abs(aggregate)
    direction = "LONG" if aggregate >= 0.0 else "SHORT"
    return DirectionScore(
        direction=direction,
        confidence=confidence,
        aggregate=aggregate,
        components=components,
        contributing_weight=contributing_weight,
    )


def should_fire(score: DirectionScore, min_confidence: float = 0.45) -> bool:
    """Gate predicate: reject low-confidence direction calls."""
    return score.confidence >= max(0.0, float(min_confidence))
