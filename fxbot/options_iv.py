"""Options-implied FX signals (FX-bot Q2 §3.7).

Two signals are extracted from CME-settled FX options:

1. **Risk reversal** (25Δ call IV − 25Δ put IV). Strongly negative
   means puts are bid — the options market is buying downside
   protection, so bias the pair SHORT. Strongly positive → LONG.

2. **1-week ATM IV percentile**. When IV is in its 90th percentile,
   mean-reversion outperforms trend. Below the 20th percentile,
   breakouts are lower-probability.

This module does NOT fetch option quotes. It consumes whatever the
caller has: a risk-reversal value in vol points, and a history of
1w ATM IV observations.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import tanh


@dataclass(frozen=True, slots=True)
class RiskReversalSignal:
    instrument: str
    value_vols: float
    bias_direction: str             # "LONG" | "SHORT" | "NONE"
    score_multiplier: float         # [0, 1.5]
    reason: str


@dataclass(frozen=True, slots=True)
class ImpliedVolRegime:
    instrument: str
    atm_iv: float | None
    percentile: float | None
    regime: str                     # "MEAN_REVERT" | "TREND" | "BREAKOUT_SOFT" | "NEUTRAL"
    reason: str


def classify_risk_reversal(
    *,
    instrument: str,
    rr_25d_vols: float,
    fade_threshold_vols: float = 0.75,
) -> RiskReversalSignal:
    """Classify a 25-delta risk reversal quoted in vol points.

    * Positive RR → calls bid → LONG bias on pair.
    * Negative RR → puts bid → SHORT bias on pair.
    * |RR| < ``fade_threshold_vols`` is NEUTRAL.
    """
    rr = float(rr_25d_vols)
    if abs(rr) < float(fade_threshold_vols):
        return RiskReversalSignal(
            instrument=instrument.upper(),
            value_vols=rr,
            bias_direction="NONE",
            score_multiplier=0.0,
            reason=f"rr_within_threshold_{rr:+.2f}",
        )
    direction = "LONG" if rr > 0 else "SHORT"
    # tanh saturates: RR 1.0 vol → 0.60, 2.0 vol → 1.07, 4.0 vol → 1.44.
    multiplier = float(1.5 * tanh(abs(rr) / 2.0))
    return RiskReversalSignal(
        instrument=instrument.upper(),
        value_vols=rr,
        bias_direction=direction,
        score_multiplier=multiplier,
        reason=f"rr_{rr:+.2f}_vols",
    )


def _percentile(series: list[float], value: float) -> float:
    if not series:
        return 50.0
    below = sum(1 for s in series if s < value)
    return 100.0 * below / len(series)


def classify_iv_regime(
    *,
    instrument: str,
    atm_iv_history: list[float],
    current_atm_iv: float | None,
    high_percentile: float = 90.0,
    low_percentile: float = 20.0,
    min_history: int = 60,
) -> ImpliedVolRegime:
    """Classify the current ATM IV into a regime bucket.

    * > ``high_percentile`` → MEAN_REVERT (favour fades).
    * < ``low_percentile`` → BREAKOUT_SOFT (breakouts are low-prob).
    * Between → TREND (neutral-to-trend).
    """
    if current_atm_iv is None or len(atm_iv_history) < int(min_history):
        return ImpliedVolRegime(
            instrument=instrument.upper(),
            atm_iv=current_atm_iv,
            percentile=None,
            regime="NEUTRAL",
            reason="insufficient_history",
        )
    hist = [float(x) for x in atm_iv_history]
    pct = _percentile(hist, float(current_atm_iv))
    if pct >= float(high_percentile):
        regime = "MEAN_REVERT"
    elif pct <= float(low_percentile):
        regime = "BREAKOUT_SOFT"
    else:
        regime = "TREND"
    return ImpliedVolRegime(
        instrument=instrument.upper(),
        atm_iv=float(current_atm_iv),
        percentile=pct,
        regime=regime,
        reason=f"iv_pct_{pct:.1f}",
    )


def strategy_weight_for_iv_regime(strategy: str, regime: ImpliedVolRegime) -> float:
    """Return a [0, 1.2] multiplier for a given strategy under the IV
    regime. Mean-reverting names (SCALPER, REVERSAL) get a boost in
    MEAN_REVERT; trend names (TREND, PULLBACK) get trimmed in
    BREAKOUT_SOFT.
    """
    s = strategy.upper()
    r = regime.regime
    if r == "MEAN_REVERT":
        return 1.2 if s in {"SCALPER", "REVERSAL"} else 0.7
    if r == "BREAKOUT_SOFT":
        return 0.6 if s in {"TREND", "PULLBACK"} else 1.0
    if r == "TREND":
        return 1.1 if s in {"TREND", "PULLBACK"} else 1.0
    return 1.0
