"""Portfolio-level regime classifier (FX-bot Sprint 3 §3.2).

Classifies the market into one of four macro regimes per the review
memo and gates strategies accordingly:

* ``RISK_ON``  — low VIX, rising equities, tight credit. Enable CARRY,
  TREND; disable REVERSAL.
* ``RISK_OFF`` — rising VIX, falling equity, yen/chf bid. Enable TREND
  (short risk-FX), SCALPER; disable CARRY.
* ``USD_TREND`` — DXY breaking out with multi-week rising EMA. Enable
  TREND, PULLBACK; disable SCALPER, REVERSAL (mean-reversion fails in
  a persistent USD trend).
* ``CHOP``     — flat DXY, low vol, range-bound. Enable SCALPER,
  REVERSAL; disable TREND, CARRY.

Classification uses three observables:

1. DXY 20d slope (percent change in the 20-day EMA).
2. VIX percentile vs its trailing 60-day distribution.
3. SPY 10d / 20d EMA ratio (proxy for equity momentum).

The function is pure: no I/O, no state. Callers assemble the inputs
from whatever data feed they use (the existing ``macro_logic.py``
already computes DXY and VIX history). Missing inputs collapse to
``CHOP`` (safe default: scalper-style tight risk).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Regime(str, Enum):
    RISK_ON = "RISK_ON"
    RISK_OFF = "RISK_OFF"
    USD_TREND = "USD_TREND"
    CHOP = "CHOP"


# Strategy → regime gate per memo §3.2 table.
STRATEGY_REGIME_MAP: dict[Regime, dict[str, bool]] = {
    Regime.RISK_ON:  {"CARRY": True,  "TREND": True,  "SCALPER": True,  "REVERSAL": False, "PULLBACK": True,  "POST_NEWS": True},
    Regime.RISK_OFF: {"CARRY": False, "TREND": True,  "SCALPER": True,  "REVERSAL": False, "PULLBACK": False, "POST_NEWS": True},
    Regime.USD_TREND:{"CARRY": True,  "TREND": True,  "SCALPER": False, "REVERSAL": False, "PULLBACK": True,  "POST_NEWS": True},
    Regime.CHOP:     {"CARRY": False, "TREND": False, "SCALPER": True,  "REVERSAL": True,  "PULLBACK": False, "POST_NEWS": True},
}


@dataclass(frozen=True, slots=True)
class RegimeAssessment:
    regime: Regime
    dxy_20d_slope_pct: float | None
    vix_percentile_60d: float | None  # 0-100
    spy_ema_ratio: float | None       # 10d EMA / 20d EMA
    reason: str


def _safe_ema(values: list[float], span: int) -> float | None:
    if len(values) < span:
        return None
    alpha = 2.0 / (span + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = alpha * v + (1.0 - alpha) * ema
    return ema


def compute_dxy_20d_slope_pct(dxy_closes: list[float]) -> float | None:
    """Percent change in the 20-day EMA over the last 20 observations."""
    if len(dxy_closes) < 25:
        return None
    ema_now = _safe_ema(dxy_closes, 20)
    ema_20_ago = _safe_ema(dxy_closes[:-20], 20)
    if ema_now is None or ema_20_ago is None or ema_20_ago == 0:
        return None
    return (ema_now - ema_20_ago) / ema_20_ago * 100.0


def compute_vix_percentile_60d(vix_history: list[float]) -> float | None:
    """Percentile of the latest VIX reading in the trailing 60d window."""
    if len(vix_history) < 30:
        return None
    window = vix_history[-60:]
    current = window[-1]
    below = sum(1 for v in window if v < current)
    return (below / len(window)) * 100.0


def compute_spy_ema_ratio(spy_closes: list[float]) -> float | None:
    if len(spy_closes) < 25:
        return None
    fast = _safe_ema(spy_closes, 10)
    slow = _safe_ema(spy_closes, 20)
    if fast is None or slow is None or slow == 0:
        return None
    return fast / slow


def classify_regime(
    *,
    dxy_closes: list[float] | None = None,
    vix_history: list[float] | None = None,
    spy_closes: list[float] | None = None,
    dxy_trend_pct_threshold: float = 1.5,
    vix_high_percentile: float = 75.0,
    vix_low_percentile: float = 25.0,
    spy_risk_on_ratio: float = 1.005,
    spy_risk_off_ratio: float = 0.995,
) -> RegimeAssessment:
    """Classify the current regime.

    Priority order (first match wins):

    1. Strong USD trend (|DXY 20d slope| >= threshold) → ``USD_TREND``.
    2. VIX > high percentile AND SPY fast/slow < ``spy_risk_off_ratio`` → ``RISK_OFF``.
    3. VIX < low percentile AND SPY fast/slow > ``spy_risk_on_ratio`` → ``RISK_ON``.
    4. Otherwise → ``CHOP``.
    """
    dxy_slope = compute_dxy_20d_slope_pct(dxy_closes or [])
    vix_pct = compute_vix_percentile_60d(vix_history or [])
    spy_ratio = compute_spy_ema_ratio(spy_closes or [])

    if dxy_slope is not None and abs(dxy_slope) >= dxy_trend_pct_threshold:
        return RegimeAssessment(
            regime=Regime.USD_TREND,
            dxy_20d_slope_pct=dxy_slope,
            vix_percentile_60d=vix_pct,
            spy_ema_ratio=spy_ratio,
            reason=f"dxy_trend_{dxy_slope:+.2f}%",
        )

    if (
        vix_pct is not None
        and vix_pct >= vix_high_percentile
        and spy_ratio is not None
        and spy_ratio < spy_risk_off_ratio
    ):
        return RegimeAssessment(
            regime=Regime.RISK_OFF,
            dxy_20d_slope_pct=dxy_slope,
            vix_percentile_60d=vix_pct,
            spy_ema_ratio=spy_ratio,
            reason=f"vix_pctile_{vix_pct:.0f}_spy_{spy_ratio:.4f}",
        )

    if (
        vix_pct is not None
        and vix_pct <= vix_low_percentile
        and spy_ratio is not None
        and spy_ratio > spy_risk_on_ratio
    ):
        return RegimeAssessment(
            regime=Regime.RISK_ON,
            dxy_20d_slope_pct=dxy_slope,
            vix_percentile_60d=vix_pct,
            spy_ema_ratio=spy_ratio,
            reason=f"vix_pctile_{vix_pct:.0f}_spy_{spy_ratio:.4f}",
        )

    return RegimeAssessment(
        regime=Regime.CHOP,
        dxy_20d_slope_pct=dxy_slope,
        vix_percentile_60d=vix_pct,
        spy_ema_ratio=spy_ratio,
        reason="fallback_chop",
    )


def is_strategy_enabled(strategy: str, regime: Regime) -> bool:
    """Return the memo's strategy/regime gate, defaulting to True if unmapped."""
    gates = STRATEGY_REGIME_MAP.get(regime, {})
    return gates.get(strategy.upper(), True)
