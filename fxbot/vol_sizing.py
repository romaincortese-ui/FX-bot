"""Volatility-target sizing (FX-bot Sprint 1 §2.4).

The current bot uses a fixed 1.5% risk-per-trade. This module computes a
realised-vol-aware adjustment so every pair contributes approximately the
same expected P&L variance to the book.

Formulation (review memo §2.4):

    risk_pct = base_risk × (target_pair_vol / realised_pair_vol)

with explicit floor/cap bounds so a quiet-vol pair cannot blow the
per-trade legacy cap and a high-vol pair cannot cut sizing to zero.

``realised_pair_vol`` is the annualised standard deviation of daily
returns over the last ``lookback_days`` days, computed from a daily close
series. A 20-day window is the convention on FX desks — shorter windows
overreact to single-day spikes.

The output is always ``>= min_risk_pct`` and ``<= max_risk_pct``. Callers
should use ``risk_pct`` as a multiplier input to their existing sizing
logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Sequence

DAILY_SECONDS_PER_YEAR = 252.0  # standard trading-day annualisation


@dataclass(frozen=True, slots=True)
class VolSizingDecision:
    base_risk_pct: float
    adjusted_risk_pct: float
    realised_annualised_vol: float | None
    target_annualised_vol: float
    multiplier: float               # adjusted / base (clamped)
    reason: str


def realised_daily_vol_annualised(
    daily_closes: Sequence[float],
    *,
    lookback_days: int = 20,
) -> float | None:
    """Annualised std dev of daily log returns over the trailing window.

    Returns ``None`` when there are fewer than ``lookback_days + 1``
    observations or the close series contains non-positive values.
    """
    closes = [float(c) for c in daily_closes if c is not None]
    n = len(closes)
    if n < lookback_days + 1:
        return None
    window = closes[-(lookback_days + 1):]
    if any(c <= 0 for c in window):
        return None
    returns: list[float] = []
    for prev, curr in zip(window[:-1], window[1:]):
        # Log returns: stable under rescaling, symmetric.
        from math import log
        returns.append(log(curr / prev))
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    if variance <= 0:
        return None
    return sqrt(variance) * sqrt(DAILY_SECONDS_PER_YEAR)


def compute_vol_adjusted_risk_pct(
    *,
    base_risk_pct: float,
    daily_closes: Sequence[float],
    target_annualised_vol: float = 0.08,
    lookback_days: int = 20,
    min_risk_pct: float | None = None,
    max_risk_pct: float | None = None,
) -> VolSizingDecision:
    """Compute a vol-scaled ``risk_pct`` for the next trade.

    * ``base_risk_pct`` — the legacy fixed risk (e.g. 0.015 for 1.5%).
    * ``target_annualised_vol`` — the per-pair annualised vol to target,
      default 8% (conservative for majors; see memo).
    * ``min_risk_pct`` / ``max_risk_pct`` — safety bounds. Default min is
      ``0.25 × base_risk_pct`` and max is ``2.0 × base_risk_pct``.

    When the realised vol cannot be computed (too little history or bad
    data), the function returns ``adjusted_risk_pct == base_risk_pct``
    with ``reason="insufficient_history"`` so the caller can log and
    keep the legacy behaviour.
    """
    floor = float(min_risk_pct) if min_risk_pct is not None else 0.25 * base_risk_pct
    cap = float(max_risk_pct) if max_risk_pct is not None else 2.0 * base_risk_pct
    if cap < floor:
        cap = floor

    realised = realised_daily_vol_annualised(
        daily_closes, lookback_days=lookback_days
    )
    if realised is None or realised <= 0:
        return VolSizingDecision(
            base_risk_pct=base_risk_pct,
            adjusted_risk_pct=max(floor, min(cap, base_risk_pct)),
            realised_annualised_vol=realised,
            target_annualised_vol=target_annualised_vol,
            multiplier=1.0,
            reason="insufficient_history",
        )

    raw_mult = target_annualised_vol / realised
    raw_adjusted = base_risk_pct * raw_mult
    adjusted = max(floor, min(cap, raw_adjusted))
    # Effective multiplier after clamping (useful for telemetry).
    effective_mult = adjusted / base_risk_pct if base_risk_pct > 0 else 1.0

    if raw_adjusted > cap:
        reason = "capped_to_max"
    elif raw_adjusted < floor:
        reason = "floored_to_min"
    else:
        reason = "vol_target_applied"

    return VolSizingDecision(
        base_risk_pct=base_risk_pct,
        adjusted_risk_pct=adjusted,
        realised_annualised_vol=realised,
        target_annualised_vol=target_annualised_vol,
        multiplier=effective_mult,
        reason=reason,
    )
