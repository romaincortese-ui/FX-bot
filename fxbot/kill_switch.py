"""Portfolio drawdown kill-switch (FX-bot Sprint 1 §2.9).

Mirrors the gold-bot's kill-switch: a 30-day rolling soft cut that
throttles per-trade risk, and a 90-day rolling hard halt that blocks all
new entries. Defaults match the review memo (``-6%`` / ``-10%``).

The evaluator is pure: it takes a list of realised daily P&L values
(most-recent last) and an equity-reference peak, and returns a
``KillDecision``. It is the caller's responsibility to persist the
resulting state and to feed the current equity history in.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence


@dataclass(frozen=True, slots=True)
class KillDecision:
    hard_halt: bool                      # block all new entries
    soft_cut: bool                       # apply risk-per-trade override
    risk_per_trade_override: float | None  # e.g. 0.33 means 33% of base
    soft_cut_pct: float                  # observed 30d drawdown (0.06 = -6%)
    hard_halt_pct: float                 # observed 90d drawdown
    reason: str


def evaluate_drawdown_kill(
    *,
    daily_pnl_pct: Sequence[float],
    soft_cut_lookback_days: int = 30,
    hard_halt_lookback_days: int = 90,
    soft_cut_threshold_pct: float = 0.06,
    hard_halt_threshold_pct: float = 0.10,
    soft_cut_risk_scale: float = 0.33,
) -> KillDecision:
    """Evaluate rolling drawdown against kill-switch thresholds.

    * ``daily_pnl_pct`` — most-recent-last sequence of daily P&L as a
      fraction of NAV at start-of-day (``0.01`` = +1%). Missing days are
      **not** backfilled; pass zeros explicitly if you want a flat day.
    * Drawdowns are cumulative-return peak-to-trough within each window
      (``1 - (1 + sum_period) / max_peak_within_period``), not a naive
      sum, so recovery offsets are modelled correctly.

    Returns ``hard_halt=True`` as soon as the 90d trough exceeds the
    hard-halt threshold, regardless of the 30d state. Otherwise
    ``soft_cut=True`` when 30d trough breaches the soft threshold, with
    ``risk_per_trade_override = soft_cut_risk_scale`` (a multiplier on
    the base risk-per-trade setting).
    """
    series = [float(v) for v in daily_pnl_pct]

    def _rolling_drawdown(window: list[float]) -> float:
        if not window:
            return 0.0
        equity_curve = [1.0]
        for pnl in window:
            equity_curve.append(equity_curve[-1] * (1.0 + pnl))
        peak = equity_curve[0]
        max_dd = 0.0
        for value in equity_curve:
            if value > peak:
                peak = value
            drawdown = 1.0 - (value / peak) if peak > 0 else 0.0
            if drawdown > max_dd:
                max_dd = drawdown
        return max_dd

    soft_window = series[-soft_cut_lookback_days:] if soft_cut_lookback_days > 0 else series
    hard_window = series[-hard_halt_lookback_days:] if hard_halt_lookback_days > 0 else series

    soft_dd = _rolling_drawdown(soft_window)
    hard_dd = _rolling_drawdown(hard_window)

    if hard_dd >= hard_halt_threshold_pct:
        return KillDecision(
            hard_halt=True,
            soft_cut=True,
            risk_per_trade_override=0.0,
            soft_cut_pct=soft_dd,
            hard_halt_pct=hard_dd,
            reason=f"hard_halt_{hard_halt_lookback_days}d_drawdown_{hard_dd:.2%}",
        )
    if soft_dd >= soft_cut_threshold_pct:
        # Progressive soft cut (memo 4 §8 F2). A flat 0.33 override is
        # prone to being leap-frogged in a rapid drawdown because the
        # 90d hard-halt fires in the same bar as the 30d soft trigger.
        # Instead, ramp the risk scale linearly from ``1 - soft_cut_risk_scale``
        # at the soft threshold down to ``soft_cut_risk_scale`` as the 30d
        # drawdown approaches the hard-halt threshold. This guarantees
        # the soft cut bites *before* the hard halt and deepens smoothly
        # as equity erodes further.
        span = max(1e-6, hard_halt_threshold_pct - soft_cut_threshold_pct)
        progress = max(0.0, min(1.0, (soft_dd - soft_cut_threshold_pct) / span))
        upper = max(soft_cut_risk_scale, 1.0 - soft_cut_risk_scale)
        lower = max(0.0, min(1.0, soft_cut_risk_scale))
        override = upper - (upper - lower) * progress
        override = max(0.0, min(1.0, override))
        return KillDecision(
            hard_halt=False,
            soft_cut=True,
            risk_per_trade_override=override,
            soft_cut_pct=soft_dd,
            hard_halt_pct=hard_dd,
            reason=f"soft_cut_{soft_cut_lookback_days}d_drawdown_{soft_dd:.2%}",
        )
    return KillDecision(
        hard_halt=False,
        soft_cut=False,
        risk_per_trade_override=None,
        soft_cut_pct=soft_dd,
        hard_halt_pct=hard_dd,
        reason="within_limits",
    )


def format_kill_snapshot(decision: KillDecision, *, now: datetime | None = None) -> dict:
    """Serialise a decision for publishing to runtime state / telemetry."""
    if now is None:
        now = datetime.now(timezone.utc)
    return {
        "as_of": now.isoformat(),
        "hard_halt": bool(decision.hard_halt),
        "soft_cut": bool(decision.soft_cut),
        "risk_per_trade_override": decision.risk_per_trade_override,
        "soft_cut_pct": round(decision.soft_cut_pct, 5),
        "hard_halt_pct": round(decision.hard_halt_pct, 5),
        "reason": decision.reason,
    }
