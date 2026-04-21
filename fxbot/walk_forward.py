"""Walk-forward calibration stability filter (FX-bot Sprint 3 §3.4).

The existing daily rolling recalibration on 180-day windows is close to
curve-fitting — every day the bot refits on yesterday's noise. Memo §3.4
prescribes a proper walk-forward:

1. Optimise on an in-sample window (e.g. D-365 .. D-90).
2. Validate on the trailing out-of-sample window (D-90 .. D).
3. **Ship** the candidate parameter set only if:

   * Validation profit factor >= ``min_validation_pf`` (default 1.15).
   * OOS return / trade is within ``max_degradation_ratio`` of IS
     (default: OOS must be >= 0.5x IS — i.e. not worse than 50% down).
   * OOS trade count >= ``min_oos_trade_count`` (default 30); otherwise
     the sample is too thin to trust.

4. Do not re-ship faster than ``min_recalibration_interval_days``
   (default 7). Daily recalibration on 180d windows is tuning to noise
   per the memo.

This module exposes pure evaluators. The calibration driver script is
left to compute the actual IS/OOS metric dicts; this module decides
whether to accept them.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Mapping


@dataclass(frozen=True, slots=True)
class CalibrationMetrics:
    profit_factor: float
    return_per_trade_pct: float
    trade_count: int


@dataclass(frozen=True, slots=True)
class WalkForwardDecision:
    accept: bool
    reason: str
    is_metrics: CalibrationMetrics
    oos_metrics: CalibrationMetrics
    degradation_ratio: float   # oos.return_per_trade / is.return_per_trade


def _coerce_metrics(d: Mapping) -> CalibrationMetrics:
    return CalibrationMetrics(
        profit_factor=float(d.get("profit_factor", 0.0) or 0.0),
        return_per_trade_pct=float(d.get("return_per_trade_pct", 0.0) or 0.0),
        trade_count=int(d.get("trade_count", 0) or 0),
    )


def evaluate_walk_forward(
    *,
    in_sample: Mapping,
    out_of_sample: Mapping,
    min_validation_pf: float = 1.15,
    max_degradation_ratio: float = 0.5,   # OOS must be >= 0.5x IS
    min_oos_trade_count: int = 30,
) -> WalkForwardDecision:
    """Decide whether a new parameter set passes the stability filter."""
    is_m = _coerce_metrics(in_sample)
    oos_m = _coerce_metrics(out_of_sample)

    # Ratio is well-defined only when IS return is meaningfully positive.
    if is_m.return_per_trade_pct > 0:
        degradation = oos_m.return_per_trade_pct / is_m.return_per_trade_pct
    elif is_m.return_per_trade_pct == 0:
        degradation = 0.0 if oos_m.return_per_trade_pct <= 0 else 1.0
    else:
        # IS was negative; OOS being "only half as bad" isn't a virtue.
        degradation = 0.0

    if oos_m.trade_count < min_oos_trade_count:
        return WalkForwardDecision(
            accept=False,
            reason=f"oos_trade_count_{oos_m.trade_count}<{min_oos_trade_count}",
            is_metrics=is_m,
            oos_metrics=oos_m,
            degradation_ratio=degradation,
        )
    if oos_m.profit_factor < min_validation_pf:
        return WalkForwardDecision(
            accept=False,
            reason=f"oos_pf_{oos_m.profit_factor:.3f}<{min_validation_pf}",
            is_metrics=is_m,
            oos_metrics=oos_m,
            degradation_ratio=degradation,
        )
    if degradation < max_degradation_ratio:
        return WalkForwardDecision(
            accept=False,
            reason=f"degradation_ratio_{degradation:.3f}<{max_degradation_ratio}",
            is_metrics=is_m,
            oos_metrics=oos_m,
            degradation_ratio=degradation,
        )
    return WalkForwardDecision(
        accept=True,
        reason="passed",
        is_metrics=is_m,
        oos_metrics=oos_m,
        degradation_ratio=degradation,
    )


def should_recalibrate_now(
    *,
    last_shipped_at: datetime | None,
    now: datetime | None = None,
    min_interval_days: int = 7,
) -> bool:
    """Return True only if ``min_interval_days`` have elapsed since the
    last shipped calibration. ``last_shipped_at=None`` means never
    shipped → always True.
    """
    if last_shipped_at is None:
        return True
    if now is None:
        now = datetime.now(timezone.utc)
    if last_shipped_at.tzinfo is None:
        last_shipped_at = last_shipped_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - last_shipped_at).total_seconds() >= min_interval_days * 86400.0
