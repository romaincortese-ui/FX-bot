"""Execution hardening (FX-bot Sprint 3 §3.8).

Three helpers called out in the memo:

1. ``plan_limit_entry`` — place entry as a limit at mid-spread (not a
   market order). Gives OANDA a short window to fill; if unfilled,
   cancel and re-quote. Much better fills on EUR/USD at London-NY
   overlap and cuts round-trip cost vs market-on-entry.
2. ``plan_staged_exit`` — split TP into three legs (40% / 30% / 30%) at
   2x, 3.5x, and trailing targets for TREND and CARRY positions.
3. ``should_flatten_for_weekend`` — Friday 21:00 UTC cutoff for all
   non-CARRY positions, avoiding Sunday-open gap risk.

All helpers are pure and parameterised; no live-broker side effects.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class LimitEntryPlan:
    limit_price: float
    wait_seconds: int
    side: str              # "LONG" or "SHORT"
    reason: str


def plan_limit_entry(
    *,
    direction: str,
    bid: float,
    ask: float,
    mid_offset_frac: float = 0.5,
    wait_seconds: int = 2,
) -> LimitEntryPlan:
    """Produce a limit-entry plan at mid-spread (or a fraction toward
    the side the order is crossing).

    * ``mid_offset_frac=0.5`` sits exactly at mid.
    * ``mid_offset_frac=0.0`` sits at passive side (best bid for LONG,
      best ask for SHORT).
    * ``mid_offset_frac=1.0`` crosses the spread (market-equivalent).

    Default 0.5 matches the memo recommendation and captures ~half the
    spread on fills.
    """
    if ask <= 0 or bid <= 0 or ask < bid:
        raise ValueError(f"invalid quote: bid={bid}, ask={ask}")
    side = direction.upper()
    frac = max(0.0, min(1.0, float(mid_offset_frac)))
    mid = (bid + ask) / 2.0
    # For LONG we place between bid (passive) and ask (aggressive).
    if side == "LONG":
        limit_price = bid + (ask - bid) * frac
    elif side == "SHORT":
        limit_price = ask - (ask - bid) * frac
    else:
        raise ValueError(f"unknown direction: {direction}")
    return LimitEntryPlan(
        limit_price=limit_price,
        wait_seconds=max(0, int(wait_seconds)),
        side=side,
        reason=f"limit_at_{frac:.2f}_of_spread",
    )


@dataclass(frozen=True, slots=True)
class StagedExitLeg:
    fraction: float
    target_atr_mult: float | None       # None means trailing stop
    kind: str                           # "tp" | "trailing"


@dataclass(frozen=True, slots=True)
class StagedExitPlan:
    legs: tuple[StagedExitLeg, ...]
    total_fraction: float


def plan_staged_exit(
    *,
    tp1_fraction: float = 0.40,
    tp2_fraction: float = 0.30,
    trailing_fraction: float = 0.30,
    tp1_atr_mult: float = 2.0,
    tp2_atr_mult: float = 3.5,
) -> StagedExitPlan:
    """Three-leg staged exit plan for TREND / CARRY positions.

    Defaults match the memo: 40% at 2x ATR, 30% at 3.5x ATR, 30% trailing.
    """
    total = tp1_fraction + tp2_fraction + trailing_fraction
    if total <= 0 or total > 1.0 + 1e-9:
        raise ValueError(f"fractions must sum to (0, 1]; got {total}")
    legs = (
        StagedExitLeg(fraction=tp1_fraction, target_atr_mult=tp1_atr_mult, kind="tp"),
        StagedExitLeg(fraction=tp2_fraction, target_atr_mult=tp2_atr_mult, kind="tp"),
        StagedExitLeg(fraction=trailing_fraction, target_atr_mult=None, kind="trailing"),
    )
    return StagedExitPlan(legs=legs, total_fraction=total)


def should_flatten_for_weekend(
    *,
    now_utc: datetime,
    strategy: str,
    flatten_hour_utc: int = 21,
    flatten_dow: int = 4,   # Friday (Mon=0)
    carry_exempt: bool = True,
) -> bool:
    """Return True if non-CARRY positions should be flattened before the
    Friday 21:00 UTC cutoff.
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)
    if carry_exempt and strategy.upper() == "CARRY":
        return False
    if now_utc.weekday() != flatten_dow:
        return False
    return now_utc.hour >= flatten_hour_utc


def should_use_limit_stop(instrument: str) -> bool:
    """Cross-pair stops should be limits, not market orders.

    GBP/JPY during Tokyo can slip 8+ pips on a 20-pip market stop.
    This helper flags cross-pairs (no USD leg) for limit-stop treatment.
    """
    if not instrument or "_" not in instrument:
        return False
    parts = instrument.upper().split("_")
    if len(parts) != 2:
        return False
    return "USD" not in parts
