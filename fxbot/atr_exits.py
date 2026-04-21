"""ATR-scaled TP/SL exits (FX-bot Sprint 2 §2.6).

Replaces the hard-coded pip caps in the current scalper (8-30 pip TP,
5-20 pip SL) with pure ATR-scaled exits, subject only to a hard spread
floor so a tight stop can never sit inside the bid-ask.

Formula (review memo §2.6):

    sl_pips = max(atr_m15 × sl_atr_mult, spread_pips × sl_spread_floor_mult)
    tp_pips = max(atr_m15 × tp_atr_mult, sl_pips × min_rr)

* ``sl_atr_mult`` default 1.2 — tight enough for scalp, wide enough to
  absorb normal noise.
* ``sl_spread_floor_mult`` default 3.0 — the stop must sit at least 3x
  the current spread away; this eliminates "stopped-by-spread" tickets.
* ``tp_atr_mult`` default 2.4 — 2:1 expected reward on ATR.
* ``min_rr`` default 1.8 — if ATR-scaled TP would violate this R:R, lift
  TP to hit the minimum.

No maximum cap. The memo is explicit: capping profits at 25 pips on a
trending day destroys the edge.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ATRExitPlan:
    sl_pips: float
    tp_pips: float
    expected_rr: float     # tp / sl
    atr_pips: float
    spread_pips: float
    sl_driver: str         # "atr" | "spread_floor"
    tp_driver: str         # "atr" | "min_rr"


def compute_atr_exits(
    *,
    atr_pips: float,
    spread_pips: float,
    sl_atr_mult: float = 1.2,
    sl_spread_floor_mult: float = 3.0,
    tp_atr_mult: float = 2.4,
    min_rr: float = 1.8,
) -> ATRExitPlan:
    """Return an ATR-scaled exit plan for the given bar.

    All inputs are in **pips** (one pip = 0.0001 on majors, 0.01 on JPY
    pairs). Callers are responsible for converting price distance to
    pips before passing values in.

    Guards: all inputs are clamped to non-negative. A zero ATR falls
    through cleanly — TP will equal ``sl_pips × min_rr`` and SL will
    equal the spread floor, so the plan is still well-defined even on
    degenerate inputs.
    """
    atr_pips = max(0.0, float(atr_pips))
    spread_pips = max(0.0, float(spread_pips))
    sl_atr_mult = max(0.0, float(sl_atr_mult))
    sl_spread_floor_mult = max(0.0, float(sl_spread_floor_mult))
    tp_atr_mult = max(0.0, float(tp_atr_mult))
    min_rr = max(1.0, float(min_rr))  # R:R < 1 is nonsensical for entries.

    sl_from_atr = atr_pips * sl_atr_mult
    sl_from_spread = spread_pips * sl_spread_floor_mult
    if sl_from_spread > sl_from_atr:
        sl_pips = sl_from_spread
        sl_driver = "spread_floor"
    else:
        sl_pips = sl_from_atr
        sl_driver = "atr"

    tp_from_atr = atr_pips * tp_atr_mult
    tp_from_rr = sl_pips * min_rr
    if tp_from_rr > tp_from_atr:
        tp_pips = tp_from_rr
        tp_driver = "min_rr"
    else:
        tp_pips = tp_from_atr
        tp_driver = "atr"

    expected_rr = (tp_pips / sl_pips) if sl_pips > 0 else 0.0
    return ATRExitPlan(
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        expected_rr=expected_rr,
        atr_pips=atr_pips,
        spread_pips=spread_pips,
        sl_driver=sl_driver,
        tp_driver=tp_driver,
    )


def pip_size_for(instrument: str) -> float:
    """Return the size of one pip in price units for ``instrument``.

    JPY pairs use 0.01; everything else uses 0.0001.
    """
    if "JPY" in instrument.upper():
        return 0.01
    return 0.0001
