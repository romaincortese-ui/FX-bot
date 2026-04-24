"""Score-percentile position sizing (Tier 2 §12 of consultant assessment).

Direction / strategy scoring today returns an integer "vote count" that
is compared against a fixed threshold — a pair scoring 41 and a pair
scoring 78 are sized identically. Institutional desks scale size by the
percentile of the *current* score against the trailing ``lookback``
distribution for that strategy.

This module is pure. Callers maintain a per-strategy score history
(``deque`` or list) and ask for a size multiplier.

Design:

* ``size_multiplier = base × (percentile / centre)`` clipped to
  ``[floor, cap]``.
* Defaults: ``centre=0.5``, ``floor=0.5``, ``cap=2.0`` — so a score at
  the median sizes 1.0×, top-quintile sizes ~1.8×, bottom-quintile
  sizes 0.5×.
* If fewer than ``min_samples`` observations have been seen, fall back
  to ``1.0`` (no-op) — we don't tune sizing on 5 data points.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

DEFAULT_LOOKBACK = 60
DEFAULT_MIN_SAMPLES = 20
DEFAULT_FLOOR = 0.5
DEFAULT_CAP = 2.0
DEFAULT_CENTRE = 0.5


@dataclass(frozen=True, slots=True)
class SizingDecision:
    multiplier: float
    percentile: float | None
    samples: int
    reason: str


def score_percentile(score: float, history: Sequence[float]) -> float:
    """Return the empirical percentile (0..1) of ``score`` within ``history``.

    Uses the "less-than-or-equal" rank convention so the lowest score
    returns 1/n and the highest returns 1.0.
    """
    if not history:
        return 0.5
    n = len(history)
    le = sum(1 for v in history if v <= score)
    return le / n


def size_by_percentile(
    *,
    score: float,
    history: Iterable[float],
    min_samples: int = DEFAULT_MIN_SAMPLES,
    floor: float = DEFAULT_FLOOR,
    cap: float = DEFAULT_CAP,
    centre: float = DEFAULT_CENTRE,
) -> SizingDecision:
    """Return a position-size multiplier keyed on score percentile."""
    hist = list(history)
    n = len(hist)
    if n < max(1, int(min_samples)):
        return SizingDecision(
            multiplier=1.0,
            percentile=None,
            samples=n,
            reason=f"insufficient_samples_{n}<{min_samples}",
        )
    pct = score_percentile(score, hist)
    c = max(1e-6, float(centre))
    mult = pct / c
    mult = max(float(floor), min(float(cap), mult))
    return SizingDecision(
        multiplier=mult,
        percentile=pct,
        samples=n,
        reason=f"percentile_{pct:.2f}_mult_{mult:.2f}",
    )
