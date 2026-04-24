"""Per-(pair × session) spread-cap auto-tune (Tier 2v2 E1).

Third-memo §8 / E1: "On live, the cap should be a percentile of realised
spreads for that pair × session, not a fixed number. Ship a SlippageLogger-
driven threshold auto-tune as a Tier 5 follow-on."

The bot currently caps spreads with fixed per-strategy constants
(``SCALPER_MAX_SPREAD_PIPS=1.8`` etc.) that were tuned for live fxTrade
conditions. On fxPractice those caps reject everything; on live they can
fall behind structural regime changes (e.g. widening London-fix spreads
around month-end). This module buckets realised spreads by
``(instrument, session)`` and returns a percentile-based cap recommendation
that the caller can blend with the static strategy cap.

Design notes
------------
* Pure library: no clock, no I/O, no logging. Caller supplies ``now_utc``
  if using the sampler's ``record()`` helper.
* Bounded memory: each bucket keeps the most recent ``max_samples`` entries
  (default 500). Oldest entries are evicted on overflow.
* Recommendation is sample-size gated: fewer than ``min_samples`` entries
  (default 30) → return ``None`` so callers fall back to the static cap.
* Percentile default is P75 — aggressive enough to reject the tail but
  loose enough to keep the median cleanly inside the cap.
* Recommendations are clipped to ``[floor, ceiling]`` so a pathological
  sample (e.g. a single NFP-window outlier) cannot blow the cap out to a
  number that nullifies the gate entirely.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Iterable


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile on a pre-sorted list.

    pct in [0.0, 1.0]. Matches numpy's default ``linear`` method so the
    result is identical if the caller cross-checks with numpy.
    """
    if not sorted_values:
        return 0.0
    if pct <= 0.0:
        return sorted_values[0]
    if pct >= 1.0:
        return sorted_values[-1]
    n = len(sorted_values)
    idx = pct * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def recommend_spread_cap(
    samples: Iterable[float],
    *,
    percentile: float = 0.75,
    min_samples: int = 30,
    floor_pips: float = 0.5,
    ceiling_pips: float = 10.0,
) -> float | None:
    """Return a recommended spread cap in pips, or ``None`` if unsafe.

    * ``None`` means: not enough data → caller must use the static cap.
    * Otherwise the returned value is the ``percentile``-th sample clipped
      to ``[floor_pips, ceiling_pips]``.
    """
    cleaned = sorted(float(s) for s in samples if s is not None and s >= 0.0)
    if len(cleaned) < max(1, int(min_samples)):
        return None
    raw = _percentile(cleaned, percentile)
    return max(floor_pips, min(ceiling_pips, raw))


def session_for_hour(hour_utc: int) -> str:
    """Coarse session label matching ``fxbot.spread_model.Session``.

    Kept local to avoid an import cycle with ``spread_model`` and to let the
    tuner be used standalone from a backtest harness.
    """
    h = int(hour_utc) % 24
    if h >= 23 or h < 7:
        return "tokyo"
    if h < 12:
        return "london"
    if h < 16:
        return "london_ny"
    if h < 21:
        return "ny"
    return "late_ny"


@dataclass(frozen=True, slots=True)
class SpreadSample:
    instrument: str
    session: str
    spread_pips: float
    ts_utc: str


class SpreadSampler:
    """In-memory (instrument, session) → deque[spread_pips] accumulator.

    Thread-safe for the common append + query pattern because ``deque``
    with a ``maxlen`` uses atomic slot writes at the C level. Callers that
    need stricter guarantees can wrap this in an external lock.
    """

    def __init__(self, *, max_samples: int = 500):
        self._max_samples = max(10, int(max_samples))
        self._buckets: dict[tuple[str, str], Deque[float]] = {}

    # ---------- writes ----------
    def record(
        self,
        *,
        instrument: str,
        spread_pips: float,
        now_utc: datetime | None = None,
        session: str | None = None,
    ) -> SpreadSample | None:
        """Record one realised spread sample. Returns ``None`` if rejected."""
        if spread_pips is None or spread_pips < 0.0:
            return None
        instrument = (instrument or "").upper().strip()
        if not instrument:
            return None
        if session is None:
            now_utc = now_utc or datetime.now(timezone.utc)
            session = session_for_hour(now_utc.hour)
        key = (instrument, session)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = deque(maxlen=self._max_samples)
            self._buckets[key] = bucket
        bucket.append(float(spread_pips))
        ts = (now_utc or datetime.now(timezone.utc)).isoformat()
        return SpreadSample(instrument=instrument, session=session, spread_pips=float(spread_pips), ts_utc=ts)

    # ---------- reads ----------
    def samples(self, instrument: str, session: str) -> list[float]:
        bucket = self._buckets.get(((instrument or "").upper(), session))
        return list(bucket) if bucket else []

    def recommend(
        self,
        *,
        instrument: str,
        session: str,
        percentile: float = 0.75,
        min_samples: int = 30,
        floor_pips: float = 0.5,
        ceiling_pips: float = 10.0,
    ) -> float | None:
        return recommend_spread_cap(
            self.samples(instrument, session),
            percentile=percentile,
            min_samples=min_samples,
            floor_pips=floor_pips,
            ceiling_pips=ceiling_pips,
        )

    def blended_cap(
        self,
        *,
        instrument: str,
        session: str,
        static_cap_pips: float,
        percentile: float = 0.75,
        min_samples: int = 30,
        floor_pips: float = 0.5,
        ceiling_pips: float = 10.0,
    ) -> float:
        """Return ``max(static_cap, auto_cap)`` — never tighter than static.

        Rationale: the static cap is the operator's explicit guardrail; the
        auto-tune is only allowed to *relax* it when realised conditions are
        chronically wider (typical of month-end, weekend, or fxPractice).
        If there is not enough data, the static cap is returned unchanged.
        """
        rec = self.recommend(
            instrument=instrument,
            session=session,
            percentile=percentile,
            min_samples=min_samples,
            floor_pips=floor_pips,
            ceiling_pips=ceiling_pips,
        )
        if rec is None:
            return float(static_cap_pips)
        return max(float(static_cap_pips), float(rec))


_default_sampler = SpreadSampler()


def get_default_sampler() -> SpreadSampler:
    return _default_sampler
