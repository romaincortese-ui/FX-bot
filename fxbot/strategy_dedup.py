"""Per-pair strategy deduplication (FX-bot Sprint 1 §2.3).

When multiple strategies (SCALPER / TREND / REVERSAL / PULLBACK / CARRY /
POST_NEWS) score on the same bar for the same instrument, the current
system can fire opposite-direction trades against its own capital. This
module resolves the conflict:

1. For each instrument, keep only the **highest-scoring** strategy.
2. If two opposing-direction candidates on the same instrument score
   within ``indeterminate_threshold`` points of each other, mute both
   (signal is indeterminate).
3. If two same-direction candidates differ by less than the threshold,
   keep the higher-scoring one — there is no conflict, just redundancy.

The filter operates on a generic list of candidate dicts. The candidate
must expose at least ``instrument``, ``strategy``, ``direction``, and
``score`` keys; everything else is passed through untouched.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping


def select_best_per_instrument(
    candidates: Iterable[Mapping],
    *,
    indeterminate_threshold: float = 5.0,
) -> list[dict]:
    """Return at most one candidate per instrument using winner-takes-all.

    Candidates ``a`` and ``b`` on the same instrument collide if
    ``a.direction != b.direction`` and ``abs(a.score - b.score) <=
    indeterminate_threshold``. When they collide, **both are dropped**
    and no trade is taken for that instrument on this bar. Otherwise the
    higher-scoring candidate wins.

    The input is not mutated. The return order matches the highest-score
    first within each instrument, then original order across instruments.
    """
    buckets: dict[str, list[dict]] = defaultdict(list)
    order: list[str] = []
    for raw in candidates:
        try:
            instrument = str(raw["instrument"])
        except (KeyError, TypeError):
            continue
        if instrument not in buckets:
            order.append(instrument)
        buckets[instrument].append(dict(raw))

    result: list[dict] = []
    for instrument in order:
        bucket = buckets[instrument]
        # Sort highest-score first, stable on insertion order for ties.
        bucket.sort(key=lambda c: float(c.get("score", 0.0) or 0.0), reverse=True)
        top = bucket[0]
        top_score = float(top.get("score", 0.0) or 0.0)
        top_dir = str(top.get("direction", "")).upper()
        # Scan the rest; mute if an opposite-direction competitor is close.
        muted = False
        for other in bucket[1:]:
            other_score = float(other.get("score", 0.0) or 0.0)
            other_dir = str(other.get("direction", "")).upper()
            if other_dir and other_dir != top_dir and abs(top_score - other_score) <= indeterminate_threshold:
                muted = True
                break
        if not muted:
            result.append(top)
    return result


def apply_per_instrument_dedup(
    candidates: Iterable[Mapping],
    *,
    enabled: bool,
    indeterminate_threshold: float = 5.0,
) -> list[dict]:
    """Convenience wrapper honouring a feature flag.

    When ``enabled`` is False this is an identity function (returns a new
    list containing the same candidates) so callers can flip the flag
    without any behaviour change.
    """
    if not enabled:
        return [dict(raw) for raw in candidates]
    return select_best_per_instrument(
        candidates, indeterminate_threshold=indeterminate_threshold
    )
