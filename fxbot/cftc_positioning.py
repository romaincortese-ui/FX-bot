"""CFTC Commitments of Traders positioning signal (FX-bot Q2 §3.5).

Mirrors the gold-bot CFTC module. Once managed-money net position in a
given currency future climbs above the 85th percentile of the last
2 years, *further* trend entries in the same direction are faded. When
it falls below the 15th percentile, short-side trend entries are faded.

The memo requires percentile logic over a 104-week (2-year) window.
This module is pure and takes a history of weekly net-position numbers
plus the latest reading; it returns a ``PositioningSignal`` the caller
can consume.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PositioningSignal:
    currency: str
    net_position: int | None
    percentile: float | None          # 0..100 over history
    extreme: str                      # "LONG" | "SHORT" | "NONE"
    score_multiplier: float           # [0, 1] — how hard to fade
    reason: str


def _percentile_rank(series: list[float], value: float) -> float:
    """Percent of samples strictly less than ``value``."""
    if not series:
        return 50.0
    below = sum(1 for s in series if s < value)
    return 100.0 * below / len(series)


def compute_positioning_signal(
    *,
    currency: str,
    net_position: int | None,
    history: list[int] | None,
    high_percentile: float = 85.0,
    low_percentile: float = 15.0,
    min_history: int = 52,
) -> PositioningSignal:
    """Classify the latest CoT net-position vs its 2-year history.

    * ``high_percentile`` triggers LONG-extreme (fade further longs).
    * ``low_percentile`` triggers SHORT-extreme (fade further shorts).
    * Below ``min_history`` weekly observations we emit NONE — not enough
      data to trust the percentile.
    """
    if net_position is None or history is None or len(history) < min_history:
        return PositioningSignal(
            currency=currency.upper(),
            net_position=None,
            percentile=None,
            extreme="NONE",
            score_multiplier=0.0,
            reason="insufficient_history",
        )
    hist = [float(x) for x in history]
    pct = _percentile_rank(hist, float(net_position))
    if pct >= float(high_percentile):
        # How far above the band — scale from 0 at band to 1 at 100th pct.
        denom = max(100.0 - float(high_percentile), 1e-6)
        strength = min(1.0, (pct - float(high_percentile)) / denom)
        return PositioningSignal(
            currency=currency.upper(),
            net_position=int(net_position),
            percentile=pct,
            extreme="LONG",
            score_multiplier=strength,
            reason=f"net_long_extreme_pct={pct:.1f}",
        )
    if pct <= float(low_percentile):
        denom = max(float(low_percentile), 1e-6)
        strength = min(1.0, (float(low_percentile) - pct) / denom)
        return PositioningSignal(
            currency=currency.upper(),
            net_position=int(net_position),
            percentile=pct,
            extreme="SHORT",
            score_multiplier=strength,
            reason=f"net_short_extreme_pct={pct:.1f}",
        )
    return PositioningSignal(
        currency=currency.upper(),
        net_position=int(net_position),
        percentile=pct,
        extreme="NONE",
        score_multiplier=0.0,
        reason=f"within_band_pct={pct:.1f}",
    )


def should_fade_entry(signal: PositioningSignal, entry_direction: str) -> bool:
    """True if the prospective entry is in the same direction as an
    extreme reading and should therefore be faded (skipped).
    """
    if signal.extreme == "NONE":
        return False
    side = entry_direction.upper()
    if side not in {"LONG", "SHORT"}:
        return False
    return signal.extreme == side


def pair_positioning_bias(
    instrument: str,
    base_signal: PositioningSignal | None,
    quote_signal: PositioningSignal | None,
) -> str:
    """Combine positioning on the two currencies of a pair into a
    directional bias: which side is more crowded? The crowded side is
    the side to fade.

    Returns "LONG", "SHORT", or "NONE".
    """
    if not instrument or "_" not in instrument:
        return "NONE"
    base_extreme = base_signal.extreme if base_signal else "NONE"
    quote_extreme = quote_signal.extreme if quote_signal else "NONE"
    # Base long-crowded → fade pair long.
    # Quote long-crowded → fade pair short.
    if base_extreme == "LONG" and quote_extreme != "LONG":
        return "LONG"
    if base_extreme == "SHORT" and quote_extreme != "SHORT":
        return "SHORT"
    if quote_extreme == "LONG" and base_extreme != "LONG":
        return "SHORT"
    if quote_extreme == "SHORT" and base_extreme != "SHORT":
        return "LONG"
    return "NONE"
