"""Session-split strategies (FX-bot Tier 3 §4.2).

FX daily returns decompose into:

* Tokyo (2300-0700 UTC) — low vol, range-bound, mean-reversion
* London open (0700-0900 UTC) — opening-range breakout edge
* Europe-NY overlap (1200-1600 UTC) — highest liquidity, trend-follow
* Late NY (2000-2200 UTC) — thin, range-bound

This module exposes:

* ``classify_session`` — which session a UTC timestamp falls in.
* ``session_strategy_bias`` — preferred strategy style per session.
* ``LondonBreakoutPlan`` — the 0700-0800 UTC opening range and a break
  entry at 0800 UTC (the 20-year documented edge).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone


@dataclass(frozen=True, slots=True)
class SessionWindow:
    name: str
    start_hour_utc: int
    end_hour_utc: int        # exclusive


_SESSIONS: tuple[SessionWindow, ...] = (
    SessionWindow("TOKYO", 23, 7),            # wraps midnight
    SessionWindow("LONDON_OPEN", 7, 9),
    SessionWindow("LONDON", 9, 12),
    SessionWindow("EU_NY_OVERLAP", 12, 16),
    SessionWindow("NY", 16, 20),
    SessionWindow("LATE_NY", 20, 23),
)


def classify_session(now_utc: datetime) -> str:
    """Return the session name for a UTC timestamp."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    h = now_utc.astimezone(timezone.utc).hour
    for w in _SESSIONS:
        if w.start_hour_utc <= w.end_hour_utc:
            if w.start_hour_utc <= h < w.end_hour_utc:
                return w.name
        else:  # wraps midnight (Tokyo)
            if h >= w.start_hour_utc or h < w.end_hour_utc:
                return w.name
    return "UNKNOWN"


def session_strategy_bias(session: str) -> dict[str, float]:
    """Return per-strategy score multipliers for the given session."""
    s = session.upper()
    if s == "TOKYO":
        return {"SCALPER": 1.2, "REVERSAL": 1.1, "TREND": 0.6, "PULLBACK": 0.6,
                "CARRY": 1.0, "POST_NEWS": 0.8}
    if s == "LONDON_OPEN":
        return {"SCALPER": 0.8, "REVERSAL": 0.7, "TREND": 1.2, "PULLBACK": 1.1,
                "CARRY": 1.0, "POST_NEWS": 1.0, "LONDON_BREAKOUT": 1.3}
    if s == "LONDON":
        return {"SCALPER": 1.0, "REVERSAL": 0.9, "TREND": 1.1, "PULLBACK": 1.1,
                "CARRY": 1.0, "POST_NEWS": 1.0}
    if s == "EU_NY_OVERLAP":
        return {"SCALPER": 0.9, "REVERSAL": 0.8, "TREND": 1.3, "PULLBACK": 1.2,
                "CARRY": 1.0, "POST_NEWS": 1.1}
    if s == "NY":
        return {"SCALPER": 1.0, "REVERSAL": 1.0, "TREND": 1.1, "PULLBACK": 1.0,
                "CARRY": 1.0, "POST_NEWS": 1.1}
    if s == "LATE_NY":
        return {"SCALPER": 1.1, "REVERSAL": 1.0, "TREND": 0.6, "PULLBACK": 0.7,
                "CARRY": 1.0, "POST_NEWS": 0.9}
    return {"SCALPER": 1.0, "REVERSAL": 1.0, "TREND": 1.0, "PULLBACK": 1.0,
            "CARRY": 1.0, "POST_NEWS": 1.0}


@dataclass(frozen=True, slots=True)
class LondonBreakoutPlan:
    instrument: str
    range_high: float
    range_low: float
    range_pips: float
    break_long_level: float
    break_short_level: float
    stop_long_level: float
    stop_short_level: float
    target_long_level: float
    target_short_level: float
    reason: str


def compute_london_opening_range(
    *,
    m5_bars: list[dict],
    start_utc: time = time(7, 0),
    end_utc: time = time(8, 0),
) -> tuple[float, float] | None:
    """Compute the high / low of the 0700-0800 UTC opening range from a
    list of M5 bars. Each bar is a mapping with keys
    ``timestamp`` (datetime, UTC), ``high`` (float), ``low`` (float).

    Returns (range_high, range_low) or None if the window is empty.
    """
    highs: list[float] = []
    lows: list[float] = []
    for bar in m5_bars:
        ts = bar["timestamp"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        t = ts.astimezone(timezone.utc).time()
        if start_utc <= t < end_utc:
            highs.append(float(bar["high"]))
            lows.append(float(bar["low"]))
    if not highs or not lows:
        return None
    return max(highs), min(lows)


def plan_london_breakout(
    *,
    instrument: str,
    range_high: float,
    range_low: float,
    pip_size: float = 0.0001,
    stop_atr: float = 0.0,
    stop_atr_mult: float = 1.0,
    target_rr: float = 2.0,
    break_buffer_pips: float = 2.0,
) -> LondonBreakoutPlan | None:
    """Build a LondonBreakoutPlan. Returns None for degenerate ranges.

    * Entry long at ``range_high + buffer``.
    * Entry short at ``range_low − buffer``.
    * Stop at opposite side of the range or ``stop_atr_mult × ATR``,
      whichever is wider (memo: stop must survive the range re-test).
    * Target = ``target_rr × risk`` from the break level.
    """
    if range_high <= range_low:
        return None
    buffer = float(break_buffer_pips) * float(pip_size)
    break_long = range_high + buffer
    break_short = range_low - buffer
    range_pips = (range_high - range_low) / float(pip_size)
    # Stop: opposite side of the range or ATR × mult, wider wins.
    atr_stop_distance = float(stop_atr) * float(stop_atr_mult)
    long_range_stop = range_low
    short_range_stop = range_high
    long_stop = min(long_range_stop, break_long - atr_stop_distance) if atr_stop_distance > 0 else long_range_stop
    short_stop = max(short_range_stop, break_short + atr_stop_distance) if atr_stop_distance > 0 else short_range_stop
    long_risk = break_long - long_stop
    short_risk = short_stop - break_short
    if long_risk <= 0 or short_risk <= 0:
        return None
    target_long = break_long + target_rr * long_risk
    target_short = break_short - target_rr * short_risk
    return LondonBreakoutPlan(
        instrument=instrument.upper(),
        range_high=float(range_high),
        range_low=float(range_low),
        range_pips=range_pips,
        break_long_level=break_long,
        break_short_level=break_short,
        stop_long_level=long_stop,
        stop_short_level=short_stop,
        target_long_level=target_long,
        target_short_level=target_short,
        reason=f"london_breakout_range={range_pips:.1f}pips_rr={target_rr}",
    )
