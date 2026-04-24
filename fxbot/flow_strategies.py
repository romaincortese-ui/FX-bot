"""Liquidity-flow strategy windows (Tier 3 §25 of consultant assessment).

Retail FX on OANDA has three recurring flow events that are predictable
in timing and in direction on the G3 crosses:

* **London 16:00 WMR fix** — 15:55–16:05 UTC daily. Large end-of-day
  portfolio rebalancing flow; realised volatility spikes 2–4× the
  5-minute average. Highest-Sharpe-per-hour window available to a
  single-venue retail FX book.
* **Tokyo 00:55 UTC fix** — similar to WMR but smaller (~40%), cleaner
  direction on JPY crosses when the Nikkei closes.
* **Month-end rebalance** — the last two trading days of the calendar
  month, 15:00–16:00 UTC, driven by asset-manager hedging. Seasonal
  but repeatable.

The module is deliberately pure — caller passes the current time and
asks "are we in a flow window, and if so which one".
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone, timedelta
from typing import Optional


@dataclass(frozen=True, slots=True)
class FlowWindow:
    in_window: bool
    event: str                   # "LONDON_FIX" | "TOKYO_FIX" | "MONTH_END" | ""
    starts_at_utc: datetime | None
    ends_at_utc: datetime | None
    minutes_remaining: float     # 0 if not in window
    risk_multiplier: float       # suggested sizing bias (1.0 = neutral)


def _to_utc(ts: datetime | None) -> datetime:
    if ts is None:
        ts = datetime.now(timezone.utc)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _last_business_day_offset(d: date, n: int) -> date:
    """Return the N-th-from-last business day of ``d``'s month (0-indexed).

    n=0 → last business day, n=1 → second-to-last, etc.
    """
    # Find the first day of the next month, then walk backwards over
    # weekdays only until we have decremented N+1 business days.
    if d.month == 12:
        first_next = date(d.year + 1, 1, 1)
    else:
        first_next = date(d.year, d.month + 1, 1)
    cursor = first_next - timedelta(days=1)
    seen = 0
    while True:
        if cursor.weekday() < 5:  # Mon..Fri
            if seen == n:
                return cursor
            seen += 1
        cursor = cursor - timedelta(days=1)


def is_london_fix_window(
    now: datetime | None = None,
    *,
    lead_minutes: int = 5,
    trail_minutes: int = 5,
) -> FlowWindow:
    """The daily 16:00 London WMR fix window."""
    ts = _to_utc(now)
    fix = datetime.combine(ts.date(), time(16, 0), tzinfo=timezone.utc)
    start = fix - timedelta(minutes=lead_minutes)
    end = fix + timedelta(minutes=trail_minutes)
    if ts.weekday() >= 5:  # Sat/Sun — no fix
        return FlowWindow(False, "", None, None, 0.0, 1.0)
    if start <= ts <= end:
        remaining = (end - ts).total_seconds() / 60.0
        return FlowWindow(True, "LONDON_FIX", start, end, remaining, 1.25)
    return FlowWindow(False, "", start, end, 0.0, 1.0)


def is_tokyo_fix_window(
    now: datetime | None = None,
    *,
    lead_minutes: int = 3,
    trail_minutes: int = 3,
) -> FlowWindow:
    """The daily Tokyo 00:55 UTC fix window."""
    ts = _to_utc(now)
    fix = datetime.combine(ts.date(), time(0, 55), tzinfo=timezone.utc)
    start = fix - timedelta(minutes=lead_minutes)
    end = fix + timedelta(minutes=trail_minutes)
    # Tokyo FX market is closed Saturday.
    if ts.weekday() == 5:
        return FlowWindow(False, "", None, None, 0.0, 1.0)
    if start <= ts <= end:
        remaining = (end - ts).total_seconds() / 60.0
        return FlowWindow(True, "TOKYO_FIX", start, end, remaining, 1.10)
    return FlowWindow(False, "", start, end, 0.0, 1.0)


def is_month_end_window(
    now: datetime | None = None,
    *,
    last_business_days: int = 2,
    utc_hour_start: int = 15,
    utc_hour_end: int = 16,
) -> FlowWindow:
    """Month-end rebalancing flow window (last 2 business days, 15:00–16:00 UTC)."""
    ts = _to_utc(now)
    candidates = {
        _last_business_day_offset(ts.date(), n)
        for n in range(max(1, int(last_business_days)))
    }
    if ts.date() not in candidates:
        return FlowWindow(False, "", None, None, 0.0, 1.0)
    start = datetime.combine(ts.date(), time(utc_hour_start, 0), tzinfo=timezone.utc)
    end = datetime.combine(ts.date(), time(utc_hour_end, 0), tzinfo=timezone.utc)
    if start <= ts <= end:
        remaining = (end - ts).total_seconds() / 60.0
        return FlowWindow(True, "MONTH_END", start, end, remaining, 1.35)
    return FlowWindow(False, "", start, end, 0.0, 1.0)


def active_flow_window(now: datetime | None = None) -> FlowWindow:
    """Return the single highest-priority active flow window, if any."""
    # Month-end overlaps London fix; pick month-end since its bias is larger.
    me = is_month_end_window(now)
    if me.in_window:
        return me
    lf = is_london_fix_window(now)
    if lf.in_window:
        return lf
    tk = is_tokyo_fix_window(now)
    if tk.in_window:
        return tk
    return FlowWindow(False, "", None, None, 0.0, 1.0)


def instrument_is_flow_eligible(instrument: str, event: str) -> bool:
    """Return True iff ``instrument`` is one of the cleaner flow pairs for ``event``."""
    if not instrument or "_" not in instrument:
        return False
    pair = instrument.upper()
    if event == "LONDON_FIX":
        return pair in {"EUR_USD", "GBP_USD", "USD_JPY", "EUR_GBP", "USD_CHF", "EUR_JPY"}
    if event == "TOKYO_FIX":
        return pair.endswith("_JPY") or pair.startswith("USD_")
    if event == "MONTH_END":
        # Month-end flows concentrate in G3; skip emerging crosses.
        return pair in {"EUR_USD", "GBP_USD", "USD_JPY", "EUR_GBP", "USD_CHF"}
    return False
