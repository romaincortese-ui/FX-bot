"""Central-bank decision-day follow-through (FX-bot Tier 5 §8).

The existing ``POST_NEWS`` pathway and the Tier 3 §24 90-second
confirmation gate block the first ~90 seconds after a high-impact
release. Empirically, the *90-second-to-15-minute* window after a
central-bank rate decision carries a statistically significant
trend-continuation bias (see BIS Quarterly Review, Mar-2017 §3 and
Neely 2005). In that window, the initial reaction has stabilised and
the market tape is still carrying one-way flow from real-money and
systematic macro re-hedging.

This module is pure. It consumes the same macro-news event objects the
bot already reads (``{"title", "currency", "impact", "pause_start",
"pause_end", ...}``) and returns a simple ``DecisionDaySignal`` that
the live path can use as a *sizing bias* — never as a standalone
entry signal.

Central-bank decisions are identified by title keyword:

* ``FOMC``, ``fed funds``, ``federal funds`` — USD
* ``ECB``, ``main refinancing`` — EUR
* ``BoE``, ``Bank of England``, ``MPC`` — GBP
* ``BoJ``, ``Bank of Japan`` — JPY
* ``SNB`` — CHF
* ``BoC``, ``Bank of Canada`` — CAD
* ``RBA``, ``Reserve Bank of Australia`` — AUD
* ``RBNZ``, ``Reserve Bank of New Zealand`` — NZD
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable


@dataclass(frozen=True, slots=True)
class DecisionDaySignal:
    in_window: bool
    event_currency: str           # "USD" / "EUR" / ... or "" if not in window
    event_title: str
    seconds_since_release: float  # 0 if not in window
    risk_multiplier: float        # 1.0 neutral; > 1.0 inside window


_DECISION_KEYWORDS: dict[str, tuple[str, ...]] = {
    "USD": ("fomc", "fed funds", "federal funds"),
    "EUR": ("ecb", "main refinancing", "main refi"),
    "GBP": ("boe", "bank of england", "mpc"),
    "JPY": ("boj", "bank of japan"),
    "CHF": ("snb",),
    "CAD": ("boc", "bank of canada"),
    "AUD": ("rba", "reserve bank of australia"),
    "NZD": ("rbnz", "reserve bank of new zealand"),
}


def _to_utc(ts: datetime | None) -> datetime:
    if ts is None:
        ts = datetime.now(timezone.utc)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def is_central_bank_decision(event: dict) -> bool:
    """Return True iff ``event`` looks like a central-bank rate decision."""
    title = str(event.get("title", "")).lower()
    currency = str(event.get("currency", "")).upper()
    keywords = _DECISION_KEYWORDS.get(currency, ())
    return any(kw in title for kw in keywords)


def _event_currencies(instrument: str) -> tuple[str, str]:
    pair = (instrument or "").upper()
    if "_" not in pair:
        return ("", "")
    base, quote = pair.split("_", 1)
    return (base, quote)


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _to_utc(ts)


def decision_day_follow_through(
    *,
    instrument: str,
    events: Iterable[dict],
    now: datetime | None = None,
    start_delay_secs: int = 90,
    window_minutes: int = 15,
    risk_multiplier: float = 1.20,
) -> DecisionDaySignal:
    """Return the decision-day follow-through signal for ``instrument``.

    The window opens ``start_delay_secs`` seconds after the event's
    ``pause_end`` and closes ``window_minutes`` minutes later. Only
    events whose currency matches one leg of ``instrument`` are
    considered; everything else is dropped.
    """
    ts = _to_utc(now)
    base, quote = _event_currencies(instrument)
    if not base or not quote:
        return DecisionDaySignal(False, "", "", 0.0, 1.0)
    start_delay = timedelta(seconds=max(0, int(start_delay_secs)))
    window = timedelta(minutes=max(1, int(window_minutes)))
    best: DecisionDaySignal | None = None
    for event in events:
        if not is_central_bank_decision(event):
            continue
        currency = str(event.get("currency", "")).upper()
        if currency not in (base, quote):
            continue
        release_ts = _parse_ts(event.get("pause_end")) or _parse_ts(event.get("time"))
        if release_ts is None:
            continue
        window_open = release_ts + start_delay
        window_close = release_ts + start_delay + window
        if window_open <= ts <= window_close:
            elapsed = (ts - release_ts).total_seconds()
            candidate = DecisionDaySignal(
                in_window=True,
                event_currency=currency,
                event_title=str(event.get("title", "")),
                seconds_since_release=elapsed,
                risk_multiplier=float(risk_multiplier),
            )
            # Prefer the most recent release if multiple overlap.
            if best is None or candidate.seconds_since_release < best.seconds_since_release:
                best = candidate
    if best is not None:
        return best
    return DecisionDaySignal(False, "", "", 0.0, 1.0)
