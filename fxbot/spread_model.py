"""Hour-of-day empirical spread model (FX-bot Sprint 2 §3.1).

The current backtester uses a flat 0.8 pip spread floor plus 0.4 pip
regular slippage. Real OANDA retail execution is session-dependent and
event-dependent. Review memo §3.1 tabulates measured spreads:

| Session (UTC)           | EUR/USD | GBP/USD | USD/JPY | NZD/JPY | Stop slip |
|-------------------------|--------:|--------:|--------:|--------:|----------:|
| Tokyo 2300-0700         |     0.6 |     1.1 |     0.9 |     2.6 |      ~0.8 |
| London 0700-1200        |     0.3 |     0.5 |     0.6 |     1.4 |      ~0.4 |
| London/NY 1200-1600     |     0.2 |     0.4 |     0.5 |     1.3 |      ~0.3 |
| NY 1600-2100            |     0.4 |     0.8 |     0.7 |     1.8 |      ~0.5 |
| NFP minute              |     4.0 |     6.5 |     4.0 |    12.0 |     3 - 8 |

This module exposes:

* ``Session`` enum and ``session_for_hour(utc_hour)``.
* ``SPREAD_TABLE`` — the memo's empirical numbers, keyed by
  ``(pair, Session)``.
* ``estimate_spread_pips(instrument, dt_utc, inside_tier1_news=False)``
  returns a pip spread estimate with the news multiplier applied.
* ``estimate_stop_slippage_pips(instrument, dt_utc, inside_tier1_news)``
  — adverse-selection-aware stop slippage.

The lookup falls back to a conservative category default for pairs not
in the table (majors vs JPY-crosses vs exotics), so the model is safe
even for instruments we haven't measured.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class Session(str, Enum):
    TOKYO = "tokyo"                     # 23:00 - 07:00 UTC
    LONDON = "london"                   # 07:00 - 12:00 UTC
    LONDON_NY_OVERLAP = "london_ny"     # 12:00 - 16:00 UTC
    NY = "ny"                           # 16:00 - 21:00 UTC
    LATE_NY = "late_ny"                 # 21:00 - 23:00 UTC (thin)


# Spread p50 in pips, from the review memo §3.1.
SPREAD_TABLE: dict[tuple[str, Session], float] = {
    ("EUR_USD", Session.TOKYO): 0.6,
    ("EUR_USD", Session.LONDON): 0.3,
    ("EUR_USD", Session.LONDON_NY_OVERLAP): 0.2,
    ("EUR_USD", Session.NY): 0.4,
    ("EUR_USD", Session.LATE_NY): 0.7,
    ("GBP_USD", Session.TOKYO): 1.1,
    ("GBP_USD", Session.LONDON): 0.5,
    ("GBP_USD", Session.LONDON_NY_OVERLAP): 0.4,
    ("GBP_USD", Session.NY): 0.8,
    ("GBP_USD", Session.LATE_NY): 1.2,
    ("USD_JPY", Session.TOKYO): 0.9,
    ("USD_JPY", Session.LONDON): 0.6,
    ("USD_JPY", Session.LONDON_NY_OVERLAP): 0.5,
    ("USD_JPY", Session.NY): 0.7,
    ("USD_JPY", Session.LATE_NY): 0.9,
    # JPY crosses as a category (NZD_JPY representative).
    ("NZD_JPY", Session.TOKYO): 2.6,
    ("NZD_JPY", Session.LONDON): 1.4,
    ("NZD_JPY", Session.LONDON_NY_OVERLAP): 1.3,
    ("NZD_JPY", Session.NY): 1.8,
    ("NZD_JPY", Session.LATE_NY): 2.4,
}

# Conservative fallback spreads by category if the pair isn't tabulated.
_FALLBACK_MAJOR = {
    Session.TOKYO: 0.7,
    Session.LONDON: 0.4,
    Session.LONDON_NY_OVERLAP: 0.3,
    Session.NY: 0.6,
    Session.LATE_NY: 0.9,
}

_FALLBACK_JPY_CROSS = {
    Session.TOKYO: 2.6,
    Session.LONDON: 1.4,
    Session.LONDON_NY_OVERLAP: 1.3,
    Session.NY: 1.8,
    Session.LATE_NY: 2.4,
}

_FALLBACK_OTHER_CROSS = {
    Session.TOKYO: 2.2,
    Session.LONDON: 1.3,
    Session.LONDON_NY_OVERLAP: 1.1,
    Session.NY: 1.6,
    Session.LATE_NY: 2.0,
}

# Stop-slippage base (memo §3.1 right column) — scaled by news multiplier.
_STOP_SLIP_MAJOR = {
    Session.TOKYO: 0.8,
    Session.LONDON: 0.4,
    Session.LONDON_NY_OVERLAP: 0.3,
    Session.NY: 0.5,
    Session.LATE_NY: 0.8,
}

_STOP_SLIP_CROSS = {
    Session.TOKYO: 1.6,
    Session.LONDON: 0.9,
    Session.LONDON_NY_OVERLAP: 0.7,
    Session.NY: 1.1,
    Session.LATE_NY: 1.6,
}

# Tier-1 news multiplier (NFP, FOMC, ECB, BoE, etc.).
NEWS_SPREAD_MULTIPLIER = 6.0
NEWS_STOP_SLIP_MULTIPLIER = 8.0


def session_for_hour(utc_hour: int) -> Session:
    h = int(utc_hour) % 24
    if 7 <= h < 12:
        return Session.LONDON
    if 12 <= h < 16:
        return Session.LONDON_NY_OVERLAP
    if 16 <= h < 21:
        return Session.NY
    if 21 <= h < 23:
        return Session.LATE_NY
    # 23:00 - 07:00 → Tokyo.
    return Session.TOKYO


def session_for_datetime(dt_utc: datetime) -> Session:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return session_for_hour(dt_utc.astimezone(timezone.utc).hour)


def _is_jpy_cross(instrument: str) -> bool:
    instrument = instrument.upper()
    return "JPY" in instrument and not instrument.startswith("USD_") and not instrument.endswith("_USD")


def _is_non_jpy_cross(instrument: str) -> bool:
    instrument = instrument.upper()
    return "USD" not in instrument.split("_")


@dataclass(frozen=True, slots=True)
class SpreadEstimate:
    instrument: str
    session: Session
    spread_pips: float
    stop_slippage_pips: float
    inside_tier1_news: bool
    source: str    # "table" | "fallback_major" | "fallback_jpy_cross" | "fallback_other_cross"


def _lookup_spread(instrument: str, session: Session) -> tuple[float, str]:
    tabulated = SPREAD_TABLE.get((instrument.upper(), session))
    if tabulated is not None:
        return tabulated, "table"
    if _is_jpy_cross(instrument):
        return _FALLBACK_JPY_CROSS[session], "fallback_jpy_cross"
    if _is_non_jpy_cross(instrument):
        return _FALLBACK_OTHER_CROSS[session], "fallback_other_cross"
    return _FALLBACK_MAJOR[session], "fallback_major"


def _lookup_stop_slip(instrument: str, session: Session) -> float:
    if _is_jpy_cross(instrument) or _is_non_jpy_cross(instrument):
        return _STOP_SLIP_CROSS[session]
    return _STOP_SLIP_MAJOR[session]


def estimate_spread_pips(
    *,
    instrument: str,
    dt_utc: datetime,
    inside_tier1_news: bool = False,
) -> SpreadEstimate:
    """Return the estimated quoted spread in pips for ``instrument``.

    ``inside_tier1_news`` applies a ~6x multiplier reflecting the
    observed widening across all majors in the minute surrounding NFP /
    FOMC / ECB / BoE prints.
    """
    session = session_for_datetime(dt_utc)
    base_spread, source = _lookup_spread(instrument, session)
    stop_slip = _lookup_stop_slip(instrument, session)
    if inside_tier1_news:
        spread_pips = base_spread * NEWS_SPREAD_MULTIPLIER
        stop_slip_pips = stop_slip * NEWS_STOP_SLIP_MULTIPLIER
    else:
        spread_pips = base_spread
        stop_slip_pips = stop_slip
    return SpreadEstimate(
        instrument=instrument.upper(),
        session=session,
        spread_pips=spread_pips,
        stop_slippage_pips=stop_slip_pips,
        inside_tier1_news=bool(inside_tier1_news),
        source=source,
    )


def estimate_stop_slippage_pips(
    *,
    instrument: str,
    dt_utc: datetime,
    inside_tier1_news: bool = False,
) -> float:
    """Adverse-selection-aware stop-slippage estimate in pips."""
    return estimate_spread_pips(
        instrument=instrument,
        dt_utc=dt_utc,
        inside_tier1_news=inside_tier1_news,
    ).stop_slippage_pips
