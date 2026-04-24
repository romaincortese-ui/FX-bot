"""Daily financing-rate ingestion from OANDA (Tier 2 §18 of consultant
assessment).

OANDA publishes long/short overnight financing rates per instrument via
``/v3/accounts/{id}/instruments``. CARRY should only enter when the
held side is paid; every other strategy should net expected financing
into its R:R.

This module is pure with respect to IO — the caller passes a ``fetch``
callable (typically ``oanda_get``) so it can be unit-tested without
network access.

Rates are returned as a mapping keyed by instrument with
``(long_bps_per_day, short_bps_per_day, financing_days_per_year)``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping


@dataclass(frozen=True, slots=True)
class FinancingQuote:
    instrument: str
    long_bps_per_day: float          # negative = cost to hold long
    short_bps_per_day: float         # negative = cost to hold short
    financing_days_per_year: float   # OANDA field; 365 for FX


def _parse_instrument(doc: Mapping) -> FinancingQuote | None:
    name = doc.get("name") or doc.get("instrument")
    if not name:
        return None
    fin = doc.get("financing") or {}
    try:
        long_rate = float(fin.get("longRate", 0.0))
        short_rate = float(fin.get("shortRate", 0.0))
        days = float(fin.get("financingDaysOfWeek", 365) or 365)
    except (TypeError, ValueError):
        return None
    # OANDA publishes annualised rates as fractions (e.g. -0.015 = -1.5%
    # per year). Convert to basis-points-per-day for downstream use.
    long_bps = (long_rate / max(days, 1.0)) * 10000.0
    short_bps = (short_rate / max(days, 1.0)) * 10000.0
    return FinancingQuote(
        instrument=str(name),
        long_bps_per_day=long_bps,
        short_bps_per_day=short_bps,
        financing_days_per_year=days,
    )


def fetch_financing_rates(
    fetch: Callable[[str], Mapping],
    account_id: str,
) -> dict[str, FinancingQuote]:
    """Fetch and parse financing rates for every tradeable instrument."""
    if not account_id:
        return {}
    try:
        payload = fetch(f"/v3/accounts/{account_id}/instruments") or {}
    except Exception:
        return {}
    out: dict[str, FinancingQuote] = {}
    for doc in payload.get("instruments", []) or []:
        quote = _parse_instrument(doc)
        if quote is not None:
            out[quote.instrument] = quote
    return out


def expected_financing_pips(
    *,
    quote: FinancingQuote | None,
    direction: str,
    hold_hours: float,
    pip_value_per_bps: float = 0.01,
) -> float:
    """Estimate financing cost (positive = cost) in pips over ``hold_hours``.

    Conservative: treats 1 bps/day ≈ ``pip_value_per_bps`` pips per day
    per 1-unit position. The ratio is instrument-dependent — callers
    that need monetary precision should override. Default is good enough
    for R:R gating on major pairs.
    """
    if quote is None or hold_hours <= 0:
        return 0.0
    d = (direction or "").upper()
    if d == "LONG":
        bps_day = quote.long_bps_per_day
    elif d == "SHORT":
        bps_day = quote.short_bps_per_day
    else:
        return 0.0
    days = hold_hours / 24.0
    # Financing is in bps — negative bps = cost. Pips-cost = -bps * days * pip_value
    return -bps_day * days * pip_value_per_bps


def is_carry_favourable(
    *,
    quote: FinancingQuote | None,
    direction: str,
    min_bps_per_day: float = 0.5,
) -> bool:
    """Return True iff holding ``direction`` pays at least ``min_bps_per_day``."""
    if quote is None:
        return False
    d = (direction or "").upper()
    if d == "LONG":
        return quote.long_bps_per_day >= float(min_bps_per_day)
    if d == "SHORT":
        return quote.short_bps_per_day >= float(min_bps_per_day)
    return False


class FinancingCache:
    """Simple daily TTL cache around ``fetch_financing_rates``.

    Fetch is lazy; the first ``get`` after the TTL elapses triggers a
    refresh. Failures keep the last-good snapshot.
    """

    def __init__(self, ttl_seconds: int = 12 * 3600):
        self._rates: dict[str, FinancingQuote] = {}
        self._fetched_at: datetime | None = None
        self._ttl = int(ttl_seconds)

    def get(self, instrument: str) -> FinancingQuote | None:
        return self._rates.get(instrument)

    def is_stale(self, now: datetime | None = None) -> bool:
        if self._fetched_at is None:
            return True
        now = now or datetime.now(timezone.utc)
        return (now - self._fetched_at).total_seconds() >= self._ttl

    def refresh(
        self,
        fetch: Callable[[str], Mapping],
        account_id: str,
        *,
        now: datetime | None = None,
    ) -> int:
        """Refresh from OANDA. Returns the number of instruments loaded."""
        rates = fetch_financing_rates(fetch, account_id)
        if rates:
            self._rates = rates
            self._fetched_at = now or datetime.now(timezone.utc)
        return len(rates)

    def snapshot(self) -> dict[str, FinancingQuote]:
        return dict(self._rates)
