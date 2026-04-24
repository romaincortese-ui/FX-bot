"""Derive per-currency deposit-rate proxies from OANDA financing rates
(Tier 4 §3 of consultant second assessment).

OANDA's ``/v3/accounts/{id}/instruments`` endpoint exposes a
``financing.longRate`` and ``financing.shortRate`` per instrument. For
an FX pair BASE/QUOTE the long rate approximates::

    longRate ≈ (BASE_deposit_rate - QUOTE_deposit_rate) − broker_markup

By averaging longRate across every pair in which currency C appears as
the base, and subtracting the average longRate where it appears as the
quote, we obtain a *relative* deposit-rate proxy suitable for ranking
currencies — which is all that :func:`fxbot.carry_basket.build_carry_basket`
needs (it sorts by ``deposit_rate_3m_pct`` and picks top/bottom).

The output is not a true deposit rate — it is a broker-markup-contaminated
proxy. That is acceptable for carry-basket *ranking*, not for
forecasting absolute carry P&L.
"""
from __future__ import annotations

from typing import Iterable, Mapping

from fxbot.carry_basket import CurrencyRate
from fxbot.financing import FinancingQuote


DEFAULT_VOLS: dict[str, float] = {
    "USD": 7.0,
    "EUR": 7.0,
    "GBP": 8.5,
    "JPY": 8.0,
    "CHF": 7.0,
    "AUD": 9.0,
    "NZD": 9.5,
    "CAD": 7.5,
}
DEFAULT_UNIVERSE = tuple(DEFAULT_VOLS.keys())


def _split(instrument: str) -> tuple[str, str] | None:
    if not instrument or "_" not in instrument:
        return None
    base, _, quote = instrument.partition("_")
    base = base.upper().strip()
    quote = quote.upper().strip()
    if not base or not quote:
        return None
    return base, quote


def derive_currency_rates(
    financing: Mapping[str, FinancingQuote],
    *,
    universe: Iterable[str] = DEFAULT_UNIVERSE,
    vols: Mapping[str, float] | None = None,
    annualisation_days: float = 365.0,
) -> list[CurrencyRate]:
    """Derive a ranked list of :class:`CurrencyRate` from OANDA financing.

    For each currency ``C`` in ``universe`` we compute::

        rate_pct = mean(longRate where C is base)
                 - mean(longRate where C is quote)

    The numbers come in as bps-per-day on ``FinancingQuote``; we
    annualise with ``annualisation_days`` (default 365) to produce a
    percentage suitable for ``CurrencyRate.deposit_rate_3m_pct``.
    """
    vols_map = {**DEFAULT_VOLS, **(vols or {})}
    universe_upper = [str(c).upper() for c in universe]
    base_rates: dict[str, list[float]] = {c: [] for c in universe_upper}
    quote_rates: dict[str, list[float]] = {c: [] for c in universe_upper}
    for inst, quote in financing.items():
        parts = _split(inst)
        if parts is None:
            continue
        base, quote_ccy = parts
        if base in base_rates:
            base_rates[base].append(float(quote.long_bps_per_day))
        if quote_ccy in quote_rates:
            quote_rates[quote_ccy].append(float(quote.long_bps_per_day))
    out: list[CurrencyRate] = []
    for ccy in universe_upper:
        base_obs = base_rates[ccy]
        quote_obs = quote_rates[ccy]
        liquid = bool(base_obs) or bool(quote_obs)
        if not liquid:
            continue
        mean_base = sum(base_obs) / len(base_obs) if base_obs else 0.0
        mean_quote = sum(quote_obs) / len(quote_obs) if quote_obs else 0.0
        # bps_per_day → pct_per_year: ×annualisation_days / 100
        rate_pct = (mean_base - mean_quote) * annualisation_days / 100.0
        vol = float(vols_map.get(ccy, 8.0))
        out.append(
            CurrencyRate(
                currency=ccy,
                deposit_rate_3m_pct=rate_pct,
                annualised_vol_pct=vol,
                liquid=True,
            )
        )
    return out
