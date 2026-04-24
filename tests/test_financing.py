"""Tests for Tier 2 §18 — OANDA financing-rate ingestion."""
from __future__ import annotations

from fxbot.financing import (
    FinancingCache,
    FinancingQuote,
    expected_financing_pips,
    fetch_financing_rates,
    is_carry_favourable,
)


def _stub_payload():
    return {
        "instruments": [
            {
                "name": "USD_JPY",
                "financing": {"longRate": 0.0365, "shortRate": -0.0400, "financingDaysOfWeek": 365},
            },
            {
                "name": "EUR_USD",
                "financing": {"longRate": -0.0200, "shortRate": 0.0100, "financingDaysOfWeek": 365},
            },
        ]
    }


def test_fetch_financing_rates_parses_instruments():
    rates = fetch_financing_rates(lambda path: _stub_payload(), account_id="abc")
    assert set(rates.keys()) == {"USD_JPY", "EUR_USD"}
    usdjpy = rates["USD_JPY"]
    assert usdjpy.long_bps_per_day > 0
    assert usdjpy.short_bps_per_day < 0


def test_fetch_financing_rates_missing_account_empty():
    assert fetch_financing_rates(lambda p: _stub_payload(), "") == {}


def test_expected_financing_pips_sign():
    q = FinancingQuote(instrument="USD_JPY", long_bps_per_day=1.0, short_bps_per_day=-1.0, financing_days_per_year=365)
    # Holding long pays → negative cost (price-improvement style).
    assert expected_financing_pips(quote=q, direction="LONG", hold_hours=24) < 0
    assert expected_financing_pips(quote=q, direction="SHORT", hold_hours=24) > 0


def test_expected_financing_pips_none_quote_zero():
    assert expected_financing_pips(quote=None, direction="LONG", hold_hours=24) == 0.0


def test_is_carry_favourable():
    q = FinancingQuote(instrument="USD_JPY", long_bps_per_day=1.0, short_bps_per_day=-1.0, financing_days_per_year=365)
    assert is_carry_favourable(quote=q, direction="LONG", min_bps_per_day=0.5)
    assert not is_carry_favourable(quote=q, direction="SHORT", min_bps_per_day=0.5)
    assert not is_carry_favourable(quote=None, direction="LONG")


def test_financing_cache_refresh_and_get():
    cache = FinancingCache(ttl_seconds=1)
    loaded = cache.refresh(lambda p: _stub_payload(), account_id="abc")
    assert loaded == 2
    assert cache.get("USD_JPY") is not None
    assert cache.get("NOT_A_PAIR") is None
    assert not cache.is_stale()
