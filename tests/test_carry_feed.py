from fxbot.carry_feed import DEFAULT_UNIVERSE, derive_currency_rates
from fxbot.financing import FinancingQuote


def _quote(inst: str, long_bps: float, short_bps: float = 0.0) -> FinancingQuote:
    return FinancingQuote(
        instrument=inst,
        long_bps_per_day=long_bps,
        short_bps_per_day=short_bps,
        financing_days_per_year=365.0,
    )


def test_derives_currency_rates_from_usd_pairs():
    fin = {
        "EUR_USD": _quote("EUR_USD", long_bps=-0.5),   # USD pays more than EUR
        "USD_JPY": _quote("USD_JPY", long_bps=1.2),    # USD pays more than JPY (long USD)
        "GBP_USD": _quote("GBP_USD", long_bps=0.2),    # GBP slightly ahead of USD
        "AUD_USD": _quote("AUD_USD", long_bps=-0.3),
        "USD_CHF": _quote("USD_CHF", long_bps=1.5),
        "USD_CAD": _quote("USD_CAD", long_bps=0.4),
        "NZD_USD": _quote("NZD_USD", long_bps=0.1),
    }
    rates = derive_currency_rates(fin)
    by_ccy = {r.currency: r for r in rates}
    # USD base in USD_{JPY,CHF,CAD} pays positive; quote in EUR_USD/AUD_USD is
    # slightly negative. Net: USD is a high-yield currency.
    assert by_ccy["USD"].deposit_rate_3m_pct > 0
    # JPY and CHF are only seen as quote-side of USD-base pairs with positive
    # long_bps → they should rank at the bottom.
    usd = by_ccy["USD"].deposit_rate_3m_pct
    assert by_ccy["JPY"].deposit_rate_3m_pct < usd
    assert by_ccy["CHF"].deposit_rate_3m_pct < usd


def test_empty_financing_returns_empty_list():
    assert derive_currency_rates({}) == []


def test_ignores_unknown_currencies():
    fin = {"XXX_YYY": _quote("XXX_YYY", long_bps=1.0)}
    rates = derive_currency_rates(fin)
    assert rates == []


def test_default_universe_has_g10():
    assert {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "NZD", "CAD"}.issubset(set(DEFAULT_UNIVERSE))


def test_custom_vols_override():
    fin = {"EUR_USD": _quote("EUR_USD", long_bps=-0.5)}
    rates = derive_currency_rates(fin, vols={"EUR": 12.5, "USD": 5.0})
    by_ccy = {r.currency: r for r in rates}
    assert by_ccy["EUR"].annualised_vol_pct == 12.5
    assert by_ccy["USD"].annualised_vol_pct == 5.0
