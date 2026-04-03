import pytest

from fxbot.fx_math import pip_size, pip_value_from_conversion, pips_to_price, price_to_pips


def test_pip_helpers_handle_jpy_pairs():
    assert pip_size("USD_JPY") == 0.01
    assert price_to_pips("USD_JPY", 0.25) == 25
    assert pips_to_price("USD_JPY", 25) == 0.25


def test_pip_helpers_handle_non_jpy_pairs():
    assert pip_size("EUR_USD") == 0.0001
    assert price_to_pips("EUR_USD", 0.0012) == pytest.approx(12)
    assert pips_to_price("EUR_USD", 12) == pytest.approx(0.0012)


def test_pip_value_respects_spread_bet_mode():
    assert pip_value_from_conversion("EUR_USD", units=2.5, quote_to_account=0.8, account_type="spread_bet", uses_native_units=False) == 2.5


def test_pip_value_uses_conversion_for_native_units():
    value = pip_value_from_conversion("EUR_USD", units=10000, quote_to_account=0.8, account_type="cfd", uses_native_units=True)
    assert round(value, 4) == 0.8
