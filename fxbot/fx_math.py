def pip_size(instrument: str) -> float:
    return 0.01 if "JPY" in instrument else 0.0001


def price_to_pips(instrument: str, price_diff: float) -> float:
    return price_diff / pip_size(instrument)


def pips_to_price(instrument: str, pips: float) -> float:
    return pips * pip_size(instrument)


def pip_value_from_conversion(
    instrument: str,
    units: float,
    quote_to_account: float | None = None,
    account_type: str = "spread_bet",
    uses_native_units: bool = False,
) -> float:
    if account_type == "spread_bet" and not uses_native_units:
        return abs(units)
    if quote_to_account is None:
        return abs(units) * pip_size(instrument)
    return abs(units) * pip_size(instrument) * quote_to_account
