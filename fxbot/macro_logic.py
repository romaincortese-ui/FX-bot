from typing import Optional


def build_rate_bias(rates: dict[str, Optional[float]], rate_spread_threshold: float) -> dict[str, str]:
    biases: dict[str, str] = {}
    us_2y = rates.get("US_2Y")
    uk_2y = rates.get("UK_2Y")
    eu_2y = rates.get("EU_2Y")
    jp_2y = rates.get("JP_2Y")

    if us_2y is not None and uk_2y is not None:
        if uk_2y - us_2y > rate_spread_threshold:
            biases["GBP_USD"] = "LONG_ONLY"
        elif us_2y - uk_2y > rate_spread_threshold:
            biases["GBP_USD"] = "SHORT_ONLY"

    if us_2y is not None and eu_2y is not None:
        if eu_2y - us_2y > rate_spread_threshold:
            biases["EUR_USD"] = "LONG_ONLY"
        elif us_2y - eu_2y > rate_spread_threshold:
            biases["EUR_USD"] = "SHORT_ONLY"

    if us_2y is not None and jp_2y is not None:
        if us_2y - jp_2y > rate_spread_threshold:
            biases["USD_JPY"] = "LONG_ONLY"
        elif jp_2y - us_2y > rate_spread_threshold:
            biases["USD_JPY"] = "SHORT_ONLY"

    return biases


def build_commodity_bias(momentum: dict[str, Optional[float]], commodity_momentum_threshold: float) -> dict[str, str]:
    biases: dict[str, str] = {}
    oil = momentum.get("OIL")
    copper = momentum.get("COPPER")
    dairy = momentum.get("DAIRY")

    if oil is not None:
        if oil > commodity_momentum_threshold:
            biases["USD_CAD"] = "SHORT_ONLY"
        elif oil < -commodity_momentum_threshold:
            biases["USD_CAD"] = "LONG_ONLY"

    if copper is not None:
        if copper > commodity_momentum_threshold:
            biases["AUD_JPY"] = "LONG_ONLY"
        elif copper < -commodity_momentum_threshold:
            biases["AUD_JPY"] = "SHORT_ONLY"

    if dairy is not None:
        if dairy > commodity_momentum_threshold:
            biases["NZD_USD"] = "LONG_ONLY"
        elif dairy < -commodity_momentum_threshold:
            biases["NZD_USD"] = "SHORT_ONLY"

    return biases


def build_market_index_bias(indices: dict[str, Optional[float]], fx_index_momentum_threshold: float) -> dict[str, str]:
    biases: dict[str, str] = {}
    dxy = indices.get("DXY")
    vix = indices.get("VIX")

    if dxy is not None:
        if dxy > fx_index_momentum_threshold:
            biases.update({
                "EUR_USD": "SHORT_ONLY",
                "GBP_USD": "SHORT_ONLY",
                "AUD_USD": "SHORT_ONLY",
                "USD_JPY": "LONG_ONLY",
                "USD_CHF": "LONG_ONLY",
                "USD_CAD": "LONG_ONLY",
            })
        elif dxy < -fx_index_momentum_threshold:
            biases.update({
                "EUR_USD": "LONG_ONLY",
                "GBP_USD": "LONG_ONLY",
                "AUD_USD": "LONG_ONLY",
                "USD_JPY": "SHORT_ONLY",
                "USD_CHF": "SHORT_ONLY",
                "USD_CAD": "SHORT_ONLY",
            })

    if vix is not None:
        if vix > fx_index_momentum_threshold:
            biases.update({
                "AUD_USD": "SHORT_ONLY",
                "NZD_USD": "SHORT_ONLY",
                "GBP_USD": "SHORT_ONLY",
                "EUR_USD": "SHORT_ONLY",
                "USD_JPY": "LONG_ONLY",
                "USD_CHF": "LONG_ONLY",
            })
        elif vix < -fx_index_momentum_threshold:
            biases.update({
                "AUD_USD": "LONG_ONLY",
                "NZD_USD": "LONG_ONLY",
                "GBP_USD": "LONG_ONLY",
                "EUR_USD": "LONG_ONLY",
                "USD_JPY": "SHORT_ONLY",
                "USD_CHF": "SHORT_ONLY",
            })

    return biases


def build_esi_bias(esi: dict[str, Optional[float]], esi_threshold: float) -> dict[str, str]:
    biases: dict[str, str] = {}
    us = esi.get("US")
    uk = esi.get("UK")
    eu = esi.get("EU")
    jp = esi.get("JP")

    if us is not None:
        if us > esi_threshold:
            biases.update({"EUR_USD": "SHORT_ONLY", "GBP_USD": "SHORT_ONLY", "AUD_USD": "SHORT_ONLY"})
        elif us < -esi_threshold:
            biases.update({"EUR_USD": "LONG_ONLY", "GBP_USD": "LONG_ONLY", "AUD_USD": "LONG_ONLY"})

    if uk is not None:
        biases["GBP_USD"] = "LONG_ONLY" if uk > esi_threshold else "SHORT_ONLY" if uk < -esi_threshold else biases.get("GBP_USD", "")
        if not biases["GBP_USD"]:
            biases.pop("GBP_USD")

    if eu is not None:
        biases["EUR_USD"] = "LONG_ONLY" if eu > esi_threshold else "SHORT_ONLY" if eu < -esi_threshold else biases.get("EUR_USD", "")
        if not biases["EUR_USD"]:
            biases.pop("EUR_USD")

    if jp is not None:
        biases["USD_JPY"] = "LONG_ONLY" if jp > esi_threshold else "SHORT_ONLY" if jp < -esi_threshold else biases.get("USD_JPY", "")
        if not biases["USD_JPY"]:
            biases.pop("USD_JPY")

    return biases


def build_liquidity_bias(risk: dict[str, Optional[float]], liquidity_risk_threshold: float) -> dict[str, str]:
    biases: dict[str, str] = {}
    ted = risk.get("TED_SPREAD")
    fra = risk.get("FRA_OIS_SPREAD")

    if (ted is not None and ted > liquidity_risk_threshold) or (fra is not None and fra > liquidity_risk_threshold):
        biases.update({
            "AUD_USD": "SHORT_ONLY",
            "NZD_USD": "SHORT_ONLY",
            "USD_CHF": "LONG_ONLY",
            "USD_JPY": "LONG_ONLY",
        })
    return biases


def merge_biases(*bias_groups: dict[str, str], logger=None) -> dict[str, str]:
    merged: dict[str, str] = {}
    for group in bias_groups:
        for symbol, value in group.items():
            existing = merged.get(symbol)
            if existing and existing != value and logger is not None:
                logger.info(f"Macro bias conflict for {symbol}: keeping {value} over {existing}")
            merged[symbol] = value
    return merged
