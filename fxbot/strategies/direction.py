from typing import Callable

import pandas as pd

from fxbot.indicators import calc_ema, calc_macd, calc_rsi


def determine_direction(
    instrument: str,
    df_5m: pd.DataFrame,
    df_1h: pd.DataFrame | None = None,
    df_4h: pd.DataFrame | None = None,
    strategy: str = "SCALPER",
    dxy_ema_gap: float | None = None,
    dxy_gate_threshold: float = 0.005,
    apply_macro_directional_bias: Callable[[str, dict], None] | None = None,
) -> str:
    signals = {"long": 0, "short": 0}
    if df_5m is not None and len(df_5m) >= 30:
        close = df_5m["close"]
        ema9 = calc_ema(close, 9)
        ema21 = calc_ema(close, 21)
        rsi = calc_rsi(close)
        if float(ema9.iloc[-1]) > float(ema21.iloc[-1]):
            signals["long"] += 2
        else:
            signals["short"] += 2
        if rsi < 40:
            signals["long"] += 1
        elif rsi > 60:
            signals["short"] += 1
        macd = calc_macd(df_5m)
        if macd["histogram"] > 0:
            signals["long"] += 1
        else:
            signals["short"] += 1
    if df_1h is not None and len(df_1h) >= 30:
        close_1h = df_1h["close"]
        ema50_1h = calc_ema(close_1h, 50)
        rsi_1h = calc_rsi(close_1h)
        if float(close_1h.iloc[-1]) > float(ema50_1h.iloc[-1]):
            signals["long"] += 3
        else:
            signals["short"] += 3
        if rsi_1h < 45:
            signals["long"] += 1
        elif rsi_1h > 55:
            signals["short"] += 1
        macd_1h = calc_macd(df_1h)
        if macd_1h["histogram"] > 0:
            signals["long"] += 2
        else:
            signals["short"] += 2
    if df_4h is not None and len(df_4h) >= 30:
        close_4h = df_4h["close"]
        ema50_4h = calc_ema(close_4h, 50)
        if float(close_4h.iloc[-1]) > float(ema50_4h.iloc[-1]):
            signals["long"] += 4
        else:
            signals["short"] += 4
    if dxy_ema_gap is not None and "USD" in instrument:
        base, _ = instrument.split("_")
        if base == "USD":
            if dxy_ema_gap > dxy_gate_threshold:
                signals["long"] += 2
            elif dxy_ema_gap < -dxy_gate_threshold:
                signals["short"] += 2
        else:
            if dxy_ema_gap > dxy_gate_threshold:
                signals["short"] += 2
            elif dxy_ema_gap < -dxy_gate_threshold:
                signals["long"] += 2

    if apply_macro_directional_bias is not None:
        apply_macro_directional_bias(instrument, signals)

    if strategy == "REVERSAL":
        signals["long"], signals["short"] = signals["short"], signals["long"]
    return "LONG" if signals["long"] >= signals["short"] else "SHORT"
