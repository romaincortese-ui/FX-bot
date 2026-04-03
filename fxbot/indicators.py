import numpy as np
import pandas as pd


def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> float:
    if len(series) < period + 1:
        return 50.0
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta.clip(upper=0))
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    value = float(rsi.iloc[-1])
    return value if not np.isnan(value) else 50.0


def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    if df is None or len(df) < period + 1:
        return 0.0
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    return float(atr.iloc[-1])


def calc_atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    atr = calc_atr(df, period)
    close = float(df["close"].iloc[-1]) if len(df) > 0 else 1.0
    return atr / close if close > 0 else 0.0


def percentile_rank(series: pd.Series) -> float:
    values = series.dropna()
    if len(values) < 5:
        return 50.0
    current = float(values.iloc[-1])
    return float((values < current).sum() / len(values) * 100)


def calc_bollinger_bands(df: pd.DataFrame, period: int = 20, std_mult: float = 2.0) -> dict:
    close = df["close"]
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = sma + std_mult * std
    lower = sma - std_mult * std
    width = (upper - lower) / sma
    width_value = float(width.iloc[-1])
    return {
        "upper": float(upper.iloc[-1]),
        "lower": float(lower.iloc[-1]),
        "mid": float(sma.iloc[-1]),
        "width": width_value if not np.isnan(width_value) else 0.04,
        "width_percentile": percentile_rank(width),
    }


def calc_macd(df: pd.DataFrame) -> dict:
    close = df["close"]
    ema12 = calc_ema(close, 12)
    ema26 = calc_ema(close, 26)
    macd_line = ema12 - ema26
    signal = calc_ema(macd_line, 9)
    histogram = macd_line - signal
    return {
        "macd": float(macd_line.iloc[-1]),
        "signal": float(signal.iloc[-1]),
        "histogram": float(histogram.iloc[-1]),
        "cross_up": float(macd_line.iloc[-1]) > float(signal.iloc[-1]) and float(macd_line.iloc[-2]) <= float(signal.iloc[-2]),
        "cross_down": float(macd_line.iloc[-1]) < float(signal.iloc[-1]) and float(macd_line.iloc[-2]) >= float(signal.iloc[-2]),
    }


def keltner_squeeze(df: pd.DataFrame, bb_period: int = 20, kc_period: int = 20, kc_mult: float = 1.5) -> dict:
    close = df["close"]
    ema = calc_ema(close, kc_period)
    atr = calc_atr(df, kc_period)
    kc_upper = float(ema.iloc[-1]) + kc_mult * atr
    kc_lower = float(ema.iloc[-1]) - kc_mult * atr
    bb = calc_bollinger_bands(df, bb_period)
    in_squeeze = bb["upper"] < kc_upper and bb["lower"] > kc_lower
    squeeze_bars = 0
    if in_squeeze:
        rolling_mean = close.rolling(bb_period).mean()
        rolling_std = close.rolling(bb_period).std()
        for index in range(min(len(df) - bb_period, 50)):
            idx = -(index + 1)
            if idx < -len(df):
                break
            bb_upper = float((rolling_mean + 2 * rolling_std).iloc[idx])
            bb_lower = float((rolling_mean - 2 * rolling_std).iloc[idx])
            kc_upper_i = float(ema.iloc[idx]) + kc_mult * atr
            kc_lower_i = float(ema.iloc[idx]) - kc_mult * atr
            if bb_upper < kc_upper_i and bb_lower > kc_lower_i:
                squeeze_bars += 1
            else:
                break
    return {
        "in_squeeze": in_squeeze,
        "squeeze_bars": squeeze_bars,
        "bb_width": bb["width"],
        "bb_percentile": bb["width_percentile"],
    }
