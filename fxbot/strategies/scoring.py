from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from fxbot.fx_math import price_to_pips
from fxbot.indicators import calc_atr, calc_bollinger_bands, calc_ema, calc_macd, calc_rsi, keltner_squeeze


@dataclass
class StrategyScoringContext:
    get_spread_pips: Callable[[str], float]
    fetch_candles: Callable[[str, str, int], Any]
    reject: Callable[[str, str, str], None]
    mark_pair_failure: Callable[..., None]
    determine_direction: Callable[..., str]
    get_post_news_events: Callable[[str, datetime | None], list[dict]]
    apply_macro_directional_bias: Callable[[str, dict], None] | None
    macro_filters: Mapping[str, str]
    macro_news: Sequence[dict]
    is_pair_paused_by_news: Callable[[str, datetime | None], bool]
    market_regime_mult: float
    adaptive_offsets: Mapping[str, float]
    dxy_ema_gap: float | None
    dxy_gate_threshold: float
    vix_level: float | None
    vix_low_threshold: float
    get_trade_calibration_adjustment: Callable[[str, str, str | None], Mapping[str, Any]] | None = None
    now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc)


def _macro_bias_for_instrument(ctx: StrategyScoringContext, instrument: str) -> str:
    return str(ctx.macro_filters.get(instrument.upper(), "NEUTRAL") or "NEUTRAL").upper()


def _macro_bias_conflicts_direction(direction: str, bias: str) -> bool:
    return (direction == "LONG" and bias == "SHORT_ONLY") or (direction == "SHORT" and bias == "LONG_ONLY")


def _macro_bias_aligns_direction(direction: str, bias: str) -> bool:
    return (direction == "LONG" and bias == "LONG_ONLY") or (direction == "SHORT" and bias == "SHORT_ONLY")


def _strategy_blocked_by_news_pause(strategy: str, is_paused: bool) -> bool:
    return is_paused and strategy != "CARRY"


def _apply_directional_macro_gate(strategy: str, instrument: str, direction: str, ctx: StrategyScoringContext, require_alignment: bool = False) -> tuple[bool, str]:
    bias = _macro_bias_for_instrument(ctx, instrument)
    if _macro_bias_conflicts_direction(direction, bias):
        ctx.reject(strategy, instrument, "macro bias conflict")
        return False, bias
    if require_alignment and not _macro_bias_aligns_direction(direction, bias):
        ctx.reject(strategy, instrument, "macro alignment required")
        return False, bias
    return True, bias


def _apply_target_adjustments(tp_pips: float, sl_pips: float, direction: str, bias: str, vix_level: float | None) -> tuple[float, float, float]:
    macro_confidence = 1.0 if _macro_bias_aligns_direction(direction, bias) else 0.0
    if macro_confidence > 0:
        tp_pips *= 1.3
        sl_pips *= 0.9
    if vix_level is not None and vix_level > 22:
        tp_pips *= 0.8
        sl_pips *= 0.8
    return tp_pips, sl_pips, macro_confidence


def _reject_if_news_paused(strategy: str, instrument: str, ctx: StrategyScoringContext) -> bool:
    now = ctx.now_provider()
    if _strategy_blocked_by_news_pause(strategy, ctx.is_pair_paused_by_news(instrument, now)):
        ctx.reject(strategy, instrument, "pre-news risk window")
        return True
    return False


def _get_trade_calibration_adjustment(ctx: StrategyScoringContext, strategy: str, instrument: str, session_name: str | None) -> dict[str, Any]:
    if ctx.get_trade_calibration_adjustment is None:
        return {"threshold_offset": 0.0, "risk_mult": 1.0, "block_reason": None, "source": None}
    raw = ctx.get_trade_calibration_adjustment(strategy, instrument, session_name)
    if not isinstance(raw, Mapping):
        return {"threshold_offset": 0.0, "risk_mult": 1.0, "block_reason": None, "source": None}
    return {
        "threshold_offset": float(raw.get("threshold_offset", 0.0) or 0.0),
        "risk_mult": float(raw.get("risk_mult", 1.0) or 1.0),
        "block_reason": raw.get("block_reason"),
        "source": raw.get("source"),
    }


def _apply_calibration(strategy: str, instrument: str, session: Mapping[str, Any], score: float, eff_threshold: float, ctx: StrategyScoringContext) -> tuple[float, float, dict[str, Any]] | None:
    adjustment = _get_trade_calibration_adjustment(ctx, strategy, instrument, str(session.get("name") or ""))
    if adjustment["block_reason"]:
        ctx.reject(strategy, instrument, str(adjustment["block_reason"]))
        return None
    selection_score = float(score) * max(0.5, float(adjustment["risk_mult"])) - float(adjustment["threshold_offset"])
    adjusted_threshold = float(eff_threshold) + float(adjustment["threshold_offset"])
    return selection_score, adjusted_threshold, adjustment


def _finalize_opportunity(
    result: dict[str, Any],
    *,
    session: Mapping[str, Any],
    bias: str,
    eff_threshold: float,
    selection_score: float,
) -> dict[str, Any]:
    result.update({
        "macro_bias": str(bias),
        "effective_threshold": round(float(eff_threshold), 2),
        "score_margin": round(float(selection_score) - float(eff_threshold), 2),
        "session_name": str(session.get("name") or "UNKNOWN"),
        "session_multiplier": float(session.get("multiplier") or 1.0),
        "session_aggression": str(session.get("aggression") or "UNKNOWN"),
        "session_is_overlap": bool(session.get("is_overlap", False)),
    })
    return result


def score_scalper(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("SCALPER", instrument, ctx):
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["SCALPER_MAX_SPREAD_PIPS"]:
        ctx.reject("SCALPER", instrument, "spread too high")
        return None

    # Scalpers need active sessions only — per Investopedia, scalping requires
    # "high market liquidity" during "peak liquidity" to avoid slippage
    if session.get("aggression") in ("MINIMAL", "LOW"):
        ctx.reject("SCALPER", instrument, "session not active enough")
        return None

    df_5m = ctx.fetch_candles(instrument, "M5", 60)
    if df_5m is None or len(df_5m) < 30:
        ctx.reject("SCALPER", instrument, "not enough M5 data")
        return None

    df_1h = ctx.fetch_candles(instrument, "H1", 60)
    close = df_5m["close"]
    volume = df_5m["volume"]
    rsi = calc_rsi(close)
    atr = calc_atr(df_5m, 14)
    atr_pct = atr / float(close.iloc[-1]) if float(close.iloc[-1]) > 0 else 0

    if rsi > settings["SCALPER_MAX_RSI"] or rsi < settings["SCALPER_MIN_RSI"]:
        ctx.reject("SCALPER", instrument, "RSI out of range")
        return None

    ema9 = calc_ema(close, 9)
    ema21 = calc_ema(close, 21)
    crossed_now = float(ema9.iloc[-1]) > float(ema21.iloc[-1]) and float(ema9.iloc[-2]) <= float(ema21.iloc[-2])
    crossed_recent = float(ema9.iloc[-2]) > float(ema21.iloc[-2]) and float(ema9.iloc[-3]) <= float(ema21.iloc[-3])
    crossed_down_now = float(ema9.iloc[-1]) < float(ema21.iloc[-1]) and float(ema9.iloc[-2]) >= float(ema21.iloc[-2])
    crossed_down_recent = float(ema9.iloc[-2]) < float(ema21.iloc[-2]) and float(ema9.iloc[-3]) >= float(ema21.iloc[-3])
    crossed = crossed_now or crossed_recent or crossed_down_now or crossed_down_recent

    # HARD GATE: require an actual crossover — no crossover, no scalp trade
    if not crossed:
        ctx.reject("SCALPER", instrument, "no EMA crossover")
        return None

    avg_vol = float(volume.iloc[-20:-1].mean()) if len(volume) >= 21 else 1
    vol_ratio = float(volume.iloc[-1]) / avg_vol if avg_vol > 0 else 1.0
    rsi_prev = calc_rsi(close.iloc[:-1])
    rsi_delta = rsi - rsi_prev if not np.isnan(rsi_prev) else 0

    ma_score = 30 if (crossed_now or crossed_down_now) else 20  # fresh cross > 1-bar-old cross
    rsi_score = max(0, 40 - min(rsi, 100 - rsi)) if rsi < 45 or rsi > 55 else 0
    vol_score = min(20, (vol_ratio - 1.2) * 15) if vol_ratio > 1.2 else 0
    confluence = settings["SCALPER_CONFLUENCE_BONUS"] if vol_ratio > 1.5 and abs(rsi_delta) > 2 else 0
    macd = calc_macd(df_5m)
    macd_bonus = 5 if macd["cross_up"] or macd["cross_down"] else 0
    spread_penalty = max(0, (spread_pips - 0.5) * 8)
    score = ma_score + rsi_score + vol_score + confluence + macd_bonus - spread_penalty

    direction = ctx.determine_direction(
        instrument,
        df_5m,
        df_1h,
        strategy="SCALPER",
        dxy_ema_gap=ctx.dxy_ema_gap,
        dxy_gate_threshold=ctx.dxy_gate_threshold,
        apply_macro_directional_bias=ctx.apply_macro_directional_bias,
    )
    if direction == "SHORT" and not (crossed_down_now or crossed_down_recent):
        score *= 0.5
    elif direction == "LONG" and not (crossed_now or crossed_recent):
        score *= 0.5
    macro_ok, bias = _apply_directional_macro_gate("SCALPER", instrument, direction, ctx)
    if not macro_ok:
        return None

    eff_threshold = settings["SCALPER_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("SCALPER", 0)
    calibrated = _apply_calibration("SCALPER", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("SCALPER", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None

    tp_pips = max(settings["SCALPER_TP_MIN_PIPS"], min(settings["SCALPER_TP_MAX_PIPS"], price_to_pips(instrument, atr * settings["SCALPER_TP_ATR_MULT"])))
    sl_pips = max(settings["SCALPER_SL_MIN_PIPS"], min(settings["SCALPER_SL_MAX_PIPS"], price_to_pips(instrument, atr * settings["SCALPER_SL_ATR_MULT"])))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    if tp_pips / sl_pips < 2.0:
        tp_pips = sl_pips * 2.0
    return _finalize_opportunity({
        "instrument": instrument,
        "score": round(score, 2),
        "selection_score": round(selection_score, 2),
        "direction": direction,
        "rsi": round(rsi, 2),
        "rsi_delta": round(rsi_delta, 2),
        "vol_ratio": round(vol_ratio, 2),
        "atr": atr,
        "atr_pct": round(atr_pct, 6),
        "spread_pips": round(spread_pips, 2),
        "tp_pips": round(tp_pips, 1),
        "sl_pips": round(sl_pips, 1),
        "trail_pips": settings["SCALPER_TRAIL_PIPS"],
        "macro_confidence": macro_confidence,
        "regime_multiplier": ctx.market_regime_mult,
        "calibration_threshold_offset": calibration["threshold_offset"],
        "calibration_risk_mult": calibration["risk_mult"],
        "calibration_source": calibration["source"],
        "crossed_now": crossed_now or crossed_down_now,
        "entry_signal": "CROSSOVER" if (crossed_now or crossed_down_now) else "CROSSOVER_RECENT",
        "macd": macd,
    }, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)


def score_trend(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("TREND", instrument, ctx):
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["TREND_MAX_SPREAD_PIPS"]:
        ctx.reject("TREND", instrument, "spread too high")
        return None
    df_5m = ctx.fetch_candles(instrument, "M5", 60)
    df_1h = ctx.fetch_candles(instrument, "H1", 100)
    df_4h = ctx.fetch_candles(instrument, "H4", 60)
    if df_1h is None or len(df_1h) < 50:
        ctx.mark_pair_failure(instrument, "insufficient H1 history for trend", "candle", timeframe="H1")
        ctx.reject("TREND", instrument, "not enough H1 data")
        return None
    if df_4h is None or len(df_4h) < 30:
        ctx.mark_pair_failure(instrument, "insufficient H4 history for trend", "candle", timeframe="H4")
        ctx.reject("TREND", instrument, "not enough H4 data")
        return None
    close_1h = df_1h["close"]
    close_4h = df_4h["close"]
    ema20_1h = calc_ema(close_1h, 20)
    ema50_1h = calc_ema(close_1h, 50)
    ema20_4h = calc_ema(close_4h, 20)
    ema50_4h = calc_ema(close_4h, 50)
    bullish_4h = float(ema20_4h.iloc[-1]) > float(ema50_4h.iloc[-1])
    bullish_1h = float(ema20_1h.iloc[-1]) > float(ema50_1h.iloc[-1])
    if bullish_4h != bullish_1h:
        ctx.reject("TREND", instrument, "H1/H4 trend not aligned")
        return None
    direction = "LONG" if bullish_4h else "SHORT"

    # --- Require an entry trigger: pullback to EMA20 OR recent crossover ---
    current_price = float(close_1h.iloc[-1])
    ema20_val = float(ema20_1h.iloc[-1])
    atr = calc_atr(df_1h, 14)
    atr_pct = atr / current_price if current_price > 0 else 0

    # Minimum volatility filter — avoid ranging/dead markets
    if atr_pct < 0.0003:
        ctx.reject("TREND", instrument, "volatility too low")
        return None

    # Pullback: price within 1x ATR of EMA20 on the correct side or slightly past
    pullback_dist = abs(current_price - ema20_val)
    is_pullback = pullback_dist <= atr * 1.5

    # Recent crossover: EMA20 crossed above EMA50 within last 10 H1 bars
    crossover_recent = False
    for i in range(-10, -1):
        try:
            prev_above = float(ema20_1h.iloc[i - 1]) > float(ema50_1h.iloc[i - 1])
            curr_above = float(ema20_1h.iloc[i]) > float(ema50_1h.iloc[i])
            if curr_above != prev_above:
                crossover_recent = True
                break
        except (IndexError, KeyError):
            continue

    if not is_pullback and not crossover_recent:
        ctx.reject("TREND", instrument, "no pullback or recent crossover")
        return None

    score = 15  # reduced base score (was 25)

    # Bonus for entry trigger quality
    if crossover_recent and is_pullback:
        score += 15  # both triggers = strong signal
    elif crossover_recent:
        score += 10
    elif is_pullback:
        pullback_depth = pullback_dist / atr if atr > 0 else 0
        score += 15 if 0.3 <= pullback_depth <= 1.0 else 8

    ema50_gap_4h = abs(float(close_4h.iloc[-1]) / float(ema50_4h.iloc[-1]) - 1)
    score += min(15, ema50_gap_4h * 800)  # reduced from min(20, *1000)
    rsi_1h = calc_rsi(close_1h)
    if (direction == "LONG" and 45 < rsi_1h < 65) or (direction == "SHORT" and 35 < rsi_1h < 55):
        score += 10  # tightened RSI range
    macd_1h = calc_macd(df_1h)
    if (direction == "LONG" and macd_1h["histogram"] > 0) or (direction == "SHORT" and macd_1h["histogram"] < 0):
        score += 10
    vol = df_1h["volume"]
    vol_ratio = float(vol.iloc[-1]) / float(vol.iloc[-20:-1].mean()) if len(vol) >= 21 else 1
    if vol_ratio > 1.5:
        score += 5  # raised from 1.2 to 1.5
    if ctx.dxy_ema_gap is not None and "USD" in instrument:
        base, _ = instrument.split("_")
        usd_is_base = base == "USD"
        dxy_long = ctx.dxy_ema_gap > ctx.dxy_gate_threshold
        dxy_short = ctx.dxy_ema_gap < -ctx.dxy_gate_threshold
        if (usd_is_base and direction == "LONG" and dxy_long) or (usd_is_base and direction == "SHORT" and dxy_short) or (not usd_is_base and direction == "SHORT" and dxy_long) or (not usd_is_base and direction == "LONG" and dxy_short):
            score += 10
    macro_ok, bias = _apply_directional_macro_gate("TREND", instrument, direction, ctx)
    if not macro_ok:
        return None
    eff_threshold = settings["TREND_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("TREND", 0)
    calibrated = _apply_calibration("TREND", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("TREND", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(15, price_to_pips(instrument, atr * settings["TREND_TP_ATR_MULT"]))
    sl_pips = max(8, price_to_pips(instrument, atr * settings["TREND_SL_ATR_MULT"]))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    partial_tp_pips = max(10, price_to_pips(instrument, atr * settings["TREND_PARTIAL_TP_ATR"]))
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": direction, "rsi": round(rsi_1h, 2), "vol_ratio": round(vol_ratio, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "partial_tp_pips": round(partial_tp_pips, 1), "trail_pips": settings["TREND_TRAIL_PIPS"], "entry_signal": "TREND_ALIGNED", "ema50_gap_4h": round(ema50_gap_4h * 100, 2), "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)


def score_reversal(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("REVERSAL", instrument, ctx):
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["REVERSAL_MAX_SPREAD_PIPS"]:
        ctx.reject("REVERSAL", instrument, "spread too high")
        return None
    if session["aggression"] == "MINIMAL":
        ctx.reject("REVERSAL", instrument, "session too quiet")
        return None
    df_5m = ctx.fetch_candles(instrument, "M5", 60)
    df_1h = ctx.fetch_candles(instrument, "H1", 60)
    if df_5m is None or len(df_5m) < 30:
        ctx.reject("REVERSAL", instrument, "not enough M5 data")
        return None
    close = df_5m["close"]
    rsi = calc_rsi(close)
    is_oversold = rsi <= settings["REVERSAL_RSI_OVERSOLD"]
    is_overbought = rsi >= settings["REVERSAL_RSI_OVERBOUGHT"]
    if not (is_oversold or is_overbought):
        ctx.reject("REVERSAL", instrument, "RSI not stretched")
        return None
    direction = "LONG" if is_oversold else "SHORT"
    score = min(30, (settings["REVERSAL_RSI_OVERSOLD"] - rsi) * 3) if is_oversold else min(30, (rsi - settings["REVERSAL_RSI_OVERBOUGHT"]) * 3)
    bb = calc_bollinger_bands(df_5m)
    price = float(close.iloc[-1])
    if (is_oversold and price <= bb["lower"]) or (is_overbought and price >= bb["upper"]):
        score += 15
    rsi_prev = calc_rsi(close.iloc[:-5])
    if (is_oversold and rsi > rsi_prev) or (is_overbought and rsi < rsi_prev):
        score += 10
    volume = df_5m["volume"]
    avg_vol = float(volume.iloc[-20:-1].mean()) if len(volume) >= 21 else 1
    vol_ratio = float(volume.iloc[-1]) / avg_vol if avg_vol > 0 else 1
    if vol_ratio > 2.0:
        score += 10
    if df_1h is not None and len(df_1h) >= 30:
        low_1h = float(df_1h["low"].iloc[-20:].min())
        high_1h = float(df_1h["high"].iloc[-20:].max())
        if is_oversold and abs(price - low_1h) / low_1h < 0.002:
            score += 10
        elif is_overbought and abs(price - high_1h) / high_1h < 0.002:
            score += 10
    atr = calc_atr(df_5m, 14)
    atr_pct = atr / float(close.iloc[-1])
    macro_ok, bias = _apply_directional_macro_gate("REVERSAL", instrument, direction, ctx)
    if not macro_ok:
        return None
    eff_threshold = settings["REVERSAL_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("REVERSAL", 0)
    calibrated = _apply_calibration("REVERSAL", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("REVERSAL", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(8, price_to_pips(instrument, atr * settings["REVERSAL_TP_ATR_MULT"]))
    sl_pips = max(5, price_to_pips(instrument, atr * settings["REVERSAL_SL_ATR_MULT"]))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": direction, "rsi": round(rsi, 2), "vol_ratio": round(vol_ratio, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "trail_pips": settings["REVERSAL_TRAIL_PIPS"], "entry_signal": "OVERSOLD_BOUNCE" if is_oversold else "OVERBOUGHT_FADE", "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)


def score_breakout(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("BREAKOUT", instrument, ctx):
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["BREAKOUT_MAX_SPREAD_PIPS"]:
        ctx.reject("BREAKOUT", instrument, "spread too high")
        return None
    if session["aggression"] in ("MINIMAL", "LOW"):
        ctx.reject("BREAKOUT", instrument, "session not active enough")
        return None
    df_15m = ctx.fetch_candles(instrument, "M15", 80)
    df_1h = ctx.fetch_candles(instrument, "H1", 60)
    if df_15m is None or len(df_15m) < 40:
        ctx.mark_pair_failure(instrument, "insufficient M15 history for breakout", "candle", timeframe="M15")
        ctx.reject("BREAKOUT", instrument, "not enough M15 data")
        return None
    squeeze = keltner_squeeze(df_15m)
    min_squeeze = int(settings.get("BREAKOUT_MIN_SQUEEZE_BARS", 8))
    if not squeeze["in_squeeze"] and squeeze["squeeze_bars"] < min_squeeze:
        ctx.reject("BREAKOUT", instrument, "no squeeze or insufficient squeeze bars")
        return None
    # Require BB compression — bb_percentile > 30 means Bollinger Bands are too wide (no real squeeze)
    if squeeze["bb_percentile"] > 30:
        ctx.reject("BREAKOUT", instrument, "BB not compressed enough")
        return None
    score = min(25, squeeze["squeeze_bars"] * 2)  # reduced multiplier
    score += 20 if squeeze["bb_percentile"] < 10 else 10 if squeeze["bb_percentile"] < 20 else 0
    volume = df_15m["volume"]
    recent_vol = float(volume.iloc[-3:].mean())
    avg_vol = float(volume.iloc[-20:-3].mean()) if len(volume) >= 23 else 1
    vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1
    # Require volume expansion to confirm breakout is real, not noise
    if vol_ratio < 1.2:
        ctx.reject("BREAKOUT", instrument, "insufficient volume for breakout")
        return None
    if vol_ratio > 1.5:
        score += 10
    elif vol_ratio > 1.3:
        score += 5
    macd = calc_macd(df_15m)
    hist_prev = float((df_15m["close"].ewm(span=12).mean() - df_15m["close"].ewm(span=26).mean() - (df_15m["close"].ewm(span=12).mean() - df_15m["close"].ewm(span=26).mean()).ewm(span=9).mean()).iloc[-2])
    if abs(macd["histogram"]) > abs(hist_prev):
        score += 10
    direction = ctx.determine_direction(instrument, df_15m, df_1h, strategy="BREAKOUT", dxy_ema_gap=ctx.dxy_ema_gap, dxy_gate_threshold=ctx.dxy_gate_threshold, apply_macro_directional_bias=ctx.apply_macro_directional_bias)
    atr = calc_atr(df_15m, 14)
    atr_pct = atr / float(df_15m["close"].iloc[-1])
    macro_ok, bias = _apply_directional_macro_gate("BREAKOUT", instrument, direction, ctx)
    if not macro_ok:
        return None
    eff_threshold = settings["BREAKOUT_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("BREAKOUT", 0)
    calibrated = _apply_calibration("BREAKOUT", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("BREAKOUT", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(15, price_to_pips(instrument, atr * settings["BREAKOUT_TP_ATR_MULT"]))
    sl_pips = max(5, price_to_pips(instrument, atr * settings["BREAKOUT_SL_ATR_MULT"]))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": direction, "squeeze_bars": squeeze["squeeze_bars"], "bb_percentile": round(squeeze["bb_percentile"], 1), "vol_ratio": round(vol_ratio, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "trail_pips": settings["BREAKOUT_TRAIL_PIPS"], "entry_signal": "BB_KC_SQUEEZE", "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)


def score_carry(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("CARRY", instrument, ctx):
        return None
    if ctx.market_regime_mult > 1.05:
        ctx.reject("CARRY", instrument, "regime too hot")
        return None
    if ctx.vix_level is not None and ctx.vix_level > settings["CARRY_VIX_MAX"]:
        ctx.reject("CARRY", instrument, "VIX too high")
        return None
    if ctx.macro_filters.get(instrument.upper()) != "LONG_ONLY":
        ctx.reject("CARRY", instrument, "no long carry bias")
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["CARRY_MAX_SPREAD_PIPS"]:
        ctx.reject("CARRY", instrument, "spread too high")
        return None
    df_4h = ctx.fetch_candles(instrument, "H4", 60)
    if df_4h is None or len(df_4h) < 30:
        ctx.mark_pair_failure(instrument, "insufficient H4 history for carry", "candle", timeframe="H4")
        ctx.reject("CARRY", instrument, "not enough H4 data")
        return None
    close_4h = df_4h["close"]
    ema20_4h = calc_ema(close_4h, 20)
    ema50_4h = calc_ema(close_4h, 50)
    bullish = float(ema20_4h.iloc[-1]) > float(ema50_4h.iloc[-1])
    if not bullish:
        ctx.reject("CARRY", instrument, "4H trend not up")
        return None
    score = 25
    ema50_gap = float(close_4h.iloc[-1]) / float(ema50_4h.iloc[-1]) - 1
    score += min(15, abs(ema50_gap) * 500)
    rsi_4h = calc_rsi(close_4h)
    score += 15 if 40 < rsi_4h < 65 else 8 if 35 < rsi_4h < 70 else 0
    atr = calc_atr(df_4h, 14)
    atr_pct = atr / float(close_4h.iloc[-1])
    if atr_pct < 0.005:
        score += 10
    macd_4h = calc_macd(df_4h)
    if macd_4h["histogram"] > 0:
        score += 10
    momentum_5d = float(close_4h.iloc[-1]) / float(close_4h.iloc[-30]) - 1 if len(close_4h) >= 30 else 0.0
    if momentum_5d <= 0:
        ctx.reject("CARRY", instrument, "5D momentum not supportive")
        return None
    score += 8 if momentum_5d > 0.01 else 4
    if ctx.vix_level is not None and ctx.vix_level < ctx.vix_low_threshold:
        score += 5
    eff_threshold = settings["CARRY_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("CARRY", 0)
    calibrated = _apply_calibration("CARRY", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("CARRY", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(15, price_to_pips(instrument, atr * settings["CARRY_TP_ATR_MULT"]))
    sl_pips = max(10, price_to_pips(instrument, atr * settings["CARRY_SL_ATR_MULT"]))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, "LONG", "LONG_ONLY", ctx.vix_level)
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": "LONG", "rsi": round(rsi_4h, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "trail_pips": settings["CARRY_TRAIL_PIPS"], "entry_signal": "CARRY_YIELD", "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "momentum_5d": round(momentum_5d, 4), "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias="LONG_ONLY", eff_threshold=eff_threshold, selection_score=selection_score)


def score_asian_fade(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("ASIAN_FADE", instrument, ctx):
        return None
    if session["name"] != "TOKYO":
        ctx.reject("ASIAN_FADE", instrument, "Tokyo only")
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["ASIAN_FADE_MAX_SPREAD_PIPS"]:
        ctx.reject("ASIAN_FADE", instrument, "spread too high")
        return None
    df_5m = ctx.fetch_candles(instrument, "M5", 60)
    if df_5m is None or len(df_5m) < 30:
        ctx.reject("ASIAN_FADE", instrument, "not enough M5 data")
        return None
    close = df_5m["close"]
    rsi = calc_rsi(close)
    is_oversold = rsi <= settings["ASIAN_FADE_RSI_LOW"]
    is_overbought = rsi >= settings["ASIAN_FADE_RSI_HIGH"]
    if not (is_oversold or is_overbought):
        ctx.reject("ASIAN_FADE", instrument, "RSI not stretched")
        return None
    direction = "LONG" if is_oversold else "SHORT"
    bb = calc_bollinger_bands(df_5m)
    price = float(close.iloc[-1])
    score = 0
    if (is_oversold and price <= bb["lower"]) or (is_overbought and price >= bb["upper"]):
        score += 25
    elif (is_oversold and price <= bb["lower"] * 1.001) or (is_overbought and price >= bb["upper"] * 0.999):
        score += 15
    else:
        ctx.reject("ASIAN_FADE", instrument, "not at range edge")
        return None
    score += min(20, (settings["ASIAN_FADE_RSI_LOW"] - rsi) * 2) if is_oversold else min(20, (rsi - settings["ASIAN_FADE_RSI_HIGH"]) * 2)
    session_high = float(df_5m["high"].iloc[-18:].max())
    session_low = float(df_5m["low"].iloc[-18:].min())
    session_range = session_high - session_low
    atr = calc_atr(df_5m, 14)
    if session_range < atr * 1.5:
        score += 10
    volume = df_5m["volume"]
    avg_vol = float(volume.iloc[-20:-1].mean()) if len(volume) >= 21 else 1
    vol_ratio = float(volume.iloc[-1]) / avg_vol if avg_vol > 0 else 1
    if vol_ratio > 1.5:
        score += 10
    rsi_prev = calc_rsi(close.iloc[:-3])
    if (is_oversold and rsi > rsi_prev) or (is_overbought and rsi < rsi_prev):
        score += 5
    atr_pct = atr / float(close.iloc[-1])
    macro_ok, bias = _apply_directional_macro_gate("ASIAN_FADE", instrument, direction, ctx)
    if not macro_ok:
        return None
    eff_threshold = settings["ASIAN_FADE_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("ASIAN_FADE", 0)
    calibrated = _apply_calibration("ASIAN_FADE", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("ASIAN_FADE", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(5, min(20, price_to_pips(instrument, atr * settings["ASIAN_FADE_TP_ATR_MULT"])))
    sl_pips = max(4, min(15, price_to_pips(instrument, atr * settings["ASIAN_FADE_SL_ATR_MULT"])))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    if tp_pips / sl_pips < 1.2:
        tp_pips = sl_pips * 1.2
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": direction, "rsi": round(rsi, 2), "vol_ratio": round(vol_ratio, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "trail_pips": settings["ASIAN_FADE_TRAIL_PIPS"], "entry_signal": "ASIAN_RANGE_FADE", "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)


def score_post_news(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if not ctx.macro_news:
        ctx.reject("POST_NEWS", instrument, "no macro news loaded")
        return None
    now = ctx.now_provider()
    matching_events = ctx.get_post_news_events(instrument, now)
    if not matching_events:
        ctx.reject("POST_NEWS", instrument, "no recent high-impact post-news window")
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["POST_NEWS_MAX_SPREAD_PIPS"]:
        ctx.reject("POST_NEWS", instrument, "spread too high")
        return None
    df_5m = ctx.fetch_candles(instrument, "M5", 30)
    if df_5m is None or len(df_5m) < 10:
        ctx.reject("POST_NEWS", instrument, "not enough M5 data")
        return None
    close = df_5m["close"]
    volume = df_5m["volume"]
    atr = calc_atr(df_5m, 14)
    pre_news_high = float(df_5m["high"].iloc[-10:-3].max())
    pre_news_low = float(df_5m["low"].iloc[-10:-3].min())
    current_close = float(close.iloc[-1])
    broke_high = current_close > pre_news_high
    broke_low = current_close < pre_news_low
    if not (broke_high or broke_low):
        ctx.reject("POST_NEWS", instrument, "no breakout yet")
        return None
    direction = "LONG" if broke_high else "SHORT"
    macro_ok, bias = _apply_directional_macro_gate("POST_NEWS", instrument, direction, ctx)
    if not macro_ok:
        return None
    score = 25
    breakout_size = abs(current_close - (pre_news_high if broke_high else pre_news_low))
    score += 15 if breakout_size > atr * 0.5 else 8 if breakout_size > atr * 0.25 else 0
    avg_vol = float(volume.iloc[-10:-3].mean()) if len(volume) >= 13 else 1
    vol_ratio = float(volume.iloc[-1]) / avg_vol if avg_vol > 0 else 1
    score += 15 if vol_ratio > 2.0 else 10 if vol_ratio > 1.5 else 0
    macd = calc_macd(df_5m)
    if (direction == "LONG" and macd["histogram"] > 0) or (direction == "SHORT" and macd["histogram"] < 0):
        score += 10
    rsi = calc_rsi(close)
    if (direction == "LONG" and 50 < rsi < 80) or (direction == "SHORT" and 20 < rsi < 50):
        score += 5
    atr_pct = atr / float(close.iloc[-1])
    eff_threshold = settings["POST_NEWS_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("POST_NEWS", 0)
    calibrated = _apply_calibration("POST_NEWS", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("POST_NEWS", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(10, price_to_pips(instrument, atr * settings["POST_NEWS_TP_ATR_MULT"]))
    sl_pips = max(5, price_to_pips(instrument, atr * settings["POST_NEWS_SL_ATR_MULT"]))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    if tp_pips / sl_pips < 1.5:
        tp_pips = sl_pips * 1.5
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": direction, "rsi": round(rsi, 2), "vol_ratio": round(vol_ratio, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "trail_pips": settings["POST_NEWS_TRAIL_PIPS"], "entry_signal": "POST_NEWS_BREAKOUT", "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)


def score_pullback(instrument: str, session: Mapping[str, Any], ctx: StrategyScoringContext, settings: Mapping[str, Any]) -> dict | None:
    if _reject_if_news_paused("PULLBACK", instrument, ctx):
        return None
    spread_pips = ctx.get_spread_pips(instrument)
    if spread_pips > settings["PULLBACK_MAX_SPREAD_PIPS"]:
        ctx.reject("PULLBACK", instrument, "spread too high")
        return None
    if session["aggression"] == "MINIMAL":
        ctx.reject("PULLBACK", instrument, "session too quiet")
        return None
    df_1h = ctx.fetch_candles(instrument, "H1", 100)
    df_4h = ctx.fetch_candles(instrument, "H4", 60)
    if df_1h is None or len(df_1h) < 50:
        ctx.mark_pair_failure(instrument, "insufficient H1 history for pullback", "candle", timeframe="H1")
        ctx.reject("PULLBACK", instrument, "not enough H1 data")
        return None
    if df_4h is None or len(df_4h) < 30:
        ctx.mark_pair_failure(instrument, "insufficient H4 history for pullback", "candle", timeframe="H4")
        ctx.reject("PULLBACK", instrument, "not enough H4 data")
        return None
    close_4h = df_4h["close"]
    close_1h = df_1h["close"]
    ema20_4h = calc_ema(close_4h, 20)
    ema50_4h = calc_ema(close_4h, 50)
    bullish_4h = float(ema20_4h.iloc[-1]) > float(ema50_4h.iloc[-1])
    ema50_gap = abs(float(close_4h.iloc[-1]) / float(ema50_4h.iloc[-1]) - 1)
    if ema50_gap < 0.002:
        ctx.reject("PULLBACK", instrument, "4H trend too weak")
        return None
    direction = "LONG" if bullish_4h else "SHORT"
    ema20_1h = calc_ema(close_1h, 20)
    current_price = float(close_1h.iloc[-1])
    ema20_val = float(ema20_1h.iloc[-1])
    atr = calc_atr(df_1h, 14)
    pullback_depth = abs(current_price - ema20_val) / atr if atr > 0 else 999
    if direction == "LONG":
        if current_price > ema20_val:
            ctx.reject("PULLBACK", instrument, "no dip yet")
            return None
        if pullback_depth > 2.0:
            ctx.reject("PULLBACK", instrument, "pullback too deep")
            return None
    else:
        if current_price < ema20_val:
            ctx.reject("PULLBACK", instrument, "no rally yet")
            return None
        if pullback_depth > 2.0:
            ctx.reject("PULLBACK", instrument, "pullback too deep")
            return None
    score = 20
    score += min(15, ema50_gap * 800)
    score += 15 if 0.5 <= pullback_depth <= 1.5 else 8 if pullback_depth <= 2.0 else 0
    rsi_1h = calc_rsi(close_1h)
    if direction == "LONG" and rsi_1h < 45:
        score += min(15, (45 - rsi_1h) * 1.5)
    elif direction == "SHORT" and rsi_1h > 55:
        score += min(15, (rsi_1h - 55) * 1.5)
    else:
        ctx.reject("PULLBACK", instrument, "RSI not supportive")
        return None
    macd_1h = calc_macd(df_1h)
    if (direction == "LONG" and macd_1h["histogram"] > 0) or (direction == "SHORT" and macd_1h["histogram"] < 0):
        score += 10
    macro_ok, bias = _apply_directional_macro_gate("PULLBACK", instrument, direction, ctx, require_alignment=True)
    if not macro_ok:
        return None
    if _macro_bias_aligns_direction(direction, bias):
        score += 10
    atr_pct = atr / float(close_1h.iloc[-1])
    eff_threshold = settings["PULLBACK_THRESHOLD"] * session["multiplier"] * ctx.market_regime_mult
    eff_threshold += ctx.adaptive_offsets.get("PULLBACK", 0)
    calibrated = _apply_calibration("PULLBACK", instrument, session, score, eff_threshold, ctx)
    if calibrated is None:
        return None
    selection_score, eff_threshold, calibration = calibrated
    if selection_score < eff_threshold:
        ctx.reject("PULLBACK", instrument, f"score {selection_score:.0f} < {eff_threshold:.0f}")
        return None
    tp_pips = max(12, price_to_pips(instrument, atr * settings["PULLBACK_TP_ATR_MULT"]))
    sl_pips = max(6, price_to_pips(instrument, atr * settings["PULLBACK_SL_ATR_MULT"]))
    tp_pips, sl_pips, macro_confidence = _apply_target_adjustments(tp_pips, sl_pips, direction, bias, ctx.vix_level)
    if tp_pips / sl_pips < 1.5:
        tp_pips = sl_pips * 1.5
    return _finalize_opportunity({"instrument": instrument, "score": round(score, 2), "selection_score": round(selection_score, 2), "direction": direction, "rsi": round(rsi_1h, 2), "atr": atr, "atr_pct": round(atr_pct, 6), "spread_pips": round(spread_pips, 2), "tp_pips": round(tp_pips, 1), "sl_pips": round(sl_pips, 1), "trail_pips": settings["PULLBACK_TRAIL_PIPS"], "entry_signal": "PULLBACK_REENTRY", "pullback_depth": round(pullback_depth, 2), "macro_confidence": macro_confidence, "regime_multiplier": ctx.market_regime_mult, "calibration_threshold_offset": calibration["threshold_offset"], "calibration_risk_mult": calibration["risk_mult"], "calibration_source": calibration["source"]}, session=session, bias=bias, eff_threshold=eff_threshold, selection_score=selection_score)