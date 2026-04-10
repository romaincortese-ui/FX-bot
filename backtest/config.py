from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fxbot.config import env_float, env_int, env_str


def _parse_utc_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("Backtest datetime cannot be empty")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_csv(value: str) -> list[str]:
    return [item.strip().upper().replace("/", "_") for item in value.split(",") if item.strip()]


@dataclass(slots=True)
class BacktestConfig:
    start: datetime
    end: datetime
    instruments: list[str]
    initial_balance: float = 10_000.0
    granularity: str = "M5"
    cache_dir: str = "backtest_cache"
    macro_state_dir: str = "backtest_macro"
    output_dir: str = "backtest_output"
    generate_macro_states: bool = True
    spread_buffer_pips: float = 0.2
    spread_floor_pips: float = 0.8
    slippage_pips: float = 0.4
    news_slippage_pips: float = 2.0
    round_trip_cost_pips: float = 0.5
    use_bid_ask_data: bool = True
    max_open_trades: int = 8
    max_correlated_trades: int = 3
    max_risk_per_trade: float = 0.015
    leverage: float = 30.0
    macro_rates_file: str = ""
    macro_momentum_file: str = ""
    macro_esi_file: str = ""
    macro_liquidity_file: str = ""
    macro_news_file: str = ""
    dxy_history_file: str = ""
    vix_history_file: str = ""
    strategies: list[str] = field(default_factory=lambda: [
        "SCALPER",
        "TREND",
        "REVERSAL",
        "CARRY",
        "POST_NEWS",
        "PULLBACK",
    ])

    @classmethod
    def from_env(cls) -> "BacktestConfig":
        start = _parse_utc_datetime(env_str("BACKTEST_START", "2023-01-01T00:00:00+00:00"))
        end = _parse_utc_datetime(env_str("BACKTEST_END", "2023-03-01T00:00:00+00:00"))
        instruments = _parse_csv(env_str("BACKTEST_INSTRUMENTS", "EUR_USD,GBP_USD,USD_JPY"))
        strategies = _parse_csv(env_str("BACKTEST_STRATEGIES", "SCALPER,TREND,REVERSAL,CARRY,POST_NEWS,PULLBACK"))
        return cls(
            start=start,
            end=end,
            instruments=instruments,
            initial_balance=env_float("BACKTEST_INITIAL_BALANCE", 10_000.0),
            granularity=env_str("BACKTEST_GRANULARITY", "M5").upper(),
            cache_dir=env_str("BACKTEST_CACHE_DIR", "backtest_cache"),
            macro_state_dir=env_str("BACKTEST_MACRO_STATE_DIR", "backtest_macro"),
            output_dir=env_str("BACKTEST_OUTPUT_DIR", "backtest_output"),
            generate_macro_states=env_str("BACKTEST_GENERATE_MACRO_STATES", "true").lower() == "true",
            spread_buffer_pips=env_float("BACKTEST_SPREAD_BUFFER_PIPS", 0.2),
            spread_floor_pips=env_float("BACKTEST_SPREAD_FLOOR_PIPS", 0.8),
            slippage_pips=env_float("BACKTEST_SLIPPAGE_PIPS", 0.4),
            news_slippage_pips=env_float("BACKTEST_NEWS_SLIPPAGE_PIPS", 2.0),
            round_trip_cost_pips=env_float("BACKTEST_ROUND_TRIP_COST_PIPS", 0.5),
            use_bid_ask_data=env_str("BACKTEST_USE_BID_ASK_DATA", "true").lower() == "true",
            max_open_trades=env_int("BACKTEST_MAX_OPEN_TRADES", env_int("MAX_OPEN_TRADES", 8)),
            max_correlated_trades=env_int("BACKTEST_MAX_CORRELATED_TRADES", env_int("MAX_CORRELATED_TRADES", 3)),
            max_risk_per_trade=env_float("BACKTEST_MAX_RISK_PER_TRADE", env_float("MAX_RISK_PER_TRADE", 0.015)),
            leverage=env_float("BACKTEST_LEVERAGE", env_float("LEVERAGE", 30.0)),
            macro_rates_file=env_str("BACKTEST_MACRO_RATES_FILE", ""),
            macro_momentum_file=env_str("BACKTEST_MACRO_MOMENTUM_FILE", ""),
            macro_esi_file=env_str("BACKTEST_MACRO_ESI_FILE", ""),
            macro_liquidity_file=env_str("BACKTEST_MACRO_LIQUIDITY_FILE", ""),
            macro_news_file=env_str("BACKTEST_MACRO_NEWS_FILE", ""),
            dxy_history_file=env_str("BACKTEST_DXY_HISTORY_FILE", ""),
            vix_history_file=env_str("BACKTEST_VIX_HISTORY_FILE", ""),
            strategies=strategies,
        )

    def strategy_settings(self) -> dict[str, Any]:
        settings: dict[str, Any] = {
            "SCALPER_MAX_SPREAD_PIPS": env_float("SCALPER_MAX_SPREAD_PIPS", 1.2),
            "SCALPER_MAX_RSI": env_int("SCALPER_MAX_RSI", 70),
            "SCALPER_MIN_RSI": env_int("SCALPER_MIN_RSI", 30),
            "SCALPER_CONFLUENCE_BONUS": env_float("SCALPER_CONFLUENCE_BONUS", 15.0),
            "SCALPER_THRESHOLD": env_int("SCALPER_THRESHOLD", 50),
            "SCALPER_TP_MIN_PIPS": env_float("SCALPER_TP_MIN_PIPS", 8.0),
            "SCALPER_TP_MAX_PIPS": env_float("SCALPER_TP_MAX_PIPS", 25.0),
            "SCALPER_SL_MIN_PIPS": env_float("SCALPER_SL_MIN_PIPS", 6.0),
            "SCALPER_SL_MAX_PIPS": env_float("SCALPER_SL_MAX_PIPS", 15.0),
            "SCALPER_TP_ATR_MULT": env_float("SCALPER_TP_ATR_MULT", 2.0),
            "SCALPER_SL_ATR_MULT": env_float("SCALPER_SL_ATR_MULT", 1.3),
            "SCALPER_TRAIL_PIPS": env_float("SCALPER_TRAIL_PIPS", 10.0),
            "SCALPER_COOLDOWN_HOURS": env_float("SCALPER_COOLDOWN_HOURS", 2.0),
            "SCALPER_MAX_KELLY": env_float("SCALPER_MAX_KELLY", 1.0),
            "TREND_MAX_SPREAD_PIPS": env_float("TREND_MAX_SPREAD_PIPS", 2.0),
            "TREND_THRESHOLD": env_int("TREND_THRESHOLD", 65),
            "TREND_TP_ATR_MULT": env_float("TREND_TP_ATR_MULT", 3.5),
            "TREND_SL_ATR_MULT": env_float("TREND_SL_ATR_MULT", 2.0),
            "TREND_PARTIAL_TP_ATR": env_float("TREND_PARTIAL_TP_ATR", 2.0),
            "TREND_TRAIL_PIPS": env_float("TREND_TRAIL_PIPS", 15.0),
            "TREND_TRAIL_ATR_MULT": env_float("TREND_TRAIL_ATR_MULT", 1.3),
            "TREND_BREAKEVEN_ATR_MULT": env_float("TREND_BREAKEVEN_ATR_MULT", 0.8),
            "TREND_MAX_HOURS": env_int("TREND_MAX_HOURS", 72),
            "TREND_PARTIAL_TP_PCT": env_float("TREND_PARTIAL_TP_PCT", 0.5),
            "TREND_COOLDOWN_HOURS": env_float("TREND_COOLDOWN_HOURS", 6.0),
            "REVERSAL_MAX_SPREAD_PIPS": env_float("REVERSAL_MAX_SPREAD_PIPS", 1.5),
            "REVERSAL_THRESHOLD": env_int("REVERSAL_THRESHOLD", 50),
            "REVERSAL_TP_ATR_MULT": env_float("REVERSAL_TP_ATR_MULT", 1.8),
            "REVERSAL_SL_ATR_MULT": env_float("REVERSAL_SL_ATR_MULT", 1.2),
            "REVERSAL_RSI_OVERSOLD": env_int("REVERSAL_RSI_OVERSOLD", 25),
            "REVERSAL_RSI_OVERBOUGHT": env_int("REVERSAL_RSI_OVERBOUGHT", 75),
            "REVERSAL_TRAIL_PIPS": env_float("REVERSAL_TRAIL_PIPS", 5.0),
            "REVERSAL_MAX_HOURS": env_int("REVERSAL_MAX_HOURS", 8),
            "BREAKOUT_MAX_SPREAD_PIPS": env_float("BREAKOUT_MAX_SPREAD_PIPS", 2.0),
            "BREAKOUT_THRESHOLD": env_int("BREAKOUT_THRESHOLD", 60),
            "BREAKOUT_TP_ATR_MULT": env_float("BREAKOUT_TP_ATR_MULT", 3.0),
            "BREAKOUT_SL_ATR_MULT": env_float("BREAKOUT_SL_ATR_MULT", 1.3),
            "BREAKOUT_TRAIL_PIPS": env_float("BREAKOUT_TRAIL_PIPS", 10.0),
            "BREAKOUT_MAX_HOURS": env_int("BREAKOUT_MAX_HOURS", 24),
            "BREAKOUT_COOLDOWN_HOURS": env_float("BREAKOUT_COOLDOWN_HOURS", 4.0),
            "BREAKOUT_MIN_SQUEEZE_BARS": env_int("BREAKOUT_MIN_SQUEEZE_BARS", 8),
            "CARRY_VIX_MAX": env_float("CARRY_VIX_MAX", 18.0),
            "CARRY_MAX_SPREAD_PIPS": env_float("CARRY_MAX_SPREAD_PIPS", 2.5),
            "CARRY_THRESHOLD": env_int("CARRY_THRESHOLD", 35),
            "CARRY_TP_ATR_MULT": env_float("CARRY_TP_ATR_MULT", 2.5),
            "CARRY_SL_ATR_MULT": env_float("CARRY_SL_ATR_MULT", 1.5),
            "CARRY_TRAIL_PIPS": env_float("CARRY_TRAIL_PIPS", 15.0),
            "CARRY_MAX_HOURS": env_int("CARRY_MAX_HOURS", 120),
            "ASIAN_FADE_MAX_SPREAD_PIPS": env_float("ASIAN_FADE_MAX_SPREAD_PIPS", 2.0),
            "ASIAN_FADE_THRESHOLD": env_int("ASIAN_FADE_THRESHOLD", 35),
            "ASIAN_FADE_TP_ATR_MULT": env_float("ASIAN_FADE_TP_ATR_MULT", 1.2),
            "ASIAN_FADE_SL_ATR_MULT": env_float("ASIAN_FADE_SL_ATR_MULT", 1.0),
            "ASIAN_FADE_RSI_LOW": env_int("ASIAN_FADE_RSI_LOW", 30),
            "ASIAN_FADE_RSI_HIGH": env_int("ASIAN_FADE_RSI_HIGH", 70),
            "ASIAN_FADE_TRAIL_PIPS": env_float("ASIAN_FADE_TRAIL_PIPS", 5.0),
            "POST_NEWS_MAX_SPREAD_PIPS": env_float("POST_NEWS_MAX_SPREAD_PIPS", 3.0),
            "POST_NEWS_THRESHOLD": env_int("POST_NEWS_THRESHOLD", 40),
            "POST_NEWS_TP_ATR_MULT": env_float("POST_NEWS_TP_ATR_MULT", 2.0),
            "POST_NEWS_SL_ATR_MULT": env_float("POST_NEWS_SL_ATR_MULT", 1.0),
            "POST_NEWS_TRAIL_PIPS": env_float("POST_NEWS_TRAIL_PIPS", 8.0),
            "POST_NEWS_WINDOW_MINS": env_int("POST_NEWS_WINDOW_MINS", 15),
            "PULLBACK_MAX_SPREAD_PIPS": env_float("PULLBACK_MAX_SPREAD_PIPS", 2.5),
            "PULLBACK_THRESHOLD": env_int("PULLBACK_THRESHOLD", 37),
            "PULLBACK_TP_ATR_MULT": env_float("PULLBACK_TP_ATR_MULT", 2.5),
            "PULLBACK_SL_ATR_MULT": env_float("PULLBACK_SL_ATR_MULT", 1.2),
            "PULLBACK_TRAIL_PIPS": env_float("PULLBACK_TRAIL_PIPS", 10.0),
            "DXY_GATE_THRESHOLD": env_float("DXY_GATE_THRESHOLD", 0.005),
            "VIX_LOW_THRESHOLD": env_float("VIX_LOW_THRESHOLD", 15.0),
            "VIX_HIGH_THRESHOLD": env_float("VIX_HIGH_THRESHOLD", 25.0),
            "REGIME_HIGH_VOL_ATR_RATIO": env_float("REGIME_HIGH_VOL_ATR_RATIO", 1.8),
            "REGIME_LOW_VOL_ATR_RATIO": env_float("REGIME_LOW_VOL_ATR_RATIO", 0.7),
            "REGIME_TIGHTEN_MULT": env_float("REGIME_TIGHTEN_MULT", 1.25),
            "REGIME_LOOSEN_MULT": env_float("REGIME_LOOSEN_MULT", 0.85),
            "SESSION_OVERLAP_MULT": env_float("SESSION_OVERLAP_MULT", 0.85),
            "SESSION_LONDON_MULT": env_float("SESSION_LONDON_MULT", 0.90),
            "SESSION_NY_MULT": env_float("SESSION_NY_MULT", 0.92),
            "SESSION_TOKYO_MULT": env_float("SESSION_TOKYO_MULT", 1.15),
            "SESSION_OFF_HOURS_MULT": env_float("SESSION_OFF_HOURS_MULT", 1.30),
            "TOKYO_OPEN_UTC": env_int("TOKYO_OPEN_UTC", 0),
            "TOKYO_CLOSE_UTC": env_int("TOKYO_CLOSE_UTC", 9),
            "LONDON_OPEN_UTC": env_int("LONDON_OPEN_UTC", 7),
            "LONDON_CLOSE_UTC": env_int("LONDON_CLOSE_UTC", 16),
            "NY_OPEN_UTC": env_int("NY_OPEN_UTC", 12),
            "NY_CLOSE_UTC": env_int("NY_CLOSE_UTC", 21),
            "KELLY_MULT_HIGH_CONF": env_float("KELLY_MULT_HIGH_CONF", 2.5),
            "KELLY_MULT_STANDARD": env_float("KELLY_MULT_STANDARD", 1.8),
            "KELLY_MULT_SOLID": env_float("KELLY_MULT_SOLID", 1.3),
            "KELLY_MULT_MARGINAL": env_float("KELLY_MULT_MARGINAL", 1.0),
            # ── Unified exit settings (all strategies) ──
            "EXIT_SL_PCT": env_float("EXIT_SL_PCT", -0.40),
            "EXIT_PEAK_TRAIL_PCT": env_float("EXIT_PEAK_TRAIL_PCT", 0.015),
            "EXIT_FLAT_HOURS": env_float("EXIT_FLAT_HOURS", 48.0),
            "EXIT_REVIEW_DAYS": env_int("EXIT_REVIEW_DAYS", 7),
            "EXIT_REVIEW_POOR_THRESHOLD": env_float("EXIT_REVIEW_POOR_THRESHOLD", -0.10),
            "EXIT_REVIEW_TREND_BARS": env_int("EXIT_REVIEW_TREND_BARS", 50),
            # ── Dynamic leverage ──
            "LEVERAGE_MIN": env_float("LEVERAGE_MIN", 10.0),
            "LEVERAGE_MAX": env_float("LEVERAGE_MAX", 30.0),
        }
        return settings

