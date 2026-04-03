"""
OANDA FX Trading Bot — Multi-Strategy + Adaptive Learning + Session Intelligence
+ Dynamic Pair Selection + Auto‑restarting Price Stream
"""

import time
import hmac
import hashlib
import logging
import logging.handlers
import requests
import json
import os
import redis
import threading
import collections
import re
import math
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

from fxbot.config import validate_main_config
from fxbot.fx_math import (
    pip_size as core_pip_size,
    pip_value_from_conversion,
    pips_to_price as core_pips_to_price,
    price_to_pips as core_price_to_pips,
)
from fxbot.indicators import (
    calc_atr as core_calc_atr,
    calc_atr_pct as core_calc_atr_pct,
    calc_bollinger_bands as core_calc_bollinger_bands,
    calc_ema as core_calc_ema,
    calc_macd as core_calc_macd,
    calc_rsi as core_calc_rsi,
    keltner_squeeze as core_keltner_squeeze,
    percentile_rank as core_percentile_rank,
)
from fxbot.pair_health import (
    apply_pair_failure,
    apply_pair_success,
    can_count_pair_health_event,
    default_pair_health,
    pair_health_block_seconds,
)
from fxbot.risk import would_breach_correlation_limit
from fxbot.strategies import StrategyScoringContext
from fxbot.strategies import determine_direction as core_determine_direction
from fxbot.strategies import score_asian_fade as core_score_asian_fade
from fxbot.strategies import score_breakout as core_score_breakout
from fxbot.strategies import score_carry as core_score_carry
from fxbot.strategies import score_post_news as core_score_post_news
from fxbot.strategies import score_pullback as core_score_pullback
from fxbot.strategies import score_reversal as core_score_reversal
from fxbot.strategies import score_scalper as core_score_scalper
from fxbot.strategies import score_trend as core_score_trend


def _parse_pair_env(var_name: str, default: str) -> list[str]:
    raw = os.getenv(var_name, default)
    pairs = []
    seen = set()
    for item in raw.split(","):
        pair = item.strip().upper().replace("/", "_")
        if not pair or pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════

OANDA_API_KEY     = os.getenv("OANDA_API_KEY", "")
OANDA_ACCOUNT_ID  = os.getenv("OANDA_ACCOUNT_ID", "")
OANDA_ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice")
ACCOUNT_TYPE      = os.getenv("ACCOUNT_TYPE", "spread_bet")

OANDA_API_URL = (
    "https://api-fxpractice.oanda.com" if OANDA_ENVIRONMENT == "practice"
    else "https://api-fxtrade.oanda.com"
)
OANDA_STREAM_URL = (
    "https://stream-fxpractice.oanda.com" if OANDA_ENVIRONMENT == "practice"
    else "https://stream-fxtrade.oanda.com"
)

PAPER_TRADE   = os.getenv("PAPER_TRADE", "False").lower() == "true"
PAPER_BALANCE = float(os.getenv("PAPER_BALANCE", "1000"))

# ── Static fallback pairs (used if dynamic fails) ────────────
STATIC_CORE_PAIRS = _parse_pair_env("CORE_PAIRS", "EUR_USD,GBP_USD,USD_JPY")
STATIC_EXTENDED_PAIRS = _parse_pair_env("EXTENDED_PAIRS", "AUD_USD,USD_CAD,EUR_GBP,USD_CHF,NZD_USD")
STATIC_ALL_PAIRS = STATIC_CORE_PAIRS + STATIC_EXTENDED_PAIRS

# ── Dynamic watchlist settings ────────────────────────────────
DYNAMIC_PAIRS = []                 # will be filled at runtime
LAST_WATCHLIST_UPDATE = 0
WATCHLIST_UPDATE_INTERVAL = int(os.getenv("WATCHLIST_UPDATE_INTERVAL", "14400"))  # 4 hours
MAX_WATCHLIST_SIZE = int(os.getenv("MAX_WATCHLIST_SIZE", "8"))
MAX_SPREAD_FILTER_PIPS = float(os.getenv("MAX_SPREAD_FILTER_PIPS", "1.5"))
SUPPORTED_PAIR_CACHE_SECS = int(os.getenv("SUPPORTED_PAIR_CACHE_SECS", "21600"))
NEWS_WINDOW_RISK_MULT = float(os.getenv("NEWS_WINDOW_RISK_MULT", "0.5"))
DXY_REGIME_THRESHOLD = float(os.getenv("DXY_REGIME_THRESHOLD", "0.008"))

# ── Spread betting min stake ──────────────────────────────────
SPREAD_BET_MIN_STAKE = float(os.getenv("SPREAD_BET_MIN_STAKE", "0.10"))

# ── Capital allocation ───────────────────────────────────────
SCALPER_ALLOCATION_PCT  = float(os.getenv("SCALPER_ALLOCATION_PCT",  "0.30"))
TREND_ALLOCATION_PCT    = float(os.getenv("TREND_ALLOCATION_PCT",    "0.40"))
REVERSAL_ALLOCATION_PCT = float(os.getenv("REVERSAL_ALLOCATION_PCT", "0.15"))
BREAKOUT_ALLOCATION_PCT = float(os.getenv("BREAKOUT_ALLOCATION_PCT", "0.15"))

# ── Risk management ─────────────────────────────────────────
MAX_RISK_PER_TRADE     = float(os.getenv("MAX_RISK_PER_TRADE",     "0.01"))
MAX_RISK_PER_PAIR      = float(os.getenv("MAX_RISK_PER_PAIR",      "0.03"))
MAX_TOTAL_EXPOSURE     = float(os.getenv("MAX_TOTAL_EXPOSURE",      "0.15"))
MAX_CORRELATED_TRADES  = int(os.getenv("MAX_CORRELATED_TRADES",     "3"))
MAX_OPEN_TRADES        = int(os.getenv("MAX_OPEN_TRADES",           "8"))
LEVERAGE               = float(os.getenv("LEVERAGE",                "30"))

# ── Session windows (UTC) ───────────────────────────────────
TOKYO_OPEN_UTC   = int(os.getenv("TOKYO_OPEN_UTC",   "0"))
TOKYO_CLOSE_UTC  = int(os.getenv("TOKYO_CLOSE_UTC",  "9"))
LONDON_OPEN_UTC  = int(os.getenv("LONDON_OPEN_UTC",  "7"))
LONDON_CLOSE_UTC = int(os.getenv("LONDON_CLOSE_UTC", "16"))
NY_OPEN_UTC      = int(os.getenv("NY_OPEN_UTC",      "12"))
NY_CLOSE_UTC     = int(os.getenv("NY_CLOSE_UTC",     "21"))
ROLLOVER_START_UTC = int(os.getenv("ROLLOVER_START_UTC", "20"))
ROLLOVER_END_UTC   = int(os.getenv("ROLLOVER_END_UTC",   "21"))

SESSION_OVERLAP_MULT   = float(os.getenv("SESSION_OVERLAP_MULT",   "0.85"))
SESSION_LONDON_MULT    = float(os.getenv("SESSION_LONDON_MULT",    "0.90"))
SESSION_NY_MULT        = float(os.getenv("SESSION_NY_MULT",        "0.92"))
SESSION_TOKYO_MULT     = float(os.getenv("SESSION_TOKYO_MULT",     "1.15"))
SESSION_OFF_HOURS_MULT = float(os.getenv("SESSION_OFF_HOURS_MULT", "1.30"))

# ── Scalper strategy ────────────────────────────────────────
SCALPER_MAX_TRADES    = int(os.getenv("SCALPER_MAX_TRADES",    "3"))
SCALPER_BUDGET_PCT    = float(os.getenv("SCALPER_BUDGET_PCT",  "0.35"))
SCALPER_THRESHOLD     = int(os.getenv("SCALPER_THRESHOLD",     "40"))
SCALPER_TP_ATR_MULT   = float(os.getenv("SCALPER_TP_ATR_MULT", "2.0"))
SCALPER_SL_ATR_MULT   = float(os.getenv("SCALPER_SL_ATR_MULT", "1.3"))
SCALPER_TP_MIN_PIPS   = float(os.getenv("SCALPER_TP_MIN_PIPS", "8"))
SCALPER_TP_MAX_PIPS   = float(os.getenv("SCALPER_TP_MAX_PIPS", "30"))
SCALPER_SL_MIN_PIPS   = float(os.getenv("SCALPER_SL_MIN_PIPS", "5"))
SCALPER_SL_MAX_PIPS   = float(os.getenv("SCALPER_SL_MAX_PIPS", "20"))
SCALPER_MAX_RSI       = int(os.getenv("SCALPER_MAX_RSI",       "70"))
SCALPER_MIN_RSI       = int(os.getenv("SCALPER_MIN_RSI",       "30"))
SCALPER_FLAT_MINS     = int(os.getenv("SCALPER_FLAT_MINS",     "30"))
SCALPER_FLAT_RANGE_PIPS = float(os.getenv("SCALPER_FLAT_RANGE_PIPS", "3"))
SCALPER_STALL_MINS    = float(os.getenv("SCALPER_STALL_MINS",  "8"))
SCALPER_STALL_GIVEBACK = float(os.getenv("SCALPER_STALL_GIVEBACK", "0.40"))
SCALPER_CONFLUENCE_BONUS = float(os.getenv("SCALPER_CONFLUENCE_BONUS", "15"))
SCALPER_MAX_SPREAD_PIPS  = float(os.getenv("SCALPER_MAX_SPREAD_PIPS", "1.5"))
SCALPER_TRAIL_PIPS = float(os.getenv("SCALPER_TRAIL_PIPS", "5"))

# ── Trend strategy ──────────────────────────────────────────
TREND_MAX_TRADES      = int(os.getenv("TREND_MAX_TRADES",      "2"))
TREND_BUDGET_PCT      = float(os.getenv("TREND_BUDGET_PCT",    "0.40"))
TREND_THRESHOLD       = int(os.getenv("TREND_THRESHOLD",       "45"))
TREND_TP_ATR_MULT     = float(os.getenv("TREND_TP_ATR_MULT",   "3.5"))
TREND_SL_ATR_MULT     = float(os.getenv("TREND_SL_ATR_MULT",   "1.5"))
TREND_MAX_HOURS       = int(os.getenv("TREND_MAX_HOURS",       "72"))
TREND_PARTIAL_TP_PCT  = float(os.getenv("TREND_PARTIAL_TP_PCT","0.50"))
TREND_PARTIAL_TP_ATR  = float(os.getenv("TREND_PARTIAL_TP_ATR","2.0"))
TREND_BREAKEVEN_ATR   = float(os.getenv("TREND_BREAKEVEN_ATR", "1.5"))
TREND_MAX_SPREAD_PIPS = float(os.getenv("TREND_MAX_SPREAD_PIPS", "2.0"))
TREND_TRAIL_PIPS      = float(os.getenv("TREND_TRAIL_PIPS",   "15"))

# ── Reversal strategy ───────────────────────────────────────
REVERSAL_MAX_TRADES   = int(os.getenv("REVERSAL_MAX_TRADES",   "2"))
REVERSAL_BUDGET_PCT   = float(os.getenv("REVERSAL_BUDGET_PCT", "0.25"))
REVERSAL_THRESHOLD    = int(os.getenv("REVERSAL_THRESHOLD",    "50"))
REVERSAL_TP_ATR_MULT  = float(os.getenv("REVERSAL_TP_ATR_MULT","1.8"))
REVERSAL_SL_ATR_MULT  = float(os.getenv("REVERSAL_SL_ATR_MULT","1.2"))
REVERSAL_MAX_HOURS    = int(os.getenv("REVERSAL_MAX_HOURS",    "8"))
REVERSAL_RSI_OVERSOLD = int(os.getenv("REVERSAL_RSI_OVERSOLD", "25"))
REVERSAL_RSI_OVERBOUGHT = int(os.getenv("REVERSAL_RSI_OVERBOUGHT", "75"))
REVERSAL_MAX_SPREAD_PIPS = float(os.getenv("REVERSAL_MAX_SPREAD_PIPS", "1.5"))
REVERSAL_TRAIL_PIPS   = float(os.getenv("REVERSAL_TRAIL_PIPS", "5"))

# ── Breakout strategy ───────────────────────────────────────
BREAKOUT_MAX_TRADES   = int(os.getenv("BREAKOUT_MAX_TRADES",   "2"))
BREAKOUT_BUDGET_PCT   = float(os.getenv("BREAKOUT_BUDGET_PCT", "0.25"))
BREAKOUT_THRESHOLD    = int(os.getenv("BREAKOUT_THRESHOLD",    "55"))
BREAKOUT_TP_ATR_MULT  = float(os.getenv("BREAKOUT_TP_ATR_MULT","3.0"))
BREAKOUT_SL_ATR_MULT  = float(os.getenv("BREAKOUT_SL_ATR_MULT","1.0"))
BREAKOUT_MAX_HOURS    = int(os.getenv("BREAKOUT_MAX_HOURS",    "24"))
BREAKOUT_BB_PERIOD    = int(os.getenv("BREAKOUT_BB_PERIOD",    "20"))
BREAKOUT_BB_SQUEEZE_THRESHOLD = float(os.getenv("BREAKOUT_BB_SQUEEZE_THRESHOLD", "0.5"))
BREAKOUT_MAX_SPREAD_PIPS = float(os.getenv("BREAKOUT_MAX_SPREAD_PIPS", "2.0"))
BREAKOUT_TRAIL_PIPS   = float(os.getenv("BREAKOUT_TRAIL_PIPS", "10"))

# ── Carry strategy ──────────────────────────────────────────
CARRY_MAX_TRADES      = int(os.getenv("CARRY_MAX_TRADES",      "1"))
CARRY_THRESHOLD       = int(os.getenv("CARRY_THRESHOLD",       "35"))
CARRY_TP_ATR_MULT     = float(os.getenv("CARRY_TP_ATR_MULT",   "2.5"))
CARRY_SL_ATR_MULT     = float(os.getenv("CARRY_SL_ATR_MULT",   "1.5"))
CARRY_MAX_HOURS       = int(os.getenv("CARRY_MAX_HOURS",       "120"))
CARRY_MAX_SPREAD_PIPS = float(os.getenv("CARRY_MAX_SPREAD_PIPS", "2.5"))
CARRY_TRAIL_PIPS      = float(os.getenv("CARRY_TRAIL_PIPS",    "15"))
CARRY_ALLOCATION_PCT  = float(os.getenv("CARRY_ALLOCATION_PCT","0.20"))
CARRY_VIX_MAX         = float(os.getenv("CARRY_VIX_MAX",       "18"))

# ── Asian Range Fade strategy ───────────────────────────────
ASIAN_FADE_MAX_TRADES      = int(os.getenv("ASIAN_FADE_MAX_TRADES",      "2"))
ASIAN_FADE_THRESHOLD       = int(os.getenv("ASIAN_FADE_THRESHOLD",       "35"))
ASIAN_FADE_TP_ATR_MULT     = float(os.getenv("ASIAN_FADE_TP_ATR_MULT",   "1.2"))
ASIAN_FADE_SL_ATR_MULT     = float(os.getenv("ASIAN_FADE_SL_ATR_MULT",   "1.0"))
ASIAN_FADE_MAX_SPREAD_PIPS = float(os.getenv("ASIAN_FADE_MAX_SPREAD_PIPS","2.0"))
ASIAN_FADE_TRAIL_PIPS      = float(os.getenv("ASIAN_FADE_TRAIL_PIPS",    "5"))
ASIAN_FADE_RSI_LOW         = int(os.getenv("ASIAN_FADE_RSI_LOW",         "30"))
ASIAN_FADE_RSI_HIGH        = int(os.getenv("ASIAN_FADE_RSI_HIGH",        "70"))

# ── Post-News Momentum strategy ─────────────────────────────
POST_NEWS_MAX_TRADES      = int(os.getenv("POST_NEWS_MAX_TRADES",      "1"))
POST_NEWS_THRESHOLD       = int(os.getenv("POST_NEWS_THRESHOLD",       "40"))
POST_NEWS_TP_ATR_MULT     = float(os.getenv("POST_NEWS_TP_ATR_MULT",   "2.0"))
POST_NEWS_SL_ATR_MULT     = float(os.getenv("POST_NEWS_SL_ATR_MULT",   "1.0"))
POST_NEWS_MAX_SPREAD_PIPS = float(os.getenv("POST_NEWS_MAX_SPREAD_PIPS","3.0"))
POST_NEWS_TRAIL_PIPS      = float(os.getenv("POST_NEWS_TRAIL_PIPS",    "8"))
POST_NEWS_WINDOW_MINS     = int(os.getenv("POST_NEWS_WINDOW_MINS",     "15"))

# ── Pullback strategy ───────────────────────────────────────
PULLBACK_MAX_TRADES      = int(os.getenv("PULLBACK_MAX_TRADES",      "2"))
PULLBACK_THRESHOLD       = int(os.getenv("PULLBACK_THRESHOLD",       "37"))
PULLBACK_TP_ATR_MULT     = float(os.getenv("PULLBACK_TP_ATR_MULT",   "2.5"))
PULLBACK_SL_ATR_MULT     = float(os.getenv("PULLBACK_SL_ATR_MULT",   "1.2"))
PULLBACK_MAX_SPREAD_PIPS = float(os.getenv("PULLBACK_MAX_SPREAD_PIPS","2.5"))
PULLBACK_TRAIL_PIPS      = float(os.getenv("PULLBACK_TRAIL_PIPS",    "10"))

# ── Macro intelligence ──────────────────────────────────────
DXY_EMA_PERIOD          = int(os.getenv("DXY_EMA_PERIOD",          "50"))
DXY_GATE_THRESHOLD      = float(os.getenv("DXY_GATE_THRESHOLD",   "0.005"))
DXY_PROXY_INSTRUMENT    = os.getenv("DXY_PROXY_INSTRUMENT", "USD_CHF")
DXY_PROXY_FALLBACKS     = [s.strip() for s in os.getenv("DXY_PROXY_FALLBACKS", "EUR_USD,GBP_USD,USD_JPY").split(",") if s.strip()]
VIX_HIGH_THRESHOLD      = float(os.getenv("VIX_HIGH_THRESHOLD",   "25"))
VIX_EXTREME_THRESHOLD   = float(os.getenv("VIX_EXTREME_THRESHOLD","35"))
VIX_LOW_THRESHOLD       = float(os.getenv("VIX_LOW_THRESHOLD",    "15"))
VIX_PROXY_PRIMARY       = os.getenv("VIX_PROXY_PRIMARY", "SPX500_USD")
VIX_PROXY_FALLBACK      = os.getenv("VIX_PROXY_FALLBACK", "USD_JPY")
VIX_PROXY_FALLBACKS     = [s.strip() for s in os.getenv("VIX_PROXY_FALLBACKS", "USD_JPY,USD_CHF,EUR_USD").split(",") if s.strip()]
MACRO_PROXY_STALE_SECONDS = int(os.getenv("MACRO_PROXY_STALE_SECONDS", "7200"))
MACRO_FILTER_FILE        = os.getenv("MACRO_FILTER_FILE", "macro_filter.json")

REGIME_HIGH_VOL_ATR_RATIO    = float(os.getenv("REGIME_HIGH_VOL_ATR_RATIO",    "1.80"))
REGIME_LOW_VOL_ATR_RATIO     = float(os.getenv("REGIME_LOW_VOL_ATR_RATIO",     "0.70"))
REGIME_TIGHTEN_MULT          = float(os.getenv("REGIME_TIGHTEN_MULT",          "1.25"))
REGIME_LOOSEN_MULT           = float(os.getenv("REGIME_LOOSEN_MULT",           "0.85"))

ADAPTIVE_WINDOW       = int(os.getenv("ADAPTIVE_WINDOW",       "20"))
ADAPTIVE_TIGHTEN_STEP = float(os.getenv("ADAPTIVE_TIGHTEN_STEP","3"))
ADAPTIVE_RELAX_STEP   = float(os.getenv("ADAPTIVE_RELAX_STEP", "2"))
ADAPTIVE_MAX_OFFSET   = float(os.getenv("ADAPTIVE_MAX_OFFSET", "10"))
ADAPTIVE_MIN_OFFSET   = float(os.getenv("ADAPTIVE_MIN_OFFSET", "-5"))

DAILY_LOSS_LIMIT_PCT     = float(os.getenv("DAILY_LOSS_LIMIT_PCT",     "0.03"))
STREAK_LOSS_MAX          = int(os.getenv("STREAK_LOSS_MAX",             "4"))
STREAK_AUTO_RESET_MINS   = int(os.getenv("STREAK_AUTO_RESET_MINS",     "60"))
SESSION_LOSS_PAUSE_PCT   = float(os.getenv("SESSION_LOSS_PAUSE_PCT",   "0.02"))
SESSION_LOSS_PAUSE_MINS  = int(os.getenv("SESSION_LOSS_PAUSE_MINS",    "30"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

PERF_REBALANCE_TRADES = int(os.getenv("PERF_REBALANCE_TRADES", "25"))
PERF_SHIFT_STEP       = float(os.getenv("PERF_SHIFT_STEP",     "0.03"))

KELLY_MULT_HIGH_CONF  = float(os.getenv("KELLY_MULT_HIGH_CONF",  "2.5"))
KELLY_MULT_STANDARD   = float(os.getenv("KELLY_MULT_STANDARD",   "1.8"))
KELLY_MULT_SOLID      = float(os.getenv("KELLY_MULT_SOLID",      "1.2"))
KELLY_MULT_MARGINAL   = float(os.getenv("KELLY_MULT_MARGINAL",   "0.8"))

SCAN_INTERVAL_BASE   = int(os.getenv("SCAN_INTERVAL_BASE",   "30"))
SCAN_INTERVAL_ACTIVE = int(os.getenv("SCAN_INTERVAL_ACTIVE", "10"))
PAIR_HEALTH_FAILURE_COOLDOWN_SECS = int(os.getenv("PAIR_HEALTH_FAILURE_COOLDOWN_SECS", "60"))
PAIR_HEALTH_SUCCESS_COOLDOWN_SECS = int(os.getenv("PAIR_HEALTH_SUCCESS_COOLDOWN_SECS", "30"))
PAIR_HEALTH_PROBE_INTERVAL_SECS = int(os.getenv("PAIR_HEALTH_PROBE_INTERVAL_SECS", "900"))
PAIR_HEALTH_RECOVERY_SUCCESSES = int(os.getenv("PAIR_HEALTH_RECOVERY_SUCCESSES", "3"))
PAIR_HEALTH_BLOCK_BASE_SECS = int(os.getenv("PAIR_HEALTH_BLOCK_BASE_SECS", "1800"))
PAIR_HEALTH_BLOCK_MAX_SECS = int(os.getenv("PAIR_HEALTH_BLOCK_MAX_SECS", "86400"))
CLOSE_RETRY_BASE_SECS = int(os.getenv("CLOSE_RETRY_BASE_SECS", "300"))
CLOSE_RETRY_MAX_SECS = int(os.getenv("CLOSE_RETRY_MAX_SECS", "7200"))
STATE_FILE          = "state.json"
MACRO_NEWS_FILE     = os.getenv("MACRO_NEWS_FILE", "macro_news.json")
REDIS_URL           = os.getenv("REDIS_URL", "")
REDIS_MACRO_STATE_KEY = os.getenv("REDIS_MACRO_STATE_KEY", "macro_state")
HTTP_RETRIES        = 3
HTTP_RETRY_DELAY    = 1.0
HEARTBEAT_INTERVAL  = int(os.getenv("HEARTBEAT_INTERVAL",  "3600"))
KLINE_CACHE_TTL     = 15
MAX_KLINE_CACHE     = 200

validate_main_config(globals())

# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler("bot.log", maxBytes=10_000_000, backupCount=5),
    ],
)
log = logging.getLogger(__name__)

REDIS_CLIENT = None
if REDIS_URL:
    try:
        REDIS_CLIENT = redis.from_url(REDIS_URL)
        log.info(f"Connected to Redis: {REDIS_URL}")
    except Exception as e:
        log.warning(f"Failed to connect to Redis: {e}")

# ═══════════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════════

trade_history      = []
open_trades        = []
last_heartbeat_at  = 0
last_daily_summary = ""
last_weekly_summary = ""

_paused              = False
_adaptive_offsets     = {"SCALPER": 0.0, "TREND": 0.0, "REVERSAL": 0.0, "BREAKOUT": 0.0,
                         "CARRY": 0.0, "ASIAN_FADE": 0.0, "POST_NEWS": 0.0, "PULLBACK": 0.0}
_last_rebalance_count = 0
_consecutive_losses   = 0
_streak_paused_at     = 0.0
_session_loss_paused_until = 0.0
_market_regime_mult  = 1.0
_dxy_ema_gap         = None
_dxy_last_good       = None
_vix_level           = None
_vix_at              = 0.0
_vix_last_good       = None
_dxy_at              = 0.0
_scanner_log_buffer  = collections.deque(maxlen=5)
_kline_cache         = {}
_kline_cache_lock    = threading.Lock()
macro_filters        = {}
_macro_filter_mtime  = 0.0
macro_news           = []
_macro_news_mtime    = 0.0
macro_news_pause_until = 0.0
_recent_scan_decisions = []
_last_scan_cycle_at   = ""
_last_scan_cycle_summary = {"active": 0, "healthy": 0, "tradable": 0, "active_pairs": [], "tradable_pairs": []}
_scan_reject_reasons = {}
_pair_health         = {}
_last_scan_pool_status = {"mode": "primary", "active": 0, "healthy": 0, "tradable": 0}
_pending_close_retries = {}

_pair_cooldowns      = {}
_thread_local        = threading.local()
_live_prices         = {}
_price_lock          = threading.Lock()
_supported_currency_pairs_cache = set()
_supported_currency_pairs_cache_at = 0.0
_unsupported_instruments = set()

PAIR_COOLDOWN_SECS   = int(os.getenv("PAIR_COOLDOWN_SECS", "900"))

CORRELATION_GROUPS = {
    "USD_LONG":  ["EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD"],
    "USD_SHORT": ["EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD"],
    "JPY_SHORT": ["USD_JPY", "EUR_JPY", "GBP_JPY"],
}

# ── Streaming thread control ───────────────────────────────────
_stream_thread = None
_stop_stream_event = threading.Event()

# ═══════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════

def pip_size(instrument: str) -> float:
    return core_pip_size(instrument)

def price_to_pips(instrument: str, price_diff: float) -> float:
    return core_price_to_pips(instrument, price_diff)

def pips_to_price(instrument: str, pips: float) -> float:
    return core_pips_to_price(instrument, pips)


def uses_oanda_native_units() -> bool:
    return bool(OANDA_API_KEY and OANDA_ACCOUNT_ID and not PAPER_TRADE)

def pip_value(instrument: str, units: float, account_currency: str = "GBP") -> float:
    base_currency, quote_currency = instrument.split("_")
    quote_to_account = estimate_fx_conversion_rate(quote_currency, account_currency)
    return pip_value_from_conversion(
        instrument,
        units,
        quote_to_account=quote_to_account,
        account_type=ACCOUNT_TYPE,
        uses_native_units=uses_oanda_native_units(),
    )


def _extract_oanda_error_message(payload: dict | None, fallback_text: str = "") -> str:
    if not isinstance(payload, dict):
        return fallback_text[:300]

    reject = payload.get("orderRejectTransaction") or payload.get("orderCancelTransaction") or {}
    fields = [
        reject.get("rejectReason"),
        reject.get("reason"),
        payload.get("errorMessage"),
        payload.get("message"),
        fallback_text,
    ]
    for field in fields:
        if field:
            return str(field)[:300]
    try:
        return json.dumps(payload)[:300]
    except Exception:
        return fallback_text[:300]


def _normalize_broker_reason(reason: str) -> str:
    return str(reason or "").strip().lower().replace("-", " ").replace("_", " ")


def _is_market_halted_reason(reason: str) -> bool:
    normalized = _normalize_broker_reason(reason)
    halt_terms = (
        "market halted",
        "instrument halted",
        "market closed",
        "trading halted",
        "close only",
        "close only mode",
        "halted",
    )
    return any(term in normalized for term in halt_terms)


def _is_hard_broker_rejection(reason: str, status_code: int | None) -> bool:
    normalized = _normalize_broker_reason(reason)
    hard_terms = ("market halted", "instrument halted", "close only", "tradeable", "tradable")
    return status_code in {400, 403, 404} or any(term in normalized for term in hard_terms)


def _default_pair_health() -> dict:
    return default_pair_health()


def _ensure_pair_health(instrument: str) -> dict:
    rec = _pair_health.get(instrument)
    if rec is None:
        rec = _default_pair_health()
        _pair_health[instrument] = rec
    return rec


def _normalize_instrument_name(instrument: str) -> str:
    return str(instrument or "").strip().upper().replace("/", "_")


def _extract_invalid_instrument_text(detail: str) -> str:
    match = re.search(r"Invalid Instrument\s+([A-Z_]+)", detail or "", re.IGNORECASE)
    return _normalize_instrument_name(match.group(1)) if match else ""


def _mark_unsupported_instrument(instrument: str, reason: str) -> None:
    normalized = _normalize_instrument_name(instrument)
    if not normalized:
        return
    _unsupported_instruments.add(normalized)
    _supported_currency_pairs_cache.discard(normalized)
    mark_pair_failure(normalized, reason, "instrument", severity="hard")
    log.warning(f"Dropping unsupported OANDA instrument {normalized}: {reason}")


def get_supported_currency_pairs(force: bool = False) -> set[str]:
    global _supported_currency_pairs_cache, _supported_currency_pairs_cache_at

    if PAPER_TRADE or not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        return set(STATIC_ALL_PAIRS)

    now = time.time()
    if (
        _supported_currency_pairs_cache
        and not force
        and now - _supported_currency_pairs_cache_at < SUPPORTED_PAIR_CACHE_SECS
    ):
        return set(_supported_currency_pairs_cache)

    try:
        resp = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/instruments")
        supported = set()
        for inst in resp.get("instruments", []):
            if inst.get("type") != "CURRENCY":
                continue
            name = _normalize_instrument_name(inst.get("name", ""))
            if not name or name in _unsupported_instruments:
                continue
            supported.add(name)
            _ensure_pair_health(name)
        if supported:
            _supported_currency_pairs_cache = supported
            _supported_currency_pairs_cache_at = now
        return set(_supported_currency_pairs_cache)
    except Exception as e:
        log.warning(f"Falling back to cached supported pairs: {e}")
        return set(_supported_currency_pairs_cache) or set(STATIC_ALL_PAIRS)


def filter_supported_pairs(pairs: list[str], context: str = "pair list") -> list[str]:
    supported = None
    if not PAPER_TRADE and OANDA_API_KEY and OANDA_ACCOUNT_ID:
        supported = get_supported_currency_pairs()

    cleaned = []
    dropped = []
    seen = set()
    for raw_pair in pairs:
        pair = _normalize_instrument_name(raw_pair)
        if not pair or pair in seen:
            continue
        seen.add(pair)
        if pair in _unsupported_instruments:
            dropped.append(pair)
            continue
        if supported is not None and pair not in supported:
            dropped.append(pair)
            continue
        cleaned.append(pair)

    if dropped:
        log.warning(f"Dropped unsupported instruments from {context}: {sorted(set(dropped))}")
    return cleaned


def _pair_health_block_seconds(block_level: int) -> int:
    return pair_health_block_seconds(block_level, PAIR_HEALTH_BLOCK_BASE_SECS, PAIR_HEALTH_BLOCK_MAX_SECS)


def _can_count_pair_health_event(rec: dict, bucket: str, success: bool) -> bool:
    return can_count_pair_health_event(
        rec,
        bucket,
        success,
        now=time.time(),
        success_cooldown=PAIR_HEALTH_SUCCESS_COOLDOWN_SECS,
        failure_cooldown=PAIR_HEALTH_FAILURE_COOLDOWN_SECS,
    )


def get_pair_health_status(instrument: str) -> str:
    return _ensure_pair_health(instrument).get("status", "healthy")


def get_pair_health_reason(instrument: str) -> str:
    return str(_ensure_pair_health(instrument).get("last_failure_reason") or "")


def mark_pair_failure(instrument: str, reason: str, source: str, severity: str = "soft", timeframe: str = "") -> None:
    rec = _ensure_pair_health(instrument)
    bucket = f"{source}:{timeframe or '-'}"
    now = time.time()
    if not can_count_pair_health_event(
        rec,
        bucket,
        success=False,
        now=now,
        success_cooldown=PAIR_HEALTH_SUCCESS_COOLDOWN_SECS,
        failure_cooldown=PAIR_HEALTH_FAILURE_COOLDOWN_SECS,
    ):
        return
    event = apply_pair_failure(
        rec,
        reason=reason,
        source=source,
        severity=severity,
        timeframe=timeframe,
        now=now,
        block_base_secs=PAIR_HEALTH_BLOCK_BASE_SECS,
        block_max_secs=PAIR_HEALTH_BLOCK_MAX_SECS,
        probe_interval_secs=PAIR_HEALTH_PROBE_INTERVAL_SECS,
    )

    if event["status_changed"]:
        if rec["status"] == "blocked":
            until_text = datetime.fromtimestamp(rec["blocked_until"], timezone.utc).strftime("%H:%M UTC")
            log.warning(f"🧱 Pair blocked: {instrument} | {reason} | until {until_text}")
        elif rec["status"] == "degraded":
            log.warning(f"⚠️ Pair degraded: {instrument} | {reason}")


def mark_pair_success(instrument: str, source: str, timeframe: str = "") -> None:
    rec = _ensure_pair_health(instrument)
    bucket = f"{source}:{timeframe or '-'}"
    now = time.time()
    if not can_count_pair_health_event(
        rec,
        bucket,
        success=True,
        now=now,
        success_cooldown=PAIR_HEALTH_SUCCESS_COOLDOWN_SECS,
        failure_cooldown=PAIR_HEALTH_FAILURE_COOLDOWN_SECS,
    ):
        return
    event = apply_pair_success(
        rec,
        source=source,
        timeframe=timeframe,
        now=now,
        probe_interval_secs=PAIR_HEALTH_PROBE_INTERVAL_SECS,
        recovery_successes=PAIR_HEALTH_RECOVERY_SUCCESSES,
    )

    if event["previous_status"] == "blocked" and rec["status"] == "degraded":
        log.info(f"🛠️ Pair recovery started: {instrument} | moved to degraded")
    elif event["current_status"] == "healthy" and event["status_changed"]:
        log.info(f"✅ Pair healthy again: {instrument}")


def is_pair_tradeable(instrument: str) -> bool:
    instrument = _normalize_instrument_name(instrument)
    if not instrument or instrument in _unsupported_instruments:
        return False
    return get_pair_health_status(instrument) != "blocked"


def get_pair_health_buckets(instruments: list[str] | None = None) -> tuple[list[str], list[str]]:
    universe = instruments or list(_pair_health.keys())
    degraded = []
    blocked = []
    for instrument in universe:
        status = get_pair_health_status(instrument)
        if status == "blocked":
            blocked.append(instrument)
        elif status == "degraded":
            degraded.append(instrument)
    return degraded, blocked


def _next_close_retry_delay(attempts: int) -> int:
    return min(CLOSE_RETRY_BASE_SECS * max(1, 2 ** max(0, attempts - 1)), CLOSE_RETRY_MAX_SECS)


def schedule_close_retry(trade: dict, error_reason: str) -> None:
    rec = _ensure_pair_health(trade["instrument"])
    attempts = int(_pending_close_retries.get(str(trade["id"]), {}).get("attempts", 0)) + 1
    now = time.time()
    next_retry_at = now + _next_close_retry_delay(attempts)
    blocked_until = float(rec.get("blocked_until", 0.0))
    next_probe_at = float(rec.get("next_probe_at", 0.0))
    next_retry_at = max(next_retry_at, blocked_until, next_probe_at)
    _pending_close_retries[str(trade["id"])] = {
        "trade_id": str(trade["id"]),
        "instrument": trade["instrument"],
        "label": trade.get("label", "RESTORED"),
        "attempts": attempts,
        "reason": error_reason[:200],
        "next_retry_at": next_retry_at,
        "scheduled_at": now,
    }
    retry_text = datetime.fromtimestamp(next_retry_at, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    log.warning(f"🕒 Scheduled forced-close retry for {trade['instrument']} at {retry_text} | reason: {error_reason[:120]}")


def clear_close_retry(trade_id: str) -> None:
    _pending_close_retries.pop(str(trade_id), None)


def process_pending_close_retries() -> None:
    if not _pending_close_retries:
        return
    open_by_id = {str(t.get("id")): t for t in open_trades if t.get("id")}
    now = time.time()
    for trade_id, pending in list(_pending_close_retries.items()):
        trade = open_by_id.get(trade_id)
        if trade is None:
            clear_close_retry(trade_id)
            continue
        if now < float(pending.get("next_retry_at", 0.0)):
            continue
        log.info(f"🔁 Retrying forced close for {trade['instrument']} ({trade_id})")
        if close_trade_exit(trade, "FORCED_CLOSE_RETRY"):
            if trade in open_trades:
                open_trades.remove(trade)
            clear_close_retry(trade_id)
        else:
            schedule_close_retry(trade, get_pair_health_reason(trade["instrument"]) or pending.get("reason", "close retry failed"))


def probe_pair_health() -> None:
    now = time.time()
    candidates = [
        instrument for instrument, rec in _pair_health.items()
        if rec.get("status") in {"degraded", "blocked"} and now >= float(rec.get("next_probe_at", 0.0))
    ]
    for instrument in candidates[:6]:
        price = get_current_price(instrument)
        if price.get("bid", 0) <= 0 or price.get("ask", 0) <= 0:
            rec = _ensure_pair_health(instrument)
            rec["next_probe_at"] = now + PAIR_HEALTH_PROBE_INTERVAL_SECS
            continue
        df_m15 = fetch_candles(instrument, "M15", 30)
        df_h1 = fetch_candles(instrument, "H1", 40)
        rec = _ensure_pair_health(instrument)
        rec["next_probe_at"] = now + PAIR_HEALTH_PROBE_INTERVAL_SECS
        if df_m15 is None or df_h1 is None:
            continue

# ═══════════════════════════════════════════════════════════════
#  DYNAMIC PAIR SELECTION
# ═══════════════════════════════════════════════════════════════

def get_daily_atr(pair: str) -> tuple[float, float]:
    """Fetch daily candles and return ATR (price) and ATR% (percentage)."""
    df = fetch_candles(pair, "D", 30)
    if df is None or len(df) < 20:
        return 0.0, 0.0
    atr = calc_atr(df, 14)
    current_price = float(df["close"].iloc[-1])
    if current_price <= 0:
        return 0.0, 0.0
    atr_pct = (atr / current_price) * 100
    return atr, atr_pct


def _extract_invalid_instrument_from_http_error(exc: requests.HTTPError) -> str:
    response = getattr(exc, "response", None)
    if response is None:
        return ""
    detail = (getattr(response, "text", "") or "")[:500]
    return _extract_invalid_instrument_text(detail)


def _fetch_pricing_chunk(chunk: list[str]) -> list[dict]:
    cleaned_chunk = filter_supported_pairs(chunk, "pricing request")
    if not cleaned_chunk:
        return []

    try:
        prices = oanda_get(
            f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing",
            {"instruments": ",".join(cleaned_chunk)},
        )
        return prices.get("prices", [])
    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code != 400:
            raise

        invalid = _extract_invalid_instrument_from_http_error(exc)
        if invalid and invalid in cleaned_chunk:
            _mark_unsupported_instrument(invalid, "pricing endpoint rejected instrument")
            return _fetch_pricing_chunk([pair for pair in cleaned_chunk if pair != invalid])

        if len(cleaned_chunk) == 1:
            _mark_unsupported_instrument(cleaned_chunk[0], "pricing request failed")
            return []

        midpoint = len(cleaned_chunk) // 2
        return _fetch_pricing_chunk(cleaned_chunk[:midpoint]) + _fetch_pricing_chunk(cleaned_chunk[midpoint:])

def build_dynamic_watchlist(top_n: int = MAX_WATCHLIST_SIZE, max_spread_pips: float = MAX_SPREAD_FILTER_PIPS) -> list:
    """Fetch all currency pairs, filter by spread, rank by ATR%, return top N."""
    if PAPER_TRADE or not OANDA_API_KEY:
        log.warning("Dynamic watchlist skipped (paper trade or no API key). Using static list.")
        return filter_supported_pairs(STATIC_ALL_PAIRS, "static watchlist") or STATIC_ALL_PAIRS

    try:
        fx_pairs = sorted(get_supported_currency_pairs(force=True))

        if not fx_pairs:
            return filter_supported_pairs(STATIC_ALL_PAIRS, "static watchlist") or STATIC_ALL_PAIRS

        log.info(f"📊 Found {len(fx_pairs)} currency pairs. Checking spreads...")

        # Filter by spread
        chunk_size = 40
        spread_ok = []
        for i in range(0, len(fx_pairs), chunk_size):
            chunk = fx_pairs[i:i+chunk_size]
            for price in _fetch_pricing_chunk(chunk):
                inst = price["instrument"]
                bid = float(price["closeoutBid"])
                ask = float(price["closeoutAsk"])
                if bid <= 0 or ask <= 0:
                    mark_pair_failure(inst, "invalid price in watchlist", "quote")
                    continue
                mark_pair_success(inst, "quote")
                spread = (ask - bid) / pip_size(inst)
                if spread <= max_spread_pips:
                    mark_pair_success(inst, "spread")
                    if is_pair_tradeable(inst):
                        spread_ok.append(inst)
                else:
                    mark_pair_failure(inst, f"spread {spread:.1f} > {max_spread_pips:.1f}", "spread")

        if not spread_ok:
            log.warning("No pairs passed spread filter. Using static list.")
            fallback_pairs = [pair for pair in STATIC_ALL_PAIRS if is_pair_tradeable(pair)]
            return filter_supported_pairs(fallback_pairs, "static fallback") or fallback_pairs or STATIC_ALL_PAIRS

        log.info(f"📊 {len(spread_ok)} pairs passed spread filter. Ranking by volatility...")

        # Rank by ATR%
        pair_volatility = {}
        for pair in spread_ok:
            _, atr_pct = get_daily_atr(pair)
            if atr_pct > 0:
                pair_volatility[pair] = atr_pct
            else:
                pair_volatility[pair] = 0.0

        if not pair_volatility:
            return spread_ok[:top_n]

        sorted_pairs = sorted(pair_volatility.items(), key=lambda x: x[1], reverse=True)
        top_pairs = [p for p, _ in sorted_pairs[:top_n]]
        log.info(f"🔄 Dynamic watchlist built: {top_pairs}")
        return filter_supported_pairs(top_pairs, "dynamic watchlist") or top_pairs

    except Exception as e:
        log.error(f"Dynamic watchlist build failed: {e}")
        return filter_supported_pairs(STATIC_ALL_PAIRS, "static watchlist") or STATIC_ALL_PAIRS

def refresh_dynamic_watchlist(force: bool = False):
    global DYNAMIC_PAIRS, LAST_WATCHLIST_UPDATE
    if not force and time.time() - LAST_WATCHLIST_UPDATE < WATCHLIST_UPDATE_INTERVAL:
        return False
    new_list = filter_supported_pairs(build_dynamic_watchlist(), "dynamic watchlist refresh")
    if new_list:
        DYNAMIC_PAIRS = new_list
        LAST_WATCHLIST_UPDATE = time.time()
        log.info(f"✅ Dynamic watchlist updated with {len(DYNAMIC_PAIRS)} pairs.")
        _restart_price_stream()
        return True
    else:
        log.warning("Dynamic watchlist refresh returned empty – keeping old list.")
        return False


def _session_pairs_from_pool(session_name: str, pair_pool: list[str]) -> list[str]:
    core_pairs = [p for p in pair_pool if p in ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "NZD_USD"]]
    if not core_pairs:
        core_pairs = pair_pool[:6]

    if session_name == "TOKYO":
        tokyo_pairs = [p for p in pair_pool if "JPY" in p or "AUD" in p or "NZD" in p]
        return tokyo_pairs or core_pairs
    if session_name == "OFF_HOURS":
        return core_pairs
    return pair_pool


def get_effective_scan_pairs(session: dict) -> tuple[list[str], list[str], list[str], str]:
    global _last_scan_pool_status
    active_pairs = list(session["pairs_allowed"])
    health_pairs = [pair for pair in active_pairs if is_pair_tradeable(pair)]
    fallback_used = False
    rebuilt_watchlist = False

    if not health_pairs and DYNAMIC_PAIRS:
        log.warning("🩺 Active dynamic watchlist is fully blocked. Rebuilding watchlist.")
        if refresh_dynamic_watchlist(force=True):
            rebuilt_watchlist = True
            refreshed_session = get_current_session()
            active_pairs = list(refreshed_session["pairs_allowed"])
            health_pairs = [pair for pair in active_pairs if is_pair_tradeable(pair)]

    if not health_pairs:
        fallback_pool = [pair for pair in STATIC_ALL_PAIRS if is_pair_tradeable(pair)]
        if fallback_pool:
            fallback_used = True
            active_pairs = _session_pairs_from_pool(session["name"], fallback_pool)
            health_pairs = [pair for pair in active_pairs if is_pair_tradeable(pair)]

    tradable_pairs = list(health_pairs)

    if not health_pairs:
        empty_reason = "pairs blocked"
    elif fallback_used:
        empty_reason = "fallback pool active"
    else:
        empty_reason = "no setup"

    _last_scan_pool_status = {
        "mode": "fallback" if fallback_used else "rebuilt" if rebuilt_watchlist else "primary",
        "active": len(active_pairs),
        "healthy": len(health_pairs),
        "tradable": len(tradable_pairs),
    }

    return active_pairs, health_pairs, tradable_pairs, empty_reason

# ═══════════════════════════════════════════════════════════════
#  PRICE STREAMING WITH RESTART
# ═══════════════════════════════════════════════════════════════

def _price_stream_worker(stream_pairs):
    """Background thread that keeps a streaming connection alive."""
    instruments = ",".join(stream_pairs)
    url = f"{OANDA_STREAM_URL}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing/stream"
    while not _stop_stream_event.is_set():
        try:
            # Use a session that can be closed on stop
            with requests.Session() as sess:
                sess.headers.update(_oanda_headers())
                resp = sess.get(url, params={"instruments": instruments}, stream=True, timeout=30)
                log.info("🔌 Price stream connected")
                for line in resp.iter_lines():
                    if _stop_stream_event.is_set():
                        break
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if data.get("type") == "PRICE":
                            inst = data["instrument"]
                            bid = float(data["bids"][0]["price"]) if data.get("bids") else 0
                            ask = float(data["asks"][0]["price"]) if data.get("asks") else 0
                            with _price_lock:
                                _live_prices[inst] = (bid, ask, time.time())
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue
        except Exception as e:
            if not _stop_stream_event.is_set():
                log.warning(f"🔌 Stream error: {e} — reconnecting in 5s")
                time.sleep(5)
        # If we exit loop due to stop, break out completely
        if _stop_stream_event.is_set():
            break

def _start_price_stream(pairs=None):
    """Start (or restart) the price stream with the given pair list."""
    global _stream_thread, _stop_stream_event

    # If a stream is already running, stop it
    if _stream_thread and _stream_thread.is_alive():
        log.info("Stopping existing price stream...")
        _stop_stream_event.set()
        _stream_thread.join(timeout=5)
        if _stream_thread.is_alive():
            log.warning("Stream thread did not stop in time, proceeding anyway.")
        _stream_thread = None

    # Determine which pairs to stream
    if pairs is None:
        pairs = DYNAMIC_PAIRS if DYNAMIC_PAIRS else STATIC_ALL_PAIRS
    # Always include pairs with open trades
    open_trade_pairs = list({t["instrument"] for t in open_trades})
    all_pairs = filter_supported_pairs(list(set(pairs + open_trade_pairs)), "price stream")
    if not all_pairs:
        log.warning("Skipping price stream start because no supported instruments remain.")
        return

    log.info(f"Starting price stream with {len(all_pairs)} pairs: {all_pairs[:5]}...")
    _stop_stream_event.clear()
    _stream_thread = threading.Thread(target=_price_stream_worker, args=(all_pairs,), daemon=True, name="price-stream")
    _stream_thread.start()

def _restart_price_stream():
    """Restart the price stream to include the latest watchlist + open trades."""
    if PAPER_TRADE or not OANDA_API_KEY:
        return
    _start_price_stream()  # uses current DYNAMIC_PAIRS and open_trades

# ═══════════════════════════════════════════════════════════════
#  SESSION DETECTION (uses dynamic list)
# ═══════════════════════════════════════════════════════════════

def get_current_session() -> dict:
    now = datetime.now(timezone.utc)
    hour = now.hour

    tokyo_active  = TOKYO_OPEN_UTC <= hour < TOKYO_CLOSE_UTC
    london_active = LONDON_OPEN_UTC <= hour < LONDON_CLOSE_UTC
    ny_active     = NY_OPEN_UTC <= hour < NY_CLOSE_UTC

    all_pairs = DYNAMIC_PAIRS if DYNAMIC_PAIRS else STATIC_ALL_PAIRS
    core_pairs = [p for p in all_pairs if p in ["EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "NZD_USD"]]
    if not core_pairs:
        core_pairs = all_pairs[:6]

    if london_active and ny_active:
        return {
            "name": "LONDON_NY_OVERLAP",
            "multiplier": SESSION_OVERLAP_MULT,
            "pairs_allowed": all_pairs,
            "is_overlap": True,
            "aggression": "HIGH",
        }
    elif london_active:
        return {
            "name": "LONDON",
            "multiplier": SESSION_LONDON_MULT,
            "pairs_allowed": all_pairs,
            "is_overlap": False,
            "aggression": "HIGH",
        }
    elif ny_active:
        return {
            "name": "NEW_YORK",
            "multiplier": SESSION_NY_MULT,
            "pairs_allowed": all_pairs,
            "is_overlap": False,
            "aggression": "MEDIUM",
        }
    elif tokyo_active:
        tokyo_pairs = [p for p in all_pairs if "JPY" in p or "AUD" in p or "NZD" in p]
        return {
            "name": "TOKYO",
            "multiplier": SESSION_TOKYO_MULT,
            "pairs_allowed": tokyo_pairs or core_pairs,
            "is_overlap": False,
            "aggression": "LOW",
        }
    else:
        return {
            "name": "OFF_HOURS",
            "multiplier": SESSION_OFF_HOURS_MULT,
            "pairs_allowed": core_pairs,
            "is_overlap": False,
            "aggression": "MINIMAL",
        }

def is_rollover_window() -> bool:
    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60.0
    return 20.75 <= hour < 21.25

def is_weekend() -> bool:
    now = datetime.now(timezone.utc)
    if now.weekday() == 4 and now.hour >= 21:
        return True
    if now.weekday() == 5:
        return True
    if now.weekday() == 6 and now.hour < 21:
        return True
    return False

# ═══════════════════════════════════════════════════════════════
#  OANDA API
# ═══════════════════════════════════════════════════════════════

def _oanda_headers() -> dict:
    return {"Authorization": f"Bearer {OANDA_API_KEY}", "Content-Type": "application/json"}

def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(_oanda_headers())
        _thread_local.session = s
    return _thread_local.session

def oanda_get(path: str, params: dict = None) -> dict:
    url = f"{OANDA_API_URL}{path}"
    for attempt in range(HTTP_RETRIES):
        try:
            r = _get_session().get(url, params=params or {}, timeout=10)
            if r.status_code in {429, 500, 502, 503, 504}:
                if attempt < HTTP_RETRIES - 1:
                    time.sleep((2 ** attempt) * HTTP_RETRY_DELAY)
                    continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            response = getattr(e, "response", None)
            status_code = response.status_code if response is not None else "unknown"
            body = (getattr(response, "text", "") or "")[:300] if response is not None else ""
            detail = body or str(e) or e.__class__.__name__
            log.error(f"OANDA GET {path} failed with HTTP {status_code}: {detail}")
            if attempt < HTTP_RETRIES - 1:
                time.sleep((2 ** attempt) * HTTP_RETRY_DELAY)
                continue
            raise
        except (requests.ConnectionError, requests.Timeout) as e:
            log.error(f"OANDA GET {path} connection failure: {e}")
            if attempt < HTTP_RETRIES - 1:
                time.sleep((2 ** attempt) * HTTP_RETRY_DELAY)
            else:
                raise
        except requests.RequestException as e:
            log.error(f"OANDA GET {path} request failure ({e.__class__.__name__}): {e}")
            if attempt < HTTP_RETRIES - 1:
                time.sleep((2 ** attempt) * HTTP_RETRY_DELAY)
            else:
                raise
    raise requests.RequestException(f"GET {path} failed after {HTTP_RETRIES} attempts")

def oanda_post(path: str, data: dict) -> dict:
    url = f"{OANDA_API_URL}{path}"
    for attempt in range(HTTP_RETRIES):
        try:
            r = _get_session().post(url, json=data, timeout=10)
            if r.status_code in {429, 500, 502, 503, 504} and attempt < HTTP_RETRIES - 1:
                time.sleep((2 ** attempt) * HTTP_RETRY_DELAY)
                continue
            if r.status_code >= 400:
                try:
                    payload = r.json()
                except ValueError:
                    payload = {}
                error_body = _extract_oanda_error_message(payload, r.text[:500])
                log.error(f"OANDA POST {path} error {r.status_code}: {error_body}")
                if payload:
                    payload["error"] = error_body
                    payload["status_code"] = r.status_code
                    return payload
                return {"error": error_body, "status_code": r.status_code}
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt < HTTP_RETRIES - 1:
                time.sleep((2 ** attempt) * HTTP_RETRY_DELAY)
            else:
                raise
    raise requests.RequestException(f"POST {path} failed after {HTTP_RETRIES} attempts")

def oanda_put(path: str, data: dict) -> dict:
    url = f"{OANDA_API_URL}{path}"
    try:
        r = _get_session().put(url, json=data, timeout=10)
        if r.status_code >= 400:
            try:
                payload = r.json()
            except ValueError:
                payload = {}
            error_body = _extract_oanda_error_message(payload, r.text[:500])
            log.error(f"OANDA PUT {path} error {r.status_code}: {error_body}")
            if payload:
                payload["error"] = error_body
                payload["status_code"] = r.status_code
                return payload
            return {"error": error_body, "status_code": r.status_code}
        return r.json()
    except Exception as e:
        log.error(f"OANDA PUT {path} failed: {e}")
        return {"error": str(e)}

def get_account_summary() -> dict:
    if PAPER_TRADE:
        pnl = sum(t.get("pnl", 0) for t in trade_history)
        return {
            "balance": PAPER_BALANCE + pnl,
            "unrealizedPL": 0,
            "NAV": PAPER_BALANCE + pnl,
            "marginUsed": 0,
            "marginAvailable": PAPER_BALANCE + pnl,
            "openTradeCount": len(open_trades),
            "currency": "GBP" if ACCOUNT_TYPE == "spread_bet" else "USD",
        }
    try:
        data = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/summary")
        acct = data.get("account", {})
        return {
            "balance": float(acct.get("balance", 0)),
            "unrealizedPL": float(acct.get("unrealizedPL", 0)),
            "NAV": float(acct.get("NAV", 0)),
            "marginUsed": float(acct.get("marginUsed", 0)),
            "marginAvailable": float(acct.get("marginAvailable", 0)),
            "openTradeCount": int(acct.get("openTradeCount", 0)),
            "currency": acct.get("currency", "GBP"),
        }
    except Exception as e:
        log.error(f"Failed to get account summary: {e}")
        return {"balance": 0, "currency": "GBP"}


def fetch_open_trades_from_oanda() -> list[dict]:
    if PAPER_TRADE or not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        return []
    try:
        data = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/openTrades")
        trades = data.get("trades", [])
        return trades if isinstance(trades, list) else []
    except Exception as e:
        log.error(f"Failed to fetch open trades from OANDA: {e}")
        return []


def get_account_currency() -> str:
    if PAPER_TRADE:
        return "GBP" if ACCOUNT_TYPE == "spread_bet" else "USD"
    acct = get_account_summary()
    return acct.get("currency", "GBP")


def _build_trade_from_oanda(raw_trade: dict, existing: dict | None = None) -> dict | None:
    try:
        current_units = float(raw_trade.get("currentUnits", raw_trade.get("initialUnits", 0)))
        if current_units == 0:
            return None
        instrument = raw_trade.get("instrument", "")
        entry_price = float(raw_trade.get("price", 0))
        if not instrument or entry_price <= 0:
            return None

        trade = dict(existing) if existing else {}
        trade_id = str(raw_trade.get("id", trade.get("id", "")))
        direction = "LONG" if current_units > 0 else "SHORT"
        price_precision = 3 if "JPY" in instrument else 5

        trade.update({
            "id": trade_id,
            "instrument": instrument,
            "direction": direction,
            "entry_price": entry_price,
            "units": current_units,
            "opened_at": raw_trade.get("openTime", trade.get("opened_at", datetime.now(timezone.utc).isoformat())),
            "opened_ts": trade.get("opened_ts", time.time()),
            "label": trade.get("label", "RESTORED"),
            "tp_price": trade.get("tp_price"),
            "sl_price": trade.get("sl_price"),
            "trail_pips": trade.get("trail_pips"),
            "tp_pips": trade.get("tp_pips", 0),
            "sl_pips": trade.get("sl_pips", 0),
            "score": trade.get("score", 0),
            "entry_signal": trade.get("entry_signal", "RESTORED"),
            "session_at_entry": trade.get("session_at_entry", "RESTORED"),
            "partial_tp_hit": trade.get("partial_tp_hit", False),
            "highest_price": trade.get("highest_price", entry_price),
            "lowest_price": trade.get("lowest_price", entry_price),
            "last_new_high_at": trade.get("last_new_high_at", time.time()),
            "unrealized_pnl": trade.get("unrealized_pnl", 0),
        })

        if raw_trade.get("takeProfitOrder") and trade.get("tp_price") is None:
            tp_raw = raw_trade["takeProfitOrder"].get("price")
            if tp_raw is not None:
                trade["tp_price"] = round(float(tp_raw), price_precision)
        if raw_trade.get("stopLossOrder") and trade.get("sl_price") is None:
            sl_raw = raw_trade["stopLossOrder"].get("price")
            if sl_raw is not None:
                trade["sl_price"] = round(float(sl_raw), price_precision)
        if raw_trade.get("trailingStopLossOrder") and trade.get("trail_pips") is None:
            distance_raw = raw_trade["trailingStopLossOrder"].get("distance")
            if distance_raw is not None:
                trade["trail_pips"] = round(price_to_pips(instrument, float(distance_raw)), 1)

        if trade.get("tp_price") is not None:
            trade["tp_pips"] = round(price_to_pips(instrument, abs(float(trade["tp_price"]) - entry_price)), 1)
        if trade.get("sl_price") is not None:
            trade["sl_pips"] = round(price_to_pips(instrument, abs(float(trade["sl_price"]) - entry_price)), 1)

        return trade
    except Exception as e:
        log.warning(f"Failed to normalize OANDA trade {raw_trade.get('id', '?')}: {e}")
        return None


def sync_open_trades_with_oanda(reason: str = "manual") -> bool:
    global open_trades
    if PAPER_TRADE or not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        return False

    broker_trades = fetch_open_trades_from_oanda()
    existing_by_id = {str(t.get("id")): t for t in open_trades if t.get("id")}
    synced_trades = []
    for raw_trade in broker_trades:
        trade_id = str(raw_trade.get("id", ""))
        normalized = _build_trade_from_oanda(raw_trade, existing_by_id.get(trade_id))
        if normalized is not None:
            synced_trades.append(normalized)

    old_ids = set(existing_by_id.keys())
    new_ids = {str(t.get("id")) for t in synced_trades if t.get("id")}
    changed = old_ids != new_ids or len(synced_trades) != len(open_trades)
    if not changed:
        for synced in synced_trades:
            existing = existing_by_id.get(str(synced.get("id")))
            if existing != synced:
                changed = True
                break

    if changed:
        open_trades = synced_trades
        save_state()
        log.info(f"🔄 Synced open trades from OANDA ({reason}): {len(open_trades)} open")
    return changed

def get_current_price(instrument: str) -> dict:
    with _price_lock:
        cached = _live_prices.get(instrument)
        if cached and time.time() - cached[2] < 30:
            if cached[0] > 0 and cached[1] > 0:
                mark_pair_success(instrument, "quote")
            return {"bid": cached[0], "ask": cached[1], "spread": cached[1] - cached[0]}

    try:
        data = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing",
                         {"instruments": instrument})
        prices = data.get("prices", [])
        if prices:
            p = prices[0]
            bid = float(p["bids"][0]["price"]) if p.get("bids") else 0
            ask = float(p["asks"][0]["price"]) if p.get("asks") else 0
            if bid <= 0 or ask <= 0:
                mark_pair_failure(instrument, "missing bid/ask", "quote")
                return {"bid": 0, "ask": 0, "spread": 999}
            with _price_lock:
                _live_prices[instrument] = (bid, ask, time.time())
            mark_pair_success(instrument, "quote")
            return {"bid": bid, "ask": ask, "spread": ask - bid}
    except Exception as e:
        log.debug(f"Price fetch failed for {instrument}: {e}")
        mark_pair_failure(instrument, str(e), "quote")
    return {"bid": 0, "ask": 0, "spread": 999}

def get_spread_pips(instrument: str) -> float:
    p = get_current_price(instrument)
    return price_to_pips(instrument, p["spread"])


def get_mid_price(instrument: str) -> float | None:
    price = get_current_price(instrument)
    bid = float(price.get("bid", 0) or 0)
    ask = float(price.get("ask", 0) or 0)
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2


def estimate_fx_conversion_rate(from_currency: str, to_currency: str, visited: set[tuple[str, str]] | None = None) -> float | None:
    if from_currency == to_currency:
        return 1.0
    if visited is None:
        visited = set()
    key = (from_currency, to_currency)
    if key in visited:
        return None
    visited.add(key)

    direct_mid = get_mid_price(f"{from_currency}_{to_currency}")
    if direct_mid is not None:
        return direct_mid

    inverse_mid = get_mid_price(f"{to_currency}_{from_currency}")
    if inverse_mid is not None and inverse_mid > 0:
        return 1.0 / inverse_mid

    for bridge in ("USD", "EUR", "JPY", "GBP", "AUD"):
        if bridge in {from_currency, to_currency}:
            continue
        leg_one = estimate_fx_conversion_rate(from_currency, bridge, visited)
        if leg_one is None:
            continue
        leg_two = estimate_fx_conversion_rate(bridge, to_currency, visited)
        if leg_two is not None:
            return leg_one * leg_two
    return None


def estimate_trade_budget(instrument: str, units: float, entry_price: float, account_currency: str) -> dict:
    base_currency, quote_currency = instrument.split("_")
    base_units = abs(float(units))
    quote_notional = base_units * entry_price
    quote_to_account = estimate_fx_conversion_rate(quote_currency, account_currency)
    base_to_account = estimate_fx_conversion_rate(base_currency, account_currency)

    notional_account = None
    if quote_to_account is not None:
        notional_account = quote_notional * quote_to_account
    elif base_to_account is not None:
        notional_account = base_units * base_to_account

    margin_account = None
    if notional_account is not None and LEVERAGE > 0:
        margin_account = notional_account / LEVERAGE

    return {
        "base_currency": base_currency,
        "quote_currency": quote_currency,
        "base_units": base_units,
        "quote_notional": quote_notional,
        "notional_account": notional_account,
        "margin_account": margin_account,
    }

def fetch_candles(instrument: str, granularity: str = "M5", count: int = 100, price: str = "M") -> pd.DataFrame | None:
    cache_key = (instrument, granularity, count, price)
    with _kline_cache_lock:
        cached = _kline_cache.get(cache_key)
        if cached:
            df_cached, fetched_at = cached
            if time.time() - fetched_at < KLINE_CACHE_TTL:
                return df_cached

    try:
        data = oanda_get(
            f"/v3/instruments/{instrument}/candles",
            {"granularity": granularity, "count": count, "price": price}
        )
        candles = data.get("candles", [])
        if not candles:
            log.warning(f"⚠️ OANDA candle fetch returned no candles for {instrument} {granularity}/{count} price={price}")
            log.debug(f"OANDA response keys for {instrument}: {list(data.keys())}")
            mark_pair_failure(instrument, f"no candles {granularity}", "candle", timeframe=granularity)
            return None

        rows = []
        for c in candles:
            if not c.get("complete", True) and granularity != "M1":
                continue
            mid = c.get("mid", {})
            rows.append({
                "time":   pd.to_datetime(c.get("time"), utc=True, errors="coerce"),
                "open":   float(mid.get("o", 0)),
                "high":   float(mid.get("h", 0)),
                "low":    float(mid.get("l", 0)),
                "close":  float(mid.get("c", 0)),
                "volume": int(c.get("volume", 0)),
                "bid_close": float(c.get("bid", {}).get("c", 0)) if c.get("bid") else 0,
                "ask_close": float(c.get("ask", {}).get("c", 0)) if c.get("ask") else 0,
            })

        if not rows:
            mark_pair_failure(instrument, f"no valid candle rows {granularity}", "candle", timeframe=granularity)
            return None

        df = pd.DataFrame(rows)
        df = df.dropna(subset=["close"])

        if len(df) < 20:
            log.debug(f"OANDA candle fetch produced only {len(df)} valid rows for {instrument} {granularity}/{count}")
            mark_pair_failure(instrument, f"short candle history {granularity} ({len(df)})", "candle", timeframe=granularity)
        else:
            mark_pair_success(instrument, "candle", timeframe=granularity)

        with _kline_cache_lock:
            if len(_kline_cache) >= MAX_KLINE_CACHE:
                stale = [k for k, (_, t) in _kline_cache.items() if time.time() - t > KLINE_CACHE_TTL]
                for k in stale:
                    del _kline_cache[k]
            _kline_cache[cache_key] = (df, time.time())

        return df if len(df) >= 20 else None

    except Exception as e:
        log.debug(f"Candle fetch error {instrument}/{granularity}: {e}")
        mark_pair_failure(instrument, str(e), "candle", timeframe=granularity)
        return None

def calculate_units(instrument: str, balance: float, sl_pips: float,
                    risk_pct: float, kelly_mult: float = 1.0,
                    account_currency: str | None = None) -> float:
    risk_amount = balance * risk_pct * kelly_mult
    if sl_pips <= 0:
        sl_pips = 10
    if ACCOUNT_TYPE == "spread_bet" and not uses_oanda_native_units():
        stake = risk_amount / sl_pips
        return max(SPREAD_BET_MIN_STAKE, round(stake, 2))
    else:
        currency = account_currency or get_account_currency()
        pip_value_per_unit = pip_value(instrument, 1.0, currency)
        if pip_value_per_unit <= 0:
            pip_value_per_unit = pip_size(instrument)
        units = risk_amount / (sl_pips * pip_value_per_unit)
        return max(1, int(round(units)))

def place_order(instrument: str, units: float, direction: str,
                tp_price: float = None, sl_price: float = None,
                trailing_sl_pips: float = None, label: str = "") -> dict:
    if PAPER_TRADE:
        price = get_current_price(instrument)
        entry = price["ask"] if direction == "LONG" else price["bid"]
        if entry > 0:
            mark_pair_success(instrument, "order")
        return {
            "id": f"PAPER_{int(time.time()*1000)}",
            "instrument": instrument,
            "units": units if direction == "LONG" else -units,
            "price": entry,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "trailing_sl_pips": trailing_sl_pips,
        }

    native_units = max(1, int(round(abs(units))))
    signed_units = native_units if direction == "LONG" else -native_units

    order_body = {
        "order": {
            "type": "MARKET",
            "instrument": instrument,
            "units": str(signed_units),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
        }
    }

    if tp_price:
        order_body["order"]["takeProfitOnFill"] = {
            "price": f"{tp_price:.5f}" if "JPY" not in instrument else f"{tp_price:.3f}"
        }
    if sl_price:
        order_body["order"]["stopLossOnFill"] = {
            "price": f"{sl_price:.5f}" if "JPY" not in instrument else f"{sl_price:.3f}"
        }
    if trailing_sl_pips:
        dist = pips_to_price(instrument, trailing_sl_pips)
        order_body["order"]["trailingStopLossOnFill"] = {
            "distance": f"{dist:.5f}" if "JPY" not in instrument else f"{dist:.3f}"
        }

    log.info(f"[{label}] Placing {direction} order: {instrument} | "
             f"units={signed_units} | TP={tp_price} | SL={sl_price} | trail={trailing_sl_pips}p")

    result = oanda_post(f"/v3/accounts/{OANDA_ACCOUNT_ID}/orders", order_body)

    if "error" in result or result.get("orderRejectTransaction") or result.get("orderCancelTransaction"):
        reject_message = _extract_oanda_error_message(result, str(result.get("error", "order rejected")))
        status_code = result.get("status_code")
        hard_failure = _is_hard_broker_rejection(reject_message, status_code)
        halted_market = _is_market_halted_reason(reject_message)
        if halted_market:
            log.warning(f"[{label}] Market halted for {instrument}: {reject_message}")
        else:
            log.error(f"[{label}] Order failed: {reject_message}")
        mark_pair_failure(instrument, reject_message[:200], "order", severity="hard" if hard_failure else "soft")
        if halted_market:
            telegram(
                f"⏸️ <b>{label} Entry Paused</b>\n"
                f"{instrument} {direction}\n"
                f"Market halted at broker. Pair temporarily blocked."
            )
        else:
            telegram(f"⚠️ <b>{label} Order Failed</b>\n{instrument} {direction}\n{reject_message[:200]}")
        return {}

    fill = result.get("orderFillTransaction", {})
    if fill:
        trade_id = fill.get("tradeOpened", {}).get("tradeID") or fill.get("id")
        fill_price = float(fill.get("price", 0))
        mark_pair_success(instrument, "order")
        log.info(f"[{label}] Order filled: {instrument} @ {fill_price} | trade_id={trade_id}")
        return {
            "id": trade_id,
            "instrument": instrument,
            "units": float(fill.get("units", signed_units)),
            "price": fill_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "trailing_sl_pips": trailing_sl_pips,
        }

    log.warning(f"[{label}] Order response has no fill: {json.dumps(result)[:300]}")
    mark_pair_failure(instrument, "order returned without fill", "order")
    return {}

def close_trade_result(trade_id: str, label: str = "", units: float = None,
                       instrument: str = "") -> tuple[bool, str | None]:
    if PAPER_TRADE:
        return True, None
    path = f"/v3/accounts/{OANDA_ACCOUNT_ID}/trades/{trade_id}/close"
    body = {}
    if units:
        body["units"] = str(abs(units))
    result = oanda_put(path, body)
    if "error" in result or result.get("orderRejectTransaction") or result.get("orderCancelTransaction"):
        reject_message = _extract_oanda_error_message(result, str(result.get("error", "close rejected")))
        log.error(f"[{label}] Close trade {trade_id} failed: {reject_message}")
        if instrument:
            hard_failure = _is_hard_broker_rejection(reject_message, result.get("status_code"))
            mark_pair_failure(instrument, reject_message[:200], "close", severity="hard" if hard_failure else "soft")
        return False, reject_message
    fill = result.get("orderFillTransaction", {})
    if fill:
        close_price = float(fill.get("price", 0))
        pnl = float(fill.get("pl", 0))
        log.info(f"[{label}] Trade {trade_id} closed @ {close_price} | P&L: {pnl:.2f}")
        if instrument:
            mark_pair_success(instrument, "close")
        return True, None
    no_fill_message = f"close returned no fill: {json.dumps(result)[:300]}"
    log.error(f"[{label}] Close trade {trade_id} {no_fill_message}")
    if instrument:
        mark_pair_failure(instrument, no_fill_message[:200], "close")
    return False, no_fill_message


def close_trade(trade_id: str, label: str = "", units: float = None, instrument: str = "") -> bool:
    success, _ = close_trade_result(trade_id, label, units, instrument=instrument)
    return success

def modify_trade(trade_id: str, tp_price: float = None, sl_price: float = None,
                 trailing_sl_pips: float = None, instrument: str = "",
                 label: str = "") -> bool:
    if PAPER_TRADE:
        return True
    path = f"/v3/accounts/{OANDA_ACCOUNT_ID}/trades/{trade_id}/orders"
    body = {}
    fmt = ".3f" if "JPY" in instrument else ".5f"

    if tp_price is not None:
        body["takeProfit"] = {"price": format(tp_price, fmt)}
    if sl_price is not None:
        body["stopLoss"] = {"price": format(sl_price, fmt)}
    if trailing_sl_pips is not None:
        dist = pips_to_price(instrument, trailing_sl_pips)
        body["trailingStopLoss"] = {"distance": format(dist, fmt)}

    if not body:
        return True
    result = oanda_put(path, body)
    if "error" in result:
        log.debug(f"[{label}] Modify trade {trade_id} warning: {result.get('error', '')[:200]}")
        return False
    return True

# ═══════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════════════

def calc_ema(series: pd.Series, period: int) -> pd.Series:
    return core_calc_ema(series, period)

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    return core_calc_rsi(series, period)

def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    return core_calc_atr(df, period)

def calc_atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    return core_calc_atr_pct(df, period)

def calc_bollinger_bands(df: pd.DataFrame, period: int = 20, std_mult: float = 2.0) -> dict:
    return core_calc_bollinger_bands(df, period, std_mult)

def _percentile_rank(series: pd.Series) -> float:
    return core_percentile_rank(series)

def calc_macd(df: pd.DataFrame) -> dict:
    return core_calc_macd(df)

def keltner_squeeze(df: pd.DataFrame, bb_period: int = 20, kc_period: int = 20,
                    kc_mult: float = 1.5) -> dict:
    return core_keltner_squeeze(df, bb_period, kc_period, kc_mult)

# ═══════════════════════════════════════════════════════════════
#  TELEGRAM & COMMANDS
# ═══════════════════════════════════════════════════════════════

def telegram(msg: str, parse_mode: str = "HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for attempt in range(2):
        try:
            r = _get_session().post(
                url,
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": parse_mode},
                timeout=8,
            )
            if r.ok:
                return
            body = r.json() if r.content else {}
            if r.status_code == 400 and "parse" in body.get("description", "").lower():
                r2 = _get_session().post(
                    url,
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": re.sub(r'<[^>]+>', '', msg)},
                    timeout=8,
                )
                if r2.ok:
                    return
                return
            log.warning(f"Telegram failed (HTTP {r.status_code}): {r.text[:200]}")
        except Exception as e:
            if attempt == 0:
                time.sleep(1)
            else:
                log.warning(f"Telegram failed: {e}")

def scanner_log(msg: str):
    _scanner_log_buffer.append(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")
    log.info(msg)


def start_scan_cycle() -> None:
    global _recent_scan_decisions, _last_scan_cycle_at, _last_scan_cycle_summary
    _recent_scan_decisions = []
    _last_scan_cycle_at = datetime.now(timezone.utc).strftime("%H:%M:%S")
    _last_scan_cycle_summary = {"active": 0, "healthy": 0, "tradable": 0, "active_pairs": [], "tradable_pairs": []}


def _format_pair_list(pairs: list[str], limit: int = 4) -> str:
    if not pairs:
        return "none"
    shown = ", ".join(pairs[:limit])
    if len(pairs) > limit:
        shown += f" (+{len(pairs) - limit})"
    return shown


def set_scan_cycle_summary(active_pairs: list[str], healthy_pairs: list[str], tradable_pairs: list[str]) -> None:
    global _last_scan_cycle_summary
    _last_scan_cycle_summary = {
        "active": len(active_pairs),
        "healthy": len(healthy_pairs),
        "tradable": len(tradable_pairs),
        "active_pairs": list(active_pairs),
        "tradable_pairs": list(tradable_pairs),
    }


def record_scan_decision(strategy: str, instrument: str, reason: str, emoji: str) -> None:
    for idx, item in enumerate(_recent_scan_decisions):
        if item["strategy"] == strategy:
            _recent_scan_decisions[idx] = {
                "at": _last_scan_cycle_at or datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "strategy": strategy,
                "instrument": instrument,
                "reason": reason,
                "emoji": emoji,
            }
            return

    _recent_scan_decisions.append({
        "at": datetime.now(timezone.utc).strftime("%H:%M:%S"),
        "strategy": strategy,
        "instrument": instrument,
        "reason": reason,
        "emoji": emoji,
    })


def _set_scan_reject_reason(strategy: str, instrument: str, reason: str) -> None:
    _scan_reject_reasons[(strategy, instrument)] = reason


def _pop_scan_reject_reason(strategy: str, instrument: str) -> str | None:
    return _scan_reject_reasons.pop((strategy, instrument), None)


def _find_best_opportunity(strategy: str, pairs: list[str], session: dict, scorer) -> tuple[dict | None, str | None, str | None]:
    best = None
    reject_pair = None
    reject_reason = None
    blocked_pair = None
    blocked_reason = None
    for pair in pairs:
        opp = scorer(pair, session)
        if opp:
            block_reason = get_entry_block_reason(opp["instrument"], opp["direction"])
            if block_reason is None:
                if best is None or opp["score"] > best["score"]:
                    best = opp
            elif blocked_reason is None:
                blocked_pair = opp["instrument"]
                blocked_reason = block_reason
            continue
        reason = _pop_scan_reject_reason(strategy, pair)
        if reject_reason is None and reason:
            reject_pair = pair
            reject_reason = reason
    if best is None and blocked_reason is not None:
        return None, blocked_pair, blocked_reason
    return best, reject_pair, reject_reason

_last_telegram_update = 0

def poll_telegram_commands():
    global _last_telegram_update, _paused
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        r = _get_session().get(url, params={"offset": _last_telegram_update + 1, "timeout": 1}, timeout=5)
        if not r.ok:
            return
        updates = r.json().get("result", [])
        for upd in updates:
            _last_telegram_update = upd["update_id"]
            msg = upd.get("message", {})
            text = msg.get("text", "").strip().lower()
            chat_id = str(msg.get("chat", {}).get("id", ""))
            if chat_id != TELEGRAM_CHAT_ID:
                continue
            if text == "/status":
                _handle_status_command()
            elif text == "/metrics":
                _handle_metrics_command()
            elif text == "/pause":
                _paused = True
                save_state()
                telegram("⏸️ <b>Bot paused.</b> No new entries. Open trades still monitored.")
            elif text == "/resume":
                _paused = False
                save_state()
                telegram("▶️ <b>Bot resumed.</b> Entries enabled.")
            elif text == "/close":
                closed_count, failed_count = close_all_open_positions(reason="MANUAL_CLOSE")
                if closed_count == 0 and failed_count == 0:
                    telegram("📭 <b>No open positions.</b>")
                elif failed_count == 0:
                    telegram(f"🛑 <b>All positions closed.</b> {closed_count} trade(s) closed.")
                else:
                    telegram(
                        f"⚠️ <b>Close all completed with errors.</b>\n"
                        f"Closed: {closed_count}\n"
                        f"Failed: {failed_count}"
                    )
            elif text == "/session":
                session = get_current_session()
                telegram(
                    f"🕐 <b>Session:</b> {session['name']}\n"
                    f"Aggression: {session['aggression']}\n"
                    f"Multiplier: {session['multiplier']:.2f}\n"
                    f"Active pairs: {len(session['pairs_allowed'])}"
                )
            elif text == "/help":
                telegram(
                    "📋 <b>Commands:</b>\n"
                    "/status — Open trades & account\n"
                    "/metrics — Win rate, PF, Sharpe\n"
                    "/session — Current trading session\n"
                    "/close — Close all open positions\n"
                    "/pause — Stop new entries\n"
                    "/resume — Resume entries\n"
                    "/help — This message"
                )
    except Exception as e:
        log.debug(f"Telegram poll error: {e}")

def _handle_status_command():
    if not PAPER_TRADE and OANDA_API_KEY and OANDA_ACCOUNT_ID:
        sync_open_trades_with_oanda(reason="status")
    acct = get_account_summary()
    session = get_current_session()
    live_unrealized = 0.0
    live_unrealized_pips = 0.0
    for trade in open_trades:
        price_data = get_current_price(trade["instrument"])
        mark_price = price_data["bid"] if trade.get("direction") == "LONG" else price_data["ask"]
        if mark_price <= 0:
            continue
        entry_price = float(trade.get("entry_price", 0))
        if entry_price <= 0:
            continue
        ps = pip_size(trade["instrument"])
        if trade.get("direction") == "LONG":
            pnl_pips = (mark_price - entry_price) / ps
        else:
            pnl_pips = (entry_price - mark_price) / ps
        trade["unrealized_pnl"] = round(pnl_pips, 1)
        live_unrealized_pips += pnl_pips
        live_unrealized += pnl_pips * pip_value(trade["instrument"], trade.get("units", 1))

    broker_unrealized = float(acct.get("unrealizedPL", 0))
    currency = acct.get("currency", "GBP")
    if open_trades:
        unrealized_text = f"{currency}{broker_unrealized:+,.2f} | live {live_unrealized_pips:+.1f}p"
    else:
        unrealized_text = f"{currency}{broker_unrealized:+,.2f}"
    nav_text = f"{currency}{float(acct.get('NAV', 0)):,.2f}"
    margin_used_text = f"{currency}{float(acct.get('marginUsed', 0)):,.2f}"
    margin_available_text = f"{currency}{float(acct.get('marginAvailable', 0)):,.2f}"

    paused_pairs = get_paused_pairs_by_news(session["pairs_allowed"])
    degraded_pairs, blocked_pairs = get_pair_health_buckets(session["pairs_allowed"])
    global_degraded_pairs, global_blocked_pairs = get_pair_health_buckets()
    paused_text = ", ".join(paused_pairs[:4]) if paused_pairs else "none"
    if len(paused_pairs) > 4:
        paused_text += f" (+{len(paused_pairs) - 4})"
    pair_health_parts = []
    if blocked_pairs:
        blocked_text = ", ".join(blocked_pairs[:3])
        if len(blocked_pairs) > 3:
            blocked_text += f" (+{len(blocked_pairs) - 3})"
        pair_health_parts.append(f"blocked {blocked_text}")
    if degraded_pairs:
        degraded_text = ", ".join(degraded_pairs[:3])
        if len(degraded_pairs) > 3:
            degraded_text += f" (+{len(degraded_pairs) - 3})"
        pair_health_parts.append(f"degraded {degraded_text}")
    pair_health_text = " | ".join(pair_health_parts) if pair_health_parts else "all healthy"
    global_health_parts = []
    if global_blocked_pairs:
        blocked_text = ", ".join(global_blocked_pairs[:3])
        if len(global_blocked_pairs) > 3:
            blocked_text += f" (+{len(global_blocked_pairs) - 3})"
        global_health_parts.append(f"blocked {blocked_text}")
    if global_degraded_pairs:
        degraded_text = ", ".join(global_degraded_pairs[:3])
        if len(global_degraded_pairs) > 3:
            degraded_text += f" (+{len(global_degraded_pairs) - 3})"
        global_health_parts.append(f"degraded {degraded_text}")
    global_health_text = " | ".join(global_health_parts) if global_health_parts else "none"
    if _pending_close_retries:
        next_retry = min(_pending_close_retries.values(), key=lambda item: float(item.get("next_retry_at", 0.0)))
        next_retry_at = datetime.fromtimestamp(float(next_retry.get("next_retry_at", 0.0)), timezone.utc).strftime("%H:%M UTC")
        forced_close_text = f"{next_retry.get('instrument', '?')} @ {next_retry_at}"
    else:
        forced_close_text = "none"
    scan_mode = _last_scan_pool_status.get("mode", "primary")
    if scan_mode == "fallback":
        scan_pool_text = (
            f"fallback pool | active {_last_scan_pool_status.get('active', 0)} | "
            f"healthy {_last_scan_pool_status.get('healthy', 0)} | tradable {_last_scan_pool_status.get('tradable', 0)}"
        )
    elif scan_mode == "rebuilt":
        scan_pool_text = (
            f"rebuilt watchlist | active {_last_scan_pool_status.get('active', 0)} | "
            f"healthy {_last_scan_pool_status.get('healthy', 0)} | tradable {_last_scan_pool_status.get('tradable', 0)}"
        )
    else:
        scan_pool_text = (
            f"primary watchlist | active {_last_scan_pool_status.get('active', 0)} | "
            f"healthy {_last_scan_pool_status.get('healthy', 0)} | tradable {_last_scan_pool_status.get('tradable', 0)}"
        )
    latest_scan_text = (
        f"active {_last_scan_cycle_summary.get('active', 0)} | "
        f"healthy {_last_scan_cycle_summary.get('healthy', 0)} | "
        f"tradable {_last_scan_cycle_summary.get('tradable', 0)}"
    )
    latest_active_pairs_text = _format_pair_list(_last_scan_cycle_summary.get("active_pairs", []))
    latest_tradable_pairs_text = _format_pair_list(_last_scan_cycle_summary.get("tradable_pairs", []))
    regime = ('🟢 BULL' if _market_regime_mult < 0.95
              else '🔴 BEAR' if _market_regime_mult > 1.10
              else '⚪ NEUTRAL')
    status_emoji = "⏸️" if _paused else "▶️"
    status_text = "Paused" if _paused else "Running"
    lines = [
        f"📊 <b>Status</b> | {session['name']}",
        f"━━━━━━━━━━━━━━━",
        f"NAV: {nav_text}",
        f"💰 Balance: {acct.get('currency', '£')}{acct.get('balance', 0):,.2f}",
        f"📉 Unrealized: {unrealized_text}",
        f"Margin used: {margin_used_text}",
        f"Margin available: {margin_available_text}",
        f"Open trades: {len(open_trades)}",
        f"Regime: {regime} ({_market_regime_mult:.2f})",
        f"Macro: DXY {f'{_dxy_ema_gap*100:+.2f}%' if _dxy_ema_gap is not None else 'unknown'} | VIX {f'{_vix_level:.1f}' if _vix_level is not None else 'unknown'}",
        f"📰 Active news blackouts: {paused_text}",
        f"🩺 Active pair health: {pair_health_text}",
        f"🌐 Global pair issues: {global_health_text}",
        f"⏳ Forced close retry: {forced_close_text}",
        f"🔎 Scan pool: {scan_pool_text}",
        f"🧮 Last scan breadth: {latest_scan_text}",
        f"📋 Last active pairs: {latest_active_pairs_text}",
        f"✅ Last tradable pairs: {latest_tradable_pairs_text}",
        f"{status_emoji} Bot: {status_text}",
    ]
    if open_trades:
        lines.append("")
        lines.append("📂 <b>Open trades</b>")
        for t in open_trades:
            direction = "🟢" if t.get("direction") == "LONG" else "🔴"
            pnl = t.get("unrealized_pnl", 0)
            lines.append(f"{direction} {t['instrument']} {t['label']} | {pnl:+.2f}p")
    if _recent_scan_decisions:
        lines.append("")
        lines.append(f"🧠 <b>Latest scan</b> ({_last_scan_cycle_at or 'n/a'})")
        strategy_labels = {
            "SCALPER": "SCALP",
            "TREND": "TREND",
            "REVERSAL": "REV",
            "BREAKOUT": "BREAK",
            "CARRY": "CARRY",
            "ASIAN": "ASIAN",
            "POST_NEWS": "NEWS",
            "PULLBACK": "PULL",
        }
        for item in _recent_scan_decisions:
            label = strategy_labels.get(item['strategy'], item['strategy'])
            instrument = item['instrument']
            health_suffix = ""
            if instrument not in {"-", "watchlist"}:
                health_state = get_pair_health_status(instrument)
                if health_state != "healthy":
                    health_suffix = f" | {health_state}"
            lines.append(f"{item['emoji']} {item['at']} {label} {instrument} | {item['reason']}{health_suffix}")
        lines.append("ℹ️ Each line shows the best or first representative rejection for that strategy, not every pair checked.")
    telegram("\n".join(lines))

def _handle_metrics_command():
    if not trade_history:
        telegram("📈 No trades yet.")
        return
    total = len(trade_history)
    wins  = sum(1 for t in trade_history if t.get("pnl", 0) > 0)
    losses = total - wins
    wr = wins / total * 100 if total else 0
    pnls = [t.get("pnl", 0) for t in trade_history]
    total_pnl = sum(pnls)
    avg_win  = np.mean([p for p in pnls if p > 0]) if wins else 0
    avg_loss = np.mean([p for p in pnls if p <= 0]) if losses else 0
    pf = abs(sum(p for p in pnls if p > 0) / sum(p for p in pnls if p < 0)) if any(p < 0 for p in pnls) else 999
    if len(pnls) > 1:
        returns = np.array(pnls)
        sharpe = (returns.mean() / returns.std()) * np.sqrt(252) if returns.std() > 0 else 0
    else:
        sharpe = 0
    by_strat = {}
    for t in trade_history:
        s = t.get("label", "UNKNOWN")
        if s not in by_strat:
            by_strat[s] = {"wins": 0, "total": 0, "pnl": 0}
        by_strat[s]["total"] += 1
        by_strat[s]["pnl"] += t.get("pnl", 0)
        if t.get("pnl", 0) > 0:
            by_strat[s]["wins"] += 1
    strat_lines = []
    for s, d in sorted(by_strat.items()):
        swr = d["wins"] / d["total"] * 100 if d["total"] else 0
        strat_lines.append(f"  {s}: {d['total']} trades | {swr:.0f}% WR | {d['pnl']:+.2f}")
    long_pnl  = sum(t.get("pnl", 0) for t in trade_history if t.get("direction") == "LONG")
    short_pnl = sum(t.get("pnl", 0) for t in trade_history if t.get("direction") == "SHORT")
    telegram(
        f"📈 <b>Metrics</b> ({total} trades)\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Win rate: {wr:.1f}% ({wins}W/{losses}L)\n"
        f"Total P&L: {total_pnl:+.2f}\n"
        f"Avg win: {avg_win:+.2f} | Avg loss: {avg_loss:+.2f}\n"
        f"Profit factor: {pf:.2f}\n"
        f"Sharpe: {sharpe:.2f}\n"
        f"Long P&L: {long_pnl:+.2f} | Short: {short_pnl:+.2f}\n"
        f"\n<b>By Strategy:</b>\n" + "\n".join(strat_lines)
    )

# ═══════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def save_state():
    try:
        payload = {
            "open_trades":           open_trades,
            "trade_history":         trade_history[-500:],
            "consecutive_losses":    _consecutive_losses,
            "streak_paused_at":      _streak_paused_at,
            "paused":                _paused,
            "adaptive_offsets":      _adaptive_offsets,
            "last_rebalance_count":  _last_rebalance_count,
            "pair_cooldowns":        _pair_cooldowns,
            "pair_health":           _pair_health,
            "pending_close_retries": _pending_close_retries,
            "saved_at":              datetime.now(timezone.utc).isoformat(),
        }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, default=str)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.warning(f"State save failed: {e}")

def load_state():
    global open_trades, trade_history, _consecutive_losses, _streak_paused_at
    global _paused, _adaptive_offsets, _last_rebalance_count, _pair_cooldowns, _pair_health, _pending_close_retries
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE) as f:
            d = json.load(f)
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(d.get("saved_at", "2000-01-01T00:00:00+00:00"))
               ).total_seconds()
        open_trades         = d.get("open_trades", [])
        trade_history       = d.get("trade_history", [])
        _consecutive_losses = d.get("consecutive_losses", 0)
        _streak_paused_at   = d.get("streak_paused_at", 0.0)
        _paused             = d.get("paused", False)
        _adaptive_offsets   = d.get("adaptive_offsets",
                                    {"SCALPER": 0.0, "TREND": 0.0, "REVERSAL": 0.0, "BREAKOUT": 0.0,
                                     "CARRY": 0.0, "ASIAN_FADE": 0.0, "POST_NEWS": 0.0, "PULLBACK": 0.0})
        _last_rebalance_count = d.get("last_rebalance_count", 0)
        _pair_cooldowns       = d.get("pair_cooldowns", {})
        _pending_close_retries = d.get("pending_close_retries", {}) if isinstance(d.get("pending_close_retries", {}), dict) else {}
        raw_pair_health       = d.get("pair_health", {})
        _pair_health = {}
        for instrument, rec in raw_pair_health.items():
            merged = _default_pair_health()
            if isinstance(rec, dict):
                merged.update(rec)
            _pair_health[instrument] = merged
        log.info(f"📂 State loaded ({age/60:.0f}min old): "
                 f"{len(open_trades)} open, {len(trade_history)} history")
    except Exception as e:
        log.warning(f"State load failed ({e}) — starting fresh")

# ═══════════════════════════════════════════════════════════════
#  MACRO INTELLIGENCE (now only reads from Redis)
# ═══════════════════════════════════════════════════════════════

def _load_macro_state_from_redis() -> dict | None:
    if REDIS_CLIENT is None:
        return None
    try:
        raw = REDIS_CLIENT.get(REDIS_MACRO_STATE_KEY)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        state = json.loads(raw)
        if isinstance(state, dict):
            return state
    except Exception as e:
        log.warning(f"⚠️ Failed to load macro state from Redis: {e}")
    return None


def load_macro_filters() -> None:
    global macro_filters, _vix_level, _dxy_ema_gap
    try:
        state = _load_macro_state_from_redis()
        if state is not None:
            data = state.get("filters", {})
            if not isinstance(data, dict):
                raise ValueError("Redis macro state filters must be a dict")
            macro_filters = {key.upper(): str(value).upper() for key, value in data.items()}
            # Read VIX and DXY from the same state
            _vix_level = state.get("vix_value")
            _dxy_ema_gap = state.get("dxy_gap")
            log.info(f"📂 Loaded macro filters from Redis: {', '.join(sorted(macro_filters.keys()))}, "
                     f"vix={_vix_level}, dxy_gap={_dxy_ema_gap}")
            return

        # Fallback to file (no VIX/DXY)
        if not os.path.exists(MACRO_FILTER_FILE):
            macro_filters = {}
            log.warning(f"⚠️ Macro filter file not found: {MACRO_FILTER_FILE}")
            return

        with open(MACRO_FILTER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("macro filter file content must be an object")

        macro_filters = {key.upper(): str(value).upper() for key, value in data.items()}
        log.info(f"📂 Loaded macro filters from {MACRO_FILTER_FILE}: {', '.join(sorted(macro_filters.keys()))}")
    except Exception as e:
        macro_filters = {}
        log.warning(f"⚠️ Failed to load macro filter file {MACRO_FILTER_FILE}: {e}")


def refresh_macro_filters() -> bool:
    global _macro_filter_mtime
    if REDIS_CLIENT is not None:
        state = _load_macro_state_from_redis()
        if state is not None:
            generated_at = state.get("generated_at")
            if generated_at and generated_at != _macro_filter_mtime:
                _macro_filter_mtime = generated_at
                load_macro_filters()
                return True
        return False

    try:
        mtime = os.path.getmtime(MACRO_FILTER_FILE)
        if mtime != _macro_filter_mtime:
            _macro_filter_mtime = mtime
            load_macro_filters()
            return True
    except FileNotFoundError:
        if macro_filters:
            macro_filters.clear()
            log.warning(f"⚠️ Macro filter file removed: {MACRO_FILTER_FILE}")
        _macro_filter_mtime = 0.0
    return False


def load_macro_news() -> None:
    global macro_news
    try:
        state = _load_macro_state_from_redis()
        if state is not None:
            data = state.get("news_events", [])
            if not isinstance(data, list):
                raise ValueError("Redis macro state news_events must be a list")
            macro_news = data
            log.info(f"📂 Loaded macro news from Redis: {len(macro_news)} events")
            return

        if not os.path.exists(MACRO_NEWS_FILE):
            macro_news = []
            log.warning(f"⚠️ Macro news file not found: {MACRO_NEWS_FILE}")
            return

        with open(MACRO_NEWS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("macro news file content must be an object")

        raw = data.get("news_events")
        if not isinstance(raw, list):
            raise ValueError("macro news file must contain a news_events list")

        macro_news = raw
        log.info(f"📂 Loaded macro news from {MACRO_NEWS_FILE}: {len(macro_news)} events")
    except Exception as e:
        macro_news = []
        log.warning(f"⚠️ Failed to load macro news file {MACRO_NEWS_FILE}: {e}")


def refresh_macro_news() -> bool:
    global _macro_news_mtime
    if REDIS_CLIENT is not None:
        state = _load_macro_state_from_redis()
        if state is not None:
            generated_at = state.get("generated_at")
            if generated_at and generated_at != _macro_news_mtime:
                _macro_news_mtime = generated_at
                load_macro_news()
                return True
        return False

    try:
        mtime = os.path.getmtime(MACRO_NEWS_FILE)
        if mtime != _macro_news_mtime:
            _macro_news_mtime = mtime
            load_macro_news()
            return True
    except FileNotFoundError:
        if macro_news:
            macro_news.clear()
            log.warning(f"⚠️ Macro news file removed: {MACRO_NEWS_FILE}")
        _macro_news_mtime = 0.0
    return False


def _parse_macro_news_timestamp(value: str) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    text = text.replace(" EST", "-05:00")
    text = text.replace(" EDT", "-04:00")
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_high_impact_news_event(event: dict) -> bool:
    impact = str(event.get("impact") or "").strip().lower()
    return impact in {"high", "red", "3", "3/3", "3 of 3", "high impact"}


def _event_affects_instrument(event: dict, instrument: str) -> bool:
    if not _is_high_impact_news_event(event):
        return False
    currency = str(event.get("currency") or "").strip().upper()
    if not currency or "_" not in instrument:
        return False
    base, quote = instrument.upper().split("_", 1)
    return currency in {base, quote}


def is_pair_paused_by_news(instrument: str, now: datetime | None = None) -> bool:
    if now is None:
        now = datetime.now(timezone.utc)
    for event in macro_news:
        if not _event_affects_instrument(event, instrument):
            continue
        pause_start = event.get("pause_start")
        pause_end = event.get("pause_end")
        if not pause_start or not pause_end:
            continue
        start_ts = _parse_macro_news_timestamp(pause_start)
        end_ts = _parse_macro_news_timestamp(pause_end)
        if start_ts is None or end_ts is None:
            continue
        if start_ts <= now < end_ts:
            return True
    return False


def get_post_news_events_for_instrument(instrument: str, now: datetime | None = None) -> list[dict]:
    if now is None:
        now = datetime.now(timezone.utc)
    matched = []
    for event in macro_news:
        if not _event_affects_instrument(event, instrument):
            continue
        pause_end_str = event.get("pause_end")
        if not pause_end_str:
            continue
        pause_end = _parse_macro_news_timestamp(pause_end_str)
        if pause_end is None:
            continue
        window_end = pause_end + timedelta(minutes=POST_NEWS_WINDOW_MINS)
        if pause_end <= now <= window_end:
            matched.append(event)
    return matched


def get_paused_pairs_by_news(instruments: list[str], now: datetime | None = None) -> list[str]:
    if now is None:
        now = datetime.now(timezone.utc)
    return [instrument for instrument in instruments if is_pair_paused_by_news(instrument, now)]


def update_macro_news_pause() -> None:
    global macro_news_pause_until
    now = datetime.now(timezone.utc)
    active_ends = []
    for event in macro_news:
        if not _is_high_impact_news_event(event):
            continue
        pause_start = event.get("pause_start")
        pause_end = event.get("pause_end")
        if not pause_start or not pause_end:
            continue
        start_ts = _parse_macro_news_timestamp(pause_start)
        end_ts = _parse_macro_news_timestamp(pause_end)
        if start_ts is None or end_ts is None:
            continue
        if start_ts <= now < end_ts:
            active_ends.append(end_ts)
    if active_ends:
        macro_news_pause_until = max(active_ends).timestamp()
    else:
        macro_news_pause_until = 0.0


def apply_macro_directional_bias(instrument: str, signals: dict) -> None:
    bias = macro_filters.get(instrument.upper())
    if bias == "LONG_ONLY":
        signals["long"] += 5
        log.debug(f"📌 Macro bias LONG_ONLY applied to {instrument}")
    elif bias == "SHORT_ONLY":
        signals["short"] += 5
        log.debug(f"📌 Macro bias SHORT_ONLY applied to {instrument}")
    elif bias == "NEUTRAL":
        log.debug(f"📌 Macro bias NEUTRAL applied to {instrument}")


def compute_market_regime(df: pd.DataFrame) -> float:
    if df is None or len(df) < 50:
        return 1.0
    close = df["close"]
    atr = calc_atr(df, 14)
    atr_pct = atr / float(close.iloc[-1]) if float(close.iloc[-1]) > 0 else 0
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr_series = tr.ewm(alpha=1/14, adjust=False).mean()
    atr_ratio = float(atr_series.iloc[-1]) / float(atr_series.iloc[-41:-1].mean()) if len(atr_series) > 40 else 1.0
    ema50 = calc_ema(close, 50)
    ema_gap = float(close.iloc[-1]) / float(ema50.iloc[-1]) - 1
    regime_atr_mult = 1.0
    if atr_ratio > REGIME_HIGH_VOL_ATR_RATIO:
        regime_atr_mult *= REGIME_TIGHTEN_MULT
    elif atr_ratio < REGIME_LOW_VOL_ATR_RATIO:
        regime_atr_mult *= REGIME_LOOSEN_MULT
    if abs(ema_gap) > 0.01:
        regime_atr_mult *= 0.90
    vix_regime = 1.0
    if _vix_level is not None:
        if _vix_level > VIX_HIGH_THRESHOLD:
            vix_regime = 1.30
        elif _vix_level < VIX_LOW_THRESHOLD:
            vix_regime = 0.75
    dxy_regime = 1.20 if abs(_dxy_ema_gap or 0.0) > DXY_REGIME_THRESHOLD else 1.0
    mult = regime_atr_mult * vix_regime * dxy_regime
    return round(mult, 3)


def _build_strategy_scoring_context() -> StrategyScoringContext:
    return StrategyScoringContext(
        get_spread_pips=get_spread_pips,
        fetch_candles=fetch_candles,
        reject=_set_scan_reject_reason,
        mark_pair_failure=mark_pair_failure,
        determine_direction=core_determine_direction,
        get_post_news_events=get_post_news_events_for_instrument,
        apply_macro_directional_bias=apply_macro_directional_bias,
        macro_filters=macro_filters,
        macro_news=macro_news,
        is_pair_paused_by_news=is_pair_paused_by_news,
        market_regime_mult=_market_regime_mult,
        adaptive_offsets=_adaptive_offsets,
        dxy_ema_gap=_dxy_ema_gap,
        dxy_gate_threshold=DXY_GATE_THRESHOLD,
        vix_level=_vix_level,
        vix_low_threshold=VIX_LOW_THRESHOLD,
    )

# ═══════════════════════════════════════════════════════════════
#  DIRECTION DETERMINATION (unchanged)
# ═══════════════════════════════════════════════════════════════

def determine_direction(instrument: str, df_5m: pd.DataFrame,
                        df_1h: pd.DataFrame = None, df_4h: pd.DataFrame = None,
                        strategy: str = "SCALPER") -> str:
    return core_determine_direction(
        instrument,
        df_5m,
        df_1h,
        df_4h,
        strategy=strategy,
        dxy_ema_gap=_dxy_ema_gap,
        dxy_gate_threshold=DXY_GATE_THRESHOLD,
        apply_macro_directional_bias=apply_macro_directional_bias,
    )

# ═══════════════════════════════════════════════════════════════
#  SCORING FUNCTIONS (unchanged)
# ═══════════════════════════════════════════════════════════════

def score_scalper(instrument: str, session: dict) -> dict | None:
    return core_score_scalper(instrument, session, _build_strategy_scoring_context(), globals())

def score_trend(instrument: str, session: dict) -> dict | None:
    return core_score_trend(instrument, session, _build_strategy_scoring_context(), globals())

def score_reversal(instrument: str, session: dict) -> dict | None:
    return core_score_reversal(instrument, session, _build_strategy_scoring_context(), globals())

def score_breakout(instrument: str, session: dict) -> dict | None:
    return core_score_breakout(instrument, session, _build_strategy_scoring_context(), globals())


def score_carry(instrument: str, session: dict) -> dict | None:
    return core_score_carry(instrument, session, _build_strategy_scoring_context(), globals())


def score_asian_fade(instrument: str, session: dict) -> dict | None:
    return core_score_asian_fade(instrument, session, _build_strategy_scoring_context(), globals())


def score_post_news(instrument: str, session: dict) -> dict | None:
    return core_score_post_news(instrument, session, _build_strategy_scoring_context(), globals())


def score_pullback(instrument: str, session: dict) -> dict | None:
    return core_score_pullback(instrument, session, _build_strategy_scoring_context(), globals())


# ═══════════════════════════════════════════════════════════════
#  ENTRY & EXIT MANAGEMENT (unchanged)
# ═══════════════════════════════════════════════════════════════

def _would_breach_correlation_limit(instrument: str, direction: str) -> tuple[bool, int, int]:
    return would_breach_correlation_limit(open_trades, instrument, direction, MAX_CORRELATED_TRADES)


def check_correlation_limit(instrument: str, direction: str) -> bool:
    breached, usd_long, usd_short = _would_breach_correlation_limit(instrument, direction)

    if breached:
        log.info(f"[CORR] Skip {instrument} {direction} — USD exposure limit "
                 f"(long={usd_long}, short={usd_short})")
        return False
    return True


def get_entry_block_reason(instrument: str, direction: str) -> str | None:
    if not is_pair_tradeable(instrument):
        reason = get_pair_health_reason(instrument)
        if _is_market_halted_reason(reason):
            return "market halted"
        return f"pair blocked{f' ({reason[:40]})' if reason else ''}"
    if time.time() < _pair_cooldowns.get(instrument, 0):
        return "cooldown"
    if any(t["instrument"] == instrument for t in open_trades):
        return "pair already open"
    breached, _, _ = _would_breach_correlation_limit(instrument, direction)
    if breached:
        return "correlation limit"
    price_data = get_current_price(instrument)
    entry_price = price_data["ask"] if direction == "LONG" else price_data["bid"]
    if entry_price <= 0:
        return "no live price"
    return None


def get_strategy_entry_block_reason(strategy: str, instrument: str, direction: str) -> str | None:
    block_reason = get_entry_block_reason(instrument, direction)
    if block_reason is not None:
        return block_reason
    if is_pair_paused_by_news(instrument) and strategy != "CARRY":
        return "pre-news risk window"
    return None


def get_entry_risk_multiplier(strategy: str, instrument: str) -> float:
    if is_pair_paused_by_news(instrument):
        return NEWS_WINDOW_RISK_MULT
    return 1.0

def open_trade_entry(opp: dict, label: str, balance: float) -> dict | None:
    instrument = opp["instrument"]
    direction  = opp["direction"]
    acct = get_account_summary()

    block_reason = get_strategy_entry_block_reason(label, instrument, direction)
    if block_reason is not None:
        return None

    kelly_gap = opp["score"] - {"SCALPER": SCALPER_THRESHOLD, "TREND": TREND_THRESHOLD,
                                 "REVERSAL": REVERSAL_THRESHOLD, "BREAKOUT": BREAKOUT_THRESHOLD,
                                 "CARRY": CARRY_THRESHOLD, "ASIAN_FADE": ASIAN_FADE_THRESHOLD,
                                 "POST_NEWS": POST_NEWS_THRESHOLD, "PULLBACK": PULLBACK_THRESHOLD}.get(label, 40)
    kelly_mult = (KELLY_MULT_HIGH_CONF if kelly_gap >= 40
                  else KELLY_MULT_STANDARD if kelly_gap >= 25
                  else KELLY_MULT_SOLID if kelly_gap >= 10
                  else KELLY_MULT_MARGINAL)
    risk_mult = get_entry_risk_multiplier(label, instrument)
    effective_kelly_mult = kelly_mult * risk_mult

    account_currency = acct.get("currency", get_account_currency())
    units = calculate_units(instrument, balance, opp["sl_pips"],
                           MAX_RISK_PER_TRADE, effective_kelly_mult, account_currency)

    price_data = get_current_price(instrument)
    entry_price = price_data["ask"] if direction == "LONG" else price_data["bid"]

    if entry_price <= 0:
        log.error(f"[{label}] No valid price for {instrument}")
        return None

    if uses_oanda_native_units():
        budget_preview = estimate_trade_budget(instrument, units, entry_price, account_currency)
        margin_required = budget_preview.get("margin_account")
        margin_available = float(acct.get("marginAvailable", 0) or 0)
        if margin_required is not None and margin_available > 0 and margin_required > margin_available:
            log.info(f"[{label}] Skip {instrument} {direction} — insufficient margin available ({margin_required:.2f} > {margin_available:.2f} {account_currency})")
            return None

    ps = pip_size(instrument)
    if direction == "LONG":
        tp_price = round(entry_price + opp["tp_pips"] * ps, 5 if "JPY" not in instrument else 3)
        sl_price = round(entry_price - opp["sl_pips"] * ps, 5 if "JPY" not in instrument else 3)
    else:
        tp_price = round(entry_price - opp["tp_pips"] * ps, 5 if "JPY" not in instrument else 3)
        sl_price = round(entry_price + opp["sl_pips"] * ps, 5 if "JPY" not in instrument else 3)

    trail_pips = opp.get("trail_pips")
    result = place_order(instrument, units, direction, tp_price, sl_price, trail_pips, label)
    if not result or not result.get("id"):
        return None

    actual_entry = result.get("price", entry_price)

    trade = {
        "id":             result["id"],
        "label":          label,
        "instrument":     instrument,
        "direction":      direction,
        "entry_price":    actual_entry,
        "units":          result.get("units", units),
        "tp_price":       tp_price,
        "sl_price":       sl_price,
        "trail_pips":     trail_pips,
        "tp_pips":        opp["tp_pips"],
        "sl_pips":        opp["sl_pips"],
        "highest_price":  actual_entry,
        "lowest_price":   actual_entry,
        "last_new_high_at": time.time(),
        "opened_at":      datetime.now(timezone.utc).isoformat(),
        "opened_ts":      time.time(),
        "score":          opp["score"],
        "rsi":            opp.get("rsi", 50),
        "entry_signal":   opp.get("entry_signal", "UNKNOWN"),
        "atr":            opp.get("atr", 0),
        "atr_pct":        opp.get("atr_pct", 0),
        "spread_at_entry": opp.get("spread_pips", 0),
        "session_at_entry": get_current_session()["name"],
        "kelly_mult":     effective_kelly_mult,
        "news_risk_mult": risk_mult,
        "partial_tp_hit": False,
        "unrealized_pnl": 0,
    }

    if label == "TREND" and opp.get("partial_tp_pips"):
        if direction == "LONG":
            trade["partial_tp_price"] = actual_entry + opp["partial_tp_pips"] * ps
        else:
            trade["partial_tp_price"] = actual_entry - opp["partial_tp_pips"] * ps

    _pair_cooldowns[instrument] = time.time() + PAIR_COOLDOWN_SECS
    save_state()

    session = get_current_session()
    account_currency = acct.get("currency", account_currency)
    nav_value = float(acct.get("NAV", balance) or balance or 0)
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
    risk_amount = balance * MAX_RISK_PER_TRADE * effective_kelly_mult
    trail_text = f"{trail_pips}p" if trail_pips else "None"
    rsi_text = f"{opp.get('rsi', 0):.1f}" if opp.get("rsi") is not None else "n/a"
    vol_text = f"{opp.get('vol_ratio', 0):.2f}x" if opp.get("vol_ratio") is not None else "n/a"
    actual_units = abs(float(result.get("units", units)))
    base_currency, quote_currency = instrument.split("_")
    budget = estimate_trade_budget(instrument, actual_units, actual_entry, account_currency)
    unit_text = f"{actual_units:.0f} {base_currency}"
    notional_text = f"{budget['base_units']:.0f} {base_currency} (~{budget['quote_notional']:.2f} {quote_currency})"
    if budget["margin_account"] is not None:
        margin_text = f"~{account_currency} {budget['margin_account']:.2f}"
    else:
        margin_text = "n/a"
    if budget["notional_account"] is not None and nav_value > 0:
        effective_leverage_text = f"{budget['notional_account'] / nav_value:.2f}x NAV"
    else:
        effective_leverage_text = "n/a"

    telegram(
        f"{dir_emoji} <b>{label} {direction}</b> | {instrument}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Entry: {actual_entry:.5f}\n"
        f"TP: {tp_price:.5f} (+{opp['tp_pips']:.1f} pips)\n"
        f"SL: {sl_price:.5f} (-{opp['sl_pips']:.1f} pips)\n"
        f"Trail: {trail_text}\n"
        f"Units: {unit_text} | Risk model: {account_currency} {risk_amount:.2f}\n"
        f"Notional: {notional_text}\n"
        f"Budget est. (margin @{LEVERAGE:.0f}x): {margin_text}\n"
        f"Effective leverage: {effective_leverage_text}\n"
        f"Score: {opp['score']:.0f} | Signal: {opp.get('entry_signal', 'UNKNOWN')}\n"
        f"RSI: {rsi_text} | Vol: {vol_text} | Spread: {opp.get('spread_pips', 0):.1f}p\n"
        f"Kelly: {effective_kelly_mult:.2f}x | Session: {session['name']}"
    )

    log.info(f"✅ [{label}] Opened {direction} {instrument} @ {actual_entry} "
             f"| TP={tp_price} SL={sl_price} | score={opp['score']}")
    return trade

def check_exit(trade: dict) -> tuple[bool, str]:
    instrument = trade["instrument"]
    label      = trade["label"]
    direction  = trade["direction"]

    price_data = get_current_price(instrument)
    if direction == "LONG":
        price = price_data["bid"]
    else:
        price = price_data["ask"]

    if price <= 0:
        return False, ""

    entry = trade["entry_price"]
    ps = pip_size(instrument)

    if direction == "LONG":
        pnl_pips = (price - entry) / ps
        pct = (price - entry) / entry
    else:
        pnl_pips = (entry - price) / ps
        pct = (entry - price) / entry

    held_min = (time.time() - trade.get("opened_ts", time.time())) / 60

    if direction == "LONG" and price > trade.get("highest_price", entry):
        trade["highest_price"] = price
        trade["last_new_high_at"] = time.time()
    elif direction == "SHORT" and price < trade.get("lowest_price", entry):
        trade["lowest_price"] = price
        trade["last_new_high_at"] = time.time()

    trade["unrealized_pnl"] = round(pnl_pips, 1)

    # Rollover protection for scalpers
    if label == "SCALPER" and is_rollover_window():
        log.info(f"🛡️ [{label}] Rollover exit: {instrument} | {pnl_pips:+.1f}p")
        return True, "ROLLOVER"

    # Scalper stall detection
    if label == "SCALPER":
        if direction == "LONG":
            peak_profit = (trade.get("highest_price", entry) - entry) / entry
        else:
            peak_profit = (entry - trade.get("lowest_price", entry)) / entry

        if pct >= 0 and peak_profit > 0.001:
            mins_since_high = (time.time() - trade.get("last_new_high_at", time.time())) / 60
            if direction == "LONG":
                peak_gain = trade["highest_price"] - entry
                giveback = (trade["highest_price"] - price) / peak_gain if peak_gain > 0 else 0
            else:
                peak_gain = entry - trade["lowest_price"]
                giveback = (price - trade["lowest_price"]) / peak_gain if peak_gain > 0 else 0

            if mins_since_high >= SCALPER_STALL_MINS and giveback >= SCALPER_STALL_GIVEBACK:
                log.info(f"🛡️ [{label}] Stall: {instrument} | {pnl_pips:+.1f}p | "
                         f"peak +{peak_profit*100:.1f}% | giveback {giveback*100:.0f}%")
                return True, "STALL_EXIT"

            if mins_since_high >= 3 and giveback >= 0.60 and peak_profit > 0.002:
                log.info(f"🛡️ [{label}] Rapid giveback: {instrument} | {pnl_pips:+.1f}p")
                return True, "RAPID_GIVEBACK"

        flat_pips = SCALPER_FLAT_RANGE_PIPS
        if held_min >= SCALPER_FLAT_MINS and abs(pnl_pips) <= flat_pips:
            log.info(f"😴 [{label}] Flat: {instrument} | {pnl_pips:+.1f}p after {held_min:.0f}min")
            return True, "FLAT_EXIT"

    # Partial TP for trend/breakout (with floor and native trailing)
    if label in ("TREND", "BREAKOUT") and not trade.get("partial_tp_hit"):
        partial_price = trade.get("partial_tp_price")
        if partial_price:
            hit = (direction == "LONG" and price >= partial_price) or \
                  (direction == "SHORT" and price <= partial_price)
            if hit:
                log.info(f"🎯 [{label}] Partial TP hit: {instrument} | +{pnl_pips:.1f}p")
                trade["partial_tp_hit"] = True

                # Move stop loss to breakeven (floor) on remaining position
                if direction == "LONG":
                    floor_price = entry + pips_to_price(instrument, 2)
                else:
                    floor_price = entry - pips_to_price(instrument, 2)

                modify_trade(trade["id"], sl_price=floor_price, instrument=instrument, label=label)
                if trade.get("trail_pips"):
                    modify_trade(trade["id"], trailing_sl_pips=trade["trail_pips"], instrument=instrument, label=label)

                # Close partial position
                partial_units = abs(trade.get("units", 0)) * TREND_PARTIAL_TP_PCT
                if partial_units > 0:
                    close_trade(trade["id"], label, units=partial_units, instrument=instrument)
                    trade["units"] = abs(trade["units"]) - partial_units
                    if trade["direction"] == "SHORT":
                        trade["units"] = -trade["units"]

                telegram(
                    f"🎯 <b>{label} Partial TP</b> | {instrument}\n"
                    f"+{pnl_pips:.1f}p | Floor @ {floor_price:.5f}\n"
                    f"Trail activated: {trade.get('trail_pips', 'N/A')}p | Remaining: {abs(trade['units']):.0f} units"
                )
                return False, ""

    # Timeout
    max_hours = {"SCALPER": 2, "TREND": TREND_MAX_HOURS,
                 "REVERSAL": REVERSAL_MAX_HOURS, "BREAKOUT": BREAKOUT_MAX_HOURS}.get(label, 24)
    if held_min >= max_hours * 60:
        log.info(f"⏰ [{label}] Timeout: {instrument} | {pnl_pips:+.1f}p after {held_min/60:.1f}h")
        return True, "TIMEOUT"

    # Session exit (off-hours)
    session = get_current_session()
    if session["name"] == "OFF_HOURS" and label == "SCALPER" and held_min > 10:
        log.info(f"🌙 [{label}] Session exit: {instrument} | off-hours")
        return True, "SESSION_EXIT"

    return False, ""

def close_trade_exit(trade: dict, reason: str):
    global _consecutive_losses

    instrument = trade["instrument"]
    label = trade["label"]
    direction = trade["direction"]

    price_data = get_current_price(instrument)
    exit_price = price_data["bid"] if direction == "LONG" else price_data["ask"]
    ps = pip_size(instrument)

    if direction == "LONG":
        pnl_pips = (exit_price - trade["entry_price"]) / ps
    else:
        pnl_pips = (trade["entry_price"] - exit_price) / ps

    success, close_error = close_trade_result(trade["id"], label, instrument=instrument)
    if not success and not PAPER_TRADE:
        if close_error and "market_halted" in close_error.lower().replace(" ", "_"):
            schedule_close_retry(trade, close_error)
            log.error(f"[{label}] Failed to close {instrument} — market halted, position remains open")
        else:
            suffix = f": {close_error}" if close_error else ""
            log.error(f"[{label}] Failed to close {instrument} — will retry{suffix}")
        return False

    pnl = pnl_pips * pip_value(instrument, trade.get("units", 1))
    held_min = (time.time() - trade.get("opened_ts", time.time())) / 60

    history_entry = {
        "instrument":   instrument,
        "label":        label,
        "direction":    direction,
        "entry_price":  trade["entry_price"],
        "exit_price":   exit_price,
        "pnl":          round(pnl, 2),
        "pnl_pips":     round(pnl_pips, 1),
        "pnl_pct":      round((exit_price / trade["entry_price"] - 1) * 100 *
                               (1 if direction == "LONG" else -1), 3),
        "reason":       reason,
        "held_minutes": round(held_min, 1),
        "score":        trade.get("score", 0),
        "entry_signal": trade.get("entry_signal", ""),
        "session":      trade.get("session_at_entry", ""),
        "closed_at":    datetime.now(timezone.utc).isoformat(),
    }
    trade_history.append(history_entry)

    if pnl > 0:
        _consecutive_losses = 0
    else:
        _consecutive_losses += 1

    emoji = "✅" if pnl > 0 else "❌"
    dir_arrow = "⬆️" if direction == "LONG" else "⬇️"
    telegram(
        f"{emoji} <b>{label} Closed</b> | {instrument} {dir_arrow}\n"
        f"Entry: {trade['entry_price']:.5f} → Exit: {exit_price:.5f}\n"
        f"P&L: {pnl:+.2f} ({pnl_pips:+.1f} pips)\n"
        f"Reason: {reason} | Held: {held_min:.0f}min"
    )

    clear_close_retry(trade["id"])
    save_state()
    return True


def close_all_open_positions(reason: str = "MANUAL_CLOSE") -> tuple[int, int]:
    global open_trades

    if not PAPER_TRADE and OANDA_API_KEY and OANDA_ACCOUNT_ID:
        sync_open_trades_with_oanda(reason="close-all")

    if not open_trades:
        return 0, 0

    closed_count = 0
    failed_count = 0
    for trade in open_trades[:]:
        if close_trade_exit(trade, reason):
            if trade in open_trades:
                open_trades.remove(trade)
            closed_count += 1
        else:
            failed_count += 1

    if not PAPER_TRADE and OANDA_API_KEY and OANDA_ACCOUNT_ID:
        sync_open_trades_with_oanda(reason="close-all-post")
    else:
        save_state()

    return closed_count, failed_count

# ═══════════════════════════════════════════════════════════════
#  ADAPTIVE LEARNING & REBALANCING
# ═══════════════════════════════════════════════════════════════

def update_adaptive_thresholds():
    global _adaptive_offsets
    DECAY_RATE = 0.15
    MIN_TRADES = max(10, ADAPTIVE_WINDOW // 2)
    for strategy in ("SCALPER", "TREND", "REVERSAL", "BREAKOUT",
                     "CARRY", "ASIAN_FADE", "POST_NEWS", "PULLBACK"):
        recent = [t for t in trade_history if t.get("label") == strategy][-ADAPTIVE_WINDOW:]
        if len(recent) < MIN_TRADES:
            continue
        pnls = [t["pnl_pips"] for t in recent]
        wins = sum(1 for p in pnls if p > 0)
        wr = wins / len(pnls)
        mean_pnl = sum(pnls) / len(pnls)
        old_offset = _adaptive_offsets.get(strategy, 0.0)
        decayed = old_offset * (1 - DECAY_RATE)
        if wr < 0.35 and mean_pnl < 0:
            new_offset = min(decayed + ADAPTIVE_TIGHTEN_STEP, ADAPTIVE_MAX_OFFSET)
        elif wr > 0.55 and mean_pnl > 0:
            new_offset = max(decayed - ADAPTIVE_RELAX_STEP, ADAPTIVE_MIN_OFFSET)
        else:
            new_offset = decayed
        new_offset = round(new_offset, 1)
        if abs(new_offset - old_offset) > 0.05:
            _adaptive_offsets[strategy] = new_offset
            log.info(f"🧠 [ADAPTIVE] {strategy}: offset {old_offset:+.1f} → {new_offset:+.1f} "
                     f"(WR={wr*100:.0f}% avg={mean_pnl:+.1f}p over {len(recent)})")

# ═══════════════════════════════════════════════════════════════
#  HEARTBEAT & SUMMARIES
# ═══════════════════════════════════════════════════════════════

def send_heartbeat(balance: float):
    global last_heartbeat_at
    if time.time() - last_heartbeat_at < HEARTBEAT_INTERVAL:
        return
    last_heartbeat_at = time.time()
    session = get_current_session()
    paused_pairs = get_paused_pairs_by_news(session["pairs_allowed"])
    paused_summary = ", ".join(paused_pairs[:4]) if paused_pairs else "none"
    if len(paused_pairs) > 4:
        paused_summary += f" (+{len(paused_pairs) - 4} more)"
    regime = ("🟢 BULL" if _market_regime_mult < 0.95
              else "🔴 BEAR" if _market_regime_mult > 1.10
              else "⚪ NEUTRAL")
    open_str = ""
    for t in open_trades:
        dir_e = "⬆️" if t["direction"] == "LONG" else "⬇️"
        open_str += f"\n  {dir_e} {t['instrument']} {t['label']} | {t.get('unrealized_pnl', 0):+.1f}p"
    telegram(
        f"💓 <b>Heartbeat</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Balance: £{balance:,.2f}\n"
        f"Open: {len(open_trades)} trades{open_str}\n"
        f"Session: {session['name']} ({session['aggression']})\n"
        f"Regime: {regime} ({_market_regime_mult:.2f})\n"
        f"DXY gap: {f'{_dxy_ema_gap*100:+.2f}%' if _dxy_ema_gap is not None else 'unknown'} | "
        f"VIX: {f'{_vix_level:.1f}' if _vix_level is not None else 'unknown'}\n"
        f"News-paused pairs: {paused_summary}\n"
        f"Today: {len([t for t in trade_history if t.get('closed_at', '').startswith(datetime.now(timezone.utc).strftime('%Y-%m-%d'))])} trades"
    )

def send_daily_summary(balance: float):
    global last_daily_summary
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if last_daily_summary == today:
        return
    if datetime.now(timezone.utc).hour != NY_CLOSE_UTC:
        return
    last_daily_summary = today
    today_trades = [t for t in trade_history
                    if t.get("closed_at", "").startswith(today)]
    if not today_trades:
        return
    pnl = sum(t.get("pnl", 0) for t in today_trades)
    wins = sum(1 for t in today_trades if t.get("pnl", 0) > 0)
    total = len(today_trades)
    by_dir = {"LONG": 0, "SHORT": 0}
    for t in today_trades:
        by_dir[t.get("direction", "LONG")] += t.get("pnl", 0)
    telegram(
        f"📅 <b>Daily Summary</b> | {today}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Trades: {total} | {wins}W/{total-wins}L\n"
        f"P&L: £{pnl:+.2f}\n"
        f"Long: £{by_dir['LONG']:+.2f} | Short: £{by_dir['SHORT']:+.2f}\n"
        f"Balance: £{balance:,.2f}"
    )

# ═══════════════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def run():
    global open_trades, _market_regime_mult, _consecutive_losses
    global _session_loss_paused_until, _streak_paused_at, DYNAMIC_PAIRS

    log.info("=" * 60)
    log.info(f"🚀 OANDA FX Bot starting")
    log.info(f"   Environment: {OANDA_ENVIRONMENT}")
    log.info(f"   Account type: {ACCOUNT_TYPE}")
    log.info(f"   Paper trade: {PAPER_TRADE}")
    log.info(f"   Static pairs: {STATIC_ALL_PAIRS}")
    log.info("=" * 60)

    load_state()

    if not PAPER_TRADE and OANDA_API_KEY and OANDA_ACCOUNT_ID:
        sync_open_trades_with_oanda(reason="startup")

    log.info("Building initial dynamic watchlist...")
    DYNAMIC_PAIRS = build_dynamic_watchlist()
    if DYNAMIC_PAIRS:
        log.info(f"✅ Dynamic watchlist ready: {len(DYNAMIC_PAIRS)} pairs")
    else:
        DYNAMIC_PAIRS = STATIC_ALL_PAIRS
        log.warning(f"⚠️ Using static pairs: {DYNAMIC_PAIRS}")

    # Start price stream (will include open trades automatically)
    if not PAPER_TRADE and OANDA_API_KEY:
        _start_price_stream()

    log.info("🔐 Verifying OANDA API credentials...")
    acct = get_account_summary()
    balance = acct.get("balance", 0)
    log.info(f"✅ OANDA account access verified: {acct.get('currency', 'GBP')} balance {balance:,.2f}")
    telegram(
        f"🚀 <b>FX Bot Started</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Balance: {acct.get('currency', '£')}{balance:,.2f}\n"
        f"Mode: {'📝 Paper' if PAPER_TRADE else '💰 Live'} | {ACCOUNT_TYPE}\n"
        f"Watchlist: {len(DYNAMIC_PAIRS)} pairs\n"
        f"Session: {get_current_session()['name']}\n"
        f"Strategies: SCALPER, TREND, REVERSAL, BREAKOUT, CARRY, ASIAN_FADE, POST_NEWS, PULLBACK\n"
        f"Open trades {'restored from state' if PAPER_TRADE else 'synced from OANDA'}: {len(open_trades)}"
    )

    log.info(f"Current working directory: {os.getcwd()}")
    if REDIS_CLIENT is not None:
        log.info(f"Expecting Redis macro state on key: {REDIS_MACRO_STATE_KEY}")
    else:
        log.info(f"Expecting macro files: {MACRO_FILTER_FILE}, {MACRO_NEWS_FILE}")

    max_wait = 30
    wait_interval = 2
    waited = 0
    while waited < max_wait:
        missing = []
        if REDIS_CLIENT is not None:
            state = _load_macro_state_from_redis()
            if state is None:
                missing.append(REDIS_MACRO_STATE_KEY)
        else:
            if not os.path.exists(MACRO_FILTER_FILE):
                missing.append(MACRO_FILTER_FILE)
            if not os.path.exists(MACRO_NEWS_FILE):
                missing.append(MACRO_NEWS_FILE)

        if not missing:
            break

        log.info(f"⏳ Waiting for macro data to be generated... missing: {', '.join(missing)} ({waited}/{max_wait}s)")
        time.sleep(wait_interval)
        waited += wait_interval

    if waited >= max_wait and missing:
        log.warning(f"⚠️ Macro data still missing after {max_wait}s: {', '.join(missing)}; continuing startup and retrying later.")

    log.info("🔄 Refreshing macro filter and news data on startup...")
    filter_reloaded = refresh_macro_filters()
    news_reloaded = refresh_macro_news()
    if not filter_reloaded:
        load_macro_filters()
    if not news_reloaded:
        load_macro_news()

    log.info(f"🔎 Macro proxy configuration: DXY proxy will be read from Redis, VIX from Redis")
    log.info(f"📰 Macro news file: {MACRO_NEWS_FILE}")

    while True:
        try:
            if is_weekend():
                log.debug("📅 Weekend — market closed. Sleeping 5min.")
                time.sleep(300)
                continue

            poll_telegram_commands()

            acct = get_account_summary()
            balance = acct.get("balance", 0)
            if balance <= 0:
                log.warning("⚠️ Zero balance — sleeping 60s")
                time.sleep(60)
                continue

            session = get_current_session()
            filters_updated = refresh_macro_filters()
            news_updated = refresh_macro_news()
            if filters_updated or news_updated:
                log.info(
                    f"🔄 Macro JSON refresh: filters={'reloaded' if filters_updated else 'unchanged'} "
                    f"news={'reloaded' if news_updated else 'unchanged'}"
                )
            update_macro_news_pause()
            # DXY and VIX are already loaded from Redis via load_macro_filters; no need to call proxies

            df_eurusd_1h = fetch_candles("EUR_USD", "H1", 100)
            if df_eurusd_1h is not None:
                _market_regime_mult = compute_market_regime(df_eurusd_1h)

            refresh_dynamic_watchlist()   # This will also restart stream if needed
            probe_pair_health()
            process_pending_close_retries()

            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_pnl = sum(t.get("pnl", 0) for t in trade_history
                           if t.get("closed_at", "").startswith(today))
            daily_loss_limit = -(balance * DAILY_LOSS_LIMIT_PCT)
            daily_cb = today_pnl < daily_loss_limit
            streak_cb = _consecutive_losses >= STREAK_LOSS_MAX

            session_paused = False
            if _session_loss_paused_until > time.time():
                session_paused = True
            elif today_pnl < -(balance * SESSION_LOSS_PAUSE_PCT) and len(trade_history) >= 3:
                _session_loss_paused_until = time.time() + SESSION_LOSS_PAUSE_MINS * 60
                session_paused = True
                telegram(f"🛑 <b>Session loss limit</b> | P&L £{today_pnl:.2f}\n"
                         f"Entries paused {SESSION_LOSS_PAUSE_MINS}min.")

            if streak_cb and not open_trades and _streak_paused_at > 0:
                if time.time() - _streak_paused_at >= STREAK_AUTO_RESET_MINS * 60:
                    _consecutive_losses = 0
                    _streak_paused_at = 0
                    streak_cb = False
                    telegram("✅ <b>Streak auto-reset</b> | Entries resumed")

            entries_allowed = (not _paused and not daily_cb and not streak_cb
                              and not session_paused)

            # Exit checks
            for trade in open_trades[:]:
                should_exit, reason = check_exit(trade)
                if should_exit:
                    if close_trade_exit(trade, reason):
                        open_trades.remove(trade)

            # Entry scans
            if entries_allowed and len(open_trades) < MAX_OPEN_TRADES:
                start_scan_cycle()
                skip_scalper = is_rollover_window()
                active_pairs, health_pairs, tradable_pairs, empty_reason = get_effective_scan_pairs(session)
                set_scan_cycle_summary(active_pairs, health_pairs, tradable_pairs)

                scalper_count  = sum(1 for t in open_trades if t["label"] == "SCALPER")
                trend_count    = sum(1 for t in open_trades if t["label"] == "TREND")
                reversal_count = sum(1 for t in open_trades if t["label"] == "REVERSAL")
                breakout_count = sum(1 for t in open_trades if t["label"] == "BREAKOUT")

                if scalper_count < SCALPER_MAX_TRADES and not skip_scalper:
                    best_scalper, reject_pair, reject_reason = _find_best_opportunity("SCALPER", tradable_pairs, session, score_scalper)
                    if not tradable_pairs:
                        record_scan_decision("SCALPER", "-", empty_reason, "📰")
                    elif best_scalper:
                        scanner_log(f"📊 [SCALPER] Best: {best_scalper['instrument']} | "
                                    f"Score: {best_scalper['score']:.0f} | "
                                    f"{best_scalper['direction']} | RSI: {best_scalper['rsi']:.0f}")
                        trade = open_trade_entry(best_scalper, "SCALPER", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("SCALPER", best_scalper["instrument"], best_scalper["direction"]) or "entry blocked"
                            record_scan_decision("SCALPER", best_scalper["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("SCALPER", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                if trend_count < TREND_MAX_TRADES and session["aggression"] != "MINIMAL":
                    best_trend, reject_pair, reject_reason = _find_best_opportunity("TREND", tradable_pairs, session, score_trend)
                    if not tradable_pairs:
                        record_scan_decision("TREND", "-", empty_reason, "📰")
                    elif best_trend:
                        scanner_log(f"📈 [TREND] Best: {best_trend['instrument']} | "
                                    f"Score: {best_trend['score']:.0f} | {best_trend['direction']}")
                        trade = open_trade_entry(best_trend, "TREND", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("TREND", best_trend["instrument"], best_trend["direction"]) or "entry blocked"
                            record_scan_decision("TREND", best_trend["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("TREND", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                if reversal_count < REVERSAL_MAX_TRADES:
                    best_reversal, reject_pair, reject_reason = _find_best_opportunity("REVERSAL", tradable_pairs, session, score_reversal)
                    if not tradable_pairs:
                        record_scan_decision("REVERSAL", "-", empty_reason, "📰")
                    elif best_reversal:
                        scanner_log(f"🔄 [REVERSAL] Best: {best_reversal['instrument']} | "
                                    f"Score: {best_reversal['score']:.0f} | {best_reversal['direction']}")
                        trade = open_trade_entry(best_reversal, "REVERSAL", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("REVERSAL", best_reversal["instrument"], best_reversal["direction"]) or "entry blocked"
                            record_scan_decision("REVERSAL", best_reversal["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("REVERSAL", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                if breakout_count < BREAKOUT_MAX_TRADES and session["aggression"] in ("HIGH",):
                    best_breakout, reject_pair, reject_reason = _find_best_opportunity("BREAKOUT", tradable_pairs, session, score_breakout)
                    if not tradable_pairs:
                        record_scan_decision("BREAKOUT", "-", empty_reason, "📰")
                    elif best_breakout:
                        scanner_log(f"💥 [BREAKOUT] Best: {best_breakout['instrument']} | "
                                    f"Score: {best_breakout['score']:.0f} | {best_breakout['direction']}")
                        trade = open_trade_entry(best_breakout, "BREAKOUT", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("BREAKOUT", best_breakout["instrument"], best_breakout["direction"]) or "entry blocked"
                            record_scan_decision("BREAKOUT", best_breakout["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("BREAKOUT", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                # ── New FX strategies ────────────────────────────
                carry_count = sum(1 for t in open_trades if t["label"] == "CARRY")
                if carry_count < CARRY_MAX_TRADES:
                    best_carry, reject_pair, reject_reason = _find_best_opportunity("CARRY", tradable_pairs, session, score_carry)
                    if not tradable_pairs:
                        record_scan_decision("CARRY", "-", empty_reason, "📰")
                    elif best_carry:
                        scanner_log(f"💰 [CARRY] Best: {best_carry['instrument']} | "
                                    f"Score: {best_carry['score']:.0f} | {best_carry['direction']}")
                        trade = open_trade_entry(best_carry, "CARRY", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("CARRY", best_carry["instrument"], best_carry["direction"]) or "entry blocked"
                            record_scan_decision("CARRY", best_carry["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("CARRY", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                asian_fade_count = sum(1 for t in open_trades if t["label"] == "ASIAN_FADE")
                if asian_fade_count < ASIAN_FADE_MAX_TRADES and session["name"] == "TOKYO":
                    best_asian, reject_pair, reject_reason = _find_best_opportunity("ASIAN_FADE", tradable_pairs, session, score_asian_fade)
                    if not tradable_pairs:
                        record_scan_decision("ASIAN", "-", empty_reason, "📰")
                    elif best_asian:
                        scanner_log(f"🌙 [ASIAN_FADE] Best: {best_asian['instrument']} | "
                                    f"Score: {best_asian['score']:.0f} | {best_asian['direction']}")
                        trade = open_trade_entry(best_asian, "ASIAN_FADE", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("ASIAN_FADE", best_asian["instrument"], best_asian["direction"]) or "entry blocked"
                            record_scan_decision("ASIAN", best_asian["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("ASIAN", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                post_news_count = sum(1 for t in open_trades if t["label"] == "POST_NEWS")
                if post_news_count < POST_NEWS_MAX_TRADES:
                    best_pn, reject_pair, reject_reason = _find_best_opportunity("POST_NEWS", tradable_pairs, session, score_post_news)
                    if not tradable_pairs:
                        record_scan_decision("POST_NEWS", "-", empty_reason, "📰")
                    elif best_pn:
                        scanner_log(f"📰 [POST_NEWS] Best: {best_pn['instrument']} | "
                                    f"Score: {best_pn['score']:.0f} | {best_pn['direction']}")
                        trade = open_trade_entry(best_pn, "POST_NEWS", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("POST_NEWS", best_pn["instrument"], best_pn["direction"]) or "entry blocked"
                            record_scan_decision("POST_NEWS", best_pn["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("POST_NEWS", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

                pullback_count = sum(1 for t in open_trades if t["label"] == "PULLBACK")
                if pullback_count < PULLBACK_MAX_TRADES and session["aggression"] != "MINIMAL":
                    best_pb, reject_pair, reject_reason = _find_best_opportunity("PULLBACK", tradable_pairs, session, score_pullback)
                    if not tradable_pairs:
                        record_scan_decision("PULLBACK", "-", empty_reason, "📰")
                    elif best_pb:
                        scanner_log(f"📐 [PULLBACK] Best: {best_pb['instrument']} | "
                                    f"Score: {best_pb['score']:.0f} | {best_pb['direction']}")
                        trade = open_trade_entry(best_pb, "PULLBACK", balance)
                        if trade:
                            open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("PULLBACK", best_pb["instrument"], best_pb["direction"]) or "entry blocked"
                            record_scan_decision("PULLBACK", best_pb["instrument"], reason, "🚫")
                    else:
                        record_scan_decision("PULLBACK", reject_pair or "watchlist", reject_reason or empty_reason, "🔍")

            if len(trade_history) % 10 == 0 and len(trade_history) > 0:
                update_adaptive_thresholds()

            send_heartbeat(balance)
            send_daily_summary(balance)

            # Dynamic scan interval
            if session["aggression"] in ("HIGH",):
                scan_interval = SCAN_INTERVAL_ACTIVE
            else:
                scan_interval = SCAN_INTERVAL_BASE

            if open_trades:
                near_target = False
                for t in open_trades:
                    price_data = get_current_price(t["instrument"])
                    mid = (price_data["bid"] + price_data["ask"]) / 2
                    if mid > 0:
                        tp_dist = abs(t["tp_price"] - mid) if t.get("tp_price") else 1e9
                        sl_dist = abs(t["sl_price"] - mid) if t.get("sl_price") else 1e9
                        atr = t.get("atr", pips_to_price(t["instrument"], 10))
                        if tp_dist < atr * 0.5 or sl_dist < atr * 0.5:
                            near_target = True
                            break
                if near_target:
                    scan_interval = 5
                else:
                    scan_interval = min(scan_interval, 15)

            time.sleep(scan_interval)

        except KeyboardInterrupt:
            log.info("🛑 Stopped.")
            save_state()
            telegram("🛑 <b>Bot stopped.</b> Check Railway.")
            break
        except Exception as e:
            log.error(f"Error: {e}", exc_info=True)
            telegram(f"⚠️ <b>Bot error:</b> {str(e)[:200]}\nRetrying in 30s.")
            time.sleep(30)

if __name__ == "__main__":
    run()