"""
OANDA FX Trading Bot — Multi-Strategy + Adaptive Learning + Session Intelligence
+ Dynamic Pair Selection + Auto‑restarting Price Stream
"""

import time
import hmac
import hashlib
import logging
import logging.handlers
import sys
import requests
import json
import os
import copy
import redis
import threading
import collections
import re
import math
import asyncio
import socket
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
# Tier 2 consultant-assessment integrations.
from fxbot.bayesian_weighting import (
    StrategyPosterior,
    allocate_weights as bayesian_allocate_weights,
    new_posterior as bayesian_new_posterior,
    update_posterior as bayesian_update_posterior,
)
from fxbot.correlation_risk import (
    default_correlation_matrix,
    would_breach_portfolio_cap,
)
from fxbot.financing import FinancingCache, is_carry_favourable
from fxbot.kill_switch import evaluate_drawdown_kill
from fxbot.percentile_sizing import size_by_percentile
from fxbot.regime import Regime, is_strategy_enabled as regime_is_strategy_enabled
from fxbot.slippage import get_default_logger as get_default_slippage_logger
from fxbot.strategy_reconciliation import (
    get_default_reconciliation as get_default_strategy_reconciliation,
)
from fxbot.runtime_status import build_runtime_status, publish_runtime_status
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
LAST_FORCED_WATCHLIST_REBUILD_AT = 0.0  # last time we force-rebuilt because all pairs were blocked
WATCHLIST_UPDATE_INTERVAL = int(os.getenv("WATCHLIST_UPDATE_INTERVAL", "3600"))  # 1 hour (was 4h; off-hours spread snapshots stale too long)
MAX_WATCHLIST_SIZE = int(os.getenv("MAX_WATCHLIST_SIZE", "8"))
MAX_SPREAD_FILTER_PIPS = float(os.getenv("MAX_SPREAD_FILTER_PIPS", "2.5"))  # raised from 1.5 — practice spreads widen off-hours
ALWAYS_INCLUDE_CORE_PAIRS = os.getenv("ALWAYS_INCLUDE_CORE_PAIRS", "true").strip().lower() in {"1", "true", "yes", "on"}
# Restrict the dynamic watchlist universe to the operator's configured pair list
# (STATIC_ALL_PAIRS). Without this, the 68-pair OANDA scan can surface exotics
# like HKD_JPY or TRY_JPY on weekends when majors' spreads temporarily widen --
# they pass the spread filter on stale quotes and end up ranked above G10 crosses.
# Set to false to opt into the full OANDA universe for scanning.
WATCHLIST_ALLOWLIST_ONLY = os.getenv("WATCHLIST_ALLOWLIST_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"}
SUPPORTED_PAIR_CACHE_SECS = int(os.getenv("SUPPORTED_PAIR_CACHE_SECS", "21600"))
NEWS_WINDOW_RISK_MULT = float(os.getenv("NEWS_WINDOW_RISK_MULT", "0.5"))
DXY_REGIME_THRESHOLD = float(os.getenv("DXY_REGIME_THRESHOLD", "0.008"))

# ── Spread betting min stake ──────────────────────────────────
SPREAD_BET_MIN_STAKE = float(os.getenv("SPREAD_BET_MIN_STAKE", "0.10"))

# ── Shared account sleeve split ───────────────────────────────
FX_BUDGET_ALLOCATION = float(os.getenv("FX_BUDGET_ALLOCATION", "0.50"))
GOLD_BUDGET_ALLOCATION = float(os.getenv("GOLD_BUDGET_ALLOCATION", "0.50"))

# ── Capital allocation ───────────────────────────────────────
SCALPER_ALLOCATION_PCT  = float(os.getenv("SCALPER_ALLOCATION_PCT",  "0.30"))
TREND_ALLOCATION_PCT    = float(os.getenv("TREND_ALLOCATION_PCT",    "0.40"))
REVERSAL_ALLOCATION_PCT = float(os.getenv("REVERSAL_ALLOCATION_PCT", "0.15"))
BREAKOUT_ALLOCATION_PCT = float(os.getenv("BREAKOUT_ALLOCATION_PCT", "0.15"))

# ── Risk management ─────────────────────────────────────────
MAX_RISK_PER_TRADE     = float(os.getenv("MAX_RISK_PER_TRADE",     "0.015"))
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
SCALPER_MAX_SPREAD_PIPS  = float(os.getenv("SCALPER_MAX_SPREAD_PIPS", "1.8"))
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

# ── Unified exit parameters (all strategies) ────────────────
EXIT_PEAK_TRAIL_PCT    = float(os.getenv("EXIT_PEAK_TRAIL_PCT",    "0.015"))
EXIT_FLAT_HOURS        = float(os.getenv("EXIT_FLAT_HOURS",        "48"))
EXIT_REVIEW_DAYS       = int(os.getenv("EXIT_REVIEW_DAYS",         "7"))
EXIT_REVIEW_POOR_THRESHOLD = float(os.getenv("EXIT_REVIEW_POOR_THRESHOLD", "-0.10"))

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
TRADE_CALIBRATION_FILE = os.getenv("TRADE_CALIBRATION_FILE", "backtest_output/calibration.json")
CALIBRATION_PAIR_MIN_TRADES = int(os.getenv("CALIBRATION_PAIR_MIN_TRADES", "20"))
CALIBRATION_SESSION_MIN_TRADES = int(os.getenv("CALIBRATION_SESSION_MIN_TRADES", "8"))
CALIBRATION_BLOCK_MAX_WIN_RATE = float(os.getenv("CALIBRATION_BLOCK_MAX_WIN_RATE", "0.20"))
CALIBRATION_BLOCK_MAX_PROFIT_FACTOR = float(os.getenv("CALIBRATION_BLOCK_MAX_PROFIT_FACTOR", "0.60"))
CALIBRATION_BLOCK_MAX_EXPECTANCY_PIPS = float(os.getenv("CALIBRATION_BLOCK_MAX_EXPECTANCY_PIPS", "-5.0"))
CALIBRATION_RISK_FLOOR = float(os.getenv("CALIBRATION_RISK_FLOOR", "0.25"))
CALIBRATION_MAX_TIGHTEN = float(os.getenv("CALIBRATION_MAX_TIGHTEN", "8.0"))
CALIBRATION_MAX_RELAX = float(os.getenv("CALIBRATION_MAX_RELAX", "2.0"))

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

# Scan cadence defaults lifted from 30/10s -> 60/30s (Tier 1 §7 item 10 of
# consultant assessment): 10s scans burn the OANDA 120 req/s budget, create
# watchlist-rebuild churn and add no real signal on M5/M15 timeframes.
SCAN_INTERVAL_BASE   = int(os.getenv("SCAN_INTERVAL_BASE",   "60"))
SCAN_INTERVAL_ACTIVE = int(os.getenv("SCAN_INTERVAL_ACTIVE", "30"))
# Forced watchlist rebuilds (the "all pairs blocked" path) must not fire more
# often than this. Tier 1 §7 item 4: the 18h W2 log shows ~3124 forced
# rebuilds in 17h40 — one every 20s — which is the primary livelock.
FORCED_WATCHLIST_REBUILD_MIN_INTERVAL_SECS = int(os.getenv("FORCED_WATCHLIST_REBUILD_MIN_INTERVAL_SECS", "600"))

# Tier 1 §7 item 9 — net-of-cost R:R floor. On spread-cost venues the reward
# leg of the R:R must be net of round-trip spread, expected slippage, and
# expected financing. Default floor 1.8 → for every 1.0 of SL risk we require
# 1.8 of net reward. Set MIN_NET_RR=0 to disable the gate.
MIN_NET_RR = float(os.getenv("MIN_NET_RR", "1.8"))
NET_RR_SLIPPAGE_PIPS = float(os.getenv("NET_RR_SLIPPAGE_PIPS", "0.3"))
NET_RR_FINANCING_PIPS = float(os.getenv("NET_RR_FINANCING_PIPS", "0.0"))
PAIR_HEALTH_FAILURE_COOLDOWN_SECS = int(os.getenv("PAIR_HEALTH_FAILURE_COOLDOWN_SECS", "60"))
PAIR_HEALTH_SUCCESS_COOLDOWN_SECS = int(os.getenv("PAIR_HEALTH_SUCCESS_COOLDOWN_SECS", "30"))
# Tier 2 §12/13/15/16/18/19/20/21/22 — feature flags and thresholds.
TIER2_PERCENTILE_SIZING_ENABLED = os.getenv("TIER2_PERCENTILE_SIZING_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_PERCENTILE_LOOKBACK = int(os.getenv("TIER2_PERCENTILE_LOOKBACK", "60"))
TIER2_PERCENTILE_FLOOR = float(os.getenv("TIER2_PERCENTILE_FLOOR", "0.5"))
TIER2_PERCENTILE_CAP = float(os.getenv("TIER2_PERCENTILE_CAP", "2.0"))
TIER2_PORTFOLIO_VOL_CAP_PCT = float(os.getenv("TIER2_PORTFOLIO_VOL_CAP_PCT", "0.03"))
TIER2_PORTFOLIO_VOL_ENABLED = os.getenv("TIER2_PORTFOLIO_VOL_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_REGIME_GATE_ENABLED = os.getenv("TIER2_REGIME_GATE_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_STRATEGY_DEDUP_ENABLED = os.getenv("TIER2_STRATEGY_DEDUP_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_FINANCING_ENABLED = os.getenv("TIER2_FINANCING_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_FINANCING_REFRESH_SECS = int(os.getenv("TIER2_FINANCING_REFRESH_SECS", str(12 * 3600)))
TIER2_CARRY_MIN_BPS_PER_DAY = float(os.getenv("TIER2_CARRY_MIN_BPS_PER_DAY", "0.5"))
TIER2_SLIPPAGE_LOG_ENABLED = os.getenv("TIER2_SLIPPAGE_LOG_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_SLIPPAGE_CSV_PATH = os.getenv("TIER2_SLIPPAGE_CSV_PATH", "slippage.csv")
TIER2_DRAWDOWN_KILL_ENABLED = os.getenv("TIER2_DRAWDOWN_KILL_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_DD_SOFT_CUT_DAYS = int(os.getenv("TIER2_DD_SOFT_CUT_DAYS", "30"))
TIER2_DD_HARD_HALT_DAYS = int(os.getenv("TIER2_DD_HARD_HALT_DAYS", "90"))
TIER2_DD_SOFT_CUT_PCT = float(os.getenv("TIER2_DD_SOFT_CUT_PCT", "0.06"))
TIER2_DD_HARD_HALT_PCT = float(os.getenv("TIER2_DD_HARD_HALT_PCT", "0.12"))
TIER2_DD_SOFT_CUT_RISK_SCALE = float(os.getenv("TIER2_DD_SOFT_CUT_RISK_SCALE", "0.5"))
TIER2_BAYESIAN_WEIGHTING_ENABLED = os.getenv("TIER2_BAYESIAN_WEIGHTING_ENABLED", "1") not in ("0", "", "false", "False")
TIER2_BAYESIAN_MIN_WEIGHT = float(os.getenv("TIER2_BAYESIAN_MIN_WEIGHT", "0.25"))
TIER2_BAYESIAN_MAX_WEIGHT = float(os.getenv("TIER2_BAYESIAN_MAX_WEIGHT", "2.0"))
SHARED_BUDGET_STRICT_REDIS = os.getenv("SHARED_BUDGET_STRICT_REDIS", "0") not in ("0", "", "false", "False")
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
REDIS_TRADE_CALIBRATION_KEY = os.getenv("REDIS_TRADE_CALIBRATION_KEY", "trade_calibration")
REDIS_BOT_STATUS_KEY = os.getenv("REDIS_BOT_STATUS_KEY", "bot_runtime_status")
SHARED_BUDGET_FILE = os.getenv("SHARED_BUDGET_FILE", os.getenv("FX_SHARED_BUDGET_FILE", "shared_budget_state.json"))
SHARED_BUDGET_KEY = os.getenv("SHARED_BUDGET_KEY", os.getenv("FX_SHARED_BUDGET_KEY", "shared_budget_state"))
BOT_STATUS_INTERVAL = int(os.getenv("BOT_STATUS_INTERVAL", "60"))
BOT_STATUS_TTL = int(os.getenv("BOT_STATUS_TTL", "180"))
IDLE_LOG_INTERVAL = int(os.getenv("IDLE_LOG_INTERVAL", "1800"))
CALIBRATION_MAX_AGE_HOURS = float(os.getenv("CALIBRATION_MAX_AGE_HOURS", "72"))
CALIBRATION_MIN_TOTAL_TRADES = int(os.getenv("CALIBRATION_MIN_TOTAL_TRADES", "50"))
HTTP_RETRIES        = 3
HTTP_RETRY_DELAY    = 1.0
HEARTBEAT_INTERVAL  = int(os.getenv("HEARTBEAT_INTERVAL",  "3600"))
KLINE_CACHE_TTL     = 15
MAX_KLINE_CACHE     = 200

validate_main_config(globals())

# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════

# Tier 1 §7 item 5 — split stdout/stderr by level so downstream log shippers
# (Railway, Datadog, etc.) that key severity off the fd can map INFO → info
# and WARNING+ → error. Previously every line was emitted to a single
# StreamHandler and every line was re-tagged `severity=error` by the shipper.
class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover
        return record.levelno < self.max_level


_log_formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(_log_formatter)
_stdout_handler.setLevel(logging.INFO)
_stdout_handler.addFilter(_MaxLevelFilter(logging.WARNING))
_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(_log_formatter)
_stderr_handler.setLevel(logging.WARNING)
_rotating_handler = logging.handlers.RotatingFileHandler(
    "bot.log", maxBytes=10_000_000, backupCount=5
)
_rotating_handler.setFormatter(_log_formatter)
logging.basicConfig(
    level=logging.INFO,
    handlers=[_stdout_handler, _stderr_handler, _rotating_handler],
    force=True,
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
last_runtime_status_at = 0
last_idle_log_at = 0
last_daily_summary = ""
last_weekly_summary = ""
_weekend_mode_active = False
_entry_pause_reason = ""

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
trade_calibration    = {}
_trade_calibration_mtime = 0.0
_recent_scan_decisions = []
_last_scan_cycle_at   = ""
_last_scan_cycle_summary = {"active": 0, "healthy": 0, "tradable": 0, "active_pairs": [], "tradable_pairs": []}
_scan_reject_reasons = {}
_pair_health         = {}
_last_scan_pool_status = {"mode": "primary", "active": 0, "healthy": 0, "tradable": 0}
_pending_close_retries = {}
# Tier 2 runtime state.
_strategy_score_history: dict[str, list[float]] = {}
_strategy_posteriors: dict[str, "StrategyPosterior"] = {}
_strategy_bayesian_weights: dict[str, float] = {}
_drawdown_risk_scale: float = 1.0
_drawdown_hard_halt: bool = False
_drawdown_reason: str = ""
_financing_cache = FinancingCache(ttl_seconds=TIER2_FINANCING_REFRESH_SECS)
_last_financing_refresh_at: float = 0.0
_slippage_logger = get_default_slippage_logger()
try:
    _slippage_logger._csv_path = TIER2_SLIPPAGE_CSV_PATH  # type: ignore[attr-defined]
except Exception:
    pass
_strategy_reconciliation = get_default_strategy_reconciliation()
_pair_cooldowns      = {}
_thread_local        = threading.local()
_live_prices         = {}
_price_lock          = threading.Lock()
_open_trades_lock    = threading.Lock()
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


def telegram_enabled() -> bool:
    return bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)


def publish_bot_runtime_status(state: str, balance: float | None = None, error: str | None = None, force: bool = False) -> bool:
    global last_runtime_status_at
    if REDIS_CLIENT is None:
        return False
    now = time.time()
    if not force and now - last_runtime_status_at < BOT_STATUS_INTERVAL:
        return False

    session_name = "UNKNOWN"
    try:
        session_name = get_current_session().get("name", "UNKNOWN")
    except Exception:
        session_name = "UNKNOWN"

    payload = build_runtime_status(
        service="bot",
        state=state,
        hostname=socket.gethostname(),
        pid=os.getpid(),
        paper_trade=PAPER_TRADE,
        paused=_paused,
        session=session_name,
        watchlist_size=len(DYNAMIC_PAIRS),
        open_trades=len(open_trades),
        balance=round(float(balance), 2) if balance is not None else None,
        telegram_enabled=telegram_enabled(),
        macro_state_key=REDIS_MACRO_STATE_KEY,
        calibration_key=REDIS_TRADE_CALIBRATION_KEY,
        calibration=_calibration_summary(),
        last_scan_cycle_at=_last_scan_cycle_at or None,
        scan_pool_mode=_last_scan_pool_status.get("mode", "primary"),
        market_regime_mult=round(float(_market_regime_mult), 4),
        error=error,
    )
    published = publish_runtime_status(REDIS_CLIENT, REDIS_BOT_STATUS_KEY, payload, BOT_STATUS_TTL)
    if published:
        last_runtime_status_at = now
    publish_fx_shared_budget_state()
    return published


def _estimate_fx_trade_reserved_risk(trade: dict) -> float:
    explicit = trade.get("risk_amount")
    if explicit is not None:
        try:
            return max(0.0, float(explicit))
        except (TypeError, ValueError):
            pass

    sl_pips = float(trade.get("sl_pips", 0) or 0)
    units = abs(float(trade.get("units", 0) or 0))
    instrument = str(trade.get("instrument", "") or "")
    if sl_pips <= 0 or units <= 0 or not instrument:
        return 0.0
    if ACCOUNT_TYPE == "spread_bet" and not uses_oanda_native_units():
        return round(sl_pips * units, 2)
    return round(sl_pips * pip_value(instrument, units, get_account_currency()), 2)


def publish_fx_shared_budget_state() -> bool:
    if REDIS_CLIENT is None and not SHARED_BUDGET_FILE:
        return False

    # Build the FX slot from current open_trades
    trades = {}
    reserved_total = 0.0
    with _open_trades_lock:
        snapshot = list(open_trades)
    for trade in snapshot:
        trade_id = str(trade.get("id", "") or "")
        if not trade_id:
            continue
        risk_amount = _estimate_fx_trade_reserved_risk(trade)
        trades[trade_id] = {
            "risk_amount": risk_amount,
            "instrument": trade.get("instrument"),
            "label": trade.get("label"),
        }
        reserved_total += risk_amount
    fx_slot = {
        "reserved_risk": round(reserved_total, 2),
        "trades": trades,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Atomic merge via Redis WATCH/MULTI (only updates FX slot, preserves Gold)
    if REDIS_CLIENT is not None and SHARED_BUDGET_KEY:
        for attempt in range(3):
            try:
                pipe = REDIS_CLIENT.pipeline(True)
                pipe.watch(SHARED_BUDGET_KEY)
                raw = pipe.get(SHARED_BUDGET_KEY)
                payload = json.loads(raw) if raw else {"bots": {}}
                if not isinstance(payload, dict):
                    payload = {"bots": {}}
                bots = payload.setdefault("bots", {})
                bots["fx"] = fx_slot
                pipe.multi()
                pipe.set(SHARED_BUDGET_KEY, json.dumps(payload))
                pipe.execute()
                break
            except redis.WatchError:
                log.debug("Shared budget WATCH conflict (attempt %d/3)", attempt + 1)
                continue
            except Exception as exc:
                log.debug(f"Shared budget Redis publish failed: {exc}")
                break

    # Also write to file
    if SHARED_BUDGET_FILE:
        if SHARED_BUDGET_STRICT_REDIS and REDIS_CLIENT is None:
            log.warning(
                "SHARED_BUDGET_STRICT_REDIS=1 but Redis is unavailable — "
                "skipping file fallback write to avoid stale FX/Gold budget."
            )
            return False
        if SHARED_BUDGET_STRICT_REDIS and REDIS_CLIENT is not None:
            # Redis was the primary path; a file write is not required in
            # strict-Redis mode. Keep it only when strict mode is off.
            return True
        try:
            payload_file = {"bots": {}}
            if os.path.exists(SHARED_BUDGET_FILE):
                with open(SHARED_BUDGET_FILE, encoding="utf-8") as handle:
                    payload_file = json.load(handle)
                    if not isinstance(payload_file, dict):
                        payload_file = {"bots": {}}
            payload_file.setdefault("bots", {})["fx"] = fx_slot
            os.makedirs(os.path.dirname(SHARED_BUDGET_FILE) or ".", exist_ok=True)
            tmp = SHARED_BUDGET_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(payload_file, handle, indent=2)
            os.replace(tmp, SHARED_BUDGET_FILE)
        except Exception as exc:
            log.debug(f"Shared budget file publish failed: {exc}")
            return False
    return True


def _load_shared_budget_payload() -> dict:
    payload = {"bots": {}}
    try:
        if REDIS_CLIENT is not None and SHARED_BUDGET_KEY:
            raw = REDIS_CLIENT.get(SHARED_BUDGET_KEY)
            if raw:
                payload = json.loads(raw)
        elif SHARED_BUDGET_FILE and os.path.exists(SHARED_BUDGET_FILE):
            if SHARED_BUDGET_STRICT_REDIS:
                log.warning(
                    "SHARED_BUDGET_STRICT_REDIS=1 but Redis is unavailable — "
                    "refusing to read shared budget from file."
                )
                return {"bots": {}}
            with open(SHARED_BUDGET_FILE, encoding="utf-8") as handle:
                payload = json.load(handle)
    except Exception:
        payload = {"bots": {}}
    return payload if isinstance(payload, dict) else {"bots": {}}


def build_fx_budget_snapshot(account_balance: float) -> dict[str, float]:
    payload = _load_shared_budget_payload()
    bots = payload.get("bots", {}) if isinstance(payload, dict) else {}
    fx_reserved = float(bots.get("fx", {}).get("reserved_risk", 0.0) or 0.0)
    gold_reserved = float(bots.get("gold", {}).get("reserved_risk", 0.0) or 0.0)
    fx_sleeve_balance = float(account_balance) * FX_BUDGET_ALLOCATION
    max_trade_risk_amount = fx_sleeve_balance * MAX_RISK_PER_TRADE
    max_total_risk_amount = fx_sleeve_balance * MAX_TOTAL_EXPOSURE
    return {
        "account_balance": float(account_balance),
        "fx_sleeve_balance": fx_sleeve_balance,
        "max_trade_risk_amount": max_trade_risk_amount,
        "max_total_risk_amount": max_total_risk_amount,
        "reserved_fx_risk": fx_reserved,
        "sibling_gold_reserved_risk": gold_reserved,
        "available_fx_risk": max(0.0, max_total_risk_amount - fx_reserved),
    }


def sleep_with_command_poll(total_seconds: int, poll_interval: int = 5) -> None:
    deadline = time.time() + max(0, total_seconds)
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        poll_telegram_commands()
        time.sleep(min(poll_interval, max(0.0, remaining)))


def log_idle_state(reason: str, balance: float | None = None, sleep_seconds: int | None = None, force: bool = False) -> None:
    global last_idle_log_at
    now = time.time()
    if not force and now - last_idle_log_at < IDLE_LOG_INTERVAL:
        return
    suffix = f" | next check {sleep_seconds}s" if sleep_seconds is not None else ""
    balance_text = f" | balance {balance:,.2f}" if balance is not None else ""
    log.info(f"⏸️ Bot idle: {reason}{balance_text}{suffix}")
    last_idle_log_at = now

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
            with _open_trades_lock:
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


def _fetch_pricing_chunk(chunk: list[str], _depth: int = 0) -> list[dict]:
    cleaned_chunk = filter_supported_pairs(chunk, "pricing request")
    if not cleaned_chunk:
        return []
    if _depth > 10:
        log.warning("_fetch_pricing_chunk recursion limit reached, skipping chunk")
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
            return _fetch_pricing_chunk([pair for pair in cleaned_chunk if pair != invalid], _depth + 1)

        if len(cleaned_chunk) == 1:
            _mark_unsupported_instrument(cleaned_chunk[0], "pricing request failed")
            return []

        midpoint = len(cleaned_chunk) // 2
        return _fetch_pricing_chunk(cleaned_chunk[:midpoint], _depth + 1) + _fetch_pricing_chunk(cleaned_chunk[midpoint:], _depth + 1)

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

        # Restrict the scan universe to the operator's configured allowlist so exotics
        # (HKD_JPY, TRY_JPY, etc.) can't sneak in via a lucky weekend spread snapshot.
        if WATCHLIST_ALLOWLIST_ONLY:
            allowlist = {p for p in STATIC_ALL_PAIRS}
            filtered = [p for p in fx_pairs if p in allowlist]
            if filtered:
                log.info(f"📊 Allowlist filter kept {len(filtered)}/{len(fx_pairs)} pairs: {filtered}")
                fx_pairs = filtered
            else:
                log.warning("Allowlist filter left 0 pairs; falling back to full OANDA scan.")

        # Filter by spread — emit structured per-pair telemetry on rejection so
        # operators can diagnose the "8/8 spread gate rejection" livelock (Tier 1
        # §7 item 3). Prior implementation wrote only a single free-text warning
        # ("No pairs passed spread filter.") which was operationally useless.
        chunk_size = 40
        spread_ok = []
        spread_rejections: list[dict] = []
        for i in range(0, len(fx_pairs), chunk_size):
            chunk = fx_pairs[i:i+chunk_size]
            for price in _fetch_pricing_chunk(chunk):
                inst = price["instrument"]
                bid = float(price["closeoutBid"])
                ask = float(price["closeoutAsk"])
                ps = pip_size(inst)
                if bid <= 0 or ask <= 0:
                    mark_pair_failure(inst, "invalid price in watchlist", "quote")
                    spread_rejections.append({"pair": inst, "bid": bid, "ask": ask, "pip_size": ps, "spread_pips": None, "reason": "invalid_quote"})
                    continue
                mark_pair_success(inst, "quote")
                spread = (ask - bid) / ps if ps > 0 else float("inf")
                if spread <= max_spread_pips:
                    mark_pair_success(inst, "spread")
                    if is_pair_tradeable(inst):
                        spread_ok.append(inst)
                        log.info(
                            f"📊 spread_gate KEEP {inst} bid={bid:.5f} ask={ask:.5f} "
                            f"pip_size={ps:g} spread_pips={spread:.2f} (<= {max_spread_pips:.2f})"
                        )
                    else:
                        spread_rejections.append({"pair": inst, "bid": bid, "ask": ask, "pip_size": ps, "spread_pips": spread, "reason": "pair_health_blocked"})
                else:
                    mark_pair_failure(inst, f"spread {spread:.1f} > {max_spread_pips:.1f}", "spread")
                    spread_rejections.append({"pair": inst, "bid": bid, "ask": ask, "pip_size": ps, "spread_pips": spread, "reason": "spread_too_wide"})

        # Aggregate WARN so the observability pipeline can alert when the gate
        # rejects everything — the 17h40 W2 log showed this condition for 3124
        # consecutive cycles with no per-pair detail.
        if spread_rejections:
            rejected_sample = spread_rejections[:8]
            log.warning(
                "📊 spread_gate rejections=%d (max=%.2fp) sample=%s",
                len(spread_rejections),
                max_spread_pips,
                rejected_sample,
            )

        if not spread_ok:
            log.warning("No pairs passed spread filter. Using static list.")
            fallback_pairs = [pair for pair in STATIC_ALL_PAIRS if is_pair_tradeable(pair)]
            return filter_supported_pairs(fallback_pairs, "static fallback") or fallback_pairs or STATIC_ALL_PAIRS

        log.info(f"📊 {len(spread_ok)} pairs passed spread filter. Ranking by volatility...")

        # Guard-rail: if the spread filter captured fewer than 3 pairs (typical of off-hours
        # snapshots or a widened DXY regime), seed the ranking pool with STATIC_CORE_PAIRS so
        # the bot cannot end up trading only exotics. Per-trade spread checks still apply.
        if ALWAYS_INCLUDE_CORE_PAIRS and len(spread_ok) < 3:
            for pair in STATIC_CORE_PAIRS:
                if pair not in spread_ok and is_pair_tradeable(pair):
                    spread_ok.append(pair)
            log.info(f"📊 Seeded spread_ok with STATIC_CORE_PAIRS -> {len(spread_ok)} pairs for volatility ranking")

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
        # Always retain STATIC_CORE_PAIRS even if their current spread snapshot was wide.
        # Per-trade SCALPER/TREND_MAX_SPREAD_PIPS checks still gate entries live.
        if ALWAYS_INCLUDE_CORE_PAIRS:
            core_to_add = [p for p in STATIC_CORE_PAIRS if p not in top_pairs and is_pair_tradeable(p)]
            if core_to_add:
                top_pairs = top_pairs + core_to_add
                log.info(f"🔄 Union with STATIC_CORE_PAIRS added: {core_to_add}")
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
        global LAST_FORCED_WATCHLIST_REBUILD_AT
        now = time.time()
        secs_since = now - LAST_FORCED_WATCHLIST_REBUILD_AT
        if secs_since < FORCED_WATCHLIST_REBUILD_MIN_INTERVAL_SECS:
            # Tier 1 §7 item 4: W2 log shows ~3123 forced rebuilds in 17h40 (one
            # every 20s) because this branch fired on every scan pool miss. Cap
            # it to at most once per FORCED_WATCHLIST_REBUILD_MIN_INTERVAL_SECS.
            log.info(
                "🩺 Active dynamic watchlist fully blocked — throttling rebuild "
                f"(last rebuild {secs_since:.0f}s ago, min interval "
                f"{FORCED_WATCHLIST_REBUILD_MIN_INTERVAL_SECS}s)."
            )
        else:
            log.warning("🩺 Active dynamic watchlist is fully blocked. Rebuilding watchlist.")
            LAST_FORCED_WATCHLIST_REBUILD_AT = now
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
    requested = sorted(set(list(pairs) + open_trade_pairs))
    all_pairs = filter_supported_pairs(requested, "price stream")
    if not all_pairs:
        log.warning("Skipping price stream start because no supported instruments remain.")
        return

    # Tier 1 §7 item 2: prior log said "Starting with N pairs" but then sliced
    # to [:5] for display, making it look like EUR_USD / AUD_USD / EUR_GBP were
    # silently dropped. Enumerate the full subscribed list and alert on any
    # allowlisted pair that was filtered out.
    dropped = sorted(set(requested) - set(all_pairs))
    log.info(f"🔌 Starting price stream with {len(all_pairs)} pairs: {all_pairs}")
    if dropped:
        log.warning(f"🔌 Stream subscription dropped {len(dropped)} pair(s) via supported-filter: {dropped}")
        try:
            if any(p in STATIC_ALL_PAIRS for p in dropped):
                telegram(f"⚠️ <b>Stream dropped allowlisted pairs</b>\nDropped: {', '.join(dropped)}")
        except Exception:
            pass
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


def next_market_reopen_utc(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    days_until_sunday = (6 - current.weekday()) % 7
    reopen_at = (current + timedelta(days=days_until_sunday)).replace(
        hour=21,
        minute=0,
        second=0,
        microsecond=0,
    )
    if reopen_at <= current:
        reopen_at += timedelta(days=7)
    return reopen_at


def format_time_utc_and_local(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc)
    local_dt = dt.astimezone()
    local_tz = local_dt.tzname() or "LOCAL"
    return (
        f"{utc_dt.strftime('%a %Y-%m-%d %H:%M UTC')}"
        f" / {local_dt.strftime('%a %Y-%m-%d %H:%M')} {local_tz}"
    )


def _categorize_entry_block_reason(reason: str) -> str:
    normalized = _normalize_broker_reason(reason)
    if not normalized:
        return "unknown"
    if _is_market_halted_reason(normalized):
        return "broker_closed"
    if "spread" in normalized:
        return "spread_wide"
    if any(term in normalized for term in ("missing bid/ask", "invalid price", "no candles", "no valid candle rows", "short candle history")):
        return "pricing_unavailable"
    if any(term in normalized for term in ("invalid instrument", "pricing request failed", "rejected instrument", "tradeable", "tradable", "close only")):
        return "broker_unavailable"
    return "other"


def _sample_entry_blockers(pairs: list[str], limit: int = 3) -> str:
    samples = []
    for instrument in pairs:
        reason = get_pair_health_reason(instrument)
        if not reason:
            continue
        samples.append(f"{instrument}: {reason[:80]}")
        if len(samples) >= limit:
            break
    return "; ".join(samples) if samples else "No broker detail available yet."


def _build_entry_pause_notice(session: dict, active_pairs: list[str], empty_reason: str) -> tuple[str, str, str]:
    pairs = list(active_pairs) or list(session.get("pairs_allowed", [])) or list(STATIC_ALL_PAIRS)
    categorized = [
        _categorize_entry_block_reason(get_pair_health_reason(instrument))
        for instrument in pairs
        if get_pair_health_reason(instrument)
    ]
    blocker_examples = _sample_entry_blockers(pairs)
    session_text = f"Session: {session['name']} ({session['aggression']})"

    if any(cat == "broker_closed" for cat in categorized):
        return (
            "broker_closed",
            "⏸️ <b>Entries paused on OANDA</b>",
            f"OANDA appears to have FX entries closed or in close-only mode right now. This can happen during bank holidays, market closures, or broker halts.\n"
            f"{session_text}\n"
            f"Examples: {blocker_examples}",
        )

    if any(cat == "pricing_unavailable" for cat in categorized):
        return (
            "pricing_unavailable",
            "⏸️ <b>Entries paused on OANDA</b>",
            f"The broker is not providing enough live pricing or candle data for safe entries across the scan universe. This can happen during bank holidays, market closures, or temporary OANDA issues.\n"
            f"{session_text}\n"
            f"Examples: {blocker_examples}",
        )

    if any(cat == "spread_wide" for cat in categorized):
        return (
            "spread_wide",
            "⏸️ <b>Entries paused on OANDA</b>",
            f"Prices are live, but spreads are still too wide for safe entries. This often happens around reopen, low-liquidity periods, or holiday-thinned trading.\n"
            f"{session_text}\n"
            f"Examples: {blocker_examples}",
        )

    return (
        "pairs_blocked",
        "⏸️ <b>Entries paused on OANDA</b>",
        f"No tradable pairs are currently available on OANDA. This can happen during bank holidays, market closures, or temporary broker issues.\n"
        f"{session_text}\n"
        f"Scan status: {empty_reason}\n"
        f"Examples: {blocker_examples}",
    )


def notify_entry_pause(reason: str, title: str, body: str) -> None:
    global _entry_pause_reason
    if _entry_pause_reason == reason:
        return
    _entry_pause_reason = reason
    telegram(f"{title}\n━━━━━━━━━━━━━━━\n{body}")
    save_state()


def notify_entry_resume(session: dict, tradable_pairs: list[str]) -> None:
    global _entry_pause_reason
    if not _entry_pause_reason:
        return
    _entry_pause_reason = ""
    telegram(
        f"✅ <b>Entries available again</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"OANDA trading conditions have normalized enough for new entries again.\n"
        f"Session: {session['name']} ({session['aggression']})\n"
        f"Tradable pairs: {_format_pair_list(tradable_pairs)}"
    )
    save_state()

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
    with _open_trades_lock:
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
        with _open_trades_lock:
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

    direct_pair = f"{from_currency}_{to_currency}"
    inverse_pair = f"{to_currency}_{from_currency}"
    supported_pairs = None
    if not PAPER_TRADE and OANDA_API_KEY and OANDA_ACCOUNT_ID:
        supported_pairs = get_supported_currency_pairs()

    if supported_pairs is None or direct_pair in supported_pairs:
        direct_mid = get_mid_price(direct_pair)
        if direct_mid is not None:
            return direct_mid

    if supported_pairs is None or inverse_pair in supported_pairs:
        inverse_mid = get_mid_price(inverse_pair)
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
    return calculate_units_for_risk_amount(instrument, risk_amount, sl_pips, account_currency)


def calculate_units_for_risk_amount(instrument: str, risk_amount: float, sl_pips: float,
                                    account_currency: str | None = None) -> float:
    if sl_pips <= 0:
        sl_pips = 10
    if ACCOUNT_TYPE == "spread_bet" and not uses_oanda_native_units():
        stake = risk_amount / sl_pips
        return max(SPREAD_BET_MIN_STAKE, math.floor(stake * 100) / 100)  # round DOWN to never exceed intended risk
    else:
        currency = account_currency or get_account_currency()
        pip_value_per_unit = pip_value(instrument, 1.0, currency)
        if pip_value_per_unit <= 0:
            pip_value_per_unit = pip_size(instrument)
        units = risk_amount / (sl_pips * pip_value_per_unit)
        return max(1, int(round(units)))

def place_order(instrument: str, units: float, direction: str,
                tp_price: float = None, sl_price: float = None,
                trailing_sl_pips: float = None, label: str = "",
                strategy: str = "",
                bid: float | None = None, ask: float | None = None,
                expected_spread_pips: float | None = None) -> dict:
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

    strategy_upper = (strategy or label or "").upper()
    price_precision = 5 if "JPY" not in instrument else 3

    # Tier 1 §7 item 6 — SCALPER / REVERSAL / PULLBACK are mean-revert strategies
    # whose realistic alpha is 1–3 pips. Paying the full spread on entry kills
    # them. Route through a mid-spread LIMIT/GTD order with a 2s expiry; fall
    # through to MARKET on cancel.
    use_limit_entry = (
        strategy_upper in {"SCALPER", "REVERSAL", "PULLBACK"}
        and bid is not None and ask is not None
        and bid > 0 and ask > bid
    )

    # Build common client extensions for idempotency + attribution (Tier 1 §7 item 8).
    ext_id = f"fxbot-{(strategy_upper or 'bot').lower()}-{instrument}-{int(time.time()*1000)}"
    client_ext = {
        "id": ext_id[:128],
        "tag": strategy_upper[:64] if strategy_upper else "BOT",
        "comment": (label or strategy_upper)[:128],
    }

    def _build_bracket(order_dict: dict) -> None:
        if tp_price:
            order_dict["takeProfitOnFill"] = {"price": f"{tp_price:.{price_precision}f}"}
        if sl_price:
            order_dict["stopLossOnFill"] = {"price": f"{sl_price:.{price_precision}f}"}
        if trailing_sl_pips:
            dist = pips_to_price(instrument, trailing_sl_pips)
            order_dict["trailingStopLossOnFill"] = {"distance": f"{dist:.{price_precision}f}"}

    def _submit(order: dict) -> dict:
        body = {"order": order}
        return oanda_post(f"/v3/accounts/{OANDA_ACCOUNT_ID}/orders", body)

    result: dict | None = None
    fill: dict = {}
    used_mode = "MARKET"

    if use_limit_entry:
        try:
            from fxbot.execution import plan_limit_entry
            plan = plan_limit_entry(direction=direction, bid=float(bid), ask=float(ask), mid_offset_frac=0.5, wait_seconds=2)
            limit_price = round(plan.limit_price, price_precision)
            gtd_time = (datetime.now(timezone.utc) + timedelta(seconds=max(plan.wait_seconds, 1))).strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
            limit_order: dict = {
                "type": "LIMIT",
                "instrument": instrument,
                "units": str(signed_units),
                "price": f"{limit_price:.{price_precision}f}",
                "timeInForce": "GTD",
                "gtdTime": gtd_time,
                "positionFill": "DEFAULT",
                "clientExtensions": dict(client_ext),
            }
            _build_bracket(limit_order)
            log.info(
                f"[{label}] Placing LIMIT {direction} {instrument} px={limit_price} "
                f"units={signed_units} gtd={plan.wait_seconds}s TP={tp_price} SL={sl_price} trail={trailing_sl_pips}"
            )
            limit_result = _submit(limit_order)
            # If the limit was accepted AND filled within the GTD window, we're done.
            fill_candidate = limit_result.get("orderFillTransaction", {})
            if fill_candidate:
                result = limit_result
                fill = fill_candidate
                used_mode = "LIMIT"
            else:
                log.info(f"[{label}] LIMIT entry not filled within {plan.wait_seconds}s — falling back to MARKET")
        except Exception as e:
            log.warning(f"[{label}] LIMIT planning failed ({e}) — using MARKET")

    if result is None:
        # MARKET fallback / default path (Tier 1 §7 items 7 + 8).
        market_order: dict = {
            "type": "MARKET",
            "instrument": instrument,
            "units": str(signed_units),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "clientExtensions": dict(client_ext),
        }
        # priceBound caps accepted fill at mid ± 1.5 × expected spread.
        try:
            mid: float | None = None
            if bid is not None and ask is not None and bid > 0 and ask > bid:
                mid = (float(bid) + float(ask)) / 2.0
            if mid is None:
                px = get_current_price(instrument)
                if px and px.get("bid", 0) > 0 and px.get("ask", 0) > 0:
                    mid = (float(px["bid"]) + float(px["ask"])) / 2.0
                    if expected_spread_pips is None:
                        expected_spread_pips = (float(px["ask"]) - float(px["bid"])) / pip_size(instrument)
            if expected_spread_pips is None and bid is not None and ask is not None and bid > 0:
                expected_spread_pips = (float(ask) - float(bid)) / pip_size(instrument)
            if expected_spread_pips is None or expected_spread_pips <= 0:
                expected_spread_pips = 2.0
            if mid is not None:
                bound_offset = 1.5 * expected_spread_pips * pip_size(instrument)
                bound_price = mid + bound_offset if direction == "LONG" else mid - bound_offset
                market_order["priceBound"] = f"{bound_price:.{price_precision}f}"
        except Exception as e:
            log.debug(f"[{label}] priceBound calc skipped: {e}")

        _build_bracket(market_order)
        log.info(
            f"[{label}] Placing MARKET {direction} {instrument} units={signed_units} "
            f"priceBound={market_order.get('priceBound','-')} TP={tp_price} SL={sl_price} trail={trailing_sl_pips}"
        )
        result = _submit(market_order)
        fill = result.get("orderFillTransaction", {})

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
            notify_entry_pause(
                "broker_closed",
                "⏸️ <b>Entries paused on OANDA</b>",
                f"OANDA rejected a live order because the market is closed or in close-only mode. This can happen during bank holidays, weekend maintenance, or broker halts.\n"
                f"Latest pair: {instrument} {direction}\n"
                f"Reason: {reject_message[:160]}"
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
        _tier2_log_slippage(
            instrument=instrument,
            strategy=strategy or label,
            direction=direction,
            bid=bid,
            ask=ask,
            fill_price=fill_price,
            label=label,
        )
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
    if not telegram_enabled():
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
            block_reason = get_strategy_entry_block_reason(strategy, opp["instrument"], opp["direction"], opp=opp, session_name=session["name"])
            if block_reason is None:
                current_score = float(opp.get("selection_score", opp.get("score", 0.0)) or 0.0)
                best_score = float(best.get("selection_score", best.get("score", 0.0)) or 0.0) if best is not None else float("-inf")
                if best is None or current_score > best_score:
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
    if not telegram_enabled():
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
            elif text == "/close" or text == "/closeall":
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
                    "/close / /closeall — Close all open positions\n"
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
        live_unrealized += pnl_pips * pip_value(trade["instrument"], trade.get("units", 1), currency)

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
    ccy = get_account_currency()
    telegram(
        f"📈 <b>Metrics</b> ({total} trades)\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Win rate: {wr:.1f}% ({wins}W/{losses}L)\n"
        f"Total P&L: {ccy}{total_pnl:+.2f}\n"
        f"Avg win: {ccy}{avg_win:+.2f} | Avg loss: {ccy}{avg_loss:+.2f}\n"
        f"Profit factor: {pf:.2f}\n"
        f"Sharpe: {sharpe:.2f}\n"
        f"Long P&L: {ccy}{long_pnl:+.2f} | Short: {ccy}{short_pnl:+.2f}\n"
        f"\n<b>By Strategy:</b>\n" + "\n".join(strat_lines)
    )

# ═══════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def save_state():
    try:
        with _open_trades_lock:
            trades_snapshot = copy.deepcopy(open_trades)
        payload = {
            "open_trades":           trades_snapshot,
            "trade_history":         trade_history[-500:],
            "consecutive_losses":    _consecutive_losses,
            "streak_paused_at":      _streak_paused_at,
            "entry_pause_reason":    _entry_pause_reason,
            "weekend_mode_active":   _weekend_mode_active,
            "paused":                _paused,
            "adaptive_offsets":      _adaptive_offsets,
            "last_rebalance_count":  _last_rebalance_count,
            "pair_cooldowns":        _pair_cooldowns,
            "pair_health":           _pair_health,
            "pending_close_retries": _pending_close_retries,
            "strategy_score_history": _strategy_score_history,
            "strategy_posteriors": {
                k: {
                    "strategy": v.strategy,
                    "alpha": v.alpha,
                    "beta": v.beta,
                    "trades": v.trades,
                    "last_update_utc": v.last_update_utc.isoformat() if v.last_update_utc else None,
                }
                for k, v in _strategy_posteriors.items()
            },
            "strategy_bayesian_weights": _strategy_bayesian_weights,
            "saved_at":              datetime.now(timezone.utc).isoformat(),
        }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, default=str)
        os.replace(tmp, STATE_FILE)
        publish_fx_shared_budget_state()
    except Exception as e:
        log.warning(f"State save failed: {e}")

def load_state():
    global open_trades, trade_history, _consecutive_losses, _streak_paused_at, _weekend_mode_active, _entry_pause_reason
    global _paused, _adaptive_offsets, _last_rebalance_count, _pair_cooldowns, _pair_health, _pending_close_retries
    global _strategy_score_history, _strategy_posteriors, _strategy_bayesian_weights
    try:
        if not os.path.exists(STATE_FILE):
            return
        with open(STATE_FILE) as f:
            d = json.load(f)
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(d.get("saved_at", "2000-01-01T00:00:00+00:00"))
               ).total_seconds()
        with _open_trades_lock:
            open_trades         = d.get("open_trades", [])
        trade_history       = d.get("trade_history", [])
        _consecutive_losses = d.get("consecutive_losses", 0)
        _streak_paused_at   = d.get("streak_paused_at", 0.0)
        _entry_pause_reason = d.get("entry_pause_reason", "weekend" if d.get("weekend_mode_active", False) else "")
        _weekend_mode_active = d.get("weekend_mode_active", _entry_pause_reason == "weekend")
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
        # Tier 2 state restoration.
        raw_history = d.get("strategy_score_history", {})
        if isinstance(raw_history, dict):
            _strategy_score_history = {
                str(k).upper(): [float(x) for x in (v or []) if isinstance(x, (int, float))]
                for k, v in raw_history.items()
            }
        raw_posteriors = d.get("strategy_posteriors", {})
        if isinstance(raw_posteriors, dict):
            restored: dict[str, StrategyPosterior] = {}
            for k, v in raw_posteriors.items():
                if not isinstance(v, dict):
                    continue
                try:
                    ts = v.get("last_update_utc")
                    ts_parsed = datetime.fromisoformat(ts) if isinstance(ts, str) else None
                    restored[str(k).upper()] = StrategyPosterior(
                        strategy=str(v.get("strategy", k)).upper(),
                        alpha=float(v.get("alpha", 5.0)),
                        beta=float(v.get("beta", 5.0)),
                        trades=int(v.get("trades", 0) or 0),
                        last_update_utc=ts_parsed,
                    )
                except Exception:
                    continue
            _strategy_posteriors = restored
        raw_weights = d.get("strategy_bayesian_weights", {})
        if isinstance(raw_weights, dict):
            _strategy_bayesian_weights = {
                str(k).upper(): float(v)
                for k, v in raw_weights.items()
                if isinstance(v, (int, float))
            }
        _tier2_refresh_drawdown_state()
    except Exception as e:
        log.warning(f"State load failed ({e}) — starting fresh")


def _count_calibration_pairs(data: dict) -> int:
    count = 0
    for pairs in data.get("by_strategy_pair", {}).values():
        if isinstance(pairs, dict):
            count += len(pairs)
    return count


def _count_calibration_trades(data: dict) -> int:
    total = data.get("total_trades")
    if isinstance(total, int):
        return total
    by_strategy = data.get("by_strategy", {})
    if isinstance(by_strategy, dict):
        return sum(int(stats.get("trades", 0) or 0) for stats in by_strategy.values() if isinstance(stats, dict))
    return 0


def _calibration_summary() -> dict:
    """Build a compact calibration summary for status payloads and heartbeat."""
    if not trade_calibration:
        return {"active": False}
    by_strategy = trade_calibration.get("by_strategy", {})
    return {
        "active": True,
        "generated_at": trade_calibration.get("generated_at"),
        "total_trades": _count_calibration_trades(trade_calibration),
        "pair_entries": _count_calibration_pairs(trade_calibration),
        "strategies": {
            name: {
                "trades": int(stats.get("trades", 0) or 0),
                "win_rate": float(stats.get("win_rate", 0) or 0),
                "profit_factor": float(stats.get("profit_factor", 0) or 0),
            }
            for name, stats in by_strategy.items()
            if isinstance(stats, dict)
        },
    }


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _validate_trade_calibration_payload(data: dict) -> tuple[bool, str | None]:
    total_trades = _count_calibration_trades(data)
    if total_trades < CALIBRATION_MIN_TOTAL_TRADES:
        return False, f"insufficient sample ({total_trades} trades < {CALIBRATION_MIN_TOTAL_TRADES})"
    generated_at = _parse_iso_utc(data.get("generated_at"))
    if generated_at is None:
        return False, "missing generated_at"
    max_age_seconds = max(0.0, CALIBRATION_MAX_AGE_HOURS * 3600.0)
    age_seconds = (datetime.now(timezone.utc) - generated_at).total_seconds()
    if age_seconds > max_age_seconds:
        age_hours = age_seconds / 3600.0
        return False, f"stale calibration ({age_hours:.1f}h > {CALIBRATION_MAX_AGE_HOURS:.1f}h)"
    return True, None


def _load_trade_calibration_from_redis() -> dict | None:
    if REDIS_CLIENT is None:
        return None
    try:
        raw = REDIS_CLIENT.get(REDIS_TRADE_CALIBRATION_KEY)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception as e:
        log.warning(f"Trade calibration Redis load failed for key {REDIS_TRADE_CALIBRATION_KEY}: {e}")
    return None


def load_trade_calibration() -> None:
    global trade_calibration
    try:
        data = _load_trade_calibration_from_redis()
        if data is not None:
            ok, reason = _validate_trade_calibration_payload(data)
            if not ok:
                trade_calibration = {}
                log.info(f"[CALIBRATION] Ignoring Redis calibration from key {REDIS_TRADE_CALIBRATION_KEY}: {reason}")
                return
            trade_calibration = data
            log.info(
                f"[CALIBRATION] Loaded trade calibration from Redis key {REDIS_TRADE_CALIBRATION_KEY}: "
                f"{_count_calibration_pairs(trade_calibration)} strategy/pair entries, "
                f"{_count_calibration_trades(trade_calibration)} trades"
            )
            return

        if not os.path.exists(TRADE_CALIBRATION_FILE):
            trade_calibration = {}
            return

        with open(TRADE_CALIBRATION_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("trade calibration file content must be an object")

        ok, reason = _validate_trade_calibration_payload(data)
        if not ok:
            trade_calibration = {}
            log.info(f"[CALIBRATION] Ignoring file calibration from {TRADE_CALIBRATION_FILE}: {reason}")
            return

        trade_calibration = data
        log.info(
            f"[CALIBRATION] Loaded trade calibration from {TRADE_CALIBRATION_FILE}: "
            f"{_count_calibration_pairs(trade_calibration)} strategy/pair entries, "
            f"{_count_calibration_trades(trade_calibration)} trades"
        )
    except Exception as e:
        trade_calibration = {}
        log.warning(f"Trade calibration load failed for {TRADE_CALIBRATION_FILE}: {e}")


def refresh_trade_calibration() -> bool:
    global _trade_calibration_mtime
    if REDIS_CLIENT is not None:
        data = _load_trade_calibration_from_redis()
        if data is not None:
            ok, reason = _validate_trade_calibration_payload(data)
            if not ok:
                if trade_calibration:
                    trade_calibration.clear()
                log.info(f"[CALIBRATION] Ignoring Redis calibration from key {REDIS_TRADE_CALIBRATION_KEY}: {reason}")
                return False
            generated_at = data.get("generated_at")
            if generated_at and generated_at != _trade_calibration_mtime:
                _trade_calibration_mtime = generated_at
                trade_calibration.clear()
                trade_calibration.update(data)
                log.info(
                    f"[CALIBRATION] Loaded trade calibration from Redis key {REDIS_TRADE_CALIBRATION_KEY}: "
                    f"{_count_calibration_pairs(trade_calibration)} strategy/pair entries, "
                    f"{_count_calibration_trades(trade_calibration)} trades"
                )
                return True
            if generated_at and not trade_calibration:
                _trade_calibration_mtime = generated_at
                trade_calibration.clear()
                trade_calibration.update(data)
                return True
            return False

    try:
        mtime = os.path.getmtime(TRADE_CALIBRATION_FILE)
        if mtime != _trade_calibration_mtime:
            _trade_calibration_mtime = mtime
            load_trade_calibration()
            return True
    except FileNotFoundError:
        if trade_calibration:
            trade_calibration.clear()
            log.warning(f"Trade calibration file removed: {TRADE_CALIBRATION_FILE}")
        _trade_calibration_mtime = 0.0
    return False


def _get_trade_calibration_stats(strategy: str, instrument: str, session_name: str | None = None) -> tuple[dict | None, str | None]:
    by_pair = trade_calibration.get("by_strategy_pair", {})
    pair_stats = None
    if isinstance(by_pair, dict):
        pair_stats = by_pair.get(strategy, {}).get(instrument)

    if session_name:
        by_session = trade_calibration.get("by_strategy_pair_session", {})
        if isinstance(by_session, dict):
            session_stats = by_session.get(strategy, {}).get(instrument, {}).get(session_name)
            if isinstance(session_stats, dict) and int(session_stats.get("trades", 0) or 0) >= CALIBRATION_SESSION_MIN_TRADES:
                return session_stats, "session"

    if isinstance(pair_stats, dict) and int(pair_stats.get("trades", 0) or 0) >= CALIBRATION_PAIR_MIN_TRADES:
        return pair_stats, "pair"
    return None, None


def get_trade_calibration_adjustment(strategy: str, instrument: str, session_name: str | None = None) -> dict:
    stats, source = _get_trade_calibration_stats(strategy, instrument, session_name)
    result = {
        "threshold_offset": 0.0,
        "risk_mult": 1.0,
        "block_reason": None,
        "source": source,
    }
    if not stats:
        return result

    trades = int(stats.get("trades", 0) or 0)
    win_rate = float(stats.get("win_rate", 0.0) or 0.0)
    profit_factor = float(stats.get("profit_factor", 0.0) or 0.0)
    expectancy_pips = float(stats.get("expectancy_pips", 0.0) or 0.0)

    if (
        win_rate <= CALIBRATION_BLOCK_MAX_WIN_RATE
        and profit_factor <= CALIBRATION_BLOCK_MAX_PROFIT_FACTOR
        and expectancy_pips <= CALIBRATION_BLOCK_MAX_EXPECTANCY_PIPS
    ):
        result["block_reason"] = (
            f"calibration block ({source}: WR {win_rate:.0%}, "
            f"exp {expectancy_pips:+.1f}p, n={trades})"
        )
        return result

    if expectancy_pips < 0 or profit_factor < 1.0:
        threshold_offset = min(CALIBRATION_MAX_TIGHTEN, abs(min(expectancy_pips, 0.0)) / 1.5)
        threshold_offset += min(3.0, max(0.0, (1.0 - profit_factor) * 4.0))
        risk_mult = max(CALIBRATION_RISK_FLOOR, 1.0 - min(0.75, abs(min(expectancy_pips, 0.0)) / 20.0))
        if win_rate < 0.40:
            risk_mult = max(CALIBRATION_RISK_FLOOR, risk_mult - 0.10)
        result["threshold_offset"] = round(threshold_offset, 1)
        result["risk_mult"] = round(risk_mult, 3)
        return result

    if expectancy_pips > 3.0 and profit_factor > 1.2 and win_rate > 0.50:
        result["threshold_offset"] = round(-min(CALIBRATION_MAX_RELAX, expectancy_pips / 10.0), 1)
    return result


def _strategy_threshold_value(strategy: str) -> float:
    return {
        "SCALPER": SCALPER_THRESHOLD,
        "TREND": TREND_THRESHOLD,
        "REVERSAL": REVERSAL_THRESHOLD,
        "BREAKOUT": BREAKOUT_THRESHOLD,
        "CARRY": CARRY_THRESHOLD,
        "ASIAN_FADE": ASIAN_FADE_THRESHOLD,
        "POST_NEWS": POST_NEWS_THRESHOLD,
        "PULLBACK": PULLBACK_THRESHOLD,
    }.get(strategy, 40)

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
    mult = max(0.5, min(1.5, regime_atr_mult * vix_regime * dxy_regime))  # cap regime multiplier
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
        get_trade_calibration_adjustment=get_trade_calibration_adjustment,
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
#  TIER 2 CONSULTANT-ASSESSMENT HELPERS (§12–§22)
# ═══════════════════════════════════════════════════════════════

def _tier2_record_score(label: str, score: float) -> None:
    """Append a strategy score to the rolling history used by percentile sizing."""
    if not label or score is None:
        return
    try:
        value = float(score)
    except (TypeError, ValueError):
        return
    key = str(label).upper()
    history = _strategy_score_history.setdefault(key, [])
    history.append(value)
    lookback = max(20, int(TIER2_PERCENTILE_LOOKBACK))
    # Keep twice the lookback so percentile estimates stay stable.
    max_len = lookback * 2
    if len(history) > max_len:
        del history[: len(history) - max_len]


def _tier2_percentile_mult(label: str, score: float) -> tuple[float, float | None, int]:
    """Return (multiplier, percentile|None, samples) for the given candidate."""
    if not TIER2_PERCENTILE_SIZING_ENABLED:
        return 1.0, None, 0
    key = str(label).upper()
    history = _strategy_score_history.get(key, [])
    decision = size_by_percentile(
        score=float(score or 0.0),
        history=history[-int(TIER2_PERCENTILE_LOOKBACK):],
        floor=TIER2_PERCENTILE_FLOOR,
        cap=TIER2_PERCENTILE_CAP,
    )
    return decision.multiplier, decision.percentile, decision.samples


def _tier2_trade_risk_pct(trade: dict, nav: float) -> float:
    """Approximate risk_pct used by the portfolio-vol correlation cap."""
    risk_amount = float(trade.get("risk_amount", 0.0) or 0.0)
    if nav <= 0 or risk_amount <= 0:
        return 0.0
    return risk_amount / nav


def _tier2_portfolio_vol_breach(
    instrument: str,
    direction: str,
    candidate_risk_amount: float,
    nav: float,
) -> str | None:
    """Return a block reason if adding this trade would exceed the portfolio-vol cap."""
    if not TIER2_PORTFOLIO_VOL_ENABLED or nav <= 0 or candidate_risk_amount <= 0:
        return None
    try:
        with _open_trades_lock:
            snapshot = [
                {
                    "instrument": t.get("instrument", ""),
                    "direction": t.get("direction", ""),
                    "risk_pct": _tier2_trade_risk_pct(t, nav),
                }
                for t in open_trades
            ]
        decision = would_breach_portfolio_cap(
            open_trades=snapshot,
            candidate_instrument=instrument,
            candidate_direction=direction,
            candidate_risk_pct=candidate_risk_amount / nav,
            cap_pct=TIER2_PORTFOLIO_VOL_CAP_PCT,
            correlation=default_correlation_matrix(),
        )
    except Exception as exc:  # pragma: no cover — defensive
        log.debug(f"portfolio-vol check error: {exc}")
        return None
    if not decision.allowed:
        return (
            f"portfolio_vol {decision.portfolio_vol_after:.3%}"
            f" > cap {decision.cap:.3%}"
        )
    return None


# Per-pair 4h ATR ratio would be ideal for a live regime classifier; use the
# existing _market_regime_mult as a lightweight proxy until the ATR feed is
# wired into main.py.
def _tier2_regime_label() -> "Regime":
    """Derive a coarse Regime from currently-known macro scalars."""
    dxy = _dxy_ema_gap
    vix = _vix_level
    if dxy is not None and abs(dxy) >= 0.015:
        return Regime.USD_TREND
    if vix is not None and vix >= 25.0:
        return Regime.RISK_OFF
    if vix is not None and vix <= 13.5:
        return Regime.RISK_ON
    return Regime.CHOP


def _tier2_regime_block_reason(strategy: str) -> str | None:
    if not TIER2_REGIME_GATE_ENABLED:
        return None
    regime = _tier2_regime_label()
    if regime_is_strategy_enabled(strategy, regime):
        return None
    return f"regime_gate {regime.value} disables {strategy}"


def _tier2_reconciliation_block(
    strategy: str,
    instrument: str,
    direction: str,
    score: float,
) -> str | None:
    if not TIER2_STRATEGY_DEDUP_ENABLED:
        return None
    decision = _strategy_reconciliation.check(
        strategy=strategy,
        instrument=instrument,
        direction=direction,
        score=float(score or 0.0),
    )
    if decision.allowed:
        return None
    return f"strategy_dedup: {decision.reason}"


def _tier2_daily_pnl_series() -> list[float]:
    """Aggregate trade_history into daily P&L-as-fraction-of-NAV series."""
    by_day: dict[str, float] = {}
    for trade in trade_history:
        closed_at = trade.get("closed_at", "")
        if not closed_at or len(closed_at) < 10:
            continue
        day = closed_at[:10]
        by_day[day] = by_day.get(day, 0.0) + float(trade.get("pnl_pct", 0.0) or 0.0) / 100.0
    ordered = [by_day[d] for d in sorted(by_day.keys())]
    lookback = max(TIER2_DD_HARD_HALT_DAYS, TIER2_DD_SOFT_CUT_DAYS) + 5
    return ordered[-lookback:]


def _tier2_refresh_drawdown_state() -> None:
    global _drawdown_risk_scale, _drawdown_hard_halt, _drawdown_reason
    if not TIER2_DRAWDOWN_KILL_ENABLED:
        _drawdown_risk_scale = 1.0
        _drawdown_hard_halt = False
        _drawdown_reason = ""
        return
    series = _tier2_daily_pnl_series()
    decision = evaluate_drawdown_kill(
        daily_pnl_pct=series,
        soft_cut_lookback_days=TIER2_DD_SOFT_CUT_DAYS,
        hard_halt_lookback_days=TIER2_DD_HARD_HALT_DAYS,
        soft_cut_threshold_pct=TIER2_DD_SOFT_CUT_PCT,
        hard_halt_threshold_pct=TIER2_DD_HARD_HALT_PCT,
        soft_cut_risk_scale=TIER2_DD_SOFT_CUT_RISK_SCALE,
    )
    _drawdown_hard_halt = decision.hard_halt
    _drawdown_reason = decision.reason
    if decision.hard_halt:
        _drawdown_risk_scale = 0.0
    elif decision.soft_cut:
        _drawdown_risk_scale = max(
            0.0, min(1.0, decision.risk_per_trade_override or TIER2_DD_SOFT_CUT_RISK_SCALE)
        )
    else:
        _drawdown_risk_scale = 1.0


def _tier2_drawdown_block_reason() -> str | None:
    if not TIER2_DRAWDOWN_KILL_ENABLED:
        return None
    if _drawdown_hard_halt:
        return f"drawdown_hard_halt ({_drawdown_reason})"
    return None


def _tier2_get_bayesian_weight(label: str) -> float:
    if not TIER2_BAYESIAN_WEIGHTING_ENABLED:
        return 1.0
    weights = _strategy_bayesian_weights
    if not weights:
        return 1.0
    key = str(label).upper()
    if key not in weights:
        return 1.0
    mean = sum(weights.values()) / max(1, len(weights))
    if mean <= 0:
        return 1.0
    raw = weights[key] / mean
    return max(TIER2_BAYESIAN_MIN_WEIGHT, min(TIER2_BAYESIAN_MAX_WEIGHT, raw))


def _tier2_rebuild_bayesian_weights() -> None:
    if not TIER2_BAYESIAN_WEIGHTING_ENABLED:
        _strategy_bayesian_weights.clear()
        return
    posteriors = list(_strategy_posteriors.values())
    if not posteriors:
        _strategy_bayesian_weights.clear()
        return
    weights = bayesian_allocate_weights(posteriors)
    _strategy_bayesian_weights.clear()
    _strategy_bayesian_weights.update(weights)


def _tier2_update_posteriors(label: str, win: bool) -> None:
    if not TIER2_BAYESIAN_WEIGHTING_ENABLED or not label:
        return
    key = str(label).upper()
    existing = _strategy_posteriors.get(key) or bayesian_new_posterior(key)
    _strategy_posteriors[key] = bayesian_update_posterior(existing, win=win)
    # Rebuild weights every PERF_REBALANCE_TRADES closes.
    total_trades = sum(p.trades for p in _strategy_posteriors.values())
    if total_trades and total_trades % max(1, PERF_REBALANCE_TRADES) == 0:
        _tier2_rebuild_bayesian_weights()


def _tier2_refresh_financing(account_id: str, fetch) -> int:
    global _last_financing_refresh_at
    if not TIER2_FINANCING_ENABLED or not account_id or fetch is None:
        return 0
    now = time.time()
    if not _financing_cache.is_stale() and (now - _last_financing_refresh_at) < TIER2_FINANCING_REFRESH_SECS:
        return 0
    try:
        loaded = _financing_cache.refresh(fetch, account_id)
    except Exception as exc:  # pragma: no cover — defensive
        log.debug(f"financing refresh error: {exc}")
        return 0
    _last_financing_refresh_at = now
    if loaded:
        log.info(f"💱 Financing rates refreshed: {loaded} instruments")
    return loaded


def _tier2_carry_block_reason(label: str, instrument: str, direction: str) -> str | None:
    if not TIER2_FINANCING_ENABLED:
        return None
    if str(label).upper() != "CARRY":
        return None
    quote = _financing_cache.get(instrument)
    if quote is None:
        return None  # Cache miss — don't hard-block legacy CARRY pathway.
    if not is_carry_favourable(
        quote=quote,
        direction=direction,
        min_bps_per_day=TIER2_CARRY_MIN_BPS_PER_DAY,
    ):
        return (
            f"carry_unfavourable long={quote.long_bps_per_day:.2f}bps/day"
            f" short={quote.short_bps_per_day:.2f}bps/day"
        )
    return None


def _tier2_log_slippage(
    *,
    instrument: str,
    strategy: str,
    direction: str,
    bid: float | None,
    ask: float | None,
    fill_price: float,
    session: str = "",
    label: str = "",
) -> None:
    if not TIER2_SLIPPAGE_LOG_ENABLED or fill_price is None:
        return
    try:
        b = float(bid or 0.0)
        a = float(ask or 0.0)
        if b <= 0 or a <= 0:
            signal_mid = float(fill_price)
        else:
            signal_mid = 0.5 * (b + a)
        ps = pip_size(instrument)
        _slippage_logger.log(
            instrument=instrument,
            strategy=strategy or label,
            direction=direction,
            signal_mid=signal_mid,
            fill_price=float(fill_price),
            pip_size=ps,
            session=session,
            label=label,
        )
    except Exception as exc:  # pragma: no cover — defensive
        log.debug(f"slippage log error: {exc}")


# ═══════════════════════════════════════════════════════════════
#  ENTRY & EXIT MANAGEMENT
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
    dd_block = _tier2_drawdown_block_reason()
    if dd_block is not None:
        return dd_block
    price_data = get_current_price(instrument)
    entry_price = price_data["ask"] if direction == "LONG" else price_data["bid"]
    if entry_price <= 0:
        return "no live price"
    return None


def get_strategy_entry_block_reason(strategy: str, instrument: str, direction: str, opp: dict | None = None, session_name: str | None = None) -> str | None:
    block_reason = get_entry_block_reason(instrument, direction)
    if block_reason is not None:
        return block_reason
    if session_name is None:
        session_name = get_current_session()["name"]
    calibration = get_trade_calibration_adjustment(strategy, instrument, session_name)
    if calibration["block_reason"] is not None:
        return calibration["block_reason"]
    if opp is not None:
        required_score = _strategy_threshold_value(strategy) + calibration["threshold_offset"]
        actual_score = float(opp.get("score", 0.0) or 0.0)
        if actual_score < required_score:
            return f"calibration threshold {actual_score:.1f} < {required_score:.1f}"
        # Tier 1 §7 item 9 — net-of-cost R:R gate. Reject entries whose TP does
        # not clear min_net_rr after subtracting round-trip spread, slippage
        # and expected financing from the TP distance. Disabled by setting
        # MIN_NET_RR=0 in the environment.
        try:
            from fxbot.cost_model import compute_net_rr
            sl_pips = float(opp.get("sl_pips", 0.0) or 0.0)
            tp_pips = float(opp.get("tp_pips", 0.0) or 0.0)
            entry_spread = float(opp.get("spread_pips", 0.0) or 0.0)
            if sl_pips > 0 and tp_pips > 0 and MIN_NET_RR > 0:
                breakdown = compute_net_rr(
                    sl_pips=sl_pips,
                    tp_pips=tp_pips,
                    entry_spread_pips=entry_spread,
                    slippage_pips=NET_RR_SLIPPAGE_PIPS,
                    financing_pips=NET_RR_FINANCING_PIPS,
                    min_net_rr=MIN_NET_RR,
                )
                if not breakdown.passed:
                    return (
                        f"net_rr {breakdown.net_rr:.2f} < {MIN_NET_RR:.2f} "
                        f"(sl={sl_pips:.1f} tp={tp_pips:.1f} spread={entry_spread:.1f})"
                    )
        except Exception as e:  # pragma: no cover - defensive
            log.debug(f"net_rr gate error: {e}")
    if is_pair_paused_by_news(instrument) and strategy != "CARRY":
        return "pre-news risk window"
    regime_block = _tier2_regime_block_reason(strategy)
    if regime_block is not None:
        return regime_block
    carry_block = _tier2_carry_block_reason(strategy, instrument, direction)
    if carry_block is not None:
        return carry_block
    if opp is not None:
        recon_block = _tier2_reconciliation_block(
            strategy,
            instrument,
            direction,
            float(opp.get("score", 0.0) or 0.0),
        )
        if recon_block is not None:
            return recon_block
    return None


def get_entry_risk_multiplier(strategy: str, instrument: str, session_name: str | None = None) -> float:
    if session_name is None:
        session_name = get_current_session()["name"]
    risk_mult = NEWS_WINDOW_RISK_MULT if is_pair_paused_by_news(instrument) else 1.0
    calibration = get_trade_calibration_adjustment(strategy, instrument, session_name)
    dd_scale = float(_drawdown_risk_scale) if TIER2_DRAWDOWN_KILL_ENABLED else 1.0
    return round(max(CALIBRATION_RISK_FLOOR, risk_mult * calibration["risk_mult"] * dd_scale), 3)

def open_trade_entry(opp: dict, label: str, balance: float) -> dict | None:
    instrument = opp["instrument"]
    direction  = opp["direction"]
    acct = get_account_summary()
    session = get_current_session()
    session_name = session["name"]

    block_reason = get_strategy_entry_block_reason(label, instrument, direction, opp=opp, session_name=session_name)
    if block_reason is not None:
        log.info(f"[{label}] Skip {instrument} {direction} — {block_reason}")
        return None

    calibration = get_trade_calibration_adjustment(label, instrument, session_name)

    eff_threshold = float(opp.get("effective_threshold", 0) or 0)
    if eff_threshold > 0:
        kelly_gap = opp["score"] - eff_threshold
    else:
        kelly_gap = opp["score"] - {"SCALPER": SCALPER_THRESHOLD, "TREND": TREND_THRESHOLD,
                                     "REVERSAL": REVERSAL_THRESHOLD, "BREAKOUT": BREAKOUT_THRESHOLD,
                                     "CARRY": CARRY_THRESHOLD, "ASIAN_FADE": ASIAN_FADE_THRESHOLD,
                                     "POST_NEWS": POST_NEWS_THRESHOLD, "PULLBACK": PULLBACK_THRESHOLD}.get(label, 40)
    kelly_mult = (KELLY_MULT_HIGH_CONF if kelly_gap >= 30
                  else KELLY_MULT_STANDARD if kelly_gap >= 15
                  else KELLY_MULT_SOLID if kelly_gap >= 5
                  else KELLY_MULT_MARGINAL)
    risk_mult = get_entry_risk_multiplier(label, instrument, session_name)
    effective_kelly_mult = kelly_mult * risk_mult

    # Tier 2 §12 — score-percentile sizing. Scale risk by where the current
    # score sits in the per-strategy rolling distribution. Default is a no-op
    # when fewer than TIER2_PERCENTILE_LOOKBACK trades of history exist.
    percentile_mult, percentile_value, percentile_samples = _tier2_percentile_mult(label, opp.get("score", 0.0))
    # Tier 2 §22 — Bayesian posterior-weight scaling.
    bayesian_weight = _tier2_get_bayesian_weight(label)
    effective_kelly_mult *= percentile_mult * bayesian_weight

    account_currency = acct.get("currency", get_account_currency())
    budget_snapshot = build_fx_budget_snapshot(balance)
    risk_amount = min(
        budget_snapshot["max_trade_risk_amount"] * effective_kelly_mult,
        budget_snapshot["available_fx_risk"],
    )
    if risk_amount <= 0:
        log.info(
            f"[{label}] Skip {instrument} {direction} — FX sleeve risk exhausted "
            f"({budget_snapshot['reserved_fx_risk']:.2f}/{budget_snapshot['max_total_risk_amount']:.2f})"
        )
        return None

    # Tier 2 §13 — portfolio-vol cap (correlation-aware).
    nav_for_cap = float(acct.get("NAV", balance) or balance or 0.0)
    vol_block = _tier2_portfolio_vol_breach(instrument, direction, risk_amount, nav_for_cap)
    if vol_block is not None:
        log.info(f"[{label}] Skip {instrument} {direction} — {vol_block}")
        return None

    units = calculate_units_for_risk_amount(instrument, risk_amount, opp["sl_pips"], account_currency)

    price_data = get_current_price(instrument)
    entry_price = price_data["ask"] if direction == "LONG" else price_data["bid"]

    if entry_price <= 0:
        log.error(f"[{label}] No valid price for {instrument}")
        return None

    if uses_oanda_native_units():
        budget_preview = estimate_trade_budget(instrument, units, entry_price, account_currency)
        margin_required = budget_preview.get("margin_account")
        margin_available = float(acct.get("marginAvailable", 0) or 0)
        sleeve_margin_cap = budget_snapshot["fx_sleeve_balance"]
        effective_margin_available = min(margin_available, sleeve_margin_cap) if margin_available > 0 else sleeve_margin_cap
        if margin_required is not None and effective_margin_available > 0 and margin_required > effective_margin_available:
            log.info(
                f"[{label}] Skip {instrument} {direction} — insufficient FX sleeve margin "
                f"({margin_required:.2f} > {effective_margin_available:.2f} {account_currency})"
            )
            return None

    ps = pip_size(instrument)
    if direction == "LONG":
        tp_price = round(entry_price + opp["tp_pips"] * ps, 5 if "JPY" not in instrument else 3)
        sl_price = round(entry_price - opp["sl_pips"] * ps, 5 if "JPY" not in instrument else 3)
    else:
        tp_price = round(entry_price - opp["tp_pips"] * ps, 5 if "JPY" not in instrument else 3)
        sl_price = round(entry_price + opp["sl_pips"] * ps, 5 if "JPY" not in instrument else 3)

    trail_pips = opp.get("trail_pips")
    result = place_order(
        instrument, units, direction, tp_price, sl_price, trail_pips, label,
        strategy=label,
        bid=price_data.get("bid"),
        ask=price_data.get("ask"),
        expected_spread_pips=opp.get("spread_pips"),
    )
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
        "session_at_entry": session_name,
        "kelly_mult":     effective_kelly_mult,
        "risk_amount":    risk_amount,
        "news_risk_mult": risk_mult,
        "calibration_source": calibration.get("source"),
        "calibration_threshold_offset": calibration.get("threshold_offset", 0.0),
        "calibration_risk_mult": calibration.get("risk_mult", 1.0),
        "partial_tp_hit": False,
        "unrealized_pnl": 0,
        "percentile_mult": percentile_mult,
        "percentile_value": percentile_value,
        "percentile_samples": percentile_samples,
        "bayesian_weight": bayesian_weight,
    }

    if label == "TREND" and opp.get("partial_tp_pips"):
        if direction == "LONG":
            trade["partial_tp_price"] = actual_entry + opp["partial_tp_pips"] * ps
        else:
            trade["partial_tp_price"] = actual_entry - opp["partial_tp_pips"] * ps

    _pair_cooldowns[instrument] = time.time() + PAIR_COOLDOWN_SECS
    _tier2_record_score(label, opp.get("score", 0.0))
    save_state()

    account_currency = acct.get("currency", account_currency)
    nav_value = float(acct.get("NAV", balance) or balance or 0)
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
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
        f"FX sleeve: {account_currency} {budget_snapshot['fx_sleeve_balance']:.2f} | Reserved: {budget_snapshot['reserved_fx_risk']:.2f}\n"
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
    """Unified exit logic for all strategies.

    1. Dynamic SL derived from the trade's own sl_pips (set by the
       strategy scorer at entry).  Aligns position sizing with exit stop.
    2. Dynamic breakeven (entry + spread cost).  Once past breakeven,
       track peak and close if price drops EXIT_PEAK_TRAIL_PCT (1.5%)
       from the peak.
    3. Flat exit: close only if above breakeven AND held > EXIT_FLAT_HOURS.
    4. 7-day review: between -10% and breakeven -> estimate trend ->
       close if recovery probability < 70%.
       Below -10% -> flag for manual review but keep alive.
    """
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
        pnl_pct = (price - entry) / entry
    else:
        pnl_pips = (entry - price) / ps
        pnl_pct = (entry - price) / entry

    held_seconds = time.time() - trade.get("opened_ts", time.time())
    held_hours = held_seconds / 3600.0

    # Update peak tracking
    if direction == "LONG" and price > trade.get("highest_price", entry):
        trade["highest_price"] = price
    elif direction == "SHORT" and price < trade.get("lowest_price", entry):
        trade["lowest_price"] = price

    trade["unrealized_pnl"] = round(pnl_pips, 1)

    # ── Dynamic breakeven: entry + spread cost ────────────────
    spread_cost_pct = float(trade.get("spread_pips", 0)) * ps / entry
    breakeven_pct = spread_cost_pct

    # ── Dynamic SL from trade's own sl_pips ─────────────────
    if "dynamic_sl_pct" not in trade:
        sl_pips_val = float(trade.get("sl_pips") or 0)
        if sl_pips_val > 0 and entry > 0:
            trade["dynamic_sl_pct"] = -(sl_pips_val * ps) / entry
        else:
            trade["dynamic_sl_pct"] = -0.01  # fallback -1%
    dynamic_sl_pct = trade["dynamic_sl_pct"]

    # ── Peak P&L % ────────────────────────────────────────────
    if direction == "LONG":
        peak_pnl_pct = (trade.get("highest_price", entry) - entry) / entry
    else:
        peak_pnl_pct = (entry - trade.get("lowest_price", entry)) / entry

    # 1. Dynamic SL
    if pnl_pct <= dynamic_sl_pct:
        log.info(f"🛑 [{label}] SL hit: {instrument} | {pnl_pips:+.1f}p | {pnl_pct*100:+.1f}% (SL={dynamic_sl_pct*100:.2f}%)")
        return True, "STOP_LOSS"

    # 2. Above breakeven → trail from peak (1.5% drop from peak)
    if peak_pnl_pct > breakeven_pct and pnl_pct > breakeven_pct:
        drawdown_from_peak = peak_pnl_pct - pnl_pct
        if drawdown_from_peak >= EXIT_PEAK_TRAIL_PCT:
            log.info(f"📉 [{label}] Peak trail: {instrument} | {pnl_pips:+.1f}p | "
                     f"peak +{peak_pnl_pct*100:.1f}% → now +{pnl_pct*100:.1f}%")
            return True, "PEAK_TRAIL"

    # 3. Flat exit: above breakeven AND held > 48h
    if held_hours >= EXIT_FLAT_HOURS:
        if pnl_pct >= breakeven_pct and pnl_pct < breakeven_pct + EXIT_PEAK_TRAIL_PCT:
            log.info(f"😴 [{label}] Flat exit: {instrument} | {pnl_pips:+.1f}p after {held_hours:.0f}h")
            return True, "FLAT_EXIT"

    # 4. Long-term review (7 days)
    review_seconds = EXIT_REVIEW_DAYS * 86400
    if held_seconds >= review_seconds:
        if EXIT_REVIEW_POOR_THRESHOLD <= pnl_pct < breakeven_pct:
            # Between -10% and breakeven → close (runtime can't estimate trend)
            log.info(f"📋 [{label}] Review close: {instrument} | {pnl_pips:+.1f}p | "
                     f"{pnl_pct*100:+.1f}% after {held_hours/24:.0f}d")
            return True, "REVIEW_CLOSE"
        elif pnl_pct < EXIT_REVIEW_POOR_THRESHOLD:
            # Below -10% → flag for manual review
            if not trade.get("_flagged_manual_review"):
                trade["_flagged_manual_review"] = True
                log.warning(f"⚠️ [{label}] MANUAL REVIEW NEEDED: {instrument} | "
                            f"{pnl_pct*100:+.1f}% after {held_hours/24:.0f}d")
                telegram(
                    f"⚠️ <b>Manual Review Required</b>\n"
                    f"{label} | {instrument} | {direction}\n"
                    f"PnL: {pnl_pct*100:+.1f}% | Held: {held_hours/24:.0f}d\n"
                    f"Consider closing manually if no recovery expected"
                )

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

    pnl = pnl_pips * pip_value(instrument, trade.get("units", 1), get_account_currency())
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

    _tier2_update_posteriors(label, win=pnl > 0)
    _tier2_refresh_drawdown_state()

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
            with _open_trades_lock:
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

def send_heartbeat(balance: float, status: str = "running"):
    global last_heartbeat_at
    if time.time() - last_heartbeat_at < HEARTBEAT_INTERVAL:
        return
    last_heartbeat_at = time.time()
    publish_bot_runtime_status(status, balance=balance, force=True)
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
    ccy = get_account_currency()
    cal = _calibration_summary()
    if cal.get("active"):
        cal_parts = [f"{cal.get('total_trades', 0)} trades", f"{cal.get('pair_entries', 0)} pairs"]
        cal_text = " | ".join(cal_parts)
    else:
        cal_text = "none loaded"
    telegram(
        f"💓 <b>Heartbeat</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Balance: {ccy}{balance:,.2f}\n"
        f"Open: {len(open_trades)} trades{open_str}\n"
        f"Session: {session['name']} ({session['aggression']})\n"
        f"Regime: {regime} ({_market_regime_mult:.2f})\n"
        f"DXY gap: {f'{_dxy_ema_gap*100:+.2f}%' if _dxy_ema_gap is not None else 'unknown'} | "
        f"VIX: {f'{_vix_level:.1f}' if _vix_level is not None else 'unknown'}\n"
        f"News-paused pairs: {paused_summary}\n"
        f"📊 Calibration: {cal_text}\n"
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
        f"P&L: {get_account_currency()}{pnl:+.2f}\n"
        f"Long: {get_account_currency()}{by_dir['LONG']:+.2f} | Short: {get_account_currency()}{by_dir['SHORT']:+.2f}\n"
        f"Balance: {get_account_currency()}{balance:,.2f}"
    )

# ═══════════════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def _oanda_preflight_auth_check() -> None:
    """Tier 1 §7 item 1: verify the OANDA API token against the configured
    account at startup. If the token has insufficient scope (401) or the
    account is unreachable (404/403), send a loud Telegram alert and abort.

    This is the single most common cause of the bot running for hours with
    no trades — the 18h W2 log window fired two HTTP 401s silently because
    the existing error handling treated them as generic soft pair failures.
    """
    if PAPER_TRADE or not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        return
    url = f"{OANDA_API_URL}/v3/accounts/{OANDA_ACCOUNT_ID}/instruments"
    try:
        r = _get_session().get(url, timeout=10)
    except (requests.ConnectionError, requests.Timeout) as e:
        # Don't hard-fail the bot on a transient network blip — let the
        # normal retry loop surface it. But log loudly so operators know.
        log.warning(f"🔐 OANDA preflight network error: {e} — continuing, will retry in main loop")
        return
    if r.status_code == 200:
        log.info(f"🔐 OANDA preflight OK: token authorised for account {OANDA_ACCOUNT_ID}")
        return
    body = (r.text or "")[:300]
    msg = (
        f"OANDA preflight FAILED — HTTP {r.status_code} on /v3/accounts/{OANDA_ACCOUNT_ID}/instruments. "
        f"Body: {body}"
    )
    log.error(f"🔐 {msg}")
    try:
        telegram(
            "🛑 <b>OANDA auth preflight failed</b>\n"
            f"HTTP {r.status_code} on account <code>{OANDA_ACCOUNT_ID}</code>.\n"
            f"<i>{body[:180]}</i>\n"
            "Fix: re-issue the OANDA API token with View+Trade scope and redeploy."
        )
    except Exception:
        pass
    publish_bot_runtime_status("auth_error", error=msg[:200], force=True)
    raise SystemExit(f"OANDA auth preflight failed: HTTP {r.status_code}")


def _bootstrap_runtime() -> None:
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

    # Tier 1 §7 item 1 — verify OANDA auth before touching anything else.
    _oanda_preflight_auth_check()

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
    if not telegram_enabled():
        log.warning("Telegram notifications disabled: set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID for /status and heartbeat messages.")
    publish_bot_runtime_status("starting", balance=balance, force=True)
    telegram(
        f"🚀 <b>FX Bot Started</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Balance: {acct.get('currency', '£')}{balance:,.2f}\n"
        f"Mode: {'📝 Paper' if PAPER_TRADE else '💰 Live'} | {ACCOUNT_TYPE}\n"
        f"Watchlist: {len(DYNAMIC_PAIRS)} pairs\n"
        f"Session: {get_current_session()['name']}\n"
        f"Strategies: SCALPER, TREND, REVERSAL, CARRY, POST_NEWS, PULLBACK\n"
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
    calibration_reloaded = refresh_trade_calibration()
    if not filter_reloaded:
        load_macro_filters()
    if not news_reloaded:
        load_macro_news()
    if not calibration_reloaded:
        load_trade_calibration()

    log.info(f"🔎 Macro proxy configuration: DXY proxy will be read from Redis, VIX from Redis")
    log.info(f"📰 Macro news file: {MACRO_NEWS_FILE}")
    publish_bot_runtime_status("running", balance=balance, force=True)


def run():
    global _market_regime_mult, _consecutive_losses, _session_loss_paused_until, _streak_paused_at, _weekend_mode_active, _entry_pause_reason

    while True:
        try:
            _bootstrap_runtime()
            break
        except KeyboardInterrupt:
            log.info("🛑 Stopped during startup.")
            publish_bot_runtime_status("stopped", error="stopped during startup", force=True)
            save_state()
            telegram("🛑 <b>Bot stopped.</b> Check Railway.")
            return
        except Exception as e:
            log.error(f"Fatal startup error: {e}", exc_info=True)
            publish_bot_runtime_status("startup_error", error=str(e)[:200], force=True)
            telegram(f"⚠️ <b>Bot startup error:</b> {str(e)[:200]}\nRetrying in 30s.")
            time.sleep(30)

    while True:
        try:
            poll_telegram_commands()

            if is_weekend():
                if not _weekend_mode_active:
                    _weekend_mode_active = True
                    reopen_at = next_market_reopen_utc()
                    notify_entry_pause(
                        "weekend",
                        "🌙 <b>Weekend market close</b>",
                        f"The weekend starts for FX markets now. No new trades will be entered until {format_time_utc_and_local(reopen_at)}.\n"
                        f"Open trades will continue to be monitored."
                    )
                acct = get_account_summary()
                balance = acct.get("balance", 0)
                send_heartbeat(balance, status="idle_weekend")
                log_idle_state("weekend market closed", sleep_seconds=300)
                sleep_with_command_poll(300)
                continue

            acct = get_account_summary()
            balance = acct.get("balance", 0)
            publish_bot_runtime_status("running", balance=balance)
            if balance <= 0:
                log_idle_state("zero balance", balance=balance, sleep_seconds=60)
                log.warning("⚠️ Zero balance — sleeping 60s")
                time.sleep(60)
                continue

            session = get_current_session()
            if _weekend_mode_active:
                _weekend_mode_active = False
            filters_updated = refresh_macro_filters()
            news_updated = refresh_macro_news()
            calibration_updated = refresh_trade_calibration()
            if filters_updated or news_updated:
                log.info(
                    f"🔄 Macro JSON refresh: filters={'reloaded' if filters_updated else 'unchanged'} "
                    f"news={'reloaded' if news_updated else 'unchanged'}"
                )
            if calibration_updated:
                log.info("🔄 Trade calibration refresh: reloaded")
            update_macro_news_pause()
            # DXY and VIX are already loaded from Redis via load_macro_filters; no need to call proxies

            df_eurusd_1h = fetch_candles("EUR_USD", "H1", 100)
            if df_eurusd_1h is not None:
                _market_regime_mult = compute_market_regime(df_eurusd_1h)

            # Tier 2 §18 — refresh OANDA financing cache daily.
            try:
                _tier2_refresh_financing(OANDA_ACCOUNT_ID, oanda_get)
            except NameError:
                pass
            # Tier 2 §20 — refresh drawdown-kill state from trade_history.
            _tier2_refresh_drawdown_state()

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
                telegram(f"🛑 <b>Session loss limit</b> | P&L {get_account_currency()}{today_pnl:.2f}\n"
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
                        with _open_trades_lock:
                            open_trades.remove(trade)

            # Tier 1 §7 item 11 — Friday 21:00 UTC weekend flatten for non-CARRY
            # strategies. Avoids Sunday-open gap risk. `should_flatten_for_weekend`
            # is idempotent so we can call it on every scan; it only returns True
            # inside the flatten window.
            try:
                from fxbot.execution import should_flatten_for_weekend
                now_utc = datetime.now(timezone.utc)
                for trade in open_trades[:]:
                    if not should_flatten_for_weekend(
                        now_utc=now_utc,
                        strategy=trade.get("label", ""),
                        flatten_hour_utc=int(os.getenv("WEEKEND_FLATTEN_HOUR_UTC", "21")),
                    ):
                        continue
                    label = trade.get("label", "")
                    inst = trade.get("instrument", "")
                    ok, err = close_trade_result(
                        trade.get("id", ""),
                        label=f"weekend_flatten:{label}",
                        units=trade.get("units"),
                        instrument=inst,
                    )
                    if ok:
                        log.warning(f"🌙 Weekend flatten closed {label} {inst}")
                        try:
                            telegram(f"🌙 <b>Weekend flatten</b>\nClosed {label} on {inst} at Friday cutoff.")
                        except Exception:
                            pass
                        with _open_trades_lock:
                            if trade in open_trades:
                                open_trades.remove(trade)
                    else:
                        log.error(f"🌙 Weekend flatten failed for {label} {inst}: {err}")
            except Exception as e:  # pragma: no cover - defensive
                log.debug(f"weekend flatten check error: {e}")

            # Entry scans
            if entries_allowed and len(open_trades) < MAX_OPEN_TRADES:
                start_scan_cycle()
                skip_scalper = is_rollover_window()
                active_pairs, health_pairs, tradable_pairs, empty_reason = get_effective_scan_pairs(session)
                set_scan_cycle_summary(active_pairs, health_pairs, tradable_pairs)

                if tradable_pairs:
                    notify_entry_resume(session, tradable_pairs)
                else:
                    pause_reason, pause_title, pause_body = _build_entry_pause_notice(session, active_pairs, empty_reason)
                    notify_entry_pause(pause_reason, pause_title, pause_body)

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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("SCALPER", best_scalper["instrument"], best_scalper["direction"], opp=best_scalper, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("TREND", best_trend["instrument"], best_trend["direction"], opp=best_trend, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("REVERSAL", best_reversal["instrument"], best_reversal["direction"], opp=best_reversal, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("BREAKOUT", best_breakout["instrument"], best_breakout["direction"], opp=best_breakout, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("CARRY", best_carry["instrument"], best_carry["direction"], opp=best_carry, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("ASIAN_FADE", best_asian["instrument"], best_asian["direction"], opp=best_asian, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("POST_NEWS", best_pn["instrument"], best_pn["direction"], opp=best_pn, session_name=session["name"]) or "entry blocked"
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
                            with _open_trades_lock:
                                open_trades.append(trade)
                        else:
                            reason = get_strategy_entry_block_reason("PULLBACK", best_pb["instrument"], best_pb["direction"], opp=best_pb, session_name=session["name"]) or "entry blocked"
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
            publish_bot_runtime_status("stopped", error="stopped by operator", force=True)
            save_state()
            telegram("🛑 <b>Bot stopped.</b> Check Railway.")
            break
        except Exception as e:
            log.error(f"Error: {e}", exc_info=True)
            publish_bot_runtime_status("runtime_error", error=str(e)[:200], force=True)
            telegram(f"⚠️ <b>Bot error:</b> {str(e)[:200]}\nRetrying in 30s.")
            time.sleep(30)

if __name__ == "__main__":
    run()