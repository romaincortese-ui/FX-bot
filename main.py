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
STATIC_CORE_PAIRS = os.getenv("CORE_PAIRS", "EUR_USD,GBP_USD,USD_JPY").split(",")
STATIC_EXTENDED_PAIRS = os.getenv("EXTENDED_PAIRS", "AUD_USD,USD_CAD,EUR_GBP,USD_CHF,NZD_USD").split(",")
STATIC_ALL_PAIRS = STATIC_CORE_PAIRS + STATIC_EXTENDED_PAIRS

# ── Dynamic watchlist settings ────────────────────────────────
DYNAMIC_PAIRS = []                 # will be filled at runtime
LAST_WATCHLIST_UPDATE = 0
WATCHLIST_UPDATE_INTERVAL = int(os.getenv("WATCHLIST_UPDATE_INTERVAL", "14400"))  # 4 hours
MAX_WATCHLIST_SIZE = int(os.getenv("MAX_WATCHLIST_SIZE", "8"))
MAX_SPREAD_FILTER_PIPS = float(os.getenv("MAX_SPREAD_FILTER_PIPS", "1.5"))

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
MAX_OPEN_TRADES        = int(os.getenv("MAX_OPEN_TRADES",           "6"))
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
STATE_FILE          = "state.json"
MACRO_NEWS_FILE     = os.getenv("MACRO_NEWS_FILE", "macro_news.json")
REDIS_URL           = os.getenv("REDIS_URL", "")
REDIS_MACRO_STATE_KEY = os.getenv("REDIS_MACRO_STATE_KEY", "macro_state")
HTTP_RETRIES        = 3
HTTP_RETRY_DELAY    = 1.0
HEARTBEAT_INTERVAL  = int(os.getenv("HEARTBEAT_INTERVAL",  "3600"))
KLINE_CACHE_TTL     = 15
MAX_KLINE_CACHE     = 200

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
_adaptive_offsets     = {"SCALPER": 0.0, "TREND": 0.0, "REVERSAL": 0.0, "BREAKOUT": 0.0}
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

_pair_cooldowns      = {}
_thread_local        = threading.local()
_live_prices         = {}
_price_lock          = threading.Lock()

_pair_cooldowns      = {}
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
    return 0.01 if "JPY" in instrument else 0.0001

def price_to_pips(instrument: str, price_diff: float) -> float:
    return price_diff / pip_size(instrument)

def pips_to_price(instrument: str, pips: float) -> float:
    return pips * pip_size(instrument)

def pip_value(instrument: str, units: float, account_currency: str = "GBP") -> float:
    if ACCOUNT_TYPE == "spread_bet":
        return abs(units)
    return abs(units) * pip_size(instrument)

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

def build_dynamic_watchlist(top_n: int = MAX_WATCHLIST_SIZE, max_spread_pips: float = MAX_SPREAD_FILTER_PIPS) -> list:
    """Fetch all currency pairs, filter by spread, rank by ATR%, return top N."""
    if PAPER_TRADE or not OANDA_API_KEY:
        log.warning("Dynamic watchlist skipped (paper trade or no API key). Using static list.")
        return STATIC_ALL_PAIRS

    try:
        resp = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/instruments")
        instruments = resp.get("instruments", [])
        fx_pairs = []
        for inst in instruments:
            if inst.get("type") == "CURRENCY":
                name = inst["name"].replace("/", "_")
                fx_pairs.append(name)

        if not fx_pairs:
            return STATIC_ALL_PAIRS

        log.info(f"📊 Found {len(fx_pairs)} currency pairs. Checking spreads...")

        # Filter by spread
        chunk_size = 40
        spread_ok = []
        for i in range(0, len(fx_pairs), chunk_size):
            chunk = fx_pairs[i:i+chunk_size]
            prices = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing",
                               {"instruments": ",".join(chunk)})
            for price in prices.get("prices", []):
                inst = price["instrument"]
                bid = float(price["closeoutBid"])
                ask = float(price["closeoutAsk"])
                spread = (ask - bid) / pip_size(inst)
                if spread <= max_spread_pips:
                    spread_ok.append(inst)

        if not spread_ok:
            log.warning("No pairs passed spread filter. Using static list.")
            return STATIC_ALL_PAIRS

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
        return top_pairs

    except Exception as e:
        log.error(f"Dynamic watchlist build failed: {e}")
        return STATIC_ALL_PAIRS

def refresh_dynamic_watchlist():
    global DYNAMIC_PAIRS, LAST_WATCHLIST_UPDATE
    if time.time() - LAST_WATCHLIST_UPDATE < WATCHLIST_UPDATE_INTERVAL:
        return
    new_list = build_dynamic_watchlist()
    if new_list:
        DYNAMIC_PAIRS = new_list
        LAST_WATCHLIST_UPDATE = time.time()
        log.info(f"✅ Dynamic watchlist updated with {len(DYNAMIC_PAIRS)} pairs.")
        _restart_price_stream()
    else:
        log.warning("Dynamic watchlist refresh returned empty – keeping old list.")

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
    all_pairs = list(set(pairs + open_trade_pairs))
    if not all_pairs:
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
            body = getattr(e.response, 'text', '')[:300] if getattr(e, 'response', None) else ''
            log.error(f"OANDA GET {path} failed with HTTP {e.response.status_code if getattr(e, 'response', None) else 'unknown'}: {body}")
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
                error_body = r.text[:500]
                log.error(f"OANDA POST {path} error {r.status_code}: {error_body}")
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
            log.error(f"OANDA PUT {path} error {r.status_code}: {r.text[:300]}")
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

def get_current_price(instrument: str) -> dict:
    with _price_lock:
        cached = _live_prices.get(instrument)
        if cached and time.time() - cached[2] < 30:
            return {"bid": cached[0], "ask": cached[1], "spread": cached[1] - cached[0]}

    try:
        data = oanda_get(f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing",
                         {"instruments": instrument})
        prices = data.get("prices", [])
        if prices:
            p = prices[0]
            bid = float(p["bids"][0]["price"]) if p.get("bids") else 0
            ask = float(p["asks"][0]["price"]) if p.get("asks") else 0
            with _price_lock:
                _live_prices[instrument] = (bid, ask, time.time())
            return {"bid": bid, "ask": ask, "spread": ask - bid}
    except Exception as e:
        log.debug(f"Price fetch failed for {instrument}: {e}")
    return {"bid": 0, "ask": 0, "spread": 999}

def get_spread_pips(instrument: str) -> float:
    p = get_current_price(instrument)
    return price_to_pips(instrument, p["spread"])

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
            return None

        rows = []
        for c in candles:
            if not c.get("complete", True) and granularity != "M1":
                continue
            mid = c.get("mid", {})
            rows.append({
                "time":   float(c.get("time", 0)),
                "open":   float(mid.get("o", 0)),
                "high":   float(mid.get("h", 0)),
                "low":    float(mid.get("l", 0)),
                "close":  float(mid.get("c", 0)),
                "volume": int(c.get("volume", 0)),
                "bid_close": float(c.get("bid", {}).get("c", 0)) if c.get("bid") else 0,
                "ask_close": float(c.get("ask", {}).get("c", 0)) if c.get("ask") else 0,
            })

        if not rows:
            return None

        df = pd.DataFrame(rows)
        df = df.dropna(subset=["close"])

        if len(df) < 20:
            log.debug(f"OANDA candle fetch produced only {len(df)} valid rows for {instrument} {granularity}/{count}")

        with _kline_cache_lock:
            if len(_kline_cache) >= MAX_KLINE_CACHE:
                stale = [k for k, (_, t) in _kline_cache.items() if time.time() - t > KLINE_CACHE_TTL]
                for k in stale:
                    del _kline_cache[k]
            _kline_cache[cache_key] = (df, time.time())

        return df if len(df) >= 20 else None

    except Exception as e:
        log.debug(f"Candle fetch error {instrument}/{granularity}: {e}")
        return None

def calculate_units(instrument: str, balance: float, sl_pips: float,
                    risk_pct: float, kelly_mult: float = 1.0) -> float:
    risk_amount = balance * risk_pct * kelly_mult
    if sl_pips <= 0:
        sl_pips = 10
    if ACCOUNT_TYPE == "spread_bet":
        stake = risk_amount / sl_pips
        return max(SPREAD_BET_MIN_STAKE, round(stake, 2))
    else:
        pv = pip_size(instrument)
        units = risk_amount / (sl_pips * pv)
        return round(units)

def place_order(instrument: str, units: float, direction: str,
                tp_price: float = None, sl_price: float = None,
                trailing_sl_pips: float = None, label: str = "") -> dict:
    if PAPER_TRADE:
        price = get_current_price(instrument)
        entry = price["ask"] if direction == "LONG" else price["bid"]
        return {
            "id": f"PAPER_{int(time.time()*1000)}",
            "instrument": instrument,
            "units": units if direction == "LONG" else -units,
            "price": entry,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "trailing_sl_pips": trailing_sl_pips,
        }

    signed_units = units if direction == "LONG" else -units
    if ACCOUNT_TYPE == "spread_bet":
        signed_units = str(signed_units)

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

    if "error" in result:
        log.error(f"[{label}] Order failed: {result['error']}")
        telegram(f"⚠️ <b>{label} Order Failed</b>\n{instrument} {direction}\n{result['error'][:200]}")
        return {}

    fill = result.get("orderFillTransaction", {})
    if fill:
        trade_id = fill.get("tradeOpened", {}).get("tradeID") or fill.get("id")
        fill_price = float(fill.get("price", 0))
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
    return {}

def close_trade(trade_id: str, label: str = "", units: float = None) -> bool:
    if PAPER_TRADE:
        return True
    path = f"/v3/accounts/{OANDA_ACCOUNT_ID}/trades/{trade_id}/close"
    body = {}
    if units:
        body["units"] = str(abs(units))
    result = oanda_put(path, body)
    if "error" in result:
        log.error(f"[{label}] Close trade {trade_id} failed: {result['error']}")
        return False
    fill = result.get("orderFillTransaction", {})
    if fill:
        close_price = float(fill.get("price", 0))
        pnl = float(fill.get("pl", 0))
        log.info(f"[{label}] Trade {trade_id} closed @ {close_price} | P&L: {pnl:.2f}")
        return True
    return False

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
    val = float(rsi.iloc[-1])
    return val if not np.isnan(val) else 50.0

def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    if df is None or len(df) < period + 1:
        return 0.0
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    return float(atr.iloc[-1])

def calc_atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    atr = calc_atr(df, period)
    close = float(df["close"].iloc[-1]) if len(df) > 0 else 1.0
    return atr / close if close > 0 else 0.0

def calc_bollinger_bands(df: pd.DataFrame, period: int = 20, std_mult: float = 2.0) -> dict:
    close = df["close"]
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = sma + std_mult * std
    lower = sma - std_mult * std
    width = (upper - lower) / sma
    return {
        "upper": float(upper.iloc[-1]),
        "lower": float(lower.iloc[-1]),
        "mid":   float(sma.iloc[-1]),
        "width": float(width.iloc[-1]) if not np.isnan(float(width.iloc[-1])) else 0.04,
        "width_percentile": _percentile_rank(width),
    }

def _percentile_rank(series: pd.Series) -> float:
    vals = series.dropna()
    if len(vals) < 5:
        return 50.0
    current = float(vals.iloc[-1])
    return float((vals < current).sum() / len(vals) * 100)

def calc_macd(df: pd.DataFrame) -> dict:
    close = df["close"]
    ema12 = calc_ema(close, 12)
    ema26 = calc_ema(close, 26)
    macd_line = ema12 - ema26
    signal = calc_ema(macd_line, 9)
    histogram = macd_line - signal
    return {
        "macd":      float(macd_line.iloc[-1]),
        "signal":    float(signal.iloc[-1]),
        "histogram": float(histogram.iloc[-1]),
        "cross_up":  float(macd_line.iloc[-1]) > float(signal.iloc[-1]) and
                     float(macd_line.iloc[-2]) <= float(signal.iloc[-2]),
        "cross_down": float(macd_line.iloc[-1]) < float(signal.iloc[-1]) and
                      float(macd_line.iloc[-2]) >= float(signal.iloc[-2]),
    }

def keltner_squeeze(df: pd.DataFrame, bb_period: int = 20, kc_period: int = 20,
                    kc_mult: float = 1.5) -> dict:
    close = df["close"]
    ema = calc_ema(close, kc_period)
    atr = calc_atr(df, kc_period)
    kc_upper = float(ema.iloc[-1]) + kc_mult * atr
    kc_lower = float(ema.iloc[-1]) - kc_mult * atr
    bb = calc_bollinger_bands(df, bb_period)
    in_squeeze = bb["upper"] < kc_upper and bb["lower"] > kc_lower
    squeeze_bars = 0
    if in_squeeze:
        for i in range(min(len(df) - bb_period, 50)):
            idx = -(i + 1)
            if idx < -len(df):
                break
            bb_u = float((close.rolling(bb_period).mean() + 2 * close.rolling(bb_period).std()).iloc[idx])
            bb_l = float((close.rolling(bb_period).mean() - 2 * close.rolling(bb_period).std()).iloc[idx])
            kc_u = float(ema.iloc[idx]) + kc_mult * atr
            kc_l = float(ema.iloc[idx]) - kc_mult * atr
            if bb_u < kc_u and bb_l > kc_l:
                squeeze_bars += 1
            else:
                break
    return {
        "in_squeeze":   in_squeeze,
        "squeeze_bars": squeeze_bars,
        "bb_width":     bb["width"],
        "bb_percentile": bb["width_percentile"],
    }

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
                    "/pause — Stop new entries\n"
                    "/resume — Resume entries\n"
                    "/help — This message"
                )
    except Exception as e:
        log.debug(f"Telegram poll error: {e}")

def _handle_status_command():
    acct = get_account_summary()
    session = get_current_session()
    lines = [
        f"📊 <b>Status</b> | {session['name']}",
        f"━━━━━━━━━━━━━━━",
        f"Balance: {acct.get('currency', '£')}{acct.get('balance', 0):,.2f}",
        f"Unrealized: {acct.get('currency', '£')}{acct.get('unrealizedPL', 0):+,.2f}",
        f"Open trades: {len(open_trades)}",
        f"Regime: {'🟢 BULL' if _market_regime_mult < 0.95 else '🔴 BEAR' if _market_regime_mult > 1.10 else '⚪ NEUTRAL'} ({_market_regime_mult:.2f})",
        f"Paused: {'Yes' if _paused else 'No'}",
    ]
    if open_trades:
        lines.append("")
        for t in open_trades:
            direction = "🟢" if t.get("direction") == "LONG" else "🔴"
            pnl = t.get("unrealized_pnl", 0)
            lines.append(f"{direction} {t['instrument']} {t['label']} | {pnl:+.2f}")
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
    global _paused, _adaptive_offsets, _last_rebalance_count, _pair_cooldowns
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
                                    {"SCALPER": 0.0, "TREND": 0.0, "REVERSAL": 0.0, "BREAKOUT": 0.0})
        _last_rebalance_count = d.get("last_rebalance_count", 0)
        _pair_cooldowns       = d.get("pair_cooldowns", {})
        log.info(f"📂 State loaded ({age/60:.0f}min old): "
                 f"{len(open_trades)} open, {len(trade_history)} history")
    except Exception as e:
        log.warning(f"State load failed ({e}) — starting fresh")

# ═══════════════════════════════════════════════════════════════
#  MACRO INTELLIGENCE
# ═══════════════════════════════════════════════════════════════

def _fetch_candles_with_fallback(instruments, granularity: str, count: int, min_rows: int = 1, price: str = "M"):
    for instrument in instruments:
        df = fetch_candles(instrument, granularity, count, price=price)
        if df is None:
            log.debug(f"Macro proxy fetch failed for {instrument} {granularity}/{count} price={price}")
            continue
        if len(df) < min_rows:
            log.debug(f"Macro proxy fetch returned {len(df)} rows for {instrument} {granularity}/{count}, need {min_rows}")
            continue
        return instrument, df
    return None, None


def update_dxy_proxy():
    global _dxy_ema_gap, _dxy_at, _dxy_last_good
    if time.time() - _dxy_at < 1800:
        return
    instruments = [DXY_PROXY_INSTRUMENT] + [instr for instr in DXY_PROXY_FALLBACKS if instr != DXY_PROXY_INSTRUMENT]
    try:
        count = 200
        source, df = _fetch_candles_with_fallback(instruments, "H1", count, min_rows=DXY_EMA_PERIOD, price="M")
        if df is None:
            raise ValueError(f"insufficient candle data for DXY proxy list: {instruments}")
        ema = calc_ema(df["close"], DXY_EMA_PERIOD)
        _dxy_ema_gap = float(df["close"].iloc[-1]) / float(ema.iloc[-1]) - 1
        _dxy_last_good = _dxy_ema_gap
        _dxy_at = time.time()
        log.info(f"📊 [MACRO] DXY proxy ({source}): EMA gap {_dxy_ema_gap*100:+.2f}%")
    except Exception as e:
        if _dxy_last_good is None:
            log.warning(f"⚠️ DXY proxy update failed for {DXY_PROXY_INSTRUMENT} and fallbacks {DXY_PROXY_FALLBACKS}: {e}")
        else:
            log.warning(f"⚠️ DXY proxy update failed, keeping last known gap {_dxy_last_good*100:+.2f}%: {e}")
            _dxy_ema_gap = _dxy_last_good
        _dxy_at = time.time()


def update_vix_level():
    global _vix_level, _vix_at, _vix_last_good
    if time.time() - _vix_at < 1800:
        return
    new_level = None
    try:
        primary_count = 200
        df_spx = fetch_candles(VIX_PROXY_PRIMARY, "D", primary_count, price="M")
        if df_spx is not None and len(df_spx) >= 20:
            atr = calc_atr(df_spx, 14)
            atr_avg = df_spx["close"].rolling(20).std().iloc[-20:-1].mean()
            vol_ratio = atr / (atr_avg if atr_avg > 0 else 1)
            new_level = 15 * vol_ratio
            log.info(f"📊 [MACRO] VIX proxy via {VIX_PROXY_PRIMARY}: {new_level:.1f} (ATR ratio {vol_ratio:.2f})")
    except Exception as e:
        log.warning(f"⚠️ {VIX_PROXY_PRIMARY} fetch failed: {e}")

    if new_level is None:
        instruments = [VIX_PROXY_FALLBACK] + [instr for instr in VIX_PROXY_FALLBACKS if instr != VIX_PROXY_FALLBACK]
        fallback_count = 200
        source, df_4h = _fetch_candles_with_fallback(instruments, "H4", fallback_count, min_rows=40, price="M")
        if source is not None:
            try:
                atr = calc_atr(df_4h, 14)
                tr = pd.concat([
                    df_4h["high"] - df_4h["low"],
                    (df_4h["high"] - df_4h["close"].shift(1)).abs(),
                    (df_4h["low"]  - df_4h["close"].shift(1)).abs(),
                ], axis=1).max(axis=1)
                atr_series = tr.ewm(alpha=1/14, adjust=False).mean()
                atr_avg = float(atr_series.iloc[-30:-1].mean())
                vol_ratio = atr / atr_avg if atr_avg > 0 else 1.0
                new_level = 15 * vol_ratio
                log.info(f"📊 [MACRO] VIX proxy via {source}: {new_level:.1f} (ATR ratio {vol_ratio:.2f})")
            except Exception as e:
                log.warning(f"⚠️ {source} VIX proxy ATR calculation failed: {e}")
        else:
            log.warning(f"⚠️ No fallback VIX proxy available from {instruments}")

    if new_level is not None:
        _vix_last_good = new_level
        _vix_level = new_level
        _vix_at = time.time()
    else:
        if _vix_last_good is not None:
            _vix_level = _vix_last_good
            log.warning(f"⚠️ VIX proxy update failed — keeping last known value {_vix_last_good:.1f}")
        else:
            _vix_level = None
            log.warning("⚠️ VIX proxy update failed and no prior value exists; macro volatility is unknown")
        _vix_at = time.time()


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
    global macro_filters
    try:
        state = _load_macro_state_from_redis()
        if state is not None:
            data = state.get("filters", {})
            if not isinstance(data, dict):
                raise ValueError("Redis macro state filters must be a dict")
            macro_filters = {key.upper(): str(value).upper() for key, value in data.items()}
            log.info(f"📂 Loaded macro filters from Redis key {REDIS_MACRO_STATE_KEY}: {', '.join(sorted(macro_filters.keys()))}")
            return

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
            log.info(f"📂 Loaded macro news from Redis key {REDIS_MACRO_STATE_KEY}: {len(macro_news)} events")
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


def update_macro_news_pause() -> None:
    global macro_news_pause_until
    now = datetime.now(timezone.utc)
    active_ends = []
    for event in macro_news:
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
    mult = 1.0
    if atr_ratio > REGIME_HIGH_VOL_ATR_RATIO:
        mult *= REGIME_TIGHTEN_MULT
    elif atr_ratio < REGIME_LOW_VOL_ATR_RATIO:
        mult *= REGIME_LOOSEN_MULT
    if abs(ema_gap) > 0.01:
        mult *= 0.90
    if _vix_level is not None:
        if _vix_level > VIX_EXTREME_THRESHOLD:
            mult *= 1.30
        elif _vix_level > VIX_HIGH_THRESHOLD:
            mult *= 1.10
    return round(mult, 3)

# ═══════════════════════════════════════════════════════════════
#  DIRECTION DETERMINATION
# ═══════════════════════════════════════════════════════════════

def determine_direction(instrument: str, df_5m: pd.DataFrame,
                        df_1h: pd.DataFrame = None, df_4h: pd.DataFrame = None,
                        strategy: str = "SCALPER") -> str:
    signals = {"long": 0, "short": 0}
    if df_5m is not None and len(df_5m) >= 30:
        close = df_5m["close"]
        ema9  = calc_ema(close, 9)
        ema21 = calc_ema(close, 21)
        rsi   = calc_rsi(close)
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
    if _dxy_ema_gap is not None and "USD" in instrument:
        base, quote = instrument.split("_")
        if base == "USD":
            if _dxy_ema_gap > DXY_GATE_THRESHOLD:
                signals["long"] += 2
            elif _dxy_ema_gap < -DXY_GATE_THRESHOLD:
                signals["short"] += 2
        else:
            if _dxy_ema_gap > DXY_GATE_THRESHOLD:
                signals["short"] += 2
            elif _dxy_ema_gap < -DXY_GATE_THRESHOLD:
                signals["long"] += 2

    apply_macro_directional_bias(instrument, signals)

    if strategy == "REVERSAL":
        signals["long"], signals["short"] = signals["short"], signals["long"]
    return "LONG" if signals["long"] >= signals["short"] else "SHORT"

# ═══════════════════════════════════════════════════════════════
#  SCORING FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def score_scalper(instrument: str, session: dict) -> dict | None:
    spread_pips = get_spread_pips(instrument)
    if spread_pips > SCALPER_MAX_SPREAD_PIPS:
        return None

    df_5m = fetch_candles(instrument, "M5", 60)
    if df_5m is None or len(df_5m) < 30:
        return None

    df_1h = fetch_candles(instrument, "H1", 60)

    close  = df_5m["close"]
    volume = df_5m["volume"]
    rsi    = calc_rsi(close)
    atr    = calc_atr(df_5m, 14)
    atr_pct = atr / float(close.iloc[-1]) if float(close.iloc[-1]) > 0 else 0

    if rsi > SCALPER_MAX_RSI or rsi < SCALPER_MIN_RSI:
        return None

    ema9  = calc_ema(close, 9)
    ema21 = calc_ema(close, 21)
    crossed_now    = ((float(ema9.iloc[-1]) > float(ema21.iloc[-1])) and
                      (float(ema9.iloc[-2]) <= float(ema21.iloc[-2])))
    crossed_recent = ((float(ema9.iloc[-2]) > float(ema21.iloc[-2])) and
                      (float(ema9.iloc[-3]) <= float(ema21.iloc[-3])))
    crossed_down_now = ((float(ema9.iloc[-1]) < float(ema21.iloc[-1])) and
                        (float(ema9.iloc[-2]) >= float(ema21.iloc[-2])))

    crossed = crossed_now or crossed_recent or crossed_down_now

    avg_vol = float(volume.iloc[-20:-1].mean()) if len(volume) >= 21 else 1
    vol_ratio = float(volume.iloc[-1]) / avg_vol if avg_vol > 0 else 1.0

    rsi_prev = calc_rsi(close.iloc[:-1])
    rsi_delta = rsi - rsi_prev if not np.isnan(rsi_prev) else 0

    ma_score = 30 if crossed else (15 if float(ema9.iloc[-1]) != float(ema21.iloc[-1]) else 0)
    rsi_score = max(0, 40 - min(rsi, 100 - rsi)) if rsi < 45 or rsi > 55 else 0
    vol_score = min(30, (vol_ratio - 1) * 15) if vol_ratio > 1 else 0

    confluence = 0
    if crossed and vol_ratio > 1.5 and abs(rsi_delta) > 2:
        confluence = SCALPER_CONFLUENCE_BONUS

    macd = calc_macd(df_5m)
    macd_bonus = 5 if macd["cross_up"] or macd["cross_down"] else 0

    spread_penalty = max(0, (spread_pips - 0.5) * 5)

    score = ma_score + rsi_score + vol_score + confluence + macd_bonus - spread_penalty

    direction = determine_direction(instrument, df_5m, df_1h, strategy="SCALPER")

    if direction == "SHORT":
        if not (crossed_down_now or (float(ema9.iloc[-1]) < float(ema21.iloc[-1]))):
            score *= 0.5

    eff_threshold = SCALPER_THRESHOLD * session["multiplier"] * _market_regime_mult
    eff_threshold += _adaptive_offsets.get("SCALPER", 0)

    if score < eff_threshold:
        return None

    tp_pips = max(SCALPER_TP_MIN_PIPS, min(SCALPER_TP_MAX_PIPS,
                  price_to_pips(instrument, atr * SCALPER_TP_ATR_MULT)))
    sl_pips = max(SCALPER_SL_MIN_PIPS, min(SCALPER_SL_MAX_PIPS,
                  price_to_pips(instrument, atr * SCALPER_SL_ATR_MULT)))

    if tp_pips / sl_pips < 1.5:
        tp_pips = sl_pips * 1.5

    return {
        "instrument": instrument,
        "score":      round(score, 2),
        "direction":  direction,
        "rsi":        round(rsi, 2),
        "rsi_delta":  round(rsi_delta, 2),
        "vol_ratio":  round(vol_ratio, 2),
        "atr":        atr,
        "atr_pct":    round(atr_pct, 6),
        "spread_pips": round(spread_pips, 2),
        "tp_pips":    round(tp_pips, 1),
        "sl_pips":    round(sl_pips, 1),
        "trail_pips": SCALPER_TRAIL_PIPS,
        "crossed_now": crossed_now or crossed_down_now,
        "entry_signal": "CROSSOVER" if crossed else ("VOL_SPIKE" if vol_ratio > 2 else "TREND"),
        "macd":       macd,
    }

def score_trend(instrument: str, session: dict) -> dict | None:
    spread_pips = get_spread_pips(instrument)
    if spread_pips > TREND_MAX_SPREAD_PIPS:
        return None

    df_5m = fetch_candles(instrument, "M5", 60)
    df_1h = fetch_candles(instrument, "H1", 100)
    df_4h = fetch_candles(instrument, "H4", 60)

    if df_1h is None or len(df_1h) < 50:
        return None
    if df_4h is None or len(df_4h) < 30:
        return None

    close_1h = df_1h["close"]
    close_4h = df_4h["close"]

    ema20_1h = calc_ema(close_1h, 20)
    ema50_1h = calc_ema(close_1h, 50)
    ema20_4h = calc_ema(close_4h, 20)
    ema50_4h = calc_ema(close_4h, 50)

    bullish_4h = float(ema20_4h.iloc[-1]) > float(ema50_4h.iloc[-1])
    bullish_1h = float(ema20_1h.iloc[-1]) > float(ema50_1h.iloc[-1])

    aligned = (bullish_4h == bullish_1h)
    if not aligned:
        return None

    direction = "LONG" if bullish_4h else "SHORT"

    score = 0
    score += 25

    ema50_gap_4h = abs(float(close_4h.iloc[-1]) / float(ema50_4h.iloc[-1]) - 1)
    score += min(20, ema50_gap_4h * 1000)

    ema20_dist = abs(float(close_1h.iloc[-1]) / float(ema20_1h.iloc[-1]) - 1)
    if ema20_dist < 0.002:
        score += 15
    elif ema20_dist < 0.005:
        score += 8

    rsi_1h = calc_rsi(close_1h)
    if direction == "LONG" and 40 < rsi_1h < 65:
        score += 10
    elif direction == "SHORT" and 35 < rsi_1h < 60:
        score += 10

    macd_1h = calc_macd(df_1h)
    if (direction == "LONG" and macd_1h["histogram"] > 0) or \
       (direction == "SHORT" and macd_1h["histogram"] < 0):
        score += 10

    vol = df_1h["volume"]
    vol_ratio = float(vol.iloc[-1]) / float(vol.iloc[-20:-1].mean()) if len(vol) >= 21 else 1
    if vol_ratio > 1.2:
        score += 5

    if _dxy_ema_gap is not None and "USD" in instrument:
        base, quote = instrument.split("_")
        usd_is_base = base == "USD"
        dxy_long = _dxy_ema_gap > DXY_GATE_THRESHOLD
        dxy_short = _dxy_ema_gap < -DXY_GATE_THRESHOLD

        if (usd_is_base and direction == "LONG" and dxy_long) or \
           (usd_is_base and direction == "SHORT" and dxy_short) or \
           (not usd_is_base and direction == "SHORT" and dxy_long) or \
           (not usd_is_base and direction == "LONG" and dxy_short):
            score += 10

    atr = calc_atr(df_1h, 14)
    atr_pct = atr / float(close_1h.iloc[-1])

    eff_threshold = TREND_THRESHOLD * session["multiplier"] * _market_regime_mult
    eff_threshold += _adaptive_offsets.get("TREND", 0)

    if score < eff_threshold:
        return None

    tp_pips = max(15, price_to_pips(instrument, atr * TREND_TP_ATR_MULT))
    sl_pips = max(8,  price_to_pips(instrument, atr * TREND_SL_ATR_MULT))
    partial_tp_pips = max(10, price_to_pips(instrument, atr * TREND_PARTIAL_TP_ATR))

    return {
        "instrument":      instrument,
        "score":           round(score, 2),
        "direction":       direction,
        "rsi":             round(rsi_1h, 2),
        "vol_ratio":       round(vol_ratio, 2),
        "atr":             atr,
        "atr_pct":         round(atr_pct, 6),
        "spread_pips":     round(spread_pips, 2),
        "tp_pips":         round(tp_pips, 1),
        "sl_pips":         round(sl_pips, 1),
        "partial_tp_pips": round(partial_tp_pips, 1),
        "trail_pips":      TREND_TRAIL_PIPS,
        "entry_signal":    "TREND_ALIGNED",
        "ema50_gap_4h":    round(ema50_gap_4h * 100, 2),
    }

def score_reversal(instrument: str, session: dict) -> dict | None:
    spread_pips = get_spread_pips(instrument)
    if spread_pips > REVERSAL_MAX_SPREAD_PIPS:
        return None

    if session["aggression"] == "MINIMAL":
        return None

    df_5m = fetch_candles(instrument, "M5", 60)
    df_1h = fetch_candles(instrument, "H1", 60)

    if df_5m is None or len(df_5m) < 30:
        return None

    close = df_5m["close"]
    rsi = calc_rsi(close)

    is_oversold  = rsi <= REVERSAL_RSI_OVERSOLD
    is_overbought = rsi >= REVERSAL_RSI_OVERBOUGHT
    if not (is_oversold or is_overbought):
        return None

    direction = "LONG" if is_oversold else "SHORT"

    score = 0
    if is_oversold:
        score += min(30, (REVERSAL_RSI_OVERSOLD - rsi) * 3)
    else:
        score += min(30, (rsi - REVERSAL_RSI_OVERBOUGHT) * 3)

    bb = calc_bollinger_bands(df_5m)
    price = float(close.iloc[-1])
    if is_oversold and price <= bb["lower"]:
        score += 15
    elif is_overbought and price >= bb["upper"]:
        score += 15

    rsi_prev = calc_rsi(close.iloc[:-5])
    if is_oversold and rsi > rsi_prev:
        score += 10
    elif is_overbought and rsi < rsi_prev:
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

    eff_threshold = REVERSAL_THRESHOLD * session["multiplier"] * _market_regime_mult
    eff_threshold += _adaptive_offsets.get("REVERSAL", 0)

    if score < eff_threshold:
        return None

    tp_pips = max(8,  price_to_pips(instrument, atr * REVERSAL_TP_ATR_MULT))
    sl_pips = max(5,  price_to_pips(instrument, atr * REVERSAL_SL_ATR_MULT))

    return {
        "instrument":  instrument,
        "score":       round(score, 2),
        "direction":   direction,
        "rsi":         round(rsi, 2),
        "vol_ratio":   round(vol_ratio, 2),
        "atr":         atr,
        "atr_pct":     round(atr_pct, 6),
        "spread_pips": round(spread_pips, 2),
        "tp_pips":     round(tp_pips, 1),
        "sl_pips":     round(sl_pips, 1),
        "trail_pips":  REVERSAL_TRAIL_PIPS,
        "entry_signal": "OVERSOLD_BOUNCE" if is_oversold else "OVERBOUGHT_FADE",
    }

def score_breakout(instrument: str, session: dict) -> dict | None:
    spread_pips = get_spread_pips(instrument)
    if spread_pips > BREAKOUT_MAX_SPREAD_PIPS:
        return None

    if session["aggression"] in ("MINIMAL", "LOW"):
        return None

    df_15m = fetch_candles(instrument, "M15", 80)
    df_1h  = fetch_candles(instrument, "H1", 60)

    if df_15m is None or len(df_15m) < 40:
        return None

    squeeze = keltner_squeeze(df_15m)

    if not squeeze["in_squeeze"] and squeeze["squeeze_bars"] < 5:
        return None

    score = 0
    score += min(25, squeeze["squeeze_bars"] * 3)

    if squeeze["bb_percentile"] < 20:
        score += 20
    elif squeeze["bb_percentile"] < 35:
        score += 10

    volume = df_15m["volume"]
    recent_vol = float(volume.iloc[-3:].mean())
    avg_vol = float(volume.iloc[-20:-3].mean()) if len(volume) >= 23 else 1
    vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1
    if vol_ratio > 1.3:
        score += 10

    macd = calc_macd(df_15m)
    if abs(macd["histogram"]) > abs(float((df_15m["close"].ewm(span=12).mean() -
                                            df_15m["close"].ewm(span=26).mean() -
                                            (df_15m["close"].ewm(span=12).mean() -
                                             df_15m["close"].ewm(span=26).mean()).ewm(span=9).mean()).iloc[-2])):
        score += 10

    direction = determine_direction(instrument, df_15m, df_1h, strategy="BREAKOUT")

    atr = calc_atr(df_15m, 14)
    atr_pct = atr / float(df_15m["close"].iloc[-1])

    eff_threshold = BREAKOUT_THRESHOLD * session["multiplier"] * _market_regime_mult
    eff_threshold += _adaptive_offsets.get("BREAKOUT", 0)

    if score < eff_threshold:
        return None

    tp_pips = max(15, price_to_pips(instrument, atr * BREAKOUT_TP_ATR_MULT))
    sl_pips = max(5,  price_to_pips(instrument, atr * BREAKOUT_SL_ATR_MULT))

    return {
        "instrument":   instrument,
        "score":        round(score, 2),
        "direction":    direction,
        "squeeze_bars": squeeze["squeeze_bars"],
        "bb_percentile": round(squeeze["bb_percentile"], 1),
        "vol_ratio":    round(vol_ratio, 2),
        "atr":          atr,
        "atr_pct":      round(atr_pct, 6),
        "spread_pips":  round(spread_pips, 2),
        "tp_pips":      round(tp_pips, 1),
        "sl_pips":      round(sl_pips, 1),
        "trail_pips":   BREAKOUT_TRAIL_PIPS,
        "entry_signal": "BB_KC_SQUEEZE",
    }

# ═══════════════════════════════════════════════════════════════
#  ENTRY & EXIT MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def check_correlation_limit(instrument: str, direction: str) -> bool:
    base, quote = instrument.split("_")
    usd_long = 0
    usd_short = 0
    for t in open_trades:
        t_base, t_quote = t["instrument"].split("_")
        t_dir = t["direction"]
        if t_base == "USD":
            if t_dir == "LONG":
                usd_long += 1
            else:
                usd_short += 1
        elif t_quote == "USD":
            if t_dir == "LONG":
                usd_short += 1
            else:
                usd_long += 1

    if quote == "USD":
        if direction == "LONG":
            usd_short += 1
        else:
            usd_long += 1
    elif base == "USD":
        if direction == "LONG":
            usd_long += 1
        else:
            usd_short += 1

    if max(usd_long, usd_short) > MAX_CORRELATED_TRADES:
        log.info(f"[CORR] Skip {instrument} {direction} — USD exposure limit "
                 f"(long={usd_long}, short={usd_short})")
        return False
    return True

def open_trade_entry(opp: dict, label: str, balance: float) -> dict | None:
    instrument = opp["instrument"]
    direction  = opp["direction"]

    if time.time() < _pair_cooldowns.get(instrument, 0):
        return None

    if any(t["instrument"] == instrument for t in open_trades):
        return None

    if not check_correlation_limit(instrument, direction):
        return None

    kelly_gap = opp["score"] - {"SCALPER": SCALPER_THRESHOLD, "TREND": TREND_THRESHOLD,
                                 "REVERSAL": REVERSAL_THRESHOLD, "BREAKOUT": BREAKOUT_THRESHOLD}.get(label, 40)
    kelly_mult = (KELLY_MULT_HIGH_CONF if kelly_gap >= 40
                  else KELLY_MULT_STANDARD if kelly_gap >= 25
                  else KELLY_MULT_SOLID if kelly_gap >= 10
                  else KELLY_MULT_MARGINAL)

    units = calculate_units(instrument, balance, opp["sl_pips"],
                           MAX_RISK_PER_TRADE, kelly_mult)

    price_data = get_current_price(instrument)
    entry_price = price_data["ask"] if direction == "LONG" else price_data["bid"]

    if entry_price <= 0:
        log.error(f"[{label}] No valid price for {instrument}")
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
        "kelly_mult":     kelly_mult,
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
    dir_emoji = "🟢" if direction == "LONG" else "🔴"
    risk_amount = balance * MAX_RISK_PER_TRADE * kelly_mult

    telegram(
        f"{dir_emoji} <b>{label} {direction}</b> | {instrument}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Entry: {actual_entry:.5f}\n"
        f"TP: {tp_price:.5f} (+{opp['tp_pips']:.1f} pips)\n"
        f"SL: {sl_price:.5f} (-{opp['sl_pips']:.1f} pips)\n"
        f"Trail: {trail_pips}p" if trail_pips else "No trail" + f"\n"
        f"Units: {units} | Risk: £{risk_amount:.2f}\n"
        f"Score: {opp['score']:.0f} | {opp.get('entry_signal', '')}\n"
        f"Session: {session['name']} | Spread: {opp.get('spread_pips', 0):.1f}p"
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
                    close_trade(trade["id"], label, units=partial_units)
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

    success = close_trade(trade["id"], label)
    if not success and not PAPER_TRADE:
        log.error(f"[{label}] Failed to close {instrument} — will retry")
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

    save_state()
    return True

# ═══════════════════════════════════════════════════════════════
#  ADAPTIVE LEARNING & REBALANCING
# ═══════════════════════════════════════════════════════════════

def update_adaptive_thresholds():
    global _adaptive_offsets
    DECAY_RATE = 0.15
    MIN_TRADES = max(10, ADAPTIVE_WINDOW // 2)
    for strategy in ("SCALPER", "TREND", "REVERSAL", "BREAKOUT"):
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
        f"News pause until: {datetime.fromtimestamp(macro_news_pause_until, timezone.utc).strftime('%H:%M UTC') if macro_news_pause_until else 'none'}\n"
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
        f"Strategies: SCALPER, TREND, REVERSAL, BREAKOUT\n"
        f"Open trades restored: {len(open_trades)}"
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

    log.info(f"🔎 Macro proxy configuration: DXY={DXY_PROXY_INSTRUMENT} (fallbacks={DXY_PROXY_FALLBACKS}), VIX primary={VIX_PROXY_PRIMARY}, VIX fallback={VIX_PROXY_FALLBACKS}")
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
            update_dxy_proxy()
            update_vix_level()

            df_eurusd_1h = fetch_candles("EUR_USD", "H1", 100)
            if df_eurusd_1h is not None:
                _market_regime_mult = compute_market_regime(df_eurusd_1h)

            refresh_dynamic_watchlist()   # This will also restart stream if needed

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

            if macro_news_pause_until > time.time():
                session_paused = True

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
                skip_scalper = is_rollover_window()
                active_pairs = session["pairs_allowed"]

                scalper_count  = sum(1 for t in open_trades if t["label"] == "SCALPER")
                trend_count    = sum(1 for t in open_trades if t["label"] == "TREND")
                reversal_count = sum(1 for t in open_trades if t["label"] == "REVERSAL")
                breakout_count = sum(1 for t in open_trades if t["label"] == "BREAKOUT")

                if scalper_count < SCALPER_MAX_TRADES and not skip_scalper:
                    best_scalper = None
                    for pair in active_pairs:
                        opp = score_scalper(pair, session)
                        if opp and (best_scalper is None or opp["score"] > best_scalper["score"]):
                            best_scalper = opp
                    if best_scalper:
                        scanner_log(f"📊 [SCALPER] Best: {best_scalper['instrument']} | "
                                    f"Score: {best_scalper['score']:.0f} | "
                                    f"{best_scalper['direction']} | RSI: {best_scalper['rsi']:.0f}")
                        trade = open_trade_entry(best_scalper, "SCALPER", balance)
                        if trade:
                            open_trades.append(trade)

                if trend_count < TREND_MAX_TRADES and session["aggression"] != "MINIMAL":
                    best_trend = None
                    for pair in active_pairs:
                        opp = score_trend(pair, session)
                        if opp and (best_trend is None or opp["score"] > best_trend["score"]):
                            best_trend = opp
                    if best_trend:
                        scanner_log(f"📈 [TREND] Best: {best_trend['instrument']} | "
                                    f"Score: {best_trend['score']:.0f} | {best_trend['direction']}")
                        trade = open_trade_entry(best_trend, "TREND", balance)
                        if trade:
                            open_trades.append(trade)

                if reversal_count < REVERSAL_MAX_TRADES:
                    best_reversal = None
                    for pair in active_pairs:
                        opp = score_reversal(pair, session)
                        if opp and (best_reversal is None or opp["score"] > best_reversal["score"]):
                            best_reversal = opp
                    if best_reversal:
                        scanner_log(f"🔄 [REVERSAL] Best: {best_reversal['instrument']} | "
                                    f"Score: {best_reversal['score']:.0f} | {best_reversal['direction']}")
                        trade = open_trade_entry(best_reversal, "REVERSAL", balance)
                        if trade:
                            open_trades.append(trade)

                if breakout_count < BREAKOUT_MAX_TRADES and session["aggression"] in ("HIGH",):
                    best_breakout = None
                    for pair in active_pairs:
                        opp = score_breakout(pair, session)
                        if opp and (best_breakout is None or opp["score"] > best_breakout["score"]):
                            best_breakout = opp
                    if best_breakout:
                        scanner_log(f"💥 [BREAKOUT] Best: {best_breakout['instrument']} | "
                                    f"Score: {best_breakout['score']:.0f} | {best_breakout['direction']}")
                        trade = open_trade_entry(best_breakout, "BREAKOUT", balance)
                        if trade:
                            open_trades.append(trade)

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