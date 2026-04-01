"""
Daily macro engine for producing a simple macro filter file for the FX bot.

This script is intentionally lightweight and configurable by environment variables.
It can be extended later with real API connectors for FRED, commodity data,
news surprise feeds, and liquidity spreads.
"""

import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import requests

try:
    import redis
except ImportError:
    redis = None  # type: ignore

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

MACRO_FILTER_FILE = os.getenv("MACRO_FILTER_FILE", "macro_filter.json")
REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_MACRO_STATE_KEY = os.getenv("REDIS_MACRO_STATE_KEY", "macro_state")
MACRO_NEWS_FILE = os.getenv("MACRO_NEWS_FILE", "macro_news.json")
RATE_SPREAD_THRESHOLD = float(os.getenv("RATE_SPREAD_THRESHOLD", "0.25"))
COMMODITY_MOMENTUM_THRESHOLD = float(os.getenv("COMMODITY_MOMENTUM_THRESHOLD", "0.03"))
ESI_THRESHOLD = float(os.getenv("ESI_THRESHOLD", "5.0"))
LIQUIDITY_RISK_THRESHOLD = float(os.getenv("LIQUIDITY_RISK_THRESHOLD", "0.50"))

OANDA_API_URL = os.getenv("OANDA_API_URL", "https://api-fxpractice.oanda.com")
OANDA_API_KEY = os.getenv("OANDA_API_KEY", "")
OANDA_COMMODITY_INSTRUMENTS = {
    "OIL": os.getenv("OANDA_OIL_INSTRUMENT", "BCO_USD"),
    "COPPER": os.getenv("OANDA_COPPER_INSTRUMENT", "XCU_USD"),
}
YFINANCE_MARKET_INDICATORS = {
    "DXY": os.getenv("DXY_TICKER", "DX-Y.NYB"),
    "VIX": os.getenv("VIX_TICKER", "^VIX"),
}
YFINANCE_COMMODITY_TICKERS = {
    "OIL": os.getenv("YFINANCE_OIL_TICKER", "CL=F"),
    "COPPER": os.getenv("YFINANCE_COPPER_TICKER", "HG=F"),
}
FX_INDEX_MOMENTUM_THRESHOLD = float(os.getenv("FX_INDEX_MOMENTUM_THRESHOLD", "0.01"))
DEFAULT_ECONOMIC_CALENDAR_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.xml",
    "https://www.forexfactory.com/ffcal_week_this.xml",
    "https://www.dailyfx.com/free-ads/economic-calendar-rss",
    "https://www.investing.com/rss/economic-calendar",
]
DEFAULT_ECONOMIC_CALENDAR_URL = DEFAULT_ECONOMIC_CALENDAR_URLS[0]
ECONOMIC_CALENDAR_URL = os.getenv("ECONOMIC_CALENDAR_URL", DEFAULT_ECONOMIC_CALENDAR_URL)
NEWS_PAUSE_BEFORE_MINUTES = int(os.getenv("NEWS_PAUSE_BEFORE_MINUTES", "15"))

LOG_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt=LOG_FORMAT,
)
log = logging.getLogger(__name__)

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    yf = None  # type: ignore
    YFINANCE_AVAILABLE = False
    log.warning("yfinance is not installed; DXY/VIX momentum will fallback to environment variables when available.")


def parse_float_env(name: str) -> Optional[float]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    try:
        return float(value)
    except ValueError:
        log.warning(f"Invalid numeric environment value for {name}: {value}")
        return None


def fetch_fred_series(series_id: str) -> Optional[float]:
    fred_key = os.getenv("FRED_API_KEY")
    if not fred_key:
        log.warning("FRED_API_KEY not set; skipping FRED data fetch.")
        return None
    try:
        from fredapi import Fred
    except ImportError:
        log.warning("fredapi is not installed. Install it with pip install fredapi")
        return None
    try:
        fred = Fred(api_key=fred_key)
        series = fred.get_series(series_id)
        if series is None or series.empty:
            return None
        return float(series.dropna().iloc[-1])
    except Exception as e:
        log.warning(f"Failed to fetch FRED series {series_id}: {e}")
        return None


def load_fred_rates() -> Dict[str, Optional[float]]:
    values = {
        "US_2Y": fetch_fred_series("DGS2"),
        "US_10Y": fetch_fred_series("DGS10"),
        "TED_SPREAD": fetch_fred_series("TEDRATE"),
    }
    if any(v is not None for v in values.values()):
        log.info(f"Loaded FRED rates: US_2Y={values['US_2Y']} US_10Y={values['US_10Y']} TED={values['TED_SPREAD']}")
    return values


def load_interest_rates() -> Dict[str, Optional[float]]:
    fred_rates = load_fred_rates()
    if any(v is not None for v in fred_rates.values()):
        return fred_rates
    log.warning("FRED data unavailable; falling back to environment rate variables.")
    return {
        "US_2Y": parse_float_env("US_2Y_YIELD"),
        "US_10Y": parse_float_env("US_10Y_YIELD"),
        "UK_2Y": parse_float_env("UK_2Y_YIELD"),
        "EU_2Y": parse_float_env("EU_2Y_YIELD"),
        "JP_2Y": parse_float_env("JP_2Y_YIELD"),
        "TED_SPREAD": parse_float_env("TED_SPREAD"),
    }


def fetch_oanda_daily_pct_change(instrument: str) -> Optional[float]:
    if not OANDA_API_KEY:
        log.warning("OANDA_API_KEY not set; cannot fetch commodity momentum.")
        return None
    url = f"{OANDA_API_URL.rstrip('/')}/v3/instruments/{instrument}/candles"
    headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}
    params = {
        "granularity": "D",
        "count": "3",
        "price": "M",
        "dailyAlignment": "0",
        "alignmentTimezone": "UTC",
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        candles = data.get("candles", [])
        closes = []
        for candle in candles:
            mid = candle.get("mid")
            if isinstance(mid, dict):
                close_price = mid.get("c")
            else:
                close_price = candle.get("close", {}).get("c")
            if close_price is not None:
                closes.append(float(close_price))
        if len(closes) < 2:
            log.warning(f"Not enough close candles for {instrument}")
            return None
        return closes[-1] / closes[-2] - 1
    except Exception as e:
        log.warning(f"Failed to fetch OANDA candles for {instrument}: {e}")
        return None


def fetch_yfinance_daily_pct_change(ticker: str) -> Optional[float]:
    if not YFINANCE_AVAILABLE or yf is None:
        return None

    try:
        ticker_data = yf.Ticker(ticker)
        history = ticker_data.history(period="3d", interval="1d")
        closes = history["Close"].dropna().tolist()
        if len(closes) < 2:
            log.warning(f"Not enough yfinance close data for {ticker}")
            return None
        return closes[-1] / closes[-2] - 1
    except Exception as e:
        log.warning(f"Failed to fetch yfinance data for {ticker}: {e}")
        return None


def _fetch_commodity_pct_change(name: str) -> Optional[float]:
    """Try OANDA first for a commodity, fall back to yfinance."""
    oanda_instrument = OANDA_COMMODITY_INSTRUMENTS.get(name)
    if oanda_instrument:
        value = fetch_oanda_daily_pct_change(oanda_instrument)
        if value is not None:
            return value
        log.info(f"OANDA unavailable for {name}; trying yfinance fallback.")
    yf_ticker = YFINANCE_COMMODITY_TICKERS.get(name)
    if yf_ticker:
        return fetch_yfinance_daily_pct_change(yf_ticker)
    return None


def load_oanda_commodity_momentum() -> Dict[str, Optional[float]]:
    return {
        "OIL": _fetch_commodity_pct_change("OIL"),
        "COPPER": _fetch_commodity_pct_change("COPPER"),
        "DAIRY": parse_float_env("DAIRY_MOMENTUM"),
    }


def load_yfinance_market_momentum() -> Dict[str, Optional[float]]:
    return {
        "DXY": fetch_yfinance_daily_pct_change(YFINANCE_MARKET_INDICATORS["DXY"]),
        "VIX": fetch_yfinance_daily_pct_change(YFINANCE_MARKET_INDICATORS["VIX"]),
    }


def get_dxy_gap() -> Optional[float]:
    return load_yfinance_market_momentum().get("DXY")


def get_vix_proxy() -> Optional[float]:
    return load_yfinance_market_momentum().get("VIX")


def load_commodity_momentum() -> Dict[str, Optional[float]]:
    momentum = load_oanda_commodity_momentum()
    market_momentum = load_yfinance_market_momentum()
    if any(momentum.get(key) is not None for key in ("OIL", "COPPER")) or any(market_momentum.get(key) is not None for key in ("DXY", "VIX")):
        return {**momentum, **market_momentum}

    log.warning("OANDA commodity and yfinance market momentum unavailable; falling back to environment variables.")
    return {
        "OIL": parse_float_env("OIL_MOMENTUM"),
        "COPPER": parse_float_env("COPPER_MOMENTUM"),
        "DAIRY": parse_float_env("DAIRY_MOMENTUM"),
        "DXY": parse_float_env("DXY_MOMENTUM"),
        "VIX": parse_float_env("VIX_MOMENTUM"),
    }


def load_economic_surprise() -> Dict[str, Optional[float]]:
    if not os.getenv("FRED_API_KEY"):
        log.warning("FRED_API_KEY not set; using fallback economic surprise values.")
        return {
            "US": parse_float_env("US_ECON_SURPRISE"),
            "UK": parse_float_env("UK_ECON_SURPRISE"),
            "EU": parse_float_env("EU_ECON_SURPRISE"),
            "JP": parse_float_env("JP_ECON_SURPRISE"),
        }

    surprise_series = {
        "US": "USEPUINDXD",
        "UK": "UKEPUINDXM",
        "EU": "EUEPUINDXM",
        "JP": "JPNEPUINDXM",
    }
    values = {country: fetch_fred_series(series_id)
              for country, series_id in surprise_series.items()}
    if any(v is not None for v in values.values()):
        log.info(f"Loaded economic surprise / policy uncertainty values: {values}")
        return values

    log.warning("Economic surprise data unavailable from FRED; using fallback values.")
    return {
        "US": parse_float_env("US_ECON_SURPRISE"),
        "UK": parse_float_env("UK_ECON_SURPRISE"),
        "EU": parse_float_env("EU_ECON_SURPRISE"),
        "JP": parse_float_env("JP_ECON_SURPRISE"),
    }


def load_liquidity_risk() -> Dict[str, Optional[float]]:
    if not os.getenv("FRED_API_KEY"):
        log.warning("FRED_API_KEY not set; using fallback liquidity risk values.")
        return {
            "TED_SPREAD": parse_float_env("TED_SPREAD"),
            "FRA_OIS_SPREAD": parse_float_env("FRA_OIS_SPREAD"),
        }

    values = {
        "TED_SPREAD": fetch_fred_series("TEDRATE"),
        "FRA_OIS_SPREAD": parse_float_env("FRA_OIS_SPREAD"),
    }
    if values["TED_SPREAD"] is not None:
        log.info(f"Loaded liquidity risk values: {values}")
        return values

    log.warning("Liquidity risk data unavailable from FRED; using fallback values.")
    return {
        "TED_SPREAD": parse_float_env("TED_SPREAD"),
        "FRA_OIS_SPREAD": parse_float_env("FRA_OIS_SPREAD"),
    }


def _parse_forex_datetime_string(value: str) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    # Normalize explicit UTC designations
    text = text.replace("Z", "+00:00")
    # Forex Factory can emit EST/EDT-like timestamps; convert these to offsets explicitly
    text = text.replace(" EST", "-05:00")
    text = text.replace(" EDT", "-04:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            if ZoneInfo is not None:
                try:
                    eastern = ZoneInfo("America/New_York")
                except Exception:
                    eastern = timezone(timedelta(hours=-5))
            else:
                eastern = timezone(timedelta(hours=-5))
            parsed = parsed.replace(tzinfo=eastern)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def parse_forex_event_time(raw: dict) -> Optional[datetime]:
    ts = raw.get("timestamp") or raw.get("dateTimestamp") or raw.get("eventTimestamp")
    if ts is not None:
        try:
            return datetime.fromtimestamp(int(ts), timezone.utc)
        except Exception:
            pass
    dt = raw.get("datetime") or raw.get("dateTime") or raw.get("eventDateTime")
    if isinstance(dt, str):
        parsed = _parse_forex_datetime_string(dt)
        if parsed is not None:
            return parsed
    date = raw.get("date")
    time_value = raw.get("time")
    if date and time_value:
        combined = f"{date}T{time_value}"
        parsed = _parse_forex_datetime_string(combined)
        if parsed is not None:
            return parsed
    return None


def is_high_impact(raw: dict) -> bool:
    impact = str(raw.get("impact") or raw.get("importance") or "").strip().lower()
    return impact in {"high", "red", "3", "3/3", "3 of 3", "high impact"}


def extract_forex_factory_events(raw: Any) -> List[dict]:
    events: List[dict] = []
    if isinstance(raw, dict):
        candidate = raw.get("events") or raw.get("calendar") or raw
        if isinstance(candidate, dict):
            for value in candidate.values():
                if isinstance(value, list):
                    events.extend(value)
        elif isinstance(candidate, list):
            events.extend(candidate)
    elif isinstance(raw, list):
        events.extend(raw)
    return [item for item in events if isinstance(item, dict)]


def load_forex_factory_news() -> List[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    urls = [ECONOMIC_CALENDAR_URL]
    urls.extend(url for url in DEFAULT_ECONOMIC_CALENDAR_URLS if url != ECONOMIC_CALENDAR_URL)

    root = None
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            log.info(f"Loaded economic calendar XML from {url}")
            break
        except Exception as e:
            log.warning(f"Failed to fetch or parse economic calendar XML from {url}: {e}")

    if root is None:
        log.error("Failed to fetch economic calendar XML from all candidate URLs.")
        return []

    events: List[dict] = []
    # Allow a configurable lookback window (in days) for news events
    news_lookback_days = int(os.getenv("NEWS_LOOKBACK_DAYS", "1"))
    now_utc = datetime.now(timezone.utc)
    min_date = (now_utc - timedelta(days=news_lookback_days)).date()
    max_date = now_utc.date()
    for item in root.findall('.//item'):
        title = item.findtext('title', default='').strip()
        link = item.findtext('link', default='').strip()
        description = item.findtext('description', default='').strip()
        raw_time = item.findtext('pubDate', default='').strip()
        event_time = None
        try:
            event_time = parsedate_to_datetime(raw_time).astimezone(timezone.utc)
        except Exception:
            # Try parsing with datetime.fromisoformat as fallback
            try:
                event_time = datetime.fromisoformat(raw_time)
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                event_time = event_time.astimezone(timezone.utc)
            except Exception:
                event_time = None

        # Accept events within the lookback window
        if event_time is None or not (min_date <= event_time.date() <= max_date):
            continue

        pause_start = event_time - timedelta(minutes=NEWS_PAUSE_BEFORE_MINUTES)
        pause_end = event_time + timedelta(minutes=NEWS_PAUSE_BEFORE_MINUTES)
        events.append({
            "currency": None,
            "event": title,
            "impact": None,
            "time": event_time.isoformat(),
            "forecast": None,
            "previous": None,
            "actual": None,
            "link": link,
            "description": description,
            "pause_start": pause_start.isoformat(),
            "pause_end": pause_end.isoformat(),
        })

    if events:
        log.info(f"Successfully loaded {len(events)} news events from economic calendar XML (lookback {news_lookback_days}d).")
    else:
        log.info("No economic calendar events found in lookback window.")
    return events


def fetch_json(url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None) -> Optional[dict]:
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.warning(f"Failed to fetch JSON from {url}: {e}")
        return None


def build_rate_bias(rates: Dict[str, Optional[float]]) -> Dict[str, str]:
    biases = {}
    us_2y = rates.get("US_2Y")
    uk_2y = rates.get("UK_2Y")
    eu_2y = rates.get("EU_2Y")
    jp_2y = rates.get("JP_2Y")

    if us_2y is not None and uk_2y is not None:
        if uk_2y - us_2y > RATE_SPREAD_THRESHOLD:
            biases["GBP_USD"] = "LONG_ONLY"
        elif us_2y - uk_2y > RATE_SPREAD_THRESHOLD:
            biases["GBP_USD"] = "SHORT_ONLY"

    if us_2y is not None and eu_2y is not None:
        if eu_2y - us_2y > RATE_SPREAD_THRESHOLD:
            biases["EUR_USD"] = "LONG_ONLY"
        elif us_2y - eu_2y > RATE_SPREAD_THRESHOLD:
            biases["EUR_USD"] = "SHORT_ONLY"

    if us_2y is not None and jp_2y is not None:
        if us_2y - jp_2y > RATE_SPREAD_THRESHOLD:
            biases["USD_JPY"] = "LONG_ONLY"
        elif jp_2y - us_2y > RATE_SPREAD_THRESHOLD:
            biases["USD_JPY"] = "SHORT_ONLY"

    return biases


def build_commodity_bias(momentum: Dict[str, Optional[float]]) -> Dict[str, str]:
    biases = {}
    oil = momentum.get("OIL")
    copper = momentum.get("COPPER")
    dairy = momentum.get("DAIRY")

    if oil is not None:
        if oil > COMMODITY_MOMENTUM_THRESHOLD:
            biases["USD_CAD"] = "SHORT_ONLY"
        elif oil < -COMMODITY_MOMENTUM_THRESHOLD:
            biases["USD_CAD"] = "LONG_ONLY"

    if copper is not None:
        if copper > COMMODITY_MOMENTUM_THRESHOLD:
            biases["AUD_JPY"] = "LONG_ONLY"
        elif copper < -COMMODITY_MOMENTUM_THRESHOLD:
            biases["AUD_JPY"] = "SHORT_ONLY"

    if dairy is not None:
        if dairy > COMMODITY_MOMENTUM_THRESHOLD:
            biases["NZD_USD"] = "LONG_ONLY"
        elif dairy < -COMMODITY_MOMENTUM_THRESHOLD:
            biases["NZD_USD"] = "SHORT_ONLY"

    return biases


def build_market_index_bias(indices: Dict[str, Optional[float]]) -> Dict[str, str]:
    biases = {}
    dxy = indices.get("DXY")
    vix = indices.get("VIX")

    if dxy is not None:
        if dxy > FX_INDEX_MOMENTUM_THRESHOLD:
            biases.update({
                "EUR_USD": "SHORT_ONLY",
                "GBP_USD": "SHORT_ONLY",
                "AUD_USD": "SHORT_ONLY",
                "USD_JPY": "LONG_ONLY",
                "USD_CHF": "LONG_ONLY",
                "USD_CAD": "LONG_ONLY",
            })
        elif dxy < -FX_INDEX_MOMENTUM_THRESHOLD:
            biases.update({
                "EUR_USD": "LONG_ONLY",
                "GBP_USD": "LONG_ONLY",
                "AUD_USD": "LONG_ONLY",
                "USD_JPY": "SHORT_ONLY",
                "USD_CHF": "SHORT_ONLY",
                "USD_CAD": "SHORT_ONLY",
            })

    if vix is not None:
        if vix > FX_INDEX_MOMENTUM_THRESHOLD:
            biases.update({
                "AUD_USD": "SHORT_ONLY",
                "NZD_USD": "SHORT_ONLY",
                "GBP_USD": "SHORT_ONLY",
                "EUR_USD": "SHORT_ONLY",
                "USD_JPY": "LONG_ONLY",
                "USD_CHF": "LONG_ONLY",
            })
        elif vix < -FX_INDEX_MOMENTUM_THRESHOLD:
            biases.update({
                "AUD_USD": "LONG_ONLY",
                "NZD_USD": "LONG_ONLY",
                "GBP_USD": "LONG_ONLY",
                "EUR_USD": "LONG_ONLY",
                "USD_JPY": "SHORT_ONLY",
                "USD_CHF": "SHORT_ONLY",
            })

    return biases


def build_esi_bias(esi: Dict[str, Optional[float]]) -> Dict[str, str]:
    biases = {}
    us = esi.get("US")
    uk = esi.get("UK")
    eu = esi.get("EU")
    jp = esi.get("JP")

    if us is not None:
        if us > ESI_THRESHOLD:
            biases.update({"EUR_USD": "SHORT_ONLY", "GBP_USD": "SHORT_ONLY", "AUD_USD": "SHORT_ONLY"})
        elif us < -ESI_THRESHOLD:
            biases.update({"EUR_USD": "LONG_ONLY", "GBP_USD": "LONG_ONLY", "AUD_USD": "LONG_ONLY"})

    if uk is not None:
        if uk > ESI_THRESHOLD:
            biases["GBP_USD"] = "LONG_ONLY"
        elif uk < -ESI_THRESHOLD:
            biases["GBP_USD"] = "SHORT_ONLY"

    if eu is not None:
        if eu > ESI_THRESHOLD:
            biases["EUR_USD"] = "LONG_ONLY"
        elif eu < -ESI_THRESHOLD:
            biases["EUR_USD"] = "SHORT_ONLY"

    if jp is not None:
        if jp > ESI_THRESHOLD:
            biases["USD_JPY"] = "LONG_ONLY"
        elif jp < -ESI_THRESHOLD:
            biases["USD_JPY"] = "SHORT_ONLY"

    return biases


def build_liquidity_bias(risk: Dict[str, Optional[float]]) -> Dict[str, str]:
    biases = {}
    ted = risk.get("TED_SPREAD")
    fra = risk.get("FRA_OIS_SPREAD")

    if (ted is not None and ted > LIQUIDITY_RISK_THRESHOLD) or (fra is not None and fra > LIQUIDITY_RISK_THRESHOLD):
        biases.update({
            "AUD_USD": "SHORT_ONLY",
            "NZD_USD": "SHORT_ONLY",
            "USD_CHF": "LONG_ONLY",
            "USD_JPY": "LONG_ONLY",
        })
    return biases


def merge_biases(*bias_groups: Dict[str, str]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for group in bias_groups:
        for symbol, value in group.items():
            existing = merged.get(symbol)
            if existing and existing != value:
                log.info(f"Macro bias conflict for {symbol}: keeping {value} over {existing}")
            merged[symbol] = value
    return merged


def generate_macro_filters() -> Dict[str, Any]:
    rates = load_interest_rates()
    momentum = load_commodity_momentum()
    esi = load_economic_surprise()
    liquidity = load_liquidity_risk()

    # Biases are merged lowest-to-highest priority. Later sources overwrite earlier
    # ones for the same symbol.  Order: ESI → Commodities → Market Index → Rates → Liquidity
    #   - ESI:        noisiest signal, easily overridden
    #   - Commodities: medium-term, commodity-linked pairs
    #   - Market Index: DXY/VIX momentum, strong but short-term
    #   - Rates:       interest-rate differentials, primary FX fundamental driver
    #   - Liquidity:   safety circuit-breaker, always gets the final say
    esi_bias = build_esi_bias(esi)
    commodity_bias = build_commodity_bias(momentum)
    market_bias = build_market_index_bias(momentum)
    rate_bias = build_rate_bias(rates)
    liquidity_bias = build_liquidity_bias(liquidity)

    filters = merge_biases(esi_bias, commodity_bias, market_bias, rate_bias, liquidity_bias)

    # Always include VIX and DXY so they appear in any Redis payload built from
    # generate_macro_filters(), regardless of whether run() is the caller.
    try:
        dxy_val = get_dxy_gap()
        filters["dxy_gap"] = float(dxy_val) if dxy_val is not None else 0.0
    except Exception:
        filters["dxy_gap"] = 0.0

    try:
        vix_val = get_vix_proxy()
        filters["vix_value"] = float(vix_val) if vix_val is not None else 15.0
    except Exception:
        filters["vix_value"] = 15.0

    if filters:
        log.info(f"Generated macro filter values: {filters}")
    else:
        log.warning("No macro filter values were generated; check environment variables or data feeds.")
    return filters


def save_macro_filters(filters: Dict[str, str], path: str = MACRO_FILTER_FILE) -> None:
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "filters": filters,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload["filters"], f, indent=2)
    log.info(f"Saved macro filter file to {path}")


def save_macro_news(news_events: List[dict], path: str = MACRO_NEWS_FILE) -> None:
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "news_events": news_events,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    log.info(f"Saved macro news file to {path}")


def run() -> None:
    log.info("Starting macro engine")

    # 1. Fetch all data (filters now includes vix_value and dxy_gap)
    filters = generate_macro_filters()
    news = load_forex_factory_news()

    # 2. Extract VIX/DXY from filters (they are always present with defaults)
    vix_value = filters.pop("vix_value", 15.0)
    dxy_gap = filters.pop("dxy_gap", 0.0)

    # 3. Build state (Ensure keys match exactly what main.py expects)
    macro_state = {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "filters": filters,
        "news_events": news,
        "vix_value": vix_value,
        "dxy_gap": dxy_gap,
    }

    # 4. Push to Redis
    if REDIS_URL and redis:
        client = redis.from_url(REDIS_URL)
        client.set(REDIS_MACRO_STATE_KEY, json.dumps(macro_state))
        log.info(f"Pushed macro state to Redis with VIX={vix_value} and DXY={dxy_gap}")
    else:
        if not REDIS_URL:
            log.warning("REDIS_URL not set; skipping Redis push.")
        elif not redis:
            log.warning("redis library not installed; skipping Redis push.")

    save_macro_filters(filters)
    save_macro_news(news)
    log.info("Macro engine finished")


if __name__ == "__main__":
    run()
