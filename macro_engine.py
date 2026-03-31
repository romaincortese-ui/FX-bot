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
from typing import Any, Dict, List, Optional

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

MACRO_FILTER_FILE = os.getenv("MACRO_FILTER_FILE", "macro_filter.json")
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
DEFAULT_FX_FACTORY_URLS = [
    "https://nfs.forexfactory.com/ff_calendar_thisweek.xml",
    "https://www.forexfactory.com/ff_calendar_thisweek.xml",
    "https://forexfactory.com/ff_calendar_thisweek.xml",
]
DEFAULT_FX_FACTORY_URL = DEFAULT_FX_FACTORY_URLS[0]
FX_FACTORY_URL = os.getenv("FOREX_FACTORY_URL", DEFAULT_FX_FACTORY_URL)
NEWS_PAUSE_BEFORE_MINUTES = int(os.getenv("NEWS_PAUSE_BEFORE_MINUTES", "15"))

LOG_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt=LOG_FORMAT,
)
log = logging.getLogger(__name__)

if FX_FACTORY_URL.endswith(".json") or "cdn-nfs.forexfactory.net" in FX_FACTORY_URL:
    log.warning(
        "Detected deprecated Forex Factory JSON endpoint in FOREX_FACTORY_URL; switching to official XML feed."
    )
    FX_FACTORY_URL = DEFAULT_FX_FACTORY_URL


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


def load_oanda_commodity_momentum() -> Dict[str, Optional[float]]:
    return {
        "OIL": fetch_oanda_daily_pct_change(OANDA_COMMODITY_INSTRUMENTS["OIL"]),
        "COPPER": fetch_oanda_daily_pct_change(OANDA_COMMODITY_INSTRUMENTS["COPPER"]),
        "DAIRY": parse_float_env("DAIRY_MOMENTUM"),
    }


def load_commodity_momentum() -> Dict[str, Optional[float]]:
    momentum = load_oanda_commodity_momentum()
    if any(momentum.get(key) is not None for key in ("OIL", "COPPER")):
        return momentum
    log.warning("OANDA commodity momentum unavailable; falling back to environment variables.")
    return {
        "OIL": parse_float_env("OIL_MOMENTUM"),
        "COPPER": parse_float_env("COPPER_MOMENTUM"),
        "DAIRY": parse_float_env("DAIRY_MOMENTUM"),
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    urls = [FX_FACTORY_URL]
    if FX_FACTORY_URL == DEFAULT_FX_FACTORY_URL:
        urls.extend(url for url in DEFAULT_FX_FACTORY_URLS if url != FX_FACTORY_URL)
    else:
        urls.extend(url for url in DEFAULT_FX_FACTORY_URLS if url != FX_FACTORY_URL)

    root = None
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            log.info(f"Loaded Forex Factory XML from {url}")
            break
        except Exception as e:
            log.warning(f"Failed to fetch or parse Forex Factory XML from {url}: {e}")

    if root is None:
        log.error("Failed to fetch Forex Factory XML from all candidate URLs.")
        return []

    raw_events = []
    for event in root.findall('.//event'):
        raw_events.append({
            'title': event.findtext('title', default=''),
            'country': event.findtext('country', default=''),
            'date': event.findtext('date', default=''),
            'time': event.findtext('time', default=''),
            'impact': event.findtext('impact', default=''),
            'forecast': event.findtext('forecast', default=''),
            'previous': event.findtext('previous', default=''),
            'actual': event.findtext('actual', default=''),
        })

    events: List[dict] = []
    today = datetime.now(timezone.utc).date()
    for raw in raw_events:
        if not is_high_impact(raw):
            continue
        event_time = parse_forex_event_time(raw)
        if event_time is None or event_time.date() != today:
            continue
        pause_start = event_time - timedelta(minutes=NEWS_PAUSE_BEFORE_MINUTES)
        pause_end = event_time + timedelta(minutes=NEWS_PAUSE_BEFORE_MINUTES)
        events.append({
            "currency": raw.get("country"),
            "event": raw.get("title"),
            "impact": raw.get("impact"),
            "time": event_time.isoformat(),
            "forecast": raw.get("forecast"),
            "previous": raw.get("previous"),
            "actual": raw.get("actual"),
            "pause_start": pause_start.isoformat(),
            "pause_end": pause_end.isoformat(),
        })

    if events:
        log.info(f"Successfully loaded {len(events)} news events from Forex Factory XML.")
    else:
        log.info("No high-impact Forex Factory events found for today.")
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


def generate_macro_filters() -> Dict[str, str]:
    rates = load_interest_rates()
    momentum = load_commodity_momentum()
    esi = load_economic_surprise()
    liquidity = load_liquidity_risk()

    rate_bias = build_rate_bias(rates)
    commodity_bias = build_commodity_bias(momentum)
    esi_bias = build_esi_bias(esi)
    liquidity_bias = build_liquidity_bias(liquidity)

    filters = merge_biases(rate_bias, commodity_bias, esi_bias, liquidity_bias)
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
    filters = generate_macro_filters()
    save_macro_filters(filters)
    news = load_forex_factory_news()
    save_macro_news(news)
    log.info("Macro engine finished")


if __name__ == "__main__":
    run()
