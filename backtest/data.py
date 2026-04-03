from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _granularity_step(granularity: str) -> timedelta:
    mapping = {
        "M1": timedelta(minutes=1),
        "M5": timedelta(minutes=5),
        "M15": timedelta(minutes=15),
        "M30": timedelta(minutes=30),
        "H1": timedelta(hours=1),
        "H4": timedelta(hours=4),
        "D": timedelta(days=1),
    }
    return mapping.get(granularity.upper(), timedelta(minutes=5))


def _request_chunk_span(granularity: str, max_candles: int = 4500) -> timedelta:
    step = _granularity_step(granularity)
    return step * max(1, max_candles)


class HistoricalDataProvider:
    def __init__(self, oanda_api_key: str = "", oanda_api_url: str = "https://api-fxpractice.oanda.com", cache_dir: str = "backtest_cache"):
        self.oanda_api_key = oanda_api_key
        self.oanda_api_url = oanda_api_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self._spread_profile_cache: dict[tuple[str, str], dict[str, float]] = {}
        if self.oanda_api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.oanda_api_key}"})

    def _cache_base(self, instrument: str, granularity: str, start: datetime, end: datetime) -> Path:
        name = f"{instrument}_{granularity}_{start:%Y%m%d%H%M}_{end:%Y%m%d%H%M}"
        return self.cache_dir / name

    def _read_cache(self, base: Path) -> pd.DataFrame | None:
        parquet_path = base.with_suffix(".parquet")
        if parquet_path.exists():
            try:
                return pd.read_parquet(parquet_path)
            except Exception:
                parquet_path.unlink(missing_ok=True)
        pickle_path = base.with_suffix(".pkl")
        if pickle_path.exists():
            return pd.read_pickle(pickle_path)
        return None

    def _write_cache(self, base: Path, df: pd.DataFrame) -> None:
        parquet_path = base.with_suffix(".parquet")
        try:
            df.to_parquet(parquet_path)
            return
        except Exception:
            pass
        df.to_pickle(base.with_suffix(".pkl"))

    def get_candles(self, instrument: str, granularity: str, start: datetime, end: datetime, price: str = "M") -> pd.DataFrame | None:
        start = _to_utc(start)
        end = _to_utc(end)
        cache_base = self._cache_base(instrument, granularity, start, end)
        cached = self._read_cache(cache_base)
        if cached is not None:
            return cached
        if not self.oanda_api_key:
            return None

        frames: list[pd.DataFrame] = []
        cursor = start
        chunk_span = _request_chunk_span(granularity)
        while cursor < end:
            chunk_end = min(cursor + chunk_span, end)
            params = {
                "price": price,
                "granularity": granularity,
                "from": cursor.isoformat().replace("+00:00", "Z"),
                "to": chunk_end.isoformat().replace("+00:00", "Z"),
            }
            url = f"{self.oanda_api_url}/v3/instruments/{instrument}/candles"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            candles = payload.get("candles", [])
            rows = []
            for candle in candles:
                mid = candle.get("mid", {})
                bid = candle.get("bid", {})
                ask = candle.get("ask", {})
                open_price = float(mid.get("o", 0) or 0)
                high_price = float(mid.get("h", 0) or 0)
                low_price = float(mid.get("l", 0) or 0)
                close_price = float(mid.get("c", 0) or 0)
                if not mid and bid and ask:
                    open_price = (float(bid.get("o", 0) or 0) + float(ask.get("o", 0) or 0)) / 2.0
                    high_price = (float(bid.get("h", 0) or 0) + float(ask.get("h", 0) or 0)) / 2.0
                    low_price = (float(bid.get("l", 0) or 0) + float(ask.get("l", 0) or 0)) / 2.0
                    close_price = (float(bid.get("c", 0) or 0) + float(ask.get("c", 0) or 0)) / 2.0
                rows.append({
                    "time": pd.to_datetime(candle.get("time"), utc=True, errors="coerce"),
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "bid_open": float(bid.get("o", 0) or 0),
                    "bid_high": float(bid.get("h", 0) or 0),
                    "bid_low": float(bid.get("l", 0) or 0),
                    "bid_close": float(bid.get("c", 0) or 0),
                    "ask_open": float(ask.get("o", 0) or 0),
                    "ask_high": float(ask.get("h", 0) or 0),
                    "ask_low": float(ask.get("l", 0) or 0),
                    "ask_close": float(ask.get("c", 0) or 0),
                    "volume": int(candle.get("volume", 0) or 0),
                    "complete": bool(candle.get("complete", True)),
                })
            if rows:
                frames.append(pd.DataFrame(rows))
            cursor = chunk_end

        if not frames:
            return None

        df = pd.concat(frames, ignore_index=True)
        df = df.dropna(subset=["time"]).drop_duplicates(subset=["time"]).sort_values("time")
        df = df[df["complete"]]
        df = df.set_index("time")
        self._write_cache(cache_base, df)
        return df

    def get_window(self, instrument: str, granularity: str, end: datetime, bars: int, price: str = "M") -> pd.DataFrame | None:
        step = _granularity_step(granularity)
        start = _to_utc(end) - step * max(bars + 5, 10)
        df = self.get_candles(instrument, granularity, start, _to_utc(end), price=price)
        if df is None or df.empty:
            return None
        sliced = df[df.index < _to_utc(end)].tail(bars)
        return sliced if not sliced.empty else None

    def get_pair_spread_profile(self, instrument: str, granularity: str, start: datetime, end: datetime) -> dict[str, float]:
        key = (instrument, granularity)
        if key in self._spread_profile_cache:
            return dict(self._spread_profile_cache[key])
        df = self.get_candles(instrument, granularity, start, end, price="BA")
        if df is None or df.empty or "bid_close" not in df.columns or "ask_close" not in df.columns:
            profile = {"default": 0.8}
            self._spread_profile_cache[key] = profile
            return dict(profile)
        valid = df[(df["bid_close"] > 0) & (df["ask_close"] > 0)].copy()
        if valid.empty:
            profile = {"default": 0.8}
            self._spread_profile_cache[key] = profile
            return dict(profile)
        pip_divisor = 0.01 if "JPY" in instrument else 0.0001
        valid["spread_pips"] = (valid["ask_close"] - valid["bid_close"]) / pip_divisor
        valid["hour"] = valid.index.hour
        profile = {"default": float(valid["spread_pips"].median())}
        for hour, group in valid.groupby("hour"):
            profile[f"hour_{int(hour):02d}"] = float(group["spread_pips"].median())
        self._spread_profile_cache[key] = profile
        return dict(profile)

    def get_bid_ask_bar(self, instrument: str, granularity: str, now: datetime) -> dict[str, float] | None:
        df = self.get_window(instrument, granularity, now + _granularity_step(granularity), 1, price="BA")
        if df is None or df.empty:
            return None
        row = df.iloc[-1]
        if float(row.get("bid_close", 0) or 0) <= 0 or float(row.get("ask_close", 0) or 0) <= 0:
            return None
        return {
            "bid_open": float(row.get("bid_open", 0) or 0),
            "bid_high": float(row.get("bid_high", 0) or 0),
            "bid_low": float(row.get("bid_low", 0) or 0),
            "bid_close": float(row.get("bid_close", 0) or 0),
            "ask_open": float(row.get("ask_open", 0) or 0),
            "ask_high": float(row.get("ask_high", 0) or 0),
            "ask_low": float(row.get("ask_low", 0) or 0),
            "ask_close": float(row.get("ask_close", 0) or 0),
        }

    def save_json_snapshot(self, payload: dict, filename: str) -> Path:
        path = self.cache_dir / filename
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path
