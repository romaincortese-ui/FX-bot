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


class HistoricalDataProvider:
    def __init__(self, oanda_api_key: str = "", oanda_api_url: str = "https://api-fxpractice.oanda.com", cache_dir: str = "backtest_cache"):
        self.oanda_api_key = oanda_api_key
        self.oanda_api_url = oanda_api_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
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
        chunk_days = 30
        while cursor < end:
            chunk_end = min(cursor + timedelta(days=chunk_days), end)
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
                rows.append({
                    "time": pd.to_datetime(candle.get("time"), utc=True, errors="coerce"),
                    "open": float(mid.get("o", 0) or 0),
                    "high": float(mid.get("h", 0) or 0),
                    "low": float(mid.get("l", 0) or 0),
                    "close": float(mid.get("c", 0) or 0),
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

    def save_json_snapshot(self, payload: dict, filename: str) -> Path:
        path = self.cache_dir / filename
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path
