from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from macro_engine import build_commodity_bias
from macro_engine import build_esi_bias
from macro_engine import build_liquidity_bias
from macro_engine import build_market_index_bias
from macro_engine import build_rate_bias
from macro_engine import merge_biases


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@dataclass(slots=True)
class MacroState:
    filters: dict[str, str]
    news_events: list[dict]
    vix_value: float | None = None
    dxy_gap: float | None = None


class MacroReplay:
    def __init__(self, states: dict[date, MacroState]):
        self.daily_states = states

    @classmethod
    def from_directory(cls, directory: str, start: datetime, end: datetime) -> "MacroReplay":
        base = Path(directory)
        states: dict[date, MacroState] = {}
        if base.exists():
            for path in sorted(base.glob("*.json")):
                payload = json.loads(path.read_text(encoding="utf-8"))
                as_of = payload.get("as_of") or payload.get("generated_at") or path.stem
                ts = _parse_timestamp(str(as_of))
                if ts is None:
                    try:
                        ts = datetime.fromisoformat(path.stem).replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                states[ts.date()] = MacroState(
                    filters={str(k).upper(): str(v).upper() for k, v in dict(payload.get("filters", {})).items()},
                    news_events=list(payload.get("news_events", [])),
                    vix_value=payload.get("vix_value"),
                    dxy_gap=payload.get("dxy_gap"),
                )
        if not states:
            states = cls._build_default_states(start, end)
        return cls(states)

    @classmethod
    def from_static_files(cls, start: datetime, end: datetime, filter_file: str = "macro_filter.json", news_file: str = "macro_news.json") -> "MacroReplay":
        filters = {}
        news_events: list[dict] = []
        filter_path = Path(filter_file)
        if filter_path.exists():
            payload = json.loads(filter_path.read_text(encoding="utf-8"))
            filters = {str(k).upper(): str(v).upper() for k, v in dict(payload).items()}
        news_path = Path(news_file)
        if news_path.exists():
            payload = json.loads(news_path.read_text(encoding="utf-8"))
            news_events = list(payload.get("news_events", [])) if isinstance(payload, dict) else []
        states = cls._build_default_states(start, end, filters=filters, news_events=news_events)
        return cls(states)

    @staticmethod
    def _build_default_states(start: datetime, end: datetime, filters: dict[str, str] | None = None, news_events: list[dict] | None = None) -> dict[date, MacroState]:
        states: dict[date, MacroState] = {}
        current = start.date()
        while current <= end.date():
            states[current] = MacroState(filters=dict(filters or {}), news_events=list(news_events or []))
            current += timedelta(days=1)
        return states

    def get_state(self, at: datetime) -> MacroState:
        key = at.astimezone(timezone.utc).date()
        return self.daily_states.get(key, MacroState(filters={}, news_events=[]))


def _load_historical_series(path: str) -> pd.DataFrame:
    file_path = Path(path)
    if not path or not file_path.exists():
        return pd.DataFrame()
    if file_path.suffix.lower() == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            df = pd.DataFrame(payload)
        elif isinstance(payload, dict):
            records = payload.get("records") if isinstance(payload.get("records"), list) else [payload]
            df = pd.DataFrame(records)
        else:
            return pd.DataFrame()
    else:
        df = pd.read_csv(file_path)
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df


def _series_value_for_day(df: pd.DataFrame, day: date) -> dict[str, float | str | None]:
    if df.empty:
        return {}
    rows = df[df["date"].dt.date <= day]
    if rows.empty:
        return {}
    row = rows.iloc[-1]
    result = {}
    for column in df.columns:
        if column == "date":
            continue
        result[column] = row[column]
    return result


def _load_historical_news(path: str) -> list[dict]:
    file_path = Path(path)
    if not path or not file_path.exists():
        return []
    payload = json.loads(file_path.read_text(encoding="utf-8")) if file_path.suffix.lower() == ".json" else []
    if isinstance(payload, dict):
        events = payload.get("news_events", [])
    elif isinstance(payload, list):
        events = payload
    else:
        events = []
    return [event for event in events if isinstance(event, dict)]


def generate_daily_macro_snapshots(start: datetime, end: datetime, output_dir: str, rates_file: str = "", momentum_file: str = "", esi_file: str = "", liquidity_file: str = "", news_file: str = "", dxy_history_file: str = "", vix_history_file: str = "") -> dict[date, MacroState]:
    rates_df = _load_historical_series(rates_file)
    momentum_df = _load_historical_series(momentum_file)
    esi_df = _load_historical_series(esi_file)
    liquidity_df = _load_historical_series(liquidity_file)
    dxy_df = _load_historical_series(dxy_history_file)
    vix_df = _load_historical_series(vix_history_file)
    news_events = _load_historical_news(news_file)

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    states: dict[date, MacroState] = {}
    current = start.date()
    while current <= end.date():
        rates = _series_value_for_day(rates_df, current)
        momentum = _series_value_for_day(momentum_df, current)
        esi = _series_value_for_day(esi_df, current)
        liquidity = _series_value_for_day(liquidity_df, current)
        dxy_row = _series_value_for_day(dxy_df, current)
        vix_row = _series_value_for_day(vix_df, current)
        historical_events = []
        for event in news_events:
            event_time = _parse_timestamp(event.get("time") or event.get("pause_start") or event.get("pause_end"))
            if event_time is not None and event_time.date() == current:
                historical_events.append(event)
        rate_bias = build_rate_bias(rates)
        commodity_bias = build_commodity_bias(momentum)
        market_inputs = dict(momentum)
        if "DXY" not in market_inputs and "dxy" in dxy_row:
            market_inputs["DXY"] = dxy_row.get("dxy")
        if "VIX" not in market_inputs and "vix" in vix_row:
            market_inputs["VIX"] = vix_row.get("vix")
        market_bias = build_market_index_bias(market_inputs)
        esi_bias = build_esi_bias(esi)
        liquidity_bias = build_liquidity_bias(liquidity)
        filters = merge_biases(esi_bias, commodity_bias, market_bias, rate_bias, liquidity_bias)
        dxy_gap = float(dxy_row.get("dxy_gap", dxy_row.get("value", 0.0) or 0.0)) if dxy_row else 0.0
        vix_value = float(vix_row.get("vix_value", vix_row.get("value", 15.0) or 15.0)) if vix_row else 15.0
        payload = {
            "generated_at": datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
            "as_of": datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
            "filters": filters,
            "news_events": historical_events,
            "vix_value": vix_value,
            "dxy_gap": dxy_gap,
        }
        (output / f"{current.isoformat()}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        states[current] = MacroState(filters=filters, news_events=historical_events, vix_value=vix_value, dxy_gap=dxy_gap)
        current += timedelta(days=1)
    return states