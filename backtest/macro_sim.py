from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


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