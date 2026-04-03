import json
import os
from datetime import datetime, timedelta, timezone


def parse_forex_datetime_string(value: str, zoneinfo_cls=None) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    text = text.replace(" EST", "-05:00")
    text = text.replace(" EDT", "-04:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            if zoneinfo_cls is not None:
                try:
                    eastern = zoneinfo_cls("America/New_York")
                except Exception:
                    eastern = timezone(timedelta(hours=-5))
            else:
                eastern = timezone(timedelta(hours=-5))
            parsed = parsed.replace(tzinfo=eastern)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def parse_calendar_event_datetime(date_text: str, time_text: str) -> datetime | None:
    if not date_text:
        return None
    normalized_date = date_text.strip()
    normalized_time = (time_text or "").strip().lower()
    if not normalized_time or normalized_time in {"all day", "tentative"}:
        normalized_time = "12:00am"
    normalized_time = normalized_time.replace(" ", "")
    combined = f"{normalized_date} {normalized_time}"
    for fmt in ("%m-%d-%Y %I:%M%p", "%Y-%m-%d %I:%M%p"):
        try:
            return datetime.strptime(combined, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def load_cached_news(cache_path: str, max_age_hours: int, logger=None) -> list[dict]:
    if not cache_path or not os.path.exists(cache_path):
        return []
    try:
        with open(cache_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        cached_at = datetime.fromisoformat(payload.get("cached_at", "2000-01-01T00:00:00+00:00"))
        age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
        if age_hours > max_age_hours:
            return []
        events = payload.get("news_events", [])
        if logger is not None and events:
            logger.warning(f"Using cached economic news feed ({len(events)} events, {age_hours:.1f}h old)")
        return events if isinstance(events, list) else []
    except Exception as exc:
        if logger is not None:
            logger.warning(f"Failed to load news cache {cache_path}: {exc}")
        return []


def save_cached_news(cache_path: str, source_url: str, news_events: list[dict], logger=None) -> None:
    if not cache_path:
        return
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "source_url": source_url,
        "news_events": news_events,
    }
    try:
        with open(cache_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        if logger is not None:
            logger.info(f"Saved economic news cache to {cache_path}")
    except Exception as exc:
        if logger is not None:
            logger.warning(f"Failed to write news cache {cache_path}: {exc}")