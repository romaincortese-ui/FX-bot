"""Event-intelligence worker for unscheduled FX news.

This process is intentionally separate from the trading loop. It fetches
auditable RSS/official feeds, scores unusual currency-specific event chatter,
and publishes a compact Redis state consumed by main.py.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
import redis

from fxbot.event_intelligence import build_event_intelligence_state, parse_feed_config, parse_feed_items
from fxbot.runtime_status import build_runtime_status, publish_runtime_status


LOG_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt=LOG_FORMAT,
)
log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "")
EVENT_INTEL_STATE_KEY = os.getenv("EVENT_INTEL_STATE_KEY", "fxbot:event_intelligence")
EVENT_INTEL_STATUS_KEY = os.getenv("EVENT_INTEL_STATUS_KEY", "event_intelligence_runtime_status")
EVENT_INTEL_STATUS_TTL = int(os.getenv("EVENT_INTEL_STATUS_TTL", "600"))
EVENT_INTEL_INTERVAL_SECS = int(os.getenv("EVENT_INTEL_INTERVAL_SECS", "60"))
EVENT_INTEL_LOOKBACK_MINS = int(os.getenv("EVENT_INTEL_LOOKBACK_MINS", "90"))
EVENT_INTEL_TTL_MINS = int(os.getenv("EVENT_INTEL_TTL_MINS", "180"))
EVENT_INTEL_MIN_SCORE = float(os.getenv("EVENT_INTEL_WORKER_MIN_SCORE", "0.35"))
EVENT_INTEL_FEEDS = os.getenv("EVENT_INTEL_RSS_FEEDS", "")
EVENT_INTEL_HTTP_TIMEOUT = float(os.getenv("EVENT_INTEL_HTTP_TIMEOUT", "12"))
EVENT_INTEL_USER_AGENT = os.getenv(
    "EVENT_INTEL_USER_AGENT",
    "FXBotEventIntelligence/1.0 (+https://github.com/romaincortese-ui/FX-bot)",
)


def publish_event_runtime_state(client, state: str, **fields) -> bool:
    payload = build_runtime_status("event_intelligence", state, pid=os.getpid(), **fields)
    return publish_runtime_status(client, EVENT_INTEL_STATUS_KEY, payload, EVENT_INTEL_STATUS_TTL)


def _fetch_feed(session: requests.Session, feed: dict[str, str], now: datetime):
    url = feed["url"]
    response = session.get(url, timeout=EVENT_INTEL_HTTP_TIMEOUT)
    response.raise_for_status()
    return parse_feed_items(
        response.content,
        source_url=url,
        source_name=feed.get("name") or url,
        source_tier=feed.get("tier") or "rss",
        now=now,
    )


def run_once(client) -> int:
    now = datetime.now(timezone.utc)
    feeds = parse_feed_config(EVENT_INTEL_FEEDS)
    headers = {"User-Agent": EVENT_INTEL_USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml, */*"}
    items = []
    failures = []
    previous_state = None
    try:
        raw_previous = client.get(EVENT_INTEL_STATE_KEY)
        if raw_previous:
            if isinstance(raw_previous, bytes):
                raw_previous = raw_previous.decode("utf-8")
            previous_state = json.loads(raw_previous)
    except Exception as exc:
        log.debug(f"Could not load previous event-intelligence state: {exc}")

    with requests.Session() as session:
        session.headers.update(headers)
        for feed in feeds:
            try:
                parsed = _fetch_feed(session, feed, now)
                items.extend(parsed)
                log.info("Loaded %d event feed items from %s", len(parsed), feed.get("name") or feed["url"])
            except Exception as exc:
                failures.append({"feed": feed.get("name") or feed.get("url"), "error": str(exc)[:160]})
                log.warning("Event feed failed: %s (%s)", feed.get("name") or feed.get("url"), exc)

    state = build_event_intelligence_state(
        items,
        previous_state=previous_state,
        now=now,
        lookback_minutes=EVENT_INTEL_LOOKBACK_MINS,
        ttl_minutes=EVENT_INTEL_TTL_MINS,
        min_score=EVENT_INTEL_MIN_SCORE,
    )
    state["feed_count"] = len(feeds)
    state["item_count"] = len(items)
    state["failures"] = failures[:8]
    client.set(EVENT_INTEL_STATE_KEY, json.dumps(state))
    publish_event_runtime_state(
        client,
        "idle",
        event_state_key=EVENT_INTEL_STATE_KEY,
        feed_count=len(feeds),
        item_count=len(items),
        currency_count=len(state.get("currencies", {})),
        failure_count=len(failures),
    )
    log.info(
        "Published event-intelligence state to %s: %d currencies, %d items, %d feed failures",
        EVENT_INTEL_STATE_KEY,
        len(state.get("currencies", {})),
        len(items),
        len(failures),
    )
    return 0


def main() -> None:
    if not REDIS_URL:
        raise SystemExit("REDIS_URL is not configured. Cannot publish event-intelligence state.")
    client = redis.from_url(REDIS_URL)
    publish_event_runtime_state(client, "running")
    while True:
        try:
            run_once(client)
        except KeyboardInterrupt:
            publish_event_runtime_state(client, "stopped")
            log.info("Event-intelligence worker stopped by user.")
            return
        except Exception as exc:
            publish_event_runtime_state(client, "error", error=str(exc)[:200])
            log.error("Event-intelligence cycle failed: %s", exc, exc_info=True)
        time.sleep(max(15, EVENT_INTEL_INTERVAL_SECS))


if __name__ == "__main__":
    main()