"""Daily runner for macro_engine.py.

This script executes the macro engine immediately, then sleeps until the next UTC midnight
and runs it again. Use it inside a long-running process or service on your server.
"""

import time
from datetime import datetime, timedelta, timezone
import os
import json

import logging
import redis
import macro_engine

from fxbot.runtime_status import build_runtime_status, publish_runtime_status

LOG_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt=LOG_FORMAT,
)
log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_MACRO_STATE_KEY = os.getenv("REDIS_MACRO_STATE_KEY", "macro_state")
REDIS_MACRO_STATUS_KEY = os.getenv("REDIS_MACRO_STATUS_KEY", "macro_runtime_status")
MACRO_STATUS_TTL = int(os.getenv("MACRO_STATUS_TTL", "172800"))


def publish_macro_runtime_state(client, state: str, **fields) -> bool:
    payload = build_runtime_status("macro", state, pid=os.getpid(), **fields)
    return publish_runtime_status(client, REDIS_MACRO_STATUS_KEY, payload, MACRO_STATUS_TTL)


def run_macro_engine() -> int:
    log.info(f"Macro engine runner working directory: {os.getcwd()}")
    if not REDIS_URL:
        log.error("REDIS_URL is not configured. Cannot save macro state to Redis.")
        return 1

    try:
        r = redis.from_url(REDIS_URL)
    except Exception as e:
        log.error(f"Failed to connect to Redis: {e}")
        return 1

    publish_macro_runtime_state(r, "running")

    try:
        filters = macro_engine.generate_macro_filters()
        news = []
        if os.getenv("DISABLE_MACRO_NEWS", "False").strip().lower() not in {"1", "true", "yes", "y"}:
            news = macro_engine.load_forex_factory_news()
        else:
            log.warning("Macro news loading disabled by DISABLE_MACRO_NEWS.")
        macro_state = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filters": filters,
            "news_events": news,
        }
        r.set(REDIS_MACRO_STATE_KEY, json.dumps(macro_state))
        publish_macro_runtime_state(
            r,
            "idle",
            macro_state_key=REDIS_MACRO_STATE_KEY,
            filter_count=len(filters),
            news_events=len(news),
        )
        log.info(f"Saved macro state to Redis key {REDIS_MACRO_STATE_KEY}")
        return 0
    except Exception as e:
        publish_macro_runtime_state(r, "error", error=str(e)[:200])
        log.error(f"Failed to generate or save macro state: {e}")
        return 1


def seconds_until_next_midnight_utc() -> float:
    now = datetime.now(timezone.utc)
    tomorrow = now.date() + timedelta(days=1)
    next_midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=timezone.utc)
    return max(0.0, (next_midnight - now).total_seconds())


def main() -> None:
    try:
        run_macro_engine()
        while True:
            delay = seconds_until_next_midnight_utc()
            if REDIS_URL:
                try:
                    client = redis.from_url(REDIS_URL)
                    publish_macro_runtime_state(client, "sleeping", next_run_in_seconds=round(delay, 1))
                except Exception:
                    pass
            log.info(f"Sleeping until next UTC midnight ({delay/3600:.2f}h)")
            time.sleep(delay)
            run_macro_engine()
    except KeyboardInterrupt:
        log.info("Macro engine runner stopped by user.")


if __name__ == "__main__":
    main()
