from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import redis

from backtest.config import BacktestConfig
from backtest.run_backtest import run_backtest
from backtest.run_backtest import validate_positive_pnl
from fxbot.config import env_int
from fxbot.runtime_status import build_runtime_status, publish_runtime_status


REDIS_CALIBRATION_STATUS_KEY = os.getenv("REDIS_CALIBRATION_STATUS_KEY", "calibration_runtime_status")
CALIBRATION_STATUS_TTL = int(os.getenv("CALIBRATION_STATUS_TTL", "172800"))


def _get_redis_client() -> redis.Redis | None:
    """Try REDIS_URL first, fall back to REDIS_PUBLIC_URL (TCP proxy)."""
    import logging
    import time
    log = logging.getLogger(__name__)
    urls_to_try = []
    for var in ("REDIS_URL", "REDIS_PUBLIC_URL"):
        url = os.getenv(var, "").strip()
        if url and url not in urls_to_try:
            urls_to_try.append(url)
    if not urls_to_try:
        return None
    for url in urls_to_try:
        for attempt in range(1, 4):
            try:
                client = redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)
                client.ping()
                host = url.split("@")[-1] if "@" in url else url.split("//")[-1]
                log.info("Redis connected via %s", host)
                return client
            except Exception as exc:
                host = url.split("@")[-1] if "@" in url else url.split("//")[-1]
                log.warning("Redis connection attempt %d/3 failed (%s): %s", attempt, host, exc)
                if attempt < 3:
                    time.sleep(2.0 * attempt)
    return None


def publish_calibration_runtime_state(state: str, **fields) -> bool:
    client = _get_redis_client()
    if client is None:
        return False
    payload = build_runtime_status("calibration", state, pid=os.getpid(), **fields)
    return publish_runtime_status(client, REDIS_CALIBRATION_STATUS_KEY, payload, CALIBRATION_STATUS_TTL)


def build_rolling_window(now: datetime, rolling_days: int, end_offset_days: int = 0) -> tuple[datetime, datetime]:
    current = now.astimezone(timezone.utc)
    end = current.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=end_offset_days)
    start = end - timedelta(days=rolling_days)
    return start, end


def _warmup_redis() -> None:
    """Trigger early Redis connection to let Wireguard tunnel establish."""
    _get_redis_client()


def main() -> None:
    _warmup_redis()
    publish_calibration_runtime_state("running")
    config = BacktestConfig.from_env()
    rolling_days = max(1, env_int("BACKTEST_ROLLING_DAYS", 180))
    end_offset_days = max(0, env_int("BACKTEST_ROLLING_END_OFFSET_DAYS", 0))
    config.start, config.end = build_rolling_window(datetime.now(timezone.utc), rolling_days, end_offset_days)
    report = run_backtest(config)
    validation_passed, validation_reason = validate_positive_pnl(report)
    calibration_state = "completed" if validation_passed else "failed"
    publish_calibration_runtime_state(
        calibration_state,
        rolling_days=rolling_days,
        end_offset_days=end_offset_days,
        start=config.start.isoformat(),
        end=config.end.isoformat(),
        total_trades=int(report.get("total_trades", 0) or 0),
        validation_reason=validation_reason,
    )
    print(
        json.dumps(
            {
                "rolling_days": rolling_days,
                "end_offset_days": end_offset_days,
                "start": config.start.isoformat(),
                "end": config.end.isoformat(),
                "report": report,
                "validation": {"passed": validation_passed, "reason": validation_reason},
            },
            indent=2,
        )
    )
    if not validation_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()