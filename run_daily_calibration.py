from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import redis

from backtest.config import BacktestConfig
from backtest.run_backtest import run_backtest
from fxbot.config import env_int
from fxbot.runtime_status import build_runtime_status, publish_runtime_status


REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_CALIBRATION_STATUS_KEY = os.getenv("REDIS_CALIBRATION_STATUS_KEY", "calibration_runtime_status")
CALIBRATION_STATUS_TTL = int(os.getenv("CALIBRATION_STATUS_TTL", "172800"))


def publish_calibration_runtime_state(state: str, **fields) -> bool:
    if not REDIS_URL:
        return False
    client = redis.from_url(REDIS_URL)
    payload = build_runtime_status("calibration", state, pid=os.getpid(), **fields)
    return publish_runtime_status(client, REDIS_CALIBRATION_STATUS_KEY, payload, CALIBRATION_STATUS_TTL)


def build_rolling_window(now: datetime, rolling_days: int, end_offset_days: int = 0) -> tuple[datetime, datetime]:
    current = now.astimezone(timezone.utc)
    end = current.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=end_offset_days)
    start = end - timedelta(days=rolling_days)
    return start, end


def main() -> None:
    publish_calibration_runtime_state("running")
    config = BacktestConfig.from_env()
    rolling_days = max(1, env_int("BACKTEST_ROLLING_DAYS", 180))
    end_offset_days = max(0, env_int("BACKTEST_ROLLING_END_OFFSET_DAYS", 0))
    config.start, config.end = build_rolling_window(datetime.now(timezone.utc), rolling_days, end_offset_days)
    report = run_backtest(config)
    publish_calibration_runtime_state(
        "completed",
        rolling_days=rolling_days,
        end_offset_days=end_offset_days,
        start=config.start.isoformat(),
        end=config.end.isoformat(),
        total_trades=int(report.get("total_trades", 0) or 0),
    )
    print(
        json.dumps(
            {
                "rolling_days": rolling_days,
                "end_offset_days": end_offset_days,
                "start": config.start.isoformat(),
                "end": config.end.isoformat(),
                "report": report,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()