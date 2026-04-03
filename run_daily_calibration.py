from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from backtest.config import BacktestConfig
from backtest.run_backtest import run_backtest
from fxbot.config import env_int


def build_rolling_window(now: datetime, rolling_days: int, end_offset_days: int = 0) -> tuple[datetime, datetime]:
    current = now.astimezone(timezone.utc)
    end = current.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=end_offset_days)
    start = end - timedelta(days=rolling_days)
    return start, end


def main() -> None:
    config = BacktestConfig.from_env()
    rolling_days = max(1, env_int("BACKTEST_ROLLING_DAYS", 180))
    end_offset_days = max(0, env_int("BACKTEST_ROLLING_END_OFFSET_DAYS", 0))
    config.start, config.end = build_rolling_window(datetime.now(timezone.utc), rolling_days, end_offset_days)
    report = run_backtest(config)
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