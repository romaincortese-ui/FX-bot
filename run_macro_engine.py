"""Daily runner for macro_engine.py.

This script executes the macro engine immediately, then sleeps until the next UTC midnight
and runs it again. Use it inside a long-running process or service on your server.
"""

import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import logging

LOG_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt=LOG_FORMAT,
)
log = logging.getLogger(__name__)

MACRO_ENGINE_SCRIPT = "macro_engine.py"


def run_macro_engine() -> int:
    cmd = [sys.executable, MACRO_ENGINE_SCRIPT]
    log.info(f"Running macro engine: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    log.info(f"Macro engine exit code: {result.returncode}")
    return result.returncode


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
            log.info(f"Sleeping until next UTC midnight ({delay/3600:.2f}h)")
            time.sleep(delay)
            run_macro_engine()
    except KeyboardInterrupt:
        log.info("Macro engine runner stopped by user.")


if __name__ == "__main__":
    main()
