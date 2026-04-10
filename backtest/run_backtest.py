from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from backtest.config import BacktestConfig
from backtest.data import HistoricalDataProvider
from backtest.engine import BacktestEngine
from backtest.macro_sim import generate_daily_macro_snapshots
from backtest.macro_sim import MacroReplay
from backtest.reporter import build_backtest_report, build_trade_calibration, export_backtest_artifacts, publish_trade_calibration
from fxbot.config import env_str


def _parse_cli_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def run_backtest(config: BacktestConfig) -> dict[str, Any]:
    provider = HistoricalDataProvider(
        oanda_api_key=env_str("OANDA_API_KEY", ""),
        oanda_api_url=env_str("OANDA_API_URL", "https://api-fxpractice.oanda.com"),
        cache_dir=config.cache_dir,
    )
    if config.generate_macro_states:
        generate_daily_macro_snapshots(
            config.start,
            config.end,
            config.macro_state_dir,
            rates_file=config.macro_rates_file,
            momentum_file=config.macro_momentum_file,
            esi_file=config.macro_esi_file,
            liquidity_file=config.macro_liquidity_file,
            news_file=config.macro_news_file,
            dxy_history_file=config.dxy_history_file,
            vix_history_file=config.vix_history_file,
        )
    macro_replay = MacroReplay.from_directory(config.macro_state_dir, config.start, config.end)
    engine = BacktestEngine(config, provider, macro_replay)
    equity_curve, trades = engine.run()
    report = build_backtest_report(equity_curve, trades)
    calibration = build_trade_calibration(trades)
    export_backtest_artifacts(config.output_dir, equity_curve, trades, report)
    publish_trade_calibration(
        env_str("REDIS_URL", "").strip(),
        env_str("REDIS_TRADE_CALIBRATION_KEY", "trade_calibration").strip(),
        calibration,
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the integrated FX bot backtester")
    parser.add_argument("--start", help="UTC ISO start datetime")
    parser.add_argument("--end", help="UTC ISO end datetime")
    parser.add_argument("--instruments", help="Comma-separated instrument list")
    parser.add_argument("--granularity", help="Base granularity such as M5 or M15")
    args = parser.parse_args()

    config = BacktestConfig.from_env()
    if args.start:
        config.start = _parse_cli_datetime(args.start)
    if args.end:
        config.end = _parse_cli_datetime(args.end)
    if args.instruments:
        config.instruments = [item.strip().upper().replace("/", "_") for item in args.instruments.split(",") if item.strip()]
    if args.granularity:
        config.granularity = args.granularity.upper()

    report = run_backtest(config)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()