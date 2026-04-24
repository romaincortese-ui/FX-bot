"""Run a 120-day overlay-aware backtest and dump summary + overlay counters.

Tier 1v2 §V2 validation harness: exercises `backtest.engine.BacktestEngine`
with all eleven Tier 1–5 overlays wired and prints a per-strategy,
per-month, and overlay-block breakdown so the caller can turn the result
into a reusable assessment document.

Usage (from FX-bot/):

    ..\\.venv\\Scripts\\python.exe -m scripts.run_overlay_backtest \\
        --start 2025-12-01T00:00:00+00:00 --end 2026-03-31T00:00:00+00:00 \\
        --instruments EUR_USD,GBP_USD,USD_JPY \\
        --output backtest_output_tier2v2_120d
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure FX-bot root is on sys.path when invoked as `python scripts/...`.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.config import BacktestConfig  # noqa: E402
from backtest.data import HistoricalDataProvider  # noqa: E402
from backtest.engine import BacktestEngine  # noqa: E402
from backtest.macro_sim import MacroReplay, generate_daily_macro_snapshots  # noqa: E402
from backtest.reporter import (  # noqa: E402
    build_backtest_report,
    build_trade_calibration,
    export_backtest_artifacts,
)
from fxbot.config import env_str  # noqa: E402


def _parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _aggregate_by_month(trades: list[dict]) -> dict[str, dict]:
    buckets: dict[str, dict] = {}
    for t in trades:
        closed = t.get("closed_at") or t.get("exit_time") or t.get("exit_ts")
        if closed is None:
            continue
        if isinstance(closed, str):
            try:
                closed_dt = _parse_dt(closed)
            except ValueError:
                continue
        elif isinstance(closed, datetime):
            closed_dt = closed.astimezone(timezone.utc)
        else:
            continue
        key = closed_dt.strftime("%Y-%m")
        b = buckets.setdefault(key, {"trades": 0, "wins": 0, "pnl": 0.0})
        b["trades"] += 1
        pnl = float(t.get("pnl", 0.0) or 0.0)
        b["pnl"] += pnl
        if pnl > 0:
            b["wins"] += 1
    for v in buckets.values():
        v["win_rate"] = round(v["wins"] / v["trades"], 4) if v["trades"] else 0.0
        v["pnl"] = round(v["pnl"], 2)
    return dict(sorted(buckets.items()))


def _dump_trades_csv(trades: list[dict], path: Path) -> None:
    if not trades:
        path.write_text("", encoding="utf-8")
        return
    keys = sorted({k for t in trades for k in t.keys()})
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        for t in trades:
            writer.writerow({k: t.get(k, "") for k in keys})


def main() -> int:
    parser = argparse.ArgumentParser(description="Overlay-aware 120-day backtest")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--instruments", default="EUR_USD,GBP_USD,USD_JPY")
    parser.add_argument("--output", default="backtest_output_tier2v2_120d")
    parser.add_argument("--granularity", default="M5")
    parser.add_argument("--skip-macro", action="store_true",
                        help="Re-use existing macro snapshots on disk (default for repeat runs).")
    args = parser.parse_args()

    config = BacktestConfig.from_env()
    config.start = _parse_dt(args.start)
    config.end = _parse_dt(args.end)
    config.instruments = [s.strip().upper() for s in args.instruments.split(",") if s.strip()]
    config.output_dir = args.output
    config.granularity = args.granularity.upper()
    if args.skip_macro:
        config.generate_macro_states = False

    print(f"[overlay-backtest] window={config.start.isoformat()} .. {config.end.isoformat()}")
    print(f"[overlay-backtest] instruments={config.instruments}")
    print(f"[overlay-backtest] output_dir={config.output_dir}")

    provider = HistoricalDataProvider(
        oanda_api_key=env_str("OANDA_API_KEY", ""),
        oanda_api_url=env_str("OANDA_API_URL", "https://api-fxpractice.oanda.com"),
        cache_dir=config.cache_dir,
    )

    if config.generate_macro_states:
        generate_daily_macro_snapshots(
            config.start, config.end, config.macro_state_dir,
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

    out_dir = Path(config.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    overlay_counts = dict(getattr(engine, "overlay_block_counts", {}))
    by_month = _aggregate_by_month(trades)

    # Read the summary emitted by the reporter and enrich it with overlay
    # telemetry + monthly rollup so the consultant doc has everything.
    summary_path = out_dir / "summary.json"
    summary: dict = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else dict(report)
    summary["overlay_block_counts"] = overlay_counts
    summary["by_month"] = by_month
    summary["config"] = {
        "start": config.start.isoformat(),
        "end": config.end.isoformat(),
        "instruments": config.instruments,
        "granularity": config.granularity,
        "initial_balance": config.initial_balance,
        "max_open_trades": config.max_open_trades,
        "max_risk_per_trade": config.max_risk_per_trade,
        "leverage": config.leverage,
        "overlay_flags": {
            "TIER1_NET_RR": os.getenv("BACKTEST_TIER1_NET_RR_ENABLED", "true"),
            "TIER1_NET_RR_MIN": os.getenv("BACKTEST_TIER1_NET_RR_MIN", "1.8"),
            "TIER2_REGIME_VETO": os.getenv("BACKTEST_TIER2_REGIME_VETO_ENABLED", "true"),
            "TIER2_KILL_SWITCH": os.getenv("BACKTEST_TIER2_KILL_SWITCH_ENABLED", "true"),
            "TIER2_PORTFOLIO_CAP": os.getenv("BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED", "true"),
            "TIER2_PORTFOLIO_CAP_PCT": os.getenv("BACKTEST_TIER2_PORTFOLIO_CAP_PCT", "0.08"),
            "TIER2_PERCENTILE_SIZING": os.getenv("BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED", "true"),
            "TIER3_NEWS_IMPACT": os.getenv("BACKTEST_TIER3_NEWS_IMPACT_ENABLED", "true"),
            "TIER3_FLOW": os.getenv("BACKTEST_TIER3_FLOW_ENABLED", "true"),
            "TIER3_SEASONALITY": os.getenv("BACKTEST_TIER3_SEASONALITY_ENABLED", "true"),
            "TIER5_DECISION_DAY": os.getenv("BACKTEST_TIER5_DECISION_DAY_ENABLED", "false"),
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    # Dump raw trades as CSV for downstream analysis.
    _dump_trades_csv(trades, out_dir / "trades_enriched.csv")

    print("\n==== overlay-aware backtest summary ====")
    print(f"total_trades   : {summary.get('total_trades', 0)}")
    print(f"win_rate       : {summary.get('win_rate', 0.0):.3f}")
    print(f"profit_factor  : {summary.get('profit_factor', 0.0):.3f}")
    print(f"total_pnl      : {summary.get('total_pnl', 0.0):.2f}")
    print(f"max_drawdown   : {summary.get('max_drawdown', 0.0):.3f}")
    print(f"expectancy     : {summary.get('expectancy', 0.0):.2f}")
    print(f"overlay_blocks : {overlay_counts}")
    print(f"months         : {list(by_month.keys())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
