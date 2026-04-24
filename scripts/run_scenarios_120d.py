"""Sequential multi-scenario runner used by the 120-day assessment task.

Each scenario mutates environment variables (overlay flags), then invokes
`scripts.run_overlay_backtest.main` so the run emits both `summary.json`
and an enriched `overlay_block_counts` section. Scenarios write to
separate output directories so the consultant doc can tabulate them.

Run:

    ..\\.venv\\Scripts\\python.exe scripts\\run_scenarios_120d.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_overlay_backtest import main as run_once  # noqa: E402

WINDOW_START = "2025-12-01T00:00:00+00:00"
WINDOW_END = "2026-03-31T00:00:00+00:00"
INSTRUMENTS = "EUR_USD,GBP_USD,USD_JPY"

SCENARIOS = [
    # (name, env overrides)
    (
        "baseline_pre_overlay",
        {
            "BACKTEST_TIER1_NET_RR_ENABLED": "false",
            "BACKTEST_TIER2_REGIME_VETO_ENABLED": "false",
            "BACKTEST_TIER2_KILL_SWITCH_ENABLED": "false",
            "BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED": "false",
            "BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED": "false",
            "BACKTEST_TIER3_NEWS_IMPACT_ENABLED": "false",
            "BACKTEST_TIER3_FLOW_ENABLED": "false",
            "BACKTEST_TIER3_SEASONALITY_ENABLED": "false",
        },
    ),
    (
        "all_overlays_default",
        {
            "BACKTEST_TIER1_NET_RR_ENABLED": "true",
            "BACKTEST_TIER2_REGIME_VETO_ENABLED": "true",
            "BACKTEST_TIER2_KILL_SWITCH_ENABLED": "true",
            "BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED": "true",
            "BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED": "true",
            "BACKTEST_TIER3_NEWS_IMPACT_ENABLED": "true",
            "BACKTEST_TIER3_FLOW_ENABLED": "true",
            "BACKTEST_TIER3_SEASONALITY_ENABLED": "true",
        },
    ),
    (
        "overlays_regime_off",
        {
            "BACKTEST_TIER1_NET_RR_ENABLED": "true",
            "BACKTEST_TIER2_REGIME_VETO_ENABLED": "false",  # classifier degenerate on cached data
            "BACKTEST_TIER2_KILL_SWITCH_ENABLED": "true",
            "BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED": "true",
            "BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED": "true",
            "BACKTEST_TIER3_NEWS_IMPACT_ENABLED": "true",
            "BACKTEST_TIER3_FLOW_ENABLED": "true",
            "BACKTEST_TIER3_SEASONALITY_ENABLED": "true",
        },
    ),
    (
        "overlays_regime_off_rr12",
        {
            "BACKTEST_TIER1_NET_RR_ENABLED": "true",
            "BACKTEST_TIER1_NET_RR_MIN": "1.2",
            "BACKTEST_TIER2_REGIME_VETO_ENABLED": "false",
            "BACKTEST_TIER2_KILL_SWITCH_ENABLED": "true",
            "BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED": "true",
            "BACKTEST_TIER2_PORTFOLIO_CAP_PCT": "0.20",
            "BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED": "true",
            "BACKTEST_TIER3_NEWS_IMPACT_ENABLED": "true",
            "BACKTEST_TIER3_FLOW_ENABLED": "true",
            "BACKTEST_TIER3_SEASONALITY_ENABLED": "true",
        },
    ),
]


def _run_scenario(name: str, env_overrides: dict[str, str]) -> None:
    out_dir = f"backtest_output_120d_{name}"
    print(f"\n############ scenario: {name} -> {out_dir} ############")
    saved = {k: os.environ.get(k) for k in env_overrides}
    try:
        for k, v in env_overrides.items():
            os.environ[k] = v
        sys.argv = [
            "run_overlay_backtest",
            "--start", WINDOW_START,
            "--end", WINDOW_END,
            "--instruments", INSTRUMENTS,
            "--output", out_dir,
            "--skip-macro",
        ]
        run_once()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def main() -> int:
    for name, overrides in SCENARIOS:
        _run_scenario(name, overrides)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
