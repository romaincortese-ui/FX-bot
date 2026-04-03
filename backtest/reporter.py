from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def build_backtest_report(equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    equity_df = pd.DataFrame(equity_curve)
    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_pnl": 0.0,
            "max_drawdown": 0.0,
            "by_strategy": {},
        }
    pnl = trades_df["pnl"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    win_rate = float((pnl > 0).mean())
    profit_factor = float(wins.sum() / abs(losses.sum())) if not losses.empty else 999.0
    total_pnl = float(pnl.sum())
    expectancy = float(pnl.mean())
    by_strategy = {}
    for label, group in trades_df.groupby("label"):
        gpnl = group["pnl"].astype(float)
        gwins = gpnl[gpnl > 0]
        glosses = gpnl[gpnl < 0]
        by_strategy[label] = {
            "trades": int(len(group)),
            "win_rate": float((gpnl > 0).mean()),
            "pnl": float(gpnl.sum()),
            "profit_factor": float(gwins.sum() / abs(glosses.sum())) if not glosses.empty else 999.0,
        }
    max_drawdown = 0.0
    if not equity_df.empty:
        curve = equity_df["equity"].astype(float)
        running_max = curve.cummax()
        drawdown = (curve - running_max) / running_max.replace(0, 1)
        max_drawdown = float(drawdown.min())
    return {
        "total_trades": int(len(trades_df)),
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "total_pnl": total_pnl,
        "expectancy": expectancy,
        "max_drawdown": max_drawdown,
        "by_strategy": by_strategy,
    }


def export_backtest_artifacts(output_dir: str, equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]], report: dict[str, Any]) -> None:
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(equity_curve).to_csv(base / "equity_curve.csv", index=False)
    pd.DataFrame(trades).to_csv(base / "trade_journal.csv", index=False)
    (base / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
