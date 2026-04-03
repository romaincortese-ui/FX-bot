from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import redis
except ImportError:
    redis = None  # type: ignore


def _profit_factor(pnl: pd.Series) -> float:
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    return float(wins.sum() / abs(losses.sum())) if not losses.empty else 999.0


def _summarize_trade_group(group: pd.DataFrame) -> dict[str, Any]:
    pnl = group["pnl"].astype(float) if "pnl" in group.columns else pd.Series(dtype=float)
    pnl_pips = group["pnl_pips"].astype(float) if "pnl_pips" in group.columns else pd.Series(dtype=float)
    expectancy_pips = float(pnl_pips.mean()) if not pnl_pips.empty else None
    return {
        "trades": int(len(group)),
        "win_rate": float((pnl > 0).mean()) if not pnl.empty else 0.0,
        "pnl": float(pnl.sum()) if not pnl.empty else 0.0,
        "total_pnl": float(pnl.sum()) if not pnl.empty else 0.0,
        "profit_factor": _profit_factor(pnl) if not pnl.empty else 0.0,
        "expectancy": float(pnl.mean()) if not pnl.empty else 0.0,
        "expectancy_pips": expectancy_pips,
    }


def _group_trade_metrics(trades_df: pd.DataFrame, keys: list[str]) -> dict[str, Any]:
    grouped: dict[str, Any] = {}
    if trades_df.empty:
        return grouped
    for raw_keys, group in trades_df.groupby(keys):
        if not isinstance(raw_keys, tuple):
            raw_keys = (raw_keys,)
        node = grouped
        for key in raw_keys[:-1]:
            node = node.setdefault(str(key), {})
        node[str(raw_keys[-1])] = _summarize_trade_group(group)
    return grouped


def build_backtest_report(equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    equity_df = pd.DataFrame(equity_curve)
    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "total_pnl": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "by_strategy": {},
        }
    summary = _summarize_trade_group(trades_df)
    by_strategy = _group_trade_metrics(trades_df, ["label"])
    max_drawdown = 0.0
    if not equity_df.empty:
        curve = equity_df["equity"].astype(float)
        running_max = curve.cummax()
        drawdown = (curve - running_max) / running_max.replace(0, 1)
        max_drawdown = float(drawdown.min())
    return {
        "total_trades": summary["trades"],
        "win_rate": summary["win_rate"],
        "profit_factor": summary["profit_factor"],
        "total_pnl": summary["total_pnl"],
        "expectancy": summary["expectancy"],
        "max_drawdown": max_drawdown,
        "by_strategy": by_strategy,
    }


def build_trade_calibration(trades: list[dict[str, Any]]) -> dict[str, Any]:
    trades_df = pd.DataFrame(trades)
    if trades_df.empty:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_trades": 0,
            "by_strategy": {},
            "by_strategy_pair": {},
            "by_strategy_pair_session": {},
        }
    normalized = trades_df.copy()
    for column in ("label", "instrument", "session_at_entry"):
        if column not in normalized.columns:
            normalized[column] = "UNKNOWN"
        normalized[column] = normalized[column].fillna("UNKNOWN").astype(str)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_trades": int(len(normalized)),
        "by_strategy": _group_trade_metrics(normalized, ["label"]),
        "by_strategy_pair": _group_trade_metrics(normalized, ["label", "instrument"]),
        "by_strategy_pair_session": _group_trade_metrics(normalized, ["label", "instrument", "session_at_entry"]),
    }


def publish_trade_calibration(redis_url: str, redis_key: str, calibration: dict[str, Any]) -> bool:
    if not redis_url or not redis_key or redis is None:
        return False
    client = redis.from_url(redis_url)
    client.set(redis_key, json.dumps(calibration))
    return True


def export_backtest_artifacts(output_dir: str, equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]], report: dict[str, Any]) -> None:
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(equity_curve).to_csv(base / "equity_curve.csv", index=False)
    pd.DataFrame(trades).to_csv(base / "trade_journal.csv", index=False)
    (base / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    calibration = build_trade_calibration(trades)
    (base / "calibration.json").write_text(json.dumps(calibration, indent=2), encoding="utf-8")
