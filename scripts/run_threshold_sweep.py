"""Memo 4 §8 F3 — threshold sweep on the overlay-aware 120-day backtest.

Goal: identify a cell of (NET_RR_MIN × PORTFOLIO_CAP_PCT × regime_veto)
that produces PF ≥ 1.2 on *default* overlay flags — i.e. without the
hand-picked relaxations used to unlock Scenario D in the memo 4 run.

Writes a single ``backtest_sweeps/threshold_sweep_summary.json`` plus
one subdirectory per cell with the standard overlay-aware ``summary.json``.

Usage:

    ..\\.venv\\Scripts\\python.exe scripts\\run_threshold_sweep.py
"""
from __future__ import annotations

import json
import os
import sys
from itertools import product
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_overlay_backtest import main as run_once  # noqa: E402

WINDOW_START = "2025-12-01T00:00:00+00:00"
WINDOW_END = "2026-03-31T00:00:00+00:00"
INSTRUMENTS = "EUR_USD,GBP_USD,USD_JPY"

NET_RR_GRID = [1.0, 1.1, 1.2, 1.3, 1.4, 1.6, 1.8]
CAP_PCT_GRID = [0.08, 0.12, 0.16, 0.20]
REGIME_GRID = ["true", "false"]  # veto ON / OFF

SWEEP_ROOT = Path("backtest_sweeps")


def _run_cell(net_rr: float, cap_pct: float, regime_on: str) -> dict:
    tag = f"rr{net_rr:.2f}_cap{int(cap_pct*100):02d}_reg{'on' if regime_on=='true' else 'off'}"
    out_dir = SWEEP_ROOT / tag
    out_dir.mkdir(parents=True, exist_ok=True)

    overrides = {
        "BACKTEST_TIER1_NET_RR_ENABLED": "true",
        "BACKTEST_TIER1_NET_RR_MIN": f"{net_rr}",
        "BACKTEST_TIER2_REGIME_VETO_ENABLED": regime_on,
        "BACKTEST_TIER2_KILL_SWITCH_ENABLED": "true",
        "BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED": "true",
        "BACKTEST_TIER2_PORTFOLIO_CAP_PCT": f"{cap_pct}",
        "BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED": "true",
        "BACKTEST_TIER3_NEWS_IMPACT_ENABLED": "true",
        "BACKTEST_TIER3_FLOW_ENABLED": "true",
        "BACKTEST_TIER3_SEASONALITY_ENABLED": "true",
    }
    saved = {k: os.environ.get(k) for k in overrides}
    try:
        for k, v in overrides.items():
            os.environ[k] = v
        sys.argv = [
            "run_overlay_backtest",
            "--start", WINDOW_START,
            "--end", WINDOW_END,
            "--instruments", INSTRUMENTS,
            "--output", str(out_dir),
            "--skip-macro",
        ]
        run_once()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    summary_path = out_dir / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    else:
        summary = {}
    return {
        "tag": tag,
        "net_rr_min": net_rr,
        "cap_pct": cap_pct,
        "regime_veto": regime_on == "true",
        "trades": summary.get("total_trades", 0),
        "win_rate": summary.get("win_rate"),
        "profit_factor": summary.get("profit_factor"),
        "total_pnl": summary.get("total_pnl"),
        "max_drawdown_pct": summary.get("max_drawdown_pct"),
        "output_dir": str(out_dir),
    }


def main() -> int:
    SWEEP_ROOT.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    total = len(NET_RR_GRID) * len(CAP_PCT_GRID) * len(REGIME_GRID)
    i = 0
    for net_rr, cap_pct, regime_on in product(NET_RR_GRID, CAP_PCT_GRID, REGIME_GRID):
        i += 1
        print(f"\n############ sweep {i}/{total} — rr={net_rr} cap={cap_pct} regime={regime_on} ############")
        try:
            results.append(_run_cell(net_rr, cap_pct, regime_on))
        except Exception as exc:  # pragma: no cover — defensive for long runs
            print(f"[warn] cell failed: {exc}")
            results.append({
                "tag": f"rr{net_rr:.2f}_cap{int(cap_pct*100):02d}_reg{'on' if regime_on=='true' else 'off'}",
                "net_rr_min": net_rr,
                "cap_pct": cap_pct,
                "regime_veto": regime_on == "true",
                "error": str(exc),
            })

    # Rank cells by PF among those that (a) meet the min-trade guard and
    # (b) contained the draw-down. This matches the memo 4 Gate-B target:
    # PF ≥ 1.2 AND DD ≤ 20 % AND N ≥ 25.
    def _score(r: dict) -> tuple[int, float]:
        trades = int(r.get("trades") or 0)
        pf = float(r.get("profit_factor") or 0.0)
        dd = float(r.get("max_drawdown_pct") or -1.0)
        gate_pass = int(trades >= 25 and pf >= 1.2 and dd >= -0.20)
        return (gate_pass, pf)

    ranked = sorted(results, key=_score, reverse=True)
    SWEEP_ROOT.joinpath("threshold_sweep_summary.json").write_text(
        json.dumps({"top": ranked[:5], "all": results}, indent=2),
        encoding="utf-8",
    )
    print("\n=== Top 5 cells by PF (Gate-B pass first) ===")
    for r in ranked[:5]:
        print(
            f"  {r['tag']:<28} trades={r.get('trades'):>3}  "
            f"PF={r.get('profit_factor')}  DD={r.get('max_drawdown_pct')}  PnL={r.get('total_pnl')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
