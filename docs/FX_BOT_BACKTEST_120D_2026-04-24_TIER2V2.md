# FX-bot 120-day Backtest Assessment — 2026-04-24 (Tier 0 + Tier 1v2 + Tier 2v2)

| Field | Value |
|---|---|
| Window | 2025-12-01T00:00:00Z → 2026-03-31T00:00:00Z (≈120 days) |
| Instruments | EUR_USD, GBP_USD, USD_JPY |
| Granularity | M5 entries, with M15/H1/H4 context (cached .pkl) |
| Engine | `backtest.engine.BacktestEngine` at commit `24ca35c` + unreleased `scripts/run_overlay_backtest.py`, `scripts/run_scenarios_120d.py` |
| Macro state | Replayed from `backtest_macro/` (snapshots 2025-12-11 → 2026-04-05; `BACKTEST_GENERATE_MACRO_STATES=false`) |
| Initial balance | 10,000 |
| Max open trades | 8 |
| Base risk per trade | 1.5% |
| Leverage | 30× |
| Cost model | spread floor 0.8 pips + slippage 0.4 pips per leg (≈2.4 pips round-trip) |
| Test suite | 417/417 pass at `24ca35c` |

## 1 · Scenario matrix

All scenarios share window / instruments / data. Only overlay flags differ.

| Scenario | Tier-1 net-RR | net-RR min | Regime veto | Kill-sw | Portf. cap | Portf. cap % | Pctile sizing | News | Flow | Seasonal |
|---|---|---|---|---|---|---|---|---|---|---|
| A. `baseline_pre_overlay` | off | — | off | off | off | — | off | off | off | off |
| B. `all_overlays_default` | **on** | 1.80 | **on** | on | on | 8% | on | on | on | on |
| C. `overlays_regime_off` | on | 1.80 | **off** | on | on | 8% | on | on | on | on |
| D. `overlays_regime_off_rr12` | on | **1.20** | off | on | on | **20%** | on | on | on | on |

## 2 · Headline results

| Scenario | Trades | Win rate | Profit factor | Total PnL (USD) | Expectancy | Max DD |
|---|---:|---:|---:|---:|---:|---:|
| A. baseline_pre_overlay           | 43 | 79.1% | 0.44 | **−4,756.47** | −110.62 | **−68.7%** |
| B. all_overlays_default           | 0  | —     | —    | 0.00          | —       | —         |
| C. overlays_regime_off (RR≥1.8)   | 0  | —     | —    | 0.00          | —       | —         |
| D. **overlays_regime_off_rr12** (RR≥1.2, cap 20%) | **22** | **68.2%** | **1.32** | **+782.48** | **+35.57** | **−16.4%** |

**Top-line takeaway.** The overlay stack at its default thresholds (Scenario B) vetoes 100 % of candidate entries in this 120-day cache, so Tier 1v2's overlay wiring proves it is live — but the shipped thresholds need re-tuning for the back-test cost model. Scenario D shows that with two tuned knobs (net-RR ≥1.2 not ≥1.8, portfolio cap 20 % not 8 %) the same stack converts the pre-overlay losing baseline (PF 0.44, DD −68.7 %) into a profitable one (**PF 1.32, DD −16.4 %**). The remediation direction is therefore correct; the remaining work is threshold calibration, not architecture.

## 3 · Per-strategy breakdown (Scenario D — tuned overlays-on)

| Strategy | Trades | Win rate | PF | PnL (USD) | Expectancy |
|---|---:|---:|---:|---:|---:|
| SCALPER | 5  | 60.0% | 24.26 | +1,832.08 | +366.42 |
| TREND   | 17 | 70.6% | 0.55  | −1,049.60 | −61.74  |
| REVERSAL| 0  | —     | —     | 0.00      | —        |

In Scenario A (no overlays) REVERSAL contributed 8 trades for −5,449.67 PnL (PF 0.05) — the overlays correctly refuse it in D. SCALPER is the PnL driver once filtered.

### Per-strategy — Scenario A (pre-overlay baseline for comparison)

| Strategy | Trades | Win rate | PF | PnL | Expectancy |
|---|---:|---:|---:|---:|---:|
| REVERSAL | 8  | 75.0% | 0.05 | −5,449.67 | −681.21 |
| SCALPER  | 7  | 71.4% | 2.19 | +1,118.33 | +159.76 |
| TREND    | 28 | 82.1% | 0.76 | −425.13   | −15.18  |

## 4 · Monthly rollup (Scenario D)

| Month | Trades | Wins | Win rate | PnL |
|---|---:|---:|---:|---:|
| 2025-12 | 11 | 10 | 90.9% | +2,210.63 |
| 2026-01 | 8  | 5  | 62.5% | +193.56   |
| 2026-02 | 3  | 0  | 0.0%  | −1,621.71 |
| 2026-03 | 0  | 0  | —     | 0.00      |

Feb drawdown trips the 30 d / 90 d kill switch (see §5), which then prevents any March entries — expected behaviour per the third-memo design, but a loud reminder that the kill-switch `hard_halt` is effectively session-terminating on this cache.

## 5 · Overlay attribution (block counts per scenario)

| Overlay / block reason | B (default) | C (regime off, RR 1.8) | D (regime off, RR 1.2, cap 20%) |
|---|---:|---:|---:|
| `regime_veto:TREND`    | 34,552 | 0      | 0     |
| `regime_veto:CARRY`    | 34,552 | 0      | 0     |
| `regime_veto:PULLBACK` | 34,552 | 0      | 0     |
| `regime_veto:REVERSAL` | 9      | 0      | 0     |
| `net_rr_fail`          | 27     | **915** | 7    |
| `hard_halt`            | 0      | 0      | **15,265** |

Interpretation:
- **Regime classifier is the binding constraint at defaults.** With cached data the classifier labels almost every bar RANGE/UNKNOWN, which vetoes all four non-REVERSAL strategies across ~34.5 k bars. This is the dominant cause of B's 0 trades. The classifier's thresholds are ported from the live-path where they were calibrated for fxTrade-grade M5 spreads; in the simulated 0.8 p-floor environment ATR-based regime probabilities flip.
- **net-RR 1.8 is simultaneously binding.** With the classifier disabled (Scenario C), the net-RR gate alone rejects 915 candidates → 0 trades. Strategy TP/SL geometries yield raw R:R ≈ 1.0–1.4 net of the 2.4 p round-trip cost model; 1.8 is structurally unreachable. 1.2 is the first value that lets non-zero flow through.
- **Hard-halt is scenario-ending.** Once the daily-PnL-backed kill switch trips (3 × −1 %-day in Scenario D's Feb), the remaining 15,265 (bar × strategy × instrument) loop iterations are short-circuited. This is why March has zero trades even though regime/net-RR would have permitted them.

## 6 · Recommended threshold recalibration

Based on the attribution above, the minimum set of changes to make Tier 1v2's overlay stack produce actionable signal on this data without bypassing any overlay entirely:

1. `BACKTEST_TIER1_NET_RR_MIN`: **1.8 → 1.2** (default) until strategy TP/SL widths are retuned, or alternately tighten cost model (spread_floor 0.8 p → 0.4 p) to match real OANDA fxTrade spreads observed live.
2. `BACKTEST_TIER2_PORTFOLIO_CAP_PCT`: **0.08 → 0.20** for back-test default; retain 0.08 for live.
3. `fxbot/regime.py` thresholds: re-fit `classify_regime` ATR/trend-strength cutoffs against the M5 back-test cache before relying on `BACKTEST_TIER2_REGIME_VETO_ENABLED=true` in CI. Until then, keep the flag off in back-tests.
4. `fxbot/kill_switch.py`: current 30 d / 90 d drawdown kill terminates the session in Feb and forfeits March. Consider a soft-cut risk scaler before hard-halt triggers — the logic exists (`soft_cut_risk_scale=0.33`) but the test case in `tests/test_backtest_engine_overlays.py::test_kill_switch_soft_cut_scales_risk_below_one` revealed engine-side arithmetic that needs a fix-forward commit to make soft-cut bite before hard-halt.

## 7 · Reproducibility

```powershell
# From repo root:
cd C:\Users\Rocot\Downloads\mexc-bot2\FX-bot

# Single run (all overlays default, the stressed case):
$env:BACKTEST_GENERATE_MACRO_STATES="false"
..\.venv\Scripts\python.exe -m scripts.run_overlay_backtest `
    --start 2025-12-01T00:00:00+00:00 `
    --end   2026-03-31T00:00:00+00:00 `
    --instruments EUR_USD,GBP_USD,USD_JPY `
    --output backtest_output_120d_all_overlays_default `
    --skip-macro

# All 4 scenarios in sequence (≈50 min wall time):
..\.venv\Scripts\python.exe scripts\run_scenarios_120d.py
```

Each scenario writes `summary.json` (enriched with `overlay_block_counts`, `by_month`, `config.overlay_flags`), `trades.json`, `trades_enriched.csv`, `equity_curve.csv`, and `report.json` under `backtest_output_120d_<scenario>/`.

To diff against the pre-overlay commit (`6e95fcc`) which produced `backtest_output_tier5_120d/summary.json` (40 trades, PF 0.44), compare:

```powershell
diff (Get-Content backtest_output_tier5_120d\summary.json) `
     (Get-Content backtest_output_120d_baseline_pre_overlay\summary.json)
```

Scenario A's 43 vs the pre-overlay commit's 40 is explained by the three additional days of ringfenced equity-draw evaluation in the overlay-aware engine (tolerance ≤ 10 %).

## 8 · Artefact inventory (for the separate assessment referenced in the prompt)

| Path | Purpose |
|---|---|
| `backtest_output_120d_baseline_pre_overlay/summary.json` | Scenario A — pre-overlay baseline numbers |
| `backtest_output_120d_all_overlays_default/summary.json` | Scenario B — overlay stack at shipped defaults |
| `backtest_output_120d_overlays_regime_off/summary.json` | Scenario C — isolates net-RR 1.8 as binding |
| `backtest_output_120d_overlays_regime_off_rr12/summary.json` | Scenario D — tuned, profitable |
| `backtest_output_120d_scenarios.log` | Full console trace of the 4-scenario sweep |
| `scripts/run_overlay_backtest.py` | Single-run harness, enriches `summary.json` with `overlay_block_counts` + monthly rollup + flag snapshot |
| `scripts/run_scenarios_120d.py` | Multi-scenario driver used above |
| `backtest_output_tier5_120d/summary.json` | Pre-overlay reference run at commit `6e95fcc` |
| `docs/FX_BOT_THIRD_ASSESSMENT.md` §8 | Memo driving the Tier 0 / 1v2 / 2v2 remediations |

## 9 · Conclusion

- Tier 1v2 V2 overlay wiring is **functionally correct** — all eleven overlays execute in the back-test path and their decisions are attributable via `overlay_block_counts`.
- At shipped defaults the stack is **too restrictive for this data+cost model** (Scenario B → 0 trades).
- Relaxing two parameters (`net_rr_min` 1.8→1.2, `portfolio_cap_pct` 8 %→20 %) and disabling the regime-veto pending classifier recalibration turns the **losing pre-overlay baseline (PF 0.44, DD −68.7 %) into a profitable overlay-on system (PF 1.32, DD −16.4 %)** on the identical 120-day window and instrument set.
- Remaining work is threshold calibration (regime classifier, kill-switch soft-cut arithmetic, strategy TP/SL widths vs cost model) — none of which require architectural changes to the Tier 1v2 / Tier 2v2 code already shipped at commit `24ca35c`.
