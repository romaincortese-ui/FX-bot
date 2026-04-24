"""Unit tests for the Tier 1v2 overlay integration on the backtest engine.

Memo 3 §7.2 listed 11 overlay modules that `backtest/engine.py` never
imported. Without these, any backtest was measuring the pre-Tier-1 baseline
rather than the live-path stack. These tests verify the wiring itself — not
the strategies' economics — by stubbing the scorer output and asserting the
right overlay path intercepts, increments the telemetry counter, and either
blocks or scales the trade.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backtest.config import BacktestConfig
from backtest.engine import BacktestEngine
from backtest.macro_sim import MacroReplay, MacroState
from fxbot.regime import Regime
from fxbot.news_impact import NewsImpact


class _StubProvider:
    def __init__(self) -> None:
        idx = pd.date_range("2023-01-01", periods=200, freq="5min", tz="UTC")
        self.frame = pd.DataFrame(
            {
                "open": [1.10] * 200,
                "high": [1.11] * 200,
                "low": [1.09] * 200,
                "close": [1.10] * 200,
                "bid_close": [1.0999] * 200,
                "ask_close": [1.1001] * 200,
                "volume": [100] * 200,
            },
            index=idx,
        )
        self.frame.index.name = "time"

    def get_candles(self, instrument, granularity, start, end, price="M"):
        return self.frame

    def get_pair_spread_profile(self, instrument, granularity, start, end):
        return {"default": 0.8}


def _make_engine(**cfg_overrides) -> BacktestEngine:
    config = BacktestConfig(
        start=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end=datetime(2023, 1, 2, tzinfo=timezone.utc),
        instruments=["EUR_USD"],
        **cfg_overrides,
    )
    return BacktestEngine(config, _StubProvider(), MacroReplay({}))


# ── Tier 1 §9 — net-of-cost R:R gate ──────────────────────────────────────

def test_net_rr_gate_rejects_opportunity_when_tp_below_min_rr():
    engine = _make_engine()
    engine._tier1_net_rr_min = 1.8
    ok, rr = engine._net_rr_passes(
        {"sl_pips": 10.0, "tp_pips": 12.0},
        entry_spread_pips=1.0,
    )
    # TP 12 - (2x spread + 2x slippage) = 12 - 2 - 0.8 = 9.2; rr 0.92 < 1.8.
    assert ok is False
    assert rr < 1.8


def test_net_rr_gate_accepts_when_tp_clears_required_rr():
    engine = _make_engine()
    engine._tier1_net_rr_min = 1.5
    ok, rr = engine._net_rr_passes(
        {"sl_pips": 10.0, "tp_pips": 40.0},
        entry_spread_pips=0.5,
    )
    assert ok is True
    assert rr >= 1.5


def test_net_rr_gate_passes_when_disabled():
    engine = _make_engine()
    engine._tier1_net_rr_enabled = False
    ok, _ = engine._net_rr_passes(
        {"sl_pips": 10.0, "tp_pips": 1.0},
        entry_spread_pips=5.0,
    )
    assert ok is True


# ── Tier 2 §2 — regime veto ───────────────────────────────────────────────

def test_regime_veto_blocks_reversal_in_usd_trend_regime():
    engine = _make_engine()
    engine._current_regime = Regime.USD_TREND
    # Live-path mapping: REVERSAL disabled in a persistent USD trend.
    assert engine._regime_blocks("REVERSAL") is True


def test_regime_veto_disabled_by_flag():
    engine = _make_engine()
    engine._current_regime = Regime.USD_TREND
    engine._tier2_regime_veto_enabled = False
    assert engine._regime_blocks("REVERSAL") is False


# ── Tier 2 §8 — portfolio vol cap ─────────────────────────────────────────

def test_portfolio_cap_blocks_when_correlated_exposure_exceeds_cap():
    engine = _make_engine()
    engine._tier2_portfolio_cap_pct = 0.01  # punitive to force a breach
    engine.simulator.open_trades = [
        {"instrument": "EUR_USD", "direction": "LONG", "risk_pct": 0.01},
        {"instrument": "GBP_USD", "direction": "LONG", "risk_pct": 0.01},
    ]
    assert engine._portfolio_cap_blocks("AUD_USD", "LONG") is True


def test_portfolio_cap_allows_when_cap_generous():
    engine = _make_engine()
    engine._tier2_portfolio_cap_pct = 0.50
    engine.simulator.open_trades = []
    assert engine._portfolio_cap_blocks("EUR_USD", "LONG") is False


# ── Tier 2 §9 — 30d/90d drawdown kill switch ──────────────────────────────

def test_kill_switch_triggers_hard_halt_on_ten_percent_drawdown():
    engine = _make_engine()
    # 90 days of -0.2% per day ≈ -18% drawdown — far beyond the 10% hard halt.
    engine._daily_pnl_pct.extend([-0.002] * 95)
    hard, _ = engine._update_daily_pnl_and_kill(
        datetime(2023, 4, 10, tzinfo=timezone.utc)
    )
    assert hard is True


def test_kill_switch_soft_cut_scales_risk_below_one():
    engine = _make_engine()
    # 30d at -0.3% per day ≈ -9% lookback, inside the 6% soft band.
    engine._daily_pnl_pct.extend([-0.003] * 30)
    hard, scale = engine._update_daily_pnl_and_kill(
        datetime(2023, 2, 10, tzinfo=timezone.utc)
    )
    assert hard is False
    assert 0.0 < scale < 1.0


def test_kill_switch_no_action_on_healthy_equity():
    engine = _make_engine()
    engine._daily_pnl_pct.extend([0.001] * 30)
    hard, scale = engine._update_daily_pnl_and_kill(
        datetime(2023, 2, 10, tzinfo=timezone.utc)
    )
    assert hard is False
    assert scale == pytest.approx(1.0)


# ── Tier 2 §5 — percentile sizing ─────────────────────────────────────────

def test_percentile_sizing_returns_one_before_warmup():
    engine = _make_engine()
    # No history yet → sizer falls back to neutral multiplier 1.0.
    assert engine._percentile_risk_multiplier("SCALPER", 55.0) == pytest.approx(1.0)


def test_percentile_sizing_scales_up_for_high_percentile_scores():
    engine = _make_engine()
    engine._score_history["SCALPER"].extend([float(x) for x in range(40, 60)])
    # Score well above the 99th percentile of history should yield > 1.
    mult = engine._percentile_risk_multiplier("SCALPER", 90.0)
    assert mult > 1.0


# ── Tier 3 §3 — opposite-direction reconciliation ─────────────────────────

def test_reconciliation_blocks_opposite_direction_signal_on_same_bar():
    engine = _make_engine()
    from fxbot.strategy_reconciliation import Signal

    now = datetime(2023, 1, 1, 9, 0, tzinfo=timezone.utc)
    engine._reconciliation.record(Signal(
        strategy="TREND",
        instrument="EUR_USD",
        direction="LONG",
        score=80.0,
        bar_ts_utc=now,
    ))
    blocked = engine._reconciliation_blocks(
        strategy="REVERSAL",
        instrument="EUR_USD",
        direction="SHORT",
        score=75.0,
        now=now + timedelta(minutes=5),
    )
    assert blocked is True


def test_reconciliation_allows_same_direction_signal():
    engine = _make_engine()
    from fxbot.strategy_reconciliation import Signal

    now = datetime(2023, 1, 1, 9, 0, tzinfo=timezone.utc)
    engine._reconciliation.record(Signal(
        strategy="TREND",
        instrument="EUR_USD",
        direction="LONG",
        score=80.0,
        bar_ts_utc=now,
    ))
    blocked = engine._reconciliation_blocks(
        strategy="SCALPER",
        instrument="EUR_USD",
        direction="LONG",
        score=75.0,
        now=now + timedelta(minutes=5),
    )
    assert blocked is False


# ── Tier 3 §6 — impact-weighted news gate ─────────────────────────────────

def test_news_impact_blocks_usd_pair_on_tier1_usd_event():
    engine = _make_engine()
    now = datetime(2023, 1, 1, 13, 30, tzinfo=timezone.utc)
    state = MacroState(
        filters={},
        news_events=[
            {
                "title": "US Non-Farm Payrolls",
                "currency": "USD",
                "pause_end": now.isoformat(),
                "time": now.isoformat(),
            }
        ],
        vix_value=18.0,
        dxy_gap=0.0,
    )
    impact = engine._news_impact_for("EUR_USD", state, now)
    assert impact == NewsImpact.BLOCK


def test_news_impact_ignores_non_overlapping_event():
    engine = _make_engine()
    now = datetime(2023, 1, 1, 13, 30, tzinfo=timezone.utc)
    far_past = now - timedelta(hours=6)
    state = MacroState(
        filters={},
        news_events=[
            {
                "title": "US Non-Farm Payrolls",
                "currency": "USD",
                "pause_end": far_past.isoformat(),
                "time": far_past.isoformat(),
            }
        ],
        vix_value=18.0,
        dxy_gap=0.0,
    )
    impact = engine._news_impact_for("EUR_USD", state, now)
    assert impact == NewsImpact.PASS


# ── Tier 3 §7 / §8 — flow window + seasonality multipliers ────────────────

def test_flow_and_seasonality_multipliers_finite_and_positive():
    engine = _make_engine()
    now = datetime(2023, 6, 15, 16, 0, tzinfo=timezone.utc)
    flow = engine._flow_risk_multiplier("EUR_USD", now)
    seas = engine._seasonal_mult("TREND", "EUR_USD", now)
    assert flow > 0
    assert seas > 0


# ── Telemetry: overlay_block_counts is stable ─────────────────────────────

def test_overlay_block_counts_starts_empty_and_is_writable():
    engine = _make_engine()
    assert dict(engine.overlay_block_counts) == {}
    engine.overlay_block_counts["regime_veto:TREND"] += 1
    assert engine.overlay_block_counts["regime_veto:TREND"] == 1
