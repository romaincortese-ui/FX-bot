from __future__ import annotations

from collections import defaultdict, deque
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import os

import pandas as pd
import requests

from fxbot.indicators import calc_atr, calc_ema
from fxbot.risk import would_breach_correlation_limit
from fxbot.strategies import StrategyScoringContext
from fxbot.strategies import determine_direction
from fxbot.strategies import score_asian_fade, score_breakout, score_carry, score_post_news, score_pullback, score_reversal, score_scalper, score_trend

# Tier 1v2 §7.2 — overlay modules, now exercised from the backtest harness
# instead of main.py only. Without these the backtest was measuring the
# pre-Tier-1 baseline (see third-memo §7.1).
from fxbot.cost_model import compute_net_rr
from fxbot.correlation_risk import would_breach_portfolio_cap
from fxbot.decision_day import decision_day_follow_through
from fxbot.flow_strategies import active_flow_window, instrument_is_flow_eligible
from fxbot.kill_switch import evaluate_drawdown_kill
from fxbot.news_impact import NewsImpact, classify_news_impact
from fxbot.percentile_sizing import size_by_percentile
from fxbot.regime import Regime, classify_regime, is_strategy_enabled
from fxbot.regime_dwell import RegimeDwellFilter
from fxbot.seasonality import seasonal_risk_multiplier
from fxbot.strategy_reconciliation import Signal, StrategyReconciliation

from .config import BacktestConfig
from .data import HistoricalDataProvider
from .macro_sim import MacroReplay, MacroState
from .simulator import SimulatorConfig, TradeSimulator


def _flag(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


class BacktestEngine:
    def __init__(self, config: BacktestConfig, data_provider: HistoricalDataProvider, macro_replay: MacroReplay):
        self.config = config
        self.data_provider = data_provider
        self.macro_replay = macro_replay
        self.settings = config.strategy_settings()
        self.data_cache: dict[tuple[str, str], pd.DataFrame] = {}
        self.reject_reasons: dict[tuple[str, str], str] = {}
        self.simulator = TradeSimulator(
            SimulatorConfig(
                initial_balance=config.initial_balance,
                max_open_trades=config.max_open_trades,
                spread_floor_pips=config.spread_floor_pips,
                spread_buffer_pips=config.spread_buffer_pips,
                slippage_pips=config.slippage_pips,
                news_slippage_pips=config.news_slippage_pips,
                round_trip_cost_pips=config.round_trip_cost_pips,
                max_risk_per_trade=config.max_risk_per_trade,
            )
        )
        self.strategy_order: list[tuple[str, Callable[..., dict[str, Any] | None]]] = [
            ("SCALPER", score_scalper),
            ("TREND", score_trend),
            ("REVERSAL", score_reversal),
            ("BREAKOUT", score_breakout),
            ("CARRY", score_carry),
            ("ASIAN_FADE", score_asian_fade),
            ("POST_NEWS", score_post_news),
            ("PULLBACK", score_pullback),
        ]
        self._spread_profiles: dict[str, dict[str, float]] = {}
        self._last_trade_close: dict[tuple[str, str], datetime] = {}  # (strategy, instrument) -> last close time

        # ── Tier 1v2 overlay state ─────────────────────────────────────────
        # Feature flags — default ON so the backtest mirrors the live path.
        # Set BACKTEST_TIER*_*_ENABLED=0 to isolate any single overlay.
        self._tier1_net_rr_enabled = _flag("BACKTEST_TIER1_NET_RR_ENABLED", "true")
        self._tier1_net_rr_min = float(os.getenv("BACKTEST_TIER1_NET_RR_MIN", "1.8"))
        self._tier2_regime_veto_enabled = _flag("BACKTEST_TIER2_REGIME_VETO_ENABLED", "true")
        self._tier2_kill_switch_enabled = _flag("BACKTEST_TIER2_KILL_SWITCH_ENABLED", "true")
        self._tier2_portfolio_cap_enabled = _flag("BACKTEST_TIER2_PORTFOLIO_CAP_ENABLED", "true")
        self._tier2_portfolio_cap_pct = float(os.getenv("BACKTEST_TIER2_PORTFOLIO_CAP_PCT", "0.08"))
        self._tier2_percentile_sizing_enabled = _flag("BACKTEST_TIER2_PERCENTILE_SIZING_ENABLED", "true")
        self._tier3_news_impact_enabled = _flag("BACKTEST_TIER3_NEWS_IMPACT_ENABLED", "true")
        self._tier3_flow_enabled = _flag("BACKTEST_TIER3_FLOW_ENABLED", "true")
        self._tier3_seasonality_enabled = _flag("BACKTEST_TIER3_SEASONALITY_ENABLED", "true")
        self._tier3_reconciliation_enabled = _flag("BACKTEST_TIER3_RECONCILIATION_ENABLED", "true")
        self._tier5_decision_day_enabled = _flag("BACKTEST_TIER5_DECISION_DAY_ENABLED", "false")

        # Rolling observables fed into classify_regime() on every step.
        self._dxy_closes: deque[float] = deque(maxlen=60)
        self._vix_history: deque[float] = deque(maxlen=120)
        self._spy_closes: deque[float] = deque(maxlen=60)
        self._regime_dwell = RegimeDwellFilter(min_dwell_bars=int(os.getenv("BACKTEST_REGIME_DWELL_BARS", "4")))
        self._current_regime: Regime = Regime.CHOP

        # Score percentile history per strategy — replaces pure Kelly multiplier
        # for all trades after a 20-sample warm-up.
        self._score_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=500))

        # Daily PnL tracking for the 30d/90d kill switch.
        self._daily_pnl_pct: deque[float] = deque(maxlen=180)
        self._last_day: date | None = None
        self._last_day_equity: float = config.initial_balance

        # Opposite-direction veto (Tier 3 §3).
        self._reconciliation = StrategyReconciliation()

        # Telemetry: per-cycle overlay block counters so the reporter can
        # attribute where the backtest mirrors third-memo §3 gates.
        self.overlay_block_counts: dict[str, int] = defaultdict(int)

    def _load_series(self, instrument: str, granularity: str) -> pd.DataFrame | None:
        key = (instrument, granularity)
        if key not in self.data_cache:
            price = "MBA" if self.config.use_bid_ask_data else "M"
            fetched = self.data_provider.get_candles(
                instrument,
                granularity,
                self.config.start - timedelta(days=10),
                self.config.end + timedelta(days=2),
                price=price,
            )
            self.data_cache[key] = fetched if fetched is not None else pd.DataFrame()
        df = self.data_cache[key]
        return df if not df.empty else None

    def _fetch_candles_until(self, instrument: str, granularity: str, count: int, now: datetime) -> pd.DataFrame | None:
        df = self._load_series(instrument, granularity)
        if df is None:
            return None
        sliced = df[df.index < now].tail(count)
        return sliced if len(sliced) >= min(count, 20) else None

    def _bar_at(self, instrument: str, granularity: str, now: datetime) -> dict[str, Any] | None:
        df = self._load_series(instrument, granularity)
        if df is None:
            return None
        rows = df[df.index == now]
        if rows.empty:
            return None
        row = rows.iloc[-1]
        return {
            "time": now,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "bid_open": float(row.get("bid_open", 0) or 0),
            "bid_high": float(row.get("bid_high", 0) or 0),
            "bid_low": float(row.get("bid_low", 0) or 0),
            "bid_close": float(row.get("bid_close", 0) or 0),
            "ask_open": float(row.get("ask_open", 0) or 0),
            "ask_high": float(row.get("ask_high", 0) or 0),
            "ask_low": float(row.get("ask_low", 0) or 0),
            "ask_close": float(row.get("ask_close", 0) or 0),
            "volume": int(row.get("volume", 0)),
        }

    def _set_reject_reason(self, strategy: str, instrument: str, reason: str) -> None:
        self.reject_reasons[(strategy, instrument)] = reason

    def _apply_macro_directional_bias(self, macro_filters: dict[str, str], instrument: str, signals: dict[str, int]) -> None:
        bias = str(macro_filters.get(instrument.upper(), "NEUTRAL") or "NEUTRAL").upper()
        if bias == "LONG_ONLY":
            signals["long"] += 5
        elif bias == "SHORT_ONLY":
            signals["short"] += 5

    def _parse_event_time(self, value: str | None) -> datetime | None:
        if not value:
            return None
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _event_affects_instrument(self, event: dict[str, Any], instrument: str) -> bool:
        currency = str(event.get("currency") or "").upper()
        if not currency or "_" not in instrument:
            return False
        base, quote = instrument.split("_", 1)
        return currency in {base, quote}

    def _is_pair_paused_by_news(self, macro_state: MacroState, instrument: str, now: datetime) -> bool:
        for event in macro_state.news_events:
            if not self._event_affects_instrument(event, instrument):
                continue
            pause_start = self._parse_event_time(event.get("pause_start"))
            pause_end = self._parse_event_time(event.get("pause_end"))
            if pause_start is None or pause_end is None:
                continue
            if pause_start <= now < pause_end:
                return True
        return False

    def _get_post_news_events(self, macro_state: MacroState, instrument: str, now: datetime) -> list[dict[str, Any]]:
        matched = []
        window = self.settings["POST_NEWS_WINDOW_MINS"]
        for event in macro_state.news_events:
            if not self._event_affects_instrument(event, instrument):
                continue
            pause_end = self._parse_event_time(event.get("pause_end"))
            if pause_end is None:
                continue
            if pause_end <= now <= pause_end + timedelta(minutes=window):
                matched.append(event)
        return matched

    def _get_session(self, now: datetime) -> dict[str, Any]:
        hour = now.hour
        tokyo_active = self.settings["TOKYO_OPEN_UTC"] <= hour < self.settings["TOKYO_CLOSE_UTC"]
        london_active = self.settings["LONDON_OPEN_UTC"] <= hour < self.settings["LONDON_CLOSE_UTC"]
        ny_active = self.settings["NY_OPEN_UTC"] <= hour < self.settings["NY_CLOSE_UTC"]
        all_pairs = list(self.config.instruments)
        core_pairs = [p for p in all_pairs if p in {"EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "NZD_USD"}] or all_pairs[:6]
        if london_active and ny_active:
            return {"name": "LONDON_NY_OVERLAP", "multiplier": self.settings["SESSION_OVERLAP_MULT"], "pairs_allowed": all_pairs, "is_overlap": True, "aggression": "HIGH"}
        if london_active:
            return {"name": "LONDON", "multiplier": self.settings["SESSION_LONDON_MULT"], "pairs_allowed": all_pairs, "is_overlap": False, "aggression": "HIGH"}
        if ny_active:
            return {"name": "NEW_YORK", "multiplier": self.settings["SESSION_NY_MULT"], "pairs_allowed": all_pairs, "is_overlap": False, "aggression": "MEDIUM"}
        if tokyo_active:
            tokyo_pairs = [p for p in all_pairs if "JPY" in p or "AUD" in p or "NZD" in p]
            return {"name": "TOKYO", "multiplier": self.settings["SESSION_TOKYO_MULT"], "pairs_allowed": tokyo_pairs or core_pairs, "is_overlap": False, "aggression": "LOW"}
        return {"name": "OFF_HOURS", "multiplier": self.settings["SESSION_OFF_HOURS_MULT"], "pairs_allowed": core_pairs, "is_overlap": False, "aggression": "MINIMAL"}

    def _compute_market_regime(self, now: datetime, macro_state: MacroState) -> float:
        df = self._fetch_candles_until("EUR_USD", "H1", 100, now)
        if df is None or len(df) < 50:
            return 1.0
        close = df["close"]
        atr = calc_atr(df, 14)
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift(1)).abs(),
            (df["low"] - df["close"].shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr_series = tr.ewm(alpha=1 / 14, adjust=False).mean()
        atr_ratio = float(atr_series.iloc[-1]) / float(atr_series.iloc[-41:-1].mean()) if len(atr_series) > 40 else 1.0
        ema50 = calc_ema(close, 50)
        ema_gap = float(close.iloc[-1]) / float(ema50.iloc[-1]) - 1
        regime_atr_mult = 1.0
        if atr_ratio > self.settings["REGIME_HIGH_VOL_ATR_RATIO"]:
            regime_atr_mult *= self.settings["REGIME_TIGHTEN_MULT"]
        elif atr_ratio < self.settings["REGIME_LOW_VOL_ATR_RATIO"]:
            regime_atr_mult *= self.settings["REGIME_LOOSEN_MULT"]
        if abs(ema_gap) > 0.01:
            regime_atr_mult *= 0.90
        vix_regime = 1.0
        if macro_state.vix_value is not None:
            if macro_state.vix_value > self.settings["VIX_HIGH_THRESHOLD"]:
                vix_regime = 1.30
            elif macro_state.vix_value < self.settings["VIX_LOW_THRESHOLD"]:
                vix_regime = 0.75
        dxy_regime = 1.20 if abs(macro_state.dxy_gap or 0.0) > 0.008 else 1.0
        return round(regime_atr_mult * vix_regime * dxy_regime, 3)

    def _estimate_spread_pips(self, instrument: str, now: datetime) -> float:
        bar = self._bar_at(instrument, self.config.granularity, now)
        if bar and float(bar.get("bid_close", 0) or 0) > 0 and float(bar.get("ask_close", 0) or 0) > 0:
            pip_divisor = 0.01 if "JPY" in instrument else 0.0001
            return round(max(self.config.spread_floor_pips, (float(bar["ask_close"]) - float(bar["bid_close"])) / pip_divisor), 3)
        if instrument not in self._spread_profiles:
            self._spread_profiles[instrument] = self.data_provider.get_pair_spread_profile(instrument, self.config.granularity, self.config.start - timedelta(days=10), self.config.end + timedelta(days=2))
        profile = self._spread_profiles[instrument]
        by_hour = profile.get(f"hour_{now.hour:02d}")
        if by_hour is not None:
            return round(max(self.config.spread_floor_pips, float(by_hour)), 3)
        default = profile.get("default")
        if default is not None:
            return round(max(self.config.spread_floor_pips, float(default)), 3)
        df = self._fetch_candles_until(instrument, self.config.granularity, 40, now)
        if df is None or len(df) < 20:
            return self.config.spread_floor_pips
        atr = calc_atr(df, 14)
        atr_pips = atr / 0.01 if "JPY" in instrument else atr / 0.0001
        estimate = max(self.config.spread_floor_pips, min(3.0, atr_pips * 0.03 + self.config.spread_buffer_pips))
        return round(estimate, 3)

    def _strategy_threshold(self, label: str) -> int:
        return int(self.settings.get(f"{label}_THRESHOLD", 40))

    def _kelly_multiplier(self, label: str, score: float, effective_threshold: float | None = None) -> float:
        threshold = effective_threshold if effective_threshold is not None else self._strategy_threshold(label)
        gap = score - threshold
        if gap >= 30:
            return self.settings["KELLY_MULT_HIGH_CONF"]
        if gap >= 15:
            return self.settings["KELLY_MULT_STANDARD"]
        if gap >= 5:
            return self.settings["KELLY_MULT_SOLID"]
        return self.settings["KELLY_MULT_MARGINAL"]

    def _conversion_rate(self, from_currency: str, to_currency: str, now: datetime) -> float | None:
        if from_currency == to_currency:
            return 1.0
        try:
            direct = self._bar_at(f"{from_currency}_{to_currency}", self.config.granularity, now)
        except requests.HTTPError:
            direct = None
        if direct:
            return float(direct["close"])
        try:
            inverse = self._bar_at(f"{to_currency}_{from_currency}", self.config.granularity, now)
        except requests.HTTPError:
            inverse = None
        if inverse and float(inverse["close"]) > 0:
            return 1.0 / float(inverse["close"])
        return None

    def _position_units(self, instrument: str, close_price: float, sl_pips: float, balance: float, kelly_mult: float, account_currency: str, now: datetime) -> float:
        risk_amount = balance * self.config.max_risk_per_trade * kelly_mult
        quote_currency = instrument.split("_")[1]
        quote_to_account = self._conversion_rate(quote_currency, account_currency, now) or 1.0
        pip_value_per_unit = (0.01 if "JPY" in instrument else 0.0001) * quote_to_account
        if sl_pips <= 0 or pip_value_per_unit <= 0:
            return 0.0
        return max(1.0, risk_amount / (sl_pips * pip_value_per_unit))

    # ── Tier 1v2 overlay helpers ──────────────────────────────────────────

    def _tick_overlay_observations(self, now: datetime, macro_state: MacroState) -> None:
        """Append rolling macro observables so classify_regime() has history."""
        if macro_state.vix_value is not None:
            self._vix_history.append(float(macro_state.vix_value))
        # DXY proxy: track EUR_USD close inverted (a rough but stable proxy
        # when a dedicated DXY series is unavailable in the backtest feed).
        bar = self._bar_at("EUR_USD", self.config.granularity, now)
        if bar:
            # Inverted close so "DXY rises" => value rises.
            self._dxy_closes.append(1.0 / float(bar["close"]) if float(bar["close"]) > 0 else 0.0)
        spy = self._bar_at("SPX500_USD", self.config.granularity, now)
        if spy:
            self._spy_closes.append(float(spy["close"]))

        # Classify regime every step; RegimeDwellFilter smooths out flicker.
        try:
            raw = classify_regime(
                dxy_closes=list(self._dxy_closes) or None,
                vix_history=list(self._vix_history) or None,
                spy_closes=list(self._spy_closes) or None,
            ).regime
        except Exception:
            raw = Regime.CHOP
        self._current_regime = self._regime_dwell.observe(raw)

    def _regime_blocks(self, strategy: str) -> bool:
        if not self._tier2_regime_veto_enabled:
            return False
        return not is_strategy_enabled(strategy, self._current_regime)

    def _news_impact_for(self, instrument: str, macro_state: MacroState, now: datetime) -> NewsImpact:
        """Return the worst NewsImpact across active + imminent events."""
        if not self._tier3_news_impact_enabled or not macro_state.news_events:
            return NewsImpact.PASS
        worst = NewsImpact.PASS
        lookahead_min = int(os.getenv("BACKTEST_NEWS_IMPACT_LOOKAHEAD_MIN", "30"))
        for event in macro_state.news_events:
            event_time = self._parse_event_time(event.get("pause_end") or event.get("time"))
            if event_time is None:
                continue
            # Consider events that are imminent or were just released.
            if not (now - timedelta(minutes=lookahead_min) <= event_time <= now + timedelta(minutes=lookahead_min)):
                continue
            decision = classify_news_impact(
                event_title=str(event.get("title", "")),
                event_currency=str(event.get("currency", "")),
                instrument=instrument,
            )
            if decision.impact == NewsImpact.BLOCK:
                return NewsImpact.BLOCK
            if decision.impact == NewsImpact.REDUCE:
                worst = NewsImpact.REDUCE
        return worst

    def _flow_risk_multiplier(self, instrument: str, now: datetime) -> float:
        if not self._tier3_flow_enabled:
            return 1.0
        window = active_flow_window(now)
        if not window.in_window:
            return 1.0
        if not instrument_is_flow_eligible(instrument, window.event):
            return 1.0
        return float(window.risk_multiplier)

    def _seasonal_mult(self, strategy: str, instrument: str, now: datetime) -> float:
        if not self._tier3_seasonality_enabled:
            return 1.0
        return float(seasonal_risk_multiplier(strategy, instrument, now))

    def _decision_day_mult(self, instrument: str, macro_state: MacroState, now: datetime) -> float:
        if not self._tier5_decision_day_enabled or not macro_state.news_events:
            return 1.0
        signal = decision_day_follow_through(
            instrument=instrument,
            events=macro_state.news_events,
            now=now,
        )
        return float(signal.risk_multiplier) if signal.in_window else 1.0

    def _percentile_risk_multiplier(self, strategy: str, score: float) -> float:
        if not self._tier2_percentile_sizing_enabled:
            return 1.0
        hist = self._score_history[strategy]
        decision = size_by_percentile(score=score, history=list(hist), min_samples=20)
        return float(decision.multiplier)

    def _net_rr_passes(self, opp: dict, entry_spread_pips: float) -> tuple[bool, float]:
        if not self._tier1_net_rr_enabled:
            return True, 0.0
        sl = float(opp.get("sl_pips", 0.0) or 0.0)
        tp = float(opp.get("tp_pips", 0.0) or 0.0)
        if sl <= 0 or tp <= 0:
            return True, 0.0
        breakdown = compute_net_rr(
            sl_pips=sl,
            tp_pips=tp,
            entry_spread_pips=float(entry_spread_pips),
            slippage_pips=float(self.config.slippage_pips),
            financing_pips=0.0,
            min_net_rr=self._tier1_net_rr_min,
        )
        return bool(breakdown.passed), float(breakdown.net_rr)

    def _portfolio_cap_blocks(self, instrument: str, direction: str) -> bool:
        if not self._tier2_portfolio_cap_enabled:
            return False
        decision = would_breach_portfolio_cap(
            open_trades=[
                {
                    "instrument": t.get("instrument"),
                    "direction": t.get("direction"),
                    "risk_pct": float(t.get("risk_pct", self.config.max_risk_per_trade) or self.config.max_risk_per_trade),
                }
                for t in self.simulator.open_trades
            ],
            candidate_instrument=instrument,
            candidate_direction=direction,
            candidate_risk_pct=float(self.config.max_risk_per_trade),
            cap_pct=self._tier2_portfolio_cap_pct,
        )
        return not decision.allowed

    def _reconciliation_blocks(self, strategy: str, instrument: str, direction: str, score: float, now: datetime) -> bool:
        if not self._tier3_reconciliation_enabled:
            return False
        decision = self._reconciliation.check(
            strategy=strategy,
            instrument=instrument,
            direction=direction,
            score=float(score),
            now_utc=now,
        )
        return not decision.allowed

    def _update_daily_pnl_and_kill(self, now: datetime) -> tuple[bool, float]:
        """Return (hard_halt, risk_scale) after rolling daily PnL history."""
        today = now.date()
        current_balance = float(self.simulator.balance)
        if self._last_day is None:
            self._last_day = today
            self._last_day_equity = current_balance
        elif today != self._last_day:
            # Close-of-day snapshot for every calendar day we rolled past.
            days_elapsed = (today - self._last_day).days
            if self._last_day_equity > 0 and days_elapsed >= 1:
                pct = (current_balance - self._last_day_equity) / self._last_day_equity
                # One data point per calendar day; if the engine skips days
                # (weekends), distribute 0% for missing days so the 30d/90d
                # lookback windows remain calendar-consistent.
                self._daily_pnl_pct.append(pct)
                for _ in range(days_elapsed - 1):
                    self._daily_pnl_pct.append(0.0)
            self._last_day = today
            self._last_day_equity = current_balance

        if not self._tier2_kill_switch_enabled or len(self._daily_pnl_pct) < 5:
            return False, 1.0
        decision = evaluate_drawdown_kill(daily_pnl_pct=list(self._daily_pnl_pct))
        if decision.hard_halt:
            return True, 0.0
        if decision.soft_cut and decision.risk_per_trade_override is not None:
            # `risk_per_trade_override` is already a multiplier ∈ [0, 1] on
            # top of the base per-trade risk (see fxbot/kill_switch.py).
            return False, max(0.0, min(1.0, float(decision.risk_per_trade_override)))
        return False, 1.0


    def _mark_prices(self, now: datetime) -> dict[str, float]:
        marks = {}
        for instrument in self.config.instruments:
            bar = self._bar_at(instrument, self.config.granularity, now)
            if bar:
                marks[instrument] = float(bar["close"])
        return marks

    def _compute_effective_leverage(self, regime_mult: float) -> float:
        """Scale leverage inversely with regime multiplier.

        regime_mult > 1 means volatile / uncertain → lower leverage.
        regime_mult < 1 means stable / trending → higher leverage.
        """
        lev_min = float(self.settings.get("LEVERAGE_MIN", 10.0))
        lev_max = float(self.settings.get("LEVERAGE_MAX", 30.0))
        # regime_mult typically 0.75 .. 2.0.  Map to leverage range.
        # 0.75 (calm + trending) → lev_max, 2.0 (volatile) → lev_min
        t = max(0.0, min(1.0, (regime_mult - 0.75) / (2.0 - 0.75)))
        return round(lev_max - t * (lev_max - lev_min), 2)

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        current = self.config.start
        step = timedelta(minutes=5) if self.config.granularity == "M5" else timedelta(minutes=15)
        while current <= self.config.end:
            macro_state = self.macro_replay.get_state(current)
            closed_trades = self.simulator.update_open_trades(
                current,
                {i: self._bar_at(i, self.config.granularity, current) for i in self.config.instruments},
                sl_pct=float(self.settings.get("EXIT_SL_PCT", -0.40)),
                peak_trail_pct=float(self.settings.get("EXIT_PEAK_TRAIL_PCT", 0.015)),
                flat_hours=float(self.settings.get("EXIT_FLAT_HOURS", 48.0)),
                review_days=int(self.settings.get("EXIT_REVIEW_DAYS", 7)),
                review_poor_threshold=float(self.settings.get("EXIT_REVIEW_POOR_THRESHOLD", -0.10)),
                review_trend_bars=int(self.settings.get("EXIT_REVIEW_TREND_BARS", 50)),
                candle_fetch=lambda i, g, c, now=current: self._fetch_candles_until(i, g, c, now),
            )
            for ct in closed_trades:
                key = (ct["label"], ct["instrument"])
                self._last_trade_close[key] = current
            self.simulator.mark_equity(current, self._mark_prices(current))

            # ── Tier 1v2 overlay tick ─────────────────────────────────────
            self._tick_overlay_observations(current, macro_state)
            hard_halt, kill_risk_scale = self._update_daily_pnl_and_kill(current)
            if hard_halt:
                # 90d DD ≥ 10% → skip all new entries for this bar. Exits
                # continue via the simulator.update_open_trades call above.
                self.overlay_block_counts["hard_halt"] += 1
                current += step
                continue

            session = self._get_session(current)
            regime_mult = self._compute_market_regime(current, macro_state)
            adaptive_offsets = defaultdict(float)

            for label, scorer in self.strategy_order:
                if label not in self.config.strategies:
                    continue
                if not self.simulator.can_open_trade():
                    break
                # Tier 2 §2 — regime veto runs ONCE per strategy per bar.
                if self._regime_blocks(label):
                    self.overlay_block_counts[f"regime_veto:{label}"] += 1
                    continue
                best_opp = None
                for instrument in session["pairs_allowed"]:
                    if any(t["instrument"] == instrument for t in self.simulator.open_trades):
                        continue
                    # Cooldown: skip if a trade for this strategy+instrument closed too recently
                    cooldown_key = (label, instrument)
                    cooldown_hours = float(self.settings.get(f"{label}_COOLDOWN_HOURS", 0))
                    if cooldown_hours > 0 and cooldown_key in self._last_trade_close:
                        hours_since = (current - self._last_trade_close[cooldown_key]).total_seconds() / 3600.0
                        if hours_since < cooldown_hours:
                            continue
                    # Tier 3 §6 — impact-weighted news block (supersedes the
                    # legacy symmetric blackout for major events).
                    news_impact = self._news_impact_for(instrument, macro_state, current)
                    if news_impact == NewsImpact.BLOCK:
                        self.overlay_block_counts["news_block"] += 1
                        continue
                    spread_pips = self._estimate_spread_pips(instrument, current)
                    ctx = StrategyScoringContext(
                        get_spread_pips=lambda _i, sp=spread_pips: sp,
                        fetch_candles=lambda i, g, c, now=current: self._fetch_candles_until(i, g, c, now),
                        reject=self._set_reject_reason,
                        mark_pair_failure=lambda *args, **kwargs: None,
                        determine_direction=determine_direction,
                        get_post_news_events=lambda i, now=None, state=macro_state: self._get_post_news_events(state, i, now or current),
                        apply_macro_directional_bias=lambda i, signals, filters=macro_state.filters: self._apply_macro_directional_bias(filters, i, signals),
                        macro_filters=macro_state.filters,
                        macro_news=macro_state.news_events,
                        is_pair_paused_by_news=lambda i, now=None, state=macro_state: self._is_pair_paused_by_news(state, i, now or current),
                        market_regime_mult=regime_mult,
                        adaptive_offsets=adaptive_offsets,
                        dxy_ema_gap=macro_state.dxy_gap,
                        dxy_gate_threshold=self.settings["DXY_GATE_THRESHOLD"],
                        vix_level=macro_state.vix_value,
                        vix_low_threshold=self.settings["VIX_LOW_THRESHOLD"],
                        get_trade_calibration_adjustment=lambda *args, **kwargs: {"threshold_offset": 0.0, "risk_mult": 1.0, "block_reason": None, "source": None},
                        now_provider=lambda now=current: now,
                    )
                    opp = scorer(instrument, session, ctx, self.settings)
                    if opp is None:
                        continue
                    # Tier 1 §9 — net-of-cost R:R gate.
                    rr_ok, _ = self._net_rr_passes(opp, spread_pips)
                    if not rr_ok:
                        self.overlay_block_counts["net_rr_fail"] += 1
                        continue
                    # Tier 3 §3 — opposite-direction reconciliation.
                    if self._reconciliation_blocks(label, instrument, opp["direction"], opp.get("score", 0.0), current):
                        self.overlay_block_counts["reconciliation"] += 1
                        continue
                    # Tier 2 §8 — portfolio vol cap (8% default) on top of the
                    # legacy correlated-count cap.
                    if self._portfolio_cap_blocks(instrument, opp["direction"]):
                        self.overlay_block_counts["portfolio_cap"] += 1
                        continue
                    breached, _, _ = would_breach_correlation_limit(self.simulator.open_trades, opp["instrument"], opp["direction"], self.config.max_correlated_trades)
                    if breached:
                        continue
                    opp["session_name"] = session["name"]
                    opp["_news_impact"] = news_impact.value if hasattr(news_impact, "value") else str(news_impact)
                    current_score = float(opp.get("selection_score", opp.get("score", 0.0)) or 0.0)
                    best_score = float(best_opp.get("selection_score", best_opp.get("score", 0.0)) or 0.0) if best_opp is not None else float("-inf")
                    if best_opp is None or current_score > best_score:
                        best_opp = opp
                if best_opp is None:
                    continue
                bar = self._bar_at(best_opp["instrument"], self.config.granularity, current)
                if bar is None:
                    continue
                eff_thresh = float(best_opp.get("effective_threshold", 0)) or None
                kelly_mult = self._kelly_multiplier(label, float(best_opp["score"]), eff_thresh)
                # Cap Kelly for scalper — per industry consensus, scalpers trade smaller positions
                max_kelly = float(self.settings.get(f"{label}_MAX_KELLY", 0))
                if max_kelly > 0:
                    kelly_mult = min(kelly_mult, max_kelly)

                # ── Tier 1v2 composite sizing multiplier ──
                #   percentile  (Tier 2 §5) × flow (Tier 3 §7) × seasonality
                #   (Tier 3 §8) × decision-day (Tier 5 §8) × kill-switch soft
                #   cut (Tier 2 §9) × news REDUCE (Tier 3 §6).
                percentile_mult = self._percentile_risk_multiplier(label, float(best_opp["score"]))
                flow_mult = self._flow_risk_multiplier(best_opp["instrument"], current)
                seasonal_mult = self._seasonal_mult(label, best_opp["instrument"], current)
                decision_mult = self._decision_day_mult(best_opp["instrument"], macro_state, current)
                news_reduce = 0.5 if best_opp.get("_news_impact") == "REDUCE" else 1.0
                overlay_scale = percentile_mult * flow_mult * seasonal_mult * decision_mult * news_reduce * kill_risk_scale
                # Clip the overlay stack to a [0.1x, 3.5x] envelope so a single
                # extreme multiplier cannot blow out sizing.
                overlay_scale = max(0.1, min(3.5, overlay_scale))
                effective_kelly = kelly_mult * overlay_scale
                units = self._position_units(best_opp["instrument"], float(bar["close"]), float(best_opp["sl_pips"]), self.simulator.balance, effective_kelly, "USD", current)
                # Dynamic leverage: scale with market regime
                effective_leverage = self._compute_effective_leverage(regime_mult)
                max_units = abs(self.simulator.balance * effective_leverage / float(bar["close"]))
                units = min(units, max_units)
                best_opp["kelly_mult"] = effective_kelly
                best_opp["overlay_scale"] = round(overlay_scale, 4)
                best_opp["regime_at_entry"] = str(getattr(self._current_regime, "value", self._current_regime))
                news_active = self._is_pair_paused_by_news(macro_state, best_opp["instrument"], current)
                self.simulator.open_trade(best_opp, label, current, float(bar["close"]), units, self._estimate_spread_pips(best_opp["instrument"], current), news_active, execution_bar=bar)

                # Record signal for the reconciliation veto on future bars,
                # and extend the score history used by size_by_percentile.
                if self._tier3_reconciliation_enabled:
                    try:
                        self._reconciliation.record(Signal(
                            strategy=label,
                            instrument=best_opp["instrument"],
                            direction=best_opp["direction"],
                            score=float(best_opp["score"]),
                            bar_ts_utc=current,
                        ))
                    except Exception:
                        pass
                self._score_history[label].append(float(best_opp["score"]))
            current += step
        return self.simulator.equity_curve, self.simulator.closed_trades
