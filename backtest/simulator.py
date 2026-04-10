from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fxbot.fx_math import pip_size, pips_to_price, price_to_pips


@dataclass(slots=True)
class SimulatorConfig:
    initial_balance: float
    max_open_trades: int
    spread_floor_pips: float
    spread_buffer_pips: float
    slippage_pips: float
    news_slippage_pips: float
    round_trip_cost_pips: float
    max_risk_per_trade: float


class TradeSimulator:
    def __init__(self, config: SimulatorConfig):
        self.config = config
        self.balance = config.initial_balance
        self.open_trades: list[dict[str, Any]] = []
        self.closed_trades: list[dict[str, Any]] = []
        self.equity_curve: list[dict[str, float | str]] = []

    def can_open_trade(self) -> bool:
        return len(self.open_trades) < self.config.max_open_trades

    def _entry_fill_price(self, instrument: str, direction: str, close_price: float, spread_pips: float, slippage_pips: float) -> float:
        offset = pips_to_price(instrument, (spread_pips / 2.0) + slippage_pips)
        return close_price + offset if direction == "LONG" else close_price - offset

    def _exit_fill_price(self, instrument: str, direction: str, raw_price: float, spread_pips: float, slippage_pips: float) -> float:
        offset = pips_to_price(instrument, (spread_pips / 2.0) + slippage_pips)
        return raw_price - offset if direction == "LONG" else raw_price + offset

    def _copy_trade_diagnostics(self, trade: dict[str, Any], opp: dict[str, Any]) -> None:
        diagnostic_keys = (
            "selection_score",
            "effective_threshold",
            "score_margin",
            "macro_bias",
            "session_multiplier",
            "session_aggression",
            "session_is_overlap",
            "spread_pips",
            "calibration_threshold_offset",
            "calibration_risk_mult",
            "calibration_source",
            "rsi_delta",
            "vol_ratio",
            "ema50_gap_4h",
            "squeeze_bars",
            "bb_percentile",
            "momentum_5d",
            "pullback_depth",
            "crossed_now",
        )
        for key in diagnostic_keys:
            if key in opp:
                trade[key] = opp[key]

    def open_trade(self, opp: dict[str, Any], label: str, opened_at: datetime, close_price: float, units: float, spread_pips: float, news_active: bool, execution_bar: dict[str, Any] | None = None) -> dict[str, Any] | None:
        if units <= 0 or not self.can_open_trade():
            return None
        direction = opp["direction"]
        effective_spread = max(spread_pips + self.config.spread_buffer_pips, self.config.spread_floor_pips)
        slippage = self.config.news_slippage_pips if news_active else self.config.slippage_pips
        if execution_bar and float(execution_bar.get("bid_close", 0) or 0) > 0 and float(execution_bar.get("ask_close", 0) or 0) > 0:
            raw_entry = float(execution_bar["ask_close"]) if direction == "LONG" else float(execution_bar["bid_close"])
            entry_price = raw_entry + pips_to_price(opp["instrument"], slippage if direction == "LONG" else -slippage)
            execution_mode = "bid_ask"
        else:
            entry_price = self._entry_fill_price(opp["instrument"], direction, close_price, effective_spread, slippage)
            execution_mode = "synthetic"
        ps = pip_size(opp["instrument"])
        if direction == "LONG":
            tp_price = entry_price + opp["tp_pips"] * ps
            sl_price = entry_price - opp["sl_pips"] * ps
        else:
            tp_price = entry_price - opp["tp_pips"] * ps
            sl_price = entry_price + opp["sl_pips"] * ps
        trade = {
            "id": f"BT_{len(self.closed_trades) + len(self.open_trades) + 1}",
            "label": label,
            "instrument": opp["instrument"],
            "direction": direction,
            "entry_price": entry_price,
            "entry_time": opened_at,
            "opened_ts": opened_at.timestamp(),
            "units": units,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "trail_pips": opp.get("trail_pips"),
            "tp_pips": opp["tp_pips"],
            "sl_pips": opp["sl_pips"],
            "spread_pips": effective_spread,
            "score": opp.get("score", 0.0),
            "rsi": opp.get("rsi"),
            "atr": opp.get("atr"),
            "atr_pct": opp.get("atr_pct"),
            "entry_signal": opp.get("entry_signal", "UNKNOWN"),
            "session_at_entry": opp.get("session_name", "UNKNOWN"),
            "kelly_mult": opp.get("kelly_mult", 1.0),
            "highest_price": entry_price,
            "lowest_price": entry_price,
            "partial_tp_hit": False,
            "partial_tp_price": None,
            "partial_tp_pips": opp.get("partial_tp_pips"),
            "macro_confidence": opp.get("macro_confidence", 0.0),
            "regime_multiplier": opp.get("regime_multiplier", 1.0),
            "news_active_at_entry": news_active,
            "execution_mode": execution_mode,
            "breakeven_done": False,
        }
        self._copy_trade_diagnostics(trade, opp)
        if label == "TREND" and opp.get("partial_tp_pips"):
            if direction == "LONG":
                trade["partial_tp_price"] = entry_price + opp["partial_tp_pips"] * ps
            else:
                trade["partial_tp_price"] = entry_price - opp["partial_tp_pips"] * ps
        self.open_trades.append(trade)
        return trade

    def _record_closed_trade(self, trade: dict[str, Any], exit_time: datetime, exit_price: float, reason: str) -> dict[str, Any]:
        direction = trade["direction"]
        pnl_pips = price_to_pips(trade["instrument"], exit_price - trade["entry_price"])
        if direction == "SHORT":
            pnl_pips *= -1
        pnl_pips -= self.config.round_trip_cost_pips
        pnl_quote = pnl_pips * abs(float(trade["units"])) * pip_size(trade["instrument"])
        # Convert PnL from quote currency to account currency (USD)
        _, quote = trade["instrument"].split("_")
        if quote == "USD":
            pnl = pnl_quote
        else:
            # For USD_JPY etc., pnl_quote is in JPY — convert using exit price
            pnl = pnl_quote / exit_price if exit_price > 0 else pnl_quote
        highest_price = float(trade.get("highest_price", trade["entry_price"]))
        lowest_price = float(trade.get("lowest_price", trade["entry_price"]))
        if direction == "LONG":
            mfe_pips = max(0.0, price_to_pips(trade["instrument"], highest_price - trade["entry_price"]))
            mae_pips = max(0.0, price_to_pips(trade["instrument"], trade["entry_price"] - lowest_price))
        else:
            mfe_pips = max(0.0, price_to_pips(trade["instrument"], trade["entry_price"] - lowest_price))
            mae_pips = max(0.0, price_to_pips(trade["instrument"], highest_price - trade["entry_price"]))
        closed = dict(trade)
        closed.update({
            "exit_price": exit_price,
            "exit_time": exit_time,
            "reason": reason,
            "pnl_pips": round(pnl_pips, 2),
            "pnl": round(pnl, 2),
            "mfe_pips": round(mfe_pips, 2),
            "mae_pips": round(mae_pips, 2),
            "held_minutes": round((exit_time.timestamp() - trade["opened_ts"]) / 60.0, 2),
        })
        self.balance += pnl
        self.closed_trades.append(closed)
        return closed

    def _apply_partial_take_profit(self, trade: dict[str, Any], bar: dict[str, Any], exit_time: datetime, partial_pct: float) -> None:
        if trade.get("partial_tp_hit") or not trade.get("partial_tp_price"):
            return
        hit = False
        if trade["direction"] == "LONG" and bar["high"] >= trade["partial_tp_price"]:
            hit = True
        elif trade["direction"] == "SHORT" and bar["low"] <= trade["partial_tp_price"]:
            hit = True
        if not hit:
            return
        partial_units = abs(float(trade["units"])) * partial_pct
        if partial_units <= 0:
            trade["partial_tp_hit"] = True
            return
        fill_price = self._exit_fill_price(trade["instrument"], trade["direction"], trade["partial_tp_price"], trade["spread_pips"], self.config.slippage_pips)
        partial_trade = dict(trade)
        partial_trade["units"] = partial_units
        self._record_closed_trade(partial_trade, exit_time, fill_price, "PARTIAL_TP")
        remaining = abs(float(trade["units"])) - partial_units
        trade["units"] = remaining if trade["direction"] == "LONG" else -remaining
        trade["partial_tp_hit"] = True
        buffer = pips_to_price(trade["instrument"], 2.0)
        trade["sl_price"] = trade["entry_price"] + buffer if trade["direction"] == "LONG" else trade["entry_price"] - buffer

    def update_open_trades(self, current_time: datetime, bar_lookup: dict[str, dict[str, Any]],
                           sl_pct: float = -0.40, peak_trail_pct: float = 0.015,
                           flat_hours: float = 48.0, review_days: int = 7,
                           review_poor_threshold: float = -0.10,
                           review_trend_bars: int = 50,
                           candle_fetch: Any = None) -> list[dict[str, Any]]:
        """Unified exit logic for all strategies.

        1. Hard SL at ``sl_pct`` of entry (default -40%).
        2. Dynamic breakeven = entry ± spread cost.  Once broken,
           track the peak and close if price drops ``peak_trail_pct``
           (1.5 %) from that peak.
        3. Flat exit: close only if above breakeven AND held > ``flat_hours``.
        4. 7-day review: between -10 % and breakeven → estimate trend →
           close if probability of recovery < 70 %.
           Below -10 % → flag for manual review but keep alive.
        """
        closed: list[dict[str, Any]] = []
        review_seconds = review_days * 86400

        for trade in list(self.open_trades):
            bar = bar_lookup.get(trade["instrument"])
            if not bar or trade["entry_time"] >= current_time:
                continue

            high = float(bar["high"])
            low = float(bar["low"])
            close = float(bar["close"])
            bid_close = float(bar.get("bid_close", 0) or 0)
            ask_close = float(bar.get("ask_close", 0) or 0)
            has_bid_ask = bid_close > 0 and ask_close > 0

            # Mark price for this bar
            if trade["direction"] == "LONG":
                mark = bid_close if has_bid_ask else close
            else:
                mark = ask_close if has_bid_ask else close

            trade["highest_price"] = max(float(trade.get("highest_price", trade["entry_price"])), high)
            trade["lowest_price"] = min(float(trade.get("lowest_price", trade["entry_price"])), low)

            entry = trade["entry_price"]
            held_seconds = current_time.timestamp() - trade["opened_ts"]
            held_hours = held_seconds / 3600.0

            # ── P&L % relative to entry ───────────────────────────
            if trade["direction"] == "LONG":
                pnl_pct = (mark - entry) / entry
            else:
                pnl_pct = (entry - mark) / entry

            # ── Dynamic breakeven: entry + spread cost ────────────
            spread_cost_pct = float(trade.get("spread_pips", 0)) * pip_size(trade["instrument"]) / entry
            breakeven_pct = spread_cost_pct  # above this = profitable

            # ── Track peak P&L % since entry ──────────────────────
            if trade["direction"] == "LONG":
                peak_pnl_pct = (float(trade["highest_price"]) - entry) / entry
            else:
                peak_pnl_pct = (entry - float(trade["lowest_price"])) / entry

            exit_reason = None
            raw_exit_price = mark

            # 1. Hard SL at -40%
            if pnl_pct <= sl_pct:
                exit_reason = "STOP_LOSS"

            # 2. Above breakeven → trail from peak
            if exit_reason is None and peak_pnl_pct > breakeven_pct and pnl_pct > breakeven_pct:
                # We have been above breakeven.  Check giveback from peak.
                drawdown_from_peak = peak_pnl_pct - pnl_pct
                if drawdown_from_peak >= peak_trail_pct:
                    exit_reason = "PEAK_TRAIL"

            # 3. Flat exit: above breakeven AND held > flat_hours
            if exit_reason is None and held_hours >= flat_hours:
                if pnl_pct >= breakeven_pct and pnl_pct < breakeven_pct + peak_trail_pct:
                    exit_reason = "FLAT_EXIT"

            # 4. Long-term review (7 days)
            if exit_reason is None and held_seconds >= review_seconds:
                if review_poor_threshold <= pnl_pct < breakeven_pct:
                    # Between -10% and breakeven → estimate trend probability
                    recovery_prob = self._estimate_recovery_probability(
                        trade, bar_lookup, candle_fetch, review_trend_bars)
                    if recovery_prob < 0.70:
                        exit_reason = "REVIEW_CLOSE"
                elif pnl_pct < review_poor_threshold:
                    # Below -10% → flag but keep alive
                    trade["_flagged_manual_review"] = True

            if exit_reason is None:
                continue

            # Fill
            if has_bid_ask:
                if trade["direction"] == "LONG":
                    fill_price = mark - pips_to_price(trade["instrument"], self.config.slippage_pips)
                else:
                    fill_price = mark + pips_to_price(trade["instrument"], self.config.slippage_pips)
            else:
                fill_price = self._exit_fill_price(trade["instrument"], trade["direction"],
                                                   mark, trade["spread_pips"], self.config.slippage_pips)
            self.open_trades.remove(trade)
            closed.append(self._record_closed_trade(trade, current_time, fill_price, exit_reason))
        return closed

    def _estimate_recovery_probability(self, trade: dict[str, Any],
                                       bar_lookup: dict[str, dict[str, Any]],
                                       candle_fetch: Any,
                                       trend_bars: int) -> float:
        """Simple trend-direction check: what fraction of recent bars
        moved favourably?  Returns 0.0-1.0."""
        if candle_fetch is None:
            return 0.5  # no data → neutral, won't trigger close
        try:
            df = candle_fetch(trade["instrument"], "H1", trend_bars)
        except Exception:
            return 0.5
        if df is None or len(df) < 10:
            return 0.5
        closes = df["close"].values
        if trade["direction"] == "LONG":
            favourable = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
        else:
            favourable = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])
        return favourable / (len(closes) - 1)

    def mark_equity(self, timestamp: datetime, mark_prices: dict[str, float]) -> float:
        unrealized = 0.0
        for trade in self.open_trades:
            current = mark_prices.get(trade["instrument"])
            if current is None:
                continue
            pnl_pips = price_to_pips(trade["instrument"], current - trade["entry_price"])
            if trade["direction"] == "SHORT":
                pnl_pips *= -1
            pnl_quote = pnl_pips * abs(float(trade["units"])) * pip_size(trade["instrument"])
            # Convert PnL from quote currency to account currency (USD)
            _, quote = trade["instrument"].split("_")
            if quote == "USD":
                unrealized += pnl_quote
            else:
                unrealized += pnl_quote / current if current > 0 else pnl_quote
        equity = self.balance + unrealized
        self.equity_curve.append({"time": timestamp.isoformat(), "balance": round(self.balance, 2), "equity": round(equity, 2)})
        return equity
