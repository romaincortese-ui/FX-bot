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

    def open_trade(self, opp: dict[str, Any], label: str, opened_at: datetime, close_price: float, units: float, spread_pips: float, news_active: bool) -> dict[str, Any] | None:
        if units <= 0 or not self.can_open_trade():
            return None
        direction = opp["direction"]
        effective_spread = max(spread_pips + self.config.spread_buffer_pips, self.config.spread_floor_pips)
        slippage = self.config.news_slippage_pips if news_active else self.config.slippage_pips
        entry_price = self._entry_fill_price(opp["instrument"], direction, close_price, effective_spread, slippage)
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
        }
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
        pnl = pnl_pips * abs(float(trade["units"])) * pip_size(trade["instrument"])
        closed = dict(trade)
        closed.update({
            "exit_price": exit_price,
            "exit_time": exit_time,
            "reason": reason,
            "pnl_pips": round(pnl_pips, 2),
            "pnl": round(pnl, 2),
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

    def update_open_trades(self, current_time: datetime, bar_lookup: dict[str, dict[str, Any]], max_hours_map: dict[str, float], partial_tp_pct: float = 0.5) -> list[dict[str, Any]]:
        closed: list[dict[str, Any]] = []
        for trade in list(self.open_trades):
            bar = bar_lookup.get(trade["instrument"])
            if not bar or trade["entry_time"] >= current_time:
                continue
            high = float(bar["high"])
            low = float(bar["low"])
            close = float(bar["close"])
            if trade["direction"] == "LONG":
                trade["highest_price"] = max(float(trade.get("highest_price", trade["entry_price"])), high)
            else:
                trade["lowest_price"] = min(float(trade.get("lowest_price", trade["entry_price"])), low)

            if trade["label"] in {"TREND", "BREAKOUT"}:
                self._apply_partial_take_profit(trade, bar, current_time, partial_tp_pct)

            trail_pips = trade.get("trail_pips")
            if trail_pips:
                trail_distance = pips_to_price(trade["instrument"], float(trail_pips))
                if trade["direction"] == "LONG":
                    trade["sl_price"] = max(float(trade["sl_price"]), float(trade.get("highest_price", close)) - trail_distance)
                else:
                    trade["sl_price"] = min(float(trade["sl_price"]), float(trade.get("lowest_price", close)) + trail_distance)

            exit_reason = None
            raw_exit_price = None
            if trade["direction"] == "LONG":
                if low <= float(trade["sl_price"]):
                    exit_reason = "STOP_LOSS"
                    raw_exit_price = float(trade["sl_price"])
                elif high >= float(trade["tp_price"]):
                    exit_reason = "TAKE_PROFIT"
                    raw_exit_price = float(trade["tp_price"])
            else:
                if high >= float(trade["sl_price"]):
                    exit_reason = "STOP_LOSS"
                    raw_exit_price = float(trade["sl_price"])
                elif low <= float(trade["tp_price"]):
                    exit_reason = "TAKE_PROFIT"
                    raw_exit_price = float(trade["tp_price"])

            max_hours = max_hours_map.get(trade["label"], 24.0)
            held_hours = (current_time.timestamp() - trade["opened_ts"]) / 3600.0
            if exit_reason is None and held_hours >= max_hours:
                exit_reason = "TIMEOUT"
                raw_exit_price = close

            if exit_reason is None or raw_exit_price is None:
                continue

            fill_price = self._exit_fill_price(trade["instrument"], trade["direction"], raw_exit_price, trade["spread_pips"], self.config.slippage_pips)
            self.open_trades.remove(trade)
            closed.append(self._record_closed_trade(trade, current_time, fill_price, exit_reason))
        return closed

    def mark_equity(self, timestamp: datetime, mark_prices: dict[str, float]) -> float:
        unrealized = 0.0
        for trade in self.open_trades:
            current = mark_prices.get(trade["instrument"])
            if current is None:
                continue
            pnl_pips = price_to_pips(trade["instrument"], current - trade["entry_price"])
            if trade["direction"] == "SHORT":
                pnl_pips *= -1
            unrealized += pnl_pips * abs(float(trade["units"])) * pip_size(trade["instrument"])
        equity = self.balance + unrealized
        self.equity_curve.append({"time": timestamp.isoformat(), "balance": round(self.balance, 2), "equity": round(equity, 2)})
        return equity
