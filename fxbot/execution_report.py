"""Weekly execution-quality report (Tier 3 §28 of consultant assessment).

Combines ``trade_history`` (P&L, win rate, expectancy per strategy) with
the Tier 2 slippage log (mean/median/p95 slip pips per strategy) into a
single compact report suitable for Telegram or Slack delivery.

Pure functions — the caller supplies the raw lists and receives a dict
or a pre-formatted message string.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class StrategyStats:
    strategy: str
    trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    expectancy_pips: float
    profit_factor: float
    mean_slip_pips: float
    p95_slip_pips: float


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        out = datetime.fromisoformat(ts)
    except (TypeError, ValueError):
        return None
    if out.tzinfo is None:
        out = out.replace(tzinfo=timezone.utc)
    return out


def filter_recent(
    history: Iterable[Mapping],
    *,
    now: datetime | None = None,
    days: int = 7,
) -> list[Mapping]:
    """Return trades closed in the last ``days`` days, UTC."""
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=int(days))
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    out: list[Mapping] = []
    for trade in history:
        closed = _parse_iso(str(trade.get("closed_at", "")))
        if closed is None:
            continue
        if closed >= cutoff:
            out.append(trade)
    return out


def aggregate_by_strategy(
    trades: Sequence[Mapping],
    slippage_by_strategy: Mapping[str, Mapping[str, float]] | None = None,
) -> dict[str, StrategyStats]:
    buckets: dict[str, list[Mapping]] = {}
    for trade in trades:
        key = str(trade.get("label", "") or "UNKNOWN").upper()
        buckets.setdefault(key, []).append(trade)
    slip = slippage_by_strategy or {}
    out: dict[str, StrategyStats] = {}
    for strategy, bucket in buckets.items():
        n = len(bucket)
        if n == 0:
            continue
        wins = sum(1 for t in bucket if float(t.get("pnl", 0.0) or 0.0) > 0)
        losses = n - wins
        total_pnl = sum(float(t.get("pnl", 0.0) or 0.0) for t in bucket)
        avg_pnl = total_pnl / n
        pips = [float(t.get("pnl_pips", 0.0) or 0.0) for t in bucket]
        expectancy = sum(pips) / n if pips else 0.0
        gross_profit = sum(p for p in pips if p > 0)
        gross_loss = -sum(p for p in pips if p < 0)
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (
            999.0 if gross_profit > 0 else 0.0
        )
        slip_stats = slip.get(strategy, {}) if isinstance(slip.get(strategy, {}), Mapping) else {}
        mean_slip = float(slip_stats.get("mean_slip_pips", 0.0) or 0.0)
        p95_slip = float(slip_stats.get("p95_slip_pips", 0.0) or 0.0)
        out[strategy] = StrategyStats(
            strategy=strategy,
            trades=n,
            wins=wins,
            losses=losses,
            win_rate=wins / n,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
            expectancy_pips=expectancy,
            profit_factor=profit_factor,
            mean_slip_pips=mean_slip,
            p95_slip_pips=p95_slip,
        )
    return out


def build_weekly_report(
    *,
    trade_history: Sequence[Mapping],
    slippage_by_strategy: Mapping[str, Mapping[str, float]] | None = None,
    now: datetime | None = None,
    days: int = 7,
) -> dict:
    recent = filter_recent(trade_history, now=now, days=days)
    per_strategy = aggregate_by_strategy(recent, slippage_by_strategy=slippage_by_strategy)
    total_trades = sum(s.trades for s in per_strategy.values())
    total_pnl = sum(s.total_pnl for s in per_strategy.values())
    total_wins = sum(s.wins for s in per_strategy.values())
    win_rate = total_wins / total_trades if total_trades else 0.0
    return {
        "period_days": int(days),
        "generated_at_utc": (now or datetime.now(timezone.utc)).isoformat(),
        "total_trades": total_trades,
        "total_pnl": total_pnl,
        "win_rate": win_rate,
        "per_strategy": {
            s.strategy: {
                "trades": s.trades,
                "wins": s.wins,
                "losses": s.losses,
                "win_rate": s.win_rate,
                "total_pnl": s.total_pnl,
                "expectancy_pips": s.expectancy_pips,
                "profit_factor": s.profit_factor,
                "mean_slip_pips": s.mean_slip_pips,
                "p95_slip_pips": s.p95_slip_pips,
            }
            for s in per_strategy.values()
        },
    }


def format_weekly_telegram(report: Mapping, *, currency: str = "£") -> str:
    """Format a ``build_weekly_report`` payload as a Telegram HTML message."""
    lines: list[str] = []
    period = int(report.get("period_days", 7) or 7)
    trades = int(report.get("total_trades", 0) or 0)
    pnl = float(report.get("total_pnl", 0.0) or 0.0)
    wr = float(report.get("win_rate", 0.0) or 0.0)
    lines.append(f"📊 <b>Execution Summary</b> (last {period}d)")
    lines.append("━━━━━━━━━━━━━━━")
    lines.append(f"Trades: {trades} | Win rate: {wr*100:.1f}%")
    lines.append(f"P&L: {currency}{pnl:+.2f}")
    per_strategy = report.get("per_strategy", {}) or {}
    if per_strategy:
        lines.append("")
        lines.append("<b>By strategy</b>")
        for strat, s in sorted(per_strategy.items(), key=lambda kv: -float(kv[1].get("total_pnl", 0.0))):
            lines.append(
                f"• {strat}: {int(s.get('trades',0))}t "
                f"{float(s.get('win_rate',0.0))*100:.0f}%W "
                f"{currency}{float(s.get('total_pnl',0.0)):+.2f} "
                f"exp={float(s.get('expectancy_pips',0.0)):+.1f}p "
                f"PF={float(s.get('profit_factor',0.0)):.2f} "
                f"slip={float(s.get('mean_slip_pips',0.0)):+.2f}/{float(s.get('p95_slip_pips',0.0)):+.2f}p"
            )
    return "\n".join(lines)
