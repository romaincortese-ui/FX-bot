from datetime import datetime, timedelta, timezone

from fxbot.execution_report import (
    aggregate_by_strategy,
    build_weekly_report,
    filter_recent,
    format_weekly_telegram,
)


def _trade(label, pnl, pnl_pips, days_ago):
    closed = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return {"label": label, "pnl": pnl, "pnl_pips": pnl_pips, "closed_at": closed.isoformat()}


def test_filter_recent_excludes_old_trades():
    history = [
        _trade("TREND", 10, 5, 1),
        _trade("TREND", -5, -3, 30),
    ]
    recent = filter_recent(history, days=7)
    assert len(recent) == 1


def test_aggregate_by_strategy():
    history = [
        _trade("TREND", 20, 10, 1),
        _trade("TREND", -10, -5, 2),
        _trade("SCALPER", 5, 2, 1),
    ]
    stats = aggregate_by_strategy(history)
    assert stats["TREND"].trades == 2
    assert stats["TREND"].wins == 1
    assert stats["TREND"].win_rate == 0.5
    assert stats["TREND"].total_pnl == 10
    assert stats["TREND"].profit_factor == 2.0  # 10 / 5
    assert stats["SCALPER"].trades == 1


def test_aggregate_includes_slippage_stats():
    history = [_trade("TREND", 10, 5, 1)]
    slip = {"TREND": {"mean_slip_pips": 0.3, "p95_slip_pips": 0.9}}
    stats = aggregate_by_strategy(history, slippage_by_strategy=slip)
    assert stats["TREND"].mean_slip_pips == 0.3
    assert stats["TREND"].p95_slip_pips == 0.9


def test_build_weekly_report_structure():
    history = [
        _trade("TREND", 20, 10, 1),
        _trade("SCALPER", -10, -5, 2),
    ]
    report = build_weekly_report(trade_history=history, days=7)
    assert report["total_trades"] == 2
    assert report["total_pnl"] == 10
    assert report["win_rate"] == 0.5
    assert "TREND" in report["per_strategy"]


def test_format_weekly_telegram_contains_key_fields():
    report = {
        "period_days": 7,
        "total_trades": 3,
        "total_pnl": 12.5,
        "win_rate": 0.67,
        "per_strategy": {
            "TREND": {
                "trades": 2, "wins": 2, "losses": 0, "win_rate": 1.0,
                "total_pnl": 15.0, "expectancy_pips": 7.5,
                "profit_factor": 999.0, "mean_slip_pips": 0.2, "p95_slip_pips": 0.5,
            }
        },
    }
    msg = format_weekly_telegram(report)
    assert "Execution Summary" in msg
    assert "TREND" in msg
    assert "7d" in msg


def test_empty_history_returns_empty_report():
    report = build_weekly_report(trade_history=[])
    assert report["total_trades"] == 0
    assert report["per_strategy"] == {}
