from backtest.reporter import build_backtest_report


def test_build_backtest_report_calculates_core_metrics():
    equity_curve = [
        {"time": "2024-01-01T00:00:00+00:00", "balance": 10000.0, "equity": 10000.0},
        {"time": "2024-01-01T01:00:00+00:00", "balance": 10050.0, "equity": 10050.0},
        {"time": "2024-01-01T02:00:00+00:00", "balance": 10020.0, "equity": 10020.0},
    ]
    trades = [
        {"label": "TREND", "pnl": 50.0},
        {"label": "TREND", "pnl": -30.0},
        {"label": "SCALPER", "pnl": 20.0},
    ]

    report = build_backtest_report(equity_curve, trades)

    assert report["total_trades"] == 3
    assert round(report["win_rate"], 4) == round(2 / 3, 4)
    assert report["by_strategy"]["TREND"]["trades"] == 2
    assert report["total_pnl"] == 40.0
