from backtest.reporter import build_backtest_report, build_trade_calibration


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


def test_build_trade_calibration_groups_by_strategy_pair_and_session():
    trades = [
        {"label": "TREND", "instrument": "EUR_USD", "session_at_entry": "LONDON", "pnl": 50.0, "pnl_pips": 10.0},
        {"label": "TREND", "instrument": "EUR_USD", "session_at_entry": "LONDON", "pnl": -30.0, "pnl_pips": -6.0},
        {"label": "TREND", "instrument": "USD_JPY", "session_at_entry": "TOKYO", "pnl": -20.0, "pnl_pips": -4.0},
    ]

    calibration = build_trade_calibration(trades)

    assert calibration["total_trades"] == 3
    assert calibration["by_strategy"]["TREND"]["trades"] == 3
    assert calibration["by_strategy_pair"]["TREND"]["EUR_USD"]["trades"] == 2
    assert calibration["by_strategy_pair_session"]["TREND"]["EUR_USD"]["LONDON"]["expectancy_pips"] == 2.0
