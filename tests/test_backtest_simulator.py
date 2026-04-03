from datetime import datetime, timezone

from backtest.simulator import SimulatorConfig, TradeSimulator


def test_simulator_closes_long_trade_at_take_profit():
    simulator = TradeSimulator(
        SimulatorConfig(
            initial_balance=10_000.0,
            max_open_trades=4,
            spread_floor_pips=0.8,
            spread_buffer_pips=0.2,
            slippage_pips=0.0,
            news_slippage_pips=0.0,
            round_trip_cost_pips=0.0,
            max_risk_per_trade=0.01,
        )
    )
    opened_at = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
    simulator.open_trade(
        {
            "instrument": "EUR_USD",
            "direction": "LONG",
            "tp_pips": 10.0,
            "sl_pips": 5.0,
            "trail_pips": None,
            "score": 50.0,
        },
        "SCALPER",
        opened_at,
        close_price=1.1000,
        units=10_000,
        spread_pips=1.0,
        news_active=False,
    )

    closed = simulator.update_open_trades(
        datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc),
        {
            "EUR_USD": {
                "open": 1.1000,
                "high": 1.1015,
                "low": 1.0998,
                "close": 1.1010,
            }
        },
        max_hours_map={"SCALPER": 2.0},
    )

    assert len(closed) == 1
    assert closed[0]["reason"] == "TAKE_PROFIT"
    assert simulator.open_trades == []
