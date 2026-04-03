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


def test_simulator_uses_bid_ask_bar_for_entry_and_exit():
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
    trade = simulator.open_trade(
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
        execution_bar={"bid_close": 1.0999, "ask_close": 1.1001},
    )

    assert trade is not None
    assert trade["entry_price"] == 1.1001
    assert trade["execution_mode"] == "bid_ask"

    closed = simulator.update_open_trades(
        datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc),
        {
            "EUR_USD": {
                "open": 1.1000,
                "high": 1.1012,
                "low": 1.0998,
                "close": 1.1010,
                "bid_high": 1.1012,
                "bid_low": 1.1000,
                "bid_close": 1.1011,
                "ask_high": 1.1014,
                "ask_low": 1.1002,
                "ask_close": 1.1013,
            }
        },
        max_hours_map={"SCALPER": 2.0},
    )

    assert len(closed) == 1
    assert closed[0]["reason"] == "TAKE_PROFIT"


def test_simulator_records_trade_diagnostics_and_excursions():
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
    trade = simulator.open_trade(
        {
            "instrument": "EUR_USD",
            "direction": "LONG",
            "tp_pips": 20.0,
            "sl_pips": 10.0,
            "trail_pips": None,
            "score": 62.0,
            "selection_score": 58.0,
            "effective_threshold": 47.0,
            "score_margin": 11.0,
            "macro_bias": "LONG_ONLY",
            "session_name": "LONDON",
            "session_multiplier": 0.9,
            "session_aggression": "HIGH",
            "session_is_overlap": False,
            "calibration_threshold_offset": 2.0,
            "calibration_risk_mult": 0.8,
            "calibration_source": "pair",
        },
        "TREND",
        opened_at,
        close_price=1.1000,
        units=10_000,
        spread_pips=1.0,
        news_active=False,
    )

    assert trade is not None
    closed = simulator.update_open_trades(
        datetime(2024, 1, 1, 10, 15, tzinfo=timezone.utc),
        {
            "EUR_USD": {
                "open": 1.1000,
                "high": 1.1018,
                "low": 1.0992,
                "close": 1.1014,
            }
        },
        max_hours_map={"TREND": 0.1},
    )

    assert len(closed) == 1
    assert closed[0]["reason"] == "TIMEOUT"
    assert closed[0]["selection_score"] == 58.0
    assert closed[0]["effective_threshold"] == 47.0
    assert closed[0]["score_margin"] == 11.0
    assert closed[0]["macro_bias"] == "LONG_ONLY"
    assert closed[0]["mfe_pips"] > 0
    assert closed[0]["mae_pips"] > 0
