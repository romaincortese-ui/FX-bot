from datetime import datetime, timezone

from backtest.simulator import SimulatorConfig, TradeSimulator


def test_simulator_closes_long_trade_at_stop_loss():
    """Trade hits -40% P&L → STOP_LOSS exit."""
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

    # Entry price ≈ 1.1006.  40% drop → price ≈ 0.66, but for FX we
    # simulate a bar whose close is far below to trigger -40%.
    entry = simulator.open_trades[0]["entry_price"]
    crash_price = entry * 0.58  # well below -40%
    closed = simulator.update_open_trades(
        datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc),
        {
            "EUR_USD": {
                "open": entry,
                "high": entry,
                "low": crash_price,
                "close": crash_price,
            }
        },
    )

    assert len(closed) == 1
    assert closed[0]["reason"] == "STOP_LOSS"
    assert simulator.open_trades == []


def test_simulator_uses_bid_ask_bar_for_entry_and_peak_trail():
    """Trade enters via bid/ask, then trails from peak and closes on 1.5% drop."""
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

    entry = trade["entry_price"]
    # First bar: push price up 3% to establish a peak
    peak = entry * 1.03
    simulator.update_open_trades(
        datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        {
            "EUR_USD": {
                "open": entry, "high": peak, "low": entry,
                "close": peak,
                "bid_high": peak, "bid_low": entry,
                "bid_close": peak,
                "ask_high": peak * 1.0002, "ask_low": entry * 1.0002,
                "ask_close": peak * 1.0002,
            }
        },
    )
    # Second bar: drop 2% from peak (>1.5% trail threshold)
    drop_price = peak * 0.98
    closed = simulator.update_open_trades(
        datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
        {
            "EUR_USD": {
                "open": peak, "high": peak, "low": drop_price,
                "close": drop_price,
                "bid_high": peak, "bid_low": drop_price,
                "bid_close": drop_price,
                "ask_high": peak * 1.0002, "ask_low": drop_price * 1.0002,
                "ask_close": drop_price * 1.0002,
            }
        },
    )

    assert len(closed) == 1
    assert closed[0]["reason"] == "PEAK_TRAIL"


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
    # Push to flat exit: above breakeven, held > 48 hours via flat_hours=0.1
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
        flat_hours=0.1,  # very short for testing
    )

    assert len(closed) == 1
    assert closed[0]["reason"] == "FLAT_EXIT"
    assert closed[0]["selection_score"] == 58.0
    assert closed[0]["effective_threshold"] == 47.0
    assert closed[0]["score_margin"] == 11.0
    assert closed[0]["macro_bias"] == "LONG_ONLY"
    assert closed[0]["mfe_pips"] > 0
    assert closed[0]["mae_pips"] > 0
