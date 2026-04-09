import importlib
from datetime import datetime, timezone


def test_run_uses_global_loss_streak_state(monkeypatch) -> None:
    main = importlib.import_module("main")

    monkeypatch.setattr(main, "_bootstrap_runtime", lambda: None)
    monkeypatch.setattr(main, "poll_telegram_commands", lambda: None)
    monkeypatch.setattr(main, "is_weekend", lambda: False)
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 1000.0})
    monkeypatch.setattr(main, "publish_bot_runtime_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON"})
    monkeypatch.setattr(main, "refresh_macro_filters", lambda: False)
    monkeypatch.setattr(main, "refresh_macro_news", lambda: False)
    monkeypatch.setattr(main, "refresh_trade_calibration", lambda: False)
    monkeypatch.setattr(main, "update_macro_news_pause", lambda: None)
    monkeypatch.setattr(main, "fetch_candles", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "refresh_dynamic_watchlist", lambda: None)
    monkeypatch.setattr(main, "probe_pair_health", lambda: None)
    monkeypatch.setattr(main, "process_pending_close_retries", lambda: None)
    monkeypatch.setattr(main, "start_scan_cycle", lambda: None)
    monkeypatch.setattr(main, "is_rollover_window", lambda: False)
    monkeypatch.setattr(main, "save_state", lambda: None)
    monkeypatch.setattr(main, "telegram", lambda *args, **kwargs: None)

    def stop_after_loss_streak_check(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main, "get_effective_scan_pairs", stop_after_loss_streak_check)

    main.trade_history = []
    main.open_trades = []
    main._paused = False
    main._consecutive_losses = 0
    main._streak_paused_at = 0.0
    main._session_loss_paused_until = 0.0

    main.run()


def test_run_sends_heartbeat_while_idle_on_weekend(monkeypatch) -> None:
    main = importlib.import_module("main")
    heartbeat_calls: list[tuple[float, str]] = []
    telegram_messages: list[str] = []

    monkeypatch.setattr(main, "_bootstrap_runtime", lambda: None)
    monkeypatch.setattr(main, "poll_telegram_commands", lambda: None)
    monkeypatch.setattr(main, "is_weekend", lambda: True)
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 1234.5})
    monkeypatch.setattr(main, "send_heartbeat", lambda balance, status="running": heartbeat_calls.append((balance, status)))
    monkeypatch.setattr(main, "telegram", lambda message, parse_mode="HTML": telegram_messages.append(message))
    monkeypatch.setattr(main, "log_idle_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "save_state", lambda: None)
    monkeypatch.setattr(
        main,
        "format_time_utc_and_local",
        lambda dt: "Sun 2026-04-05 21:00 UTC / Sun 2026-04-05 22:00 BST",
    )

    def stop_sleep(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main, "sleep_with_command_poll", stop_sleep)
    main._weekend_mode_active = False
    main._entry_pause_reason = ""

    main.run()

    assert heartbeat_calls == [(1234.5, "idle_weekend")]
    assert any(
        "No new trades will be entered until Sun 2026-04-05 21:00 UTC / Sun 2026-04-05 22:00 BST" in message
        for message in telegram_messages
    )
    assert main._entry_pause_reason == "weekend"


def test_build_entry_pause_notice_identifies_broker_closure(monkeypatch) -> None:
    main = importlib.import_module("main")

    monkeypatch.setattr(main, "get_pair_health_reason", lambda instrument: "market closed at broker")

    reason, title, body = main._build_entry_pause_notice(
        {"name": "LONDON", "aggression": "HIGH", "pairs_allowed": ["EUR_USD"]},
        ["EUR_USD"],
        "pairs blocked",
    )

    assert reason == "broker_closed"
    assert "Entries paused on OANDA" in title
    assert "bank holidays" in body


def test_run_does_not_resume_until_tradable_pairs_return(monkeypatch) -> None:
    main = importlib.import_module("main")
    telegram_messages: list[str] = []

    monkeypatch.setattr(main, "_bootstrap_runtime", lambda: None)
    monkeypatch.setattr(main, "poll_telegram_commands", lambda: None)
    monkeypatch.setattr(main, "is_weekend", lambda: False)
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 1000.0})
    monkeypatch.setattr(main, "publish_bot_runtime_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON", "aggression": "HIGH", "pairs_allowed": ["EUR_USD"]})
    monkeypatch.setattr(main, "telegram", lambda message, parse_mode="HTML": telegram_messages.append(message))
    monkeypatch.setattr(main, "save_state", lambda: None)
    monkeypatch.setattr(main, "refresh_macro_filters", lambda: False)
    monkeypatch.setattr(main, "refresh_macro_news", lambda: False)
    monkeypatch.setattr(main, "refresh_trade_calibration", lambda: False)
    monkeypatch.setattr(main, "update_macro_news_pause", lambda: None)
    monkeypatch.setattr(main, "fetch_candles", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "refresh_dynamic_watchlist", lambda: None)
    monkeypatch.setattr(main, "probe_pair_health", lambda: None)
    monkeypatch.setattr(main, "process_pending_close_retries", lambda: None)
    monkeypatch.setattr(main, "start_scan_cycle", lambda: None)
    monkeypatch.setattr(main, "is_rollover_window", lambda: False)
    monkeypatch.setattr(main, "get_effective_scan_pairs", lambda session: (["EUR_USD"], [], [], "pairs blocked"))
    monkeypatch.setattr(main, "get_pair_health_reason", lambda instrument: "spread 4.2 > 1.5")

    def stop_after_notification(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main, "send_heartbeat", lambda *args, **kwargs: stop_after_notification())

    main.trade_history = []
    main.open_trades = []
    main._paused = False
    main._consecutive_losses = 0
    main._streak_paused_at = 0.0
    main._session_loss_paused_until = 0.0
    main._weekend_mode_active = True
    main._entry_pause_reason = "weekend"

    main.run()

    assert any("spreads are still too wide" in message for message in telegram_messages)
    assert not any("Entries available again" in message for message in telegram_messages)
    assert main._weekend_mode_active is False
    assert main._entry_pause_reason == "spread_wide"


def test_run_announces_entries_available_again_when_tradable_pairs_return(monkeypatch) -> None:
    main = importlib.import_module("main")
    telegram_messages: list[str] = []

    monkeypatch.setattr(main, "_bootstrap_runtime", lambda: None)
    monkeypatch.setattr(main, "poll_telegram_commands", lambda: None)
    monkeypatch.setattr(main, "is_weekend", lambda: False)
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 1000.0})
    monkeypatch.setattr(main, "publish_bot_runtime_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON", "aggression": "HIGH", "pairs_allowed": ["EUR_USD"]})
    monkeypatch.setattr(main, "telegram", lambda message, parse_mode="HTML": telegram_messages.append(message))
    monkeypatch.setattr(main, "save_state", lambda: None)
    monkeypatch.setattr(main, "refresh_macro_filters", lambda: False)
    monkeypatch.setattr(main, "refresh_macro_news", lambda: False)
    monkeypatch.setattr(main, "refresh_trade_calibration", lambda: False)
    monkeypatch.setattr(main, "update_macro_news_pause", lambda: None)
    monkeypatch.setattr(main, "fetch_candles", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "refresh_dynamic_watchlist", lambda: None)
    monkeypatch.setattr(main, "probe_pair_health", lambda: None)
    monkeypatch.setattr(main, "process_pending_close_retries", lambda: None)
    monkeypatch.setattr(main, "start_scan_cycle", lambda: None)
    monkeypatch.setattr(main, "is_rollover_window", lambda: False)
    monkeypatch.setattr(main, "get_effective_scan_pairs", lambda session: (["EUR_USD"], ["EUR_USD"], ["EUR_USD"], "no setup"))

    def stop_after_resume(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main, "_find_best_opportunity", lambda *args, **kwargs: stop_after_resume())

    main.trade_history = []
    main.open_trades = []
    main._paused = False
    main._consecutive_losses = 0
    main._streak_paused_at = 0.0
    main._session_loss_paused_until = 0.0
    main._weekend_mode_active = False
    main._entry_pause_reason = "spread_wide"

    main.run()

    assert any("Entries available again" in message for message in telegram_messages)
    assert any("Tradable pairs: EUR_USD" in message for message in telegram_messages)
    assert main._entry_pause_reason == ""


def test_build_fx_budget_snapshot_uses_shared_sleeve_state(monkeypatch, tmp_path) -> None:
    main = importlib.import_module("main")

    shared_budget = tmp_path / "shared_budget_state.json"
    shared_budget.write_text(
        '{"bots": {"fx": {"reserved_risk": 30.0}, "gold": {"reserved_risk": 12.5}}}',
        encoding="utf-8",
    )

    monkeypatch.setattr(main, "REDIS_CLIENT", None)
    monkeypatch.setattr(main, "SHARED_BUDGET_FILE", str(shared_budget))
    monkeypatch.setattr(main, "FX_BUDGET_ALLOCATION", 0.5)
    monkeypatch.setattr(main, "MAX_RISK_PER_TRADE", 0.01)
    monkeypatch.setattr(main, "MAX_TOTAL_EXPOSURE", 0.15)

    snapshot = main.build_fx_budget_snapshot(1000.0)

    assert snapshot["fx_sleeve_balance"] == 500.0
    assert snapshot["max_trade_risk_amount"] == 5.0
    assert snapshot["max_total_risk_amount"] == 75.0
    assert snapshot["reserved_fx_risk"] == 30.0
    assert snapshot["sibling_gold_reserved_risk"] == 12.5
    assert snapshot["available_fx_risk"] == 45.0


def test_open_trade_entry_caps_risk_to_available_fx_sleeve(monkeypatch) -> None:
    main = importlib.import_module("main")
    captured: dict[str, float] = {}

    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 1000.0, "currency": "USD", "NAV": 1000.0})
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON"})
    monkeypatch.setattr(main, "get_strategy_entry_block_reason", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "get_trade_calibration_adjustment", lambda *args, **kwargs: {"source": "test", "threshold_offset": 0.0, "risk_mult": 1.0, "block_reason": None})
    monkeypatch.setattr(main, "get_entry_risk_multiplier", lambda *args, **kwargs: 1.0)
    monkeypatch.setattr(
        main,
        "build_fx_budget_snapshot",
        lambda balance: {
            "account_balance": balance,
            "fx_sleeve_balance": 500.0,
            "max_trade_risk_amount": 5.0,
            "max_total_risk_amount": 75.0,
            "reserved_fx_risk": 72.0,
            "sibling_gold_reserved_risk": 0.0,
            "available_fx_risk": 3.0,
        },
    )
    monkeypatch.setattr(main, "calculate_units_for_risk_amount", lambda instrument, risk_amount, sl_pips, account_currency=None: captured.setdefault("risk_amount", risk_amount) or 100)
    monkeypatch.setattr(main, "get_current_price", lambda instrument: {"ask": 1.2, "bid": 1.1998})
    monkeypatch.setattr(main, "uses_oanda_native_units", lambda: False)
    monkeypatch.setattr(main, "pip_size", lambda instrument: 0.0001)
    monkeypatch.setattr(main, "place_order", lambda *args, **kwargs: {"id": "trade-1", "price": 1.2, "units": 100})
    monkeypatch.setattr(main, "save_state", lambda: None)
    monkeypatch.setattr(main, "telegram", lambda *args, **kwargs: None)

    main._pair_cooldowns = {}

    trade = main.open_trade_entry(
        {"instrument": "EUR_USD", "direction": "LONG", "score": 60, "sl_pips": 10.0, "tp_pips": 20.0},
        "TREND",
        1000.0,
    )

    assert captured["risk_amount"] == 3.0
    assert trade is not None
    assert trade["risk_amount"] == 3.0


def test_estimate_fx_conversion_rate_prefers_supported_inverse_pair(monkeypatch) -> None:
    main = importlib.import_module("main")
    requested: list[str] = []

    monkeypatch.setattr(main, "PAPER_TRADE", False)
    monkeypatch.setattr(main, "OANDA_API_KEY", "token")
    monkeypatch.setattr(main, "OANDA_ACCOUNT_ID", "acct")
    monkeypatch.setattr(main, "get_supported_currency_pairs", lambda force=False: {"GBP_JPY"})

    def fake_get_mid_price(instrument: str):
        requested.append(instrument)
        if instrument == "GBP_JPY":
            return 200.0
        return None

    monkeypatch.setattr(main, "get_mid_price", fake_get_mid_price)

    rate = main.estimate_fx_conversion_rate("JPY", "GBP")

    assert requested == ["GBP_JPY"]
    assert rate == 1.0 / 200.0
