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
        "next_market_reopen_utc",
        lambda now=None: datetime(2026, 4, 5, 21, 0, tzinfo=timezone.utc),
    )

    def stop_sleep(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main, "sleep_with_command_poll", stop_sleep)
    main._weekend_mode_active = False

    main.run()

    assert heartbeat_calls == [(1234.5, "idle_weekend")]
    assert any(
        "No new trades will be entered until Sun 2026-04-05 21:00 UTC" in message
        for message in telegram_messages
    )


def test_run_announces_market_reopen_after_weekend(monkeypatch) -> None:
    main = importlib.import_module("main")
    telegram_messages: list[str] = []

    monkeypatch.setattr(main, "_bootstrap_runtime", lambda: None)
    monkeypatch.setattr(main, "poll_telegram_commands", lambda: None)
    monkeypatch.setattr(main, "is_weekend", lambda: False)
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 1000.0})
    monkeypatch.setattr(main, "publish_bot_runtime_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON", "aggression": "HIGH", "pairs_allowed": []})
    monkeypatch.setattr(main, "telegram", lambda message, parse_mode="HTML": telegram_messages.append(message))
    monkeypatch.setattr(main, "save_state", lambda: None)

    def stop_after_reopen_notice(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(main, "refresh_macro_filters", stop_after_reopen_notice)

    main.trade_history = []
    main.open_trades = []
    main._paused = False
    main._consecutive_losses = 0
    main._streak_paused_at = 0.0
    main._session_loss_paused_until = 0.0
    main._weekend_mode_active = True

    main.run()

    assert any("Forex markets are open again" in message for message in telegram_messages)
    assert any("Current session: LONDON (HIGH)" in message for message in telegram_messages)
    assert main._weekend_mode_active is False
