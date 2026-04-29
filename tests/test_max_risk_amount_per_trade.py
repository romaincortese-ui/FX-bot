"""Tests for MAX_RISK_AMOUNT_PER_TRADE absolute-£ cap (small-account safety)."""
import importlib


def _patch_common(monkeypatch, main):
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 200.0, "currency": "GBP", "NAV": 200.0})
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON"})
    monkeypatch.setattr(main, "get_strategy_entry_block_reason", lambda *a, **k: None)
    monkeypatch.setattr(main, "get_trade_calibration_adjustment", lambda *a, **k: {"source": "test", "threshold_offset": 0.0, "risk_mult": 1.0, "block_reason": None})
    monkeypatch.setattr(main, "get_entry_risk_multiplier", lambda *a, **k: 1.0)
    monkeypatch.setattr(
        main,
        "build_fx_budget_snapshot",
        lambda balance: {
            "account_balance": balance,
            "fx_sleeve_balance": 100.0,
            "max_trade_risk_amount": 1.5,
            "max_total_risk_amount": 30.0,
            "reserved_fx_risk": 0.0,
            "sibling_gold_reserved_risk": 0.0,
            "available_fx_risk": 1.5,
        },
    )
    monkeypatch.setattr(main, "get_current_price", lambda instrument: {"ask": 1.2, "bid": 1.1998})
    monkeypatch.setattr(main, "uses_oanda_native_units", lambda: False)
    monkeypatch.setattr(main, "ACCOUNT_TYPE", "spread_bet")
    monkeypatch.setattr(main, "pip_size", lambda instrument: 0.0001)
    monkeypatch.setattr(main, "place_order", lambda *a, **k: {"id": "trade-1", "price": 1.2, "units": 1})
    monkeypatch.setattr(main, "save_state", lambda: None)
    monkeypatch.setattr(main, "telegram", lambda *a, **k: None)
    main._pair_cooldowns = {}


def test_max_risk_amount_per_trade_rejects_oversized_trade(monkeypatch):
    """On a small spread-bet account, a 50-pip stop with £0.10/pip min stake = £5
    realised risk. With cap=£3 the trade must be rejected."""
    main = importlib.import_module("main")
    _patch_common(monkeypatch, main)
    monkeypatch.setattr(main, "MAX_RISK_AMOUNT_PER_TRADE", 3.0)
    # Min-stake clamp: 1.5 / 50 = 0.03 → clamped to 0.10 → realised risk = 0.10*50 = £5
    monkeypatch.setattr(main, "calculate_units_for_risk_amount", lambda *a, **k: 0.10)

    trade = main.open_trade_entry(
        {"instrument": "EUR_USD", "direction": "LONG", "score": 60, "sl_pips": 50.0, "tp_pips": 100.0},
        "TREND",
        200.0,
    )
    assert trade is None, "trade should be rejected because realised risk £5 > cap £3"


def test_max_risk_amount_per_trade_allows_in_budget_trade(monkeypatch):
    """A 15-pip stop at £0.10/pip min stake = £1.50 realised risk, well below the £3 cap."""
    main = importlib.import_module("main")
    _patch_common(monkeypatch, main)
    monkeypatch.setattr(main, "MAX_RISK_AMOUNT_PER_TRADE", 3.0)
    monkeypatch.setattr(main, "calculate_units_for_risk_amount", lambda *a, **k: 0.10)

    trade = main.open_trade_entry(
        {"instrument": "EUR_USD", "direction": "LONG", "score": 60, "sl_pips": 15.0, "tp_pips": 30.0},
        "TREND",
        200.0,
    )
    assert trade is not None, "trade should be allowed: realised risk £1.50 <= cap £3"


def test_max_risk_amount_per_trade_disabled_when_zero(monkeypatch):
    """When the cap is 0 (default), the new gate is a no-op."""
    main = importlib.import_module("main")
    _patch_common(monkeypatch, main)
    monkeypatch.setattr(main, "MAX_RISK_AMOUNT_PER_TRADE", 0.0)
    # Same oversized scenario as test 1, but cap disabled → trade goes through.
    monkeypatch.setattr(main, "calculate_units_for_risk_amount", lambda *a, **k: 0.10)

    trade = main.open_trade_entry(
        {"instrument": "EUR_USD", "direction": "LONG", "score": 60, "sl_pips": 50.0, "tp_pips": 100.0},
        "TREND",
        200.0,
    )
    assert trade is not None, "with cap disabled, oversized trade is not blocked by this gate"
