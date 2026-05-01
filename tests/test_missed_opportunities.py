from __future__ import annotations

import importlib


def test_missed_opportunity_records_spread_and_forward_marks(monkeypatch, caplog) -> None:
    main = importlib.import_module("main")
    now = {"ts": 1_000.0}

    monkeypatch.setattr(main.time, "time", lambda: now["ts"])
    monkeypatch.setattr(main, "MISSED_OPPORTUNITY_REPORT_ENABLED", True, raising=False)
    monkeypatch.setattr(main, "_missed_opportunities", {}, raising=False)
    monkeypatch.setattr(main, "_missed_opportunities_dirty", False, raising=False)
    monkeypatch.setattr(main, "pip_size", lambda instrument: 0.0001, raising=False)

    opp = {
        "instrument": "USD_CHF",
        "direction": "LONG",
        "score": 74.0,
        "selection_score": 76.0,
        "effective_threshold": 37.0,
        "sl_pips": 12.0,
        "tp_pips": 30.0,
    }
    first_spread = main._entry_spread_audit_snapshot(
        "PULLBACK",
        "USD_CHF",
        "LONG",
        label="PULLBACK",
        bid=0.9000,
        ask=0.9002,
        context="pre_block",
        reason="portfolio_vol",
    )

    with caplog.at_level("INFO"):
        main._record_missed_opportunity(
            "PULLBACK",
            "USD_CHF",
            "LONG",
            "portfolio_vol 3.750% > cap 3.000%",
            opp=opp,
            session_name="LONDON",
            risk_amount=3.75,
            spread_snapshot=first_spread,
        )

    key = "PULLBACK:USD_CHF:LONG"
    record = main._missed_opportunities[key]
    assert record["spread_pips"] == 2.0
    assert record["spread_cap_pips"] == main.PULLBACK_MAX_SPREAD_PIPS
    assert record["theoretical_entry_price"] == 0.9002
    assert record["score"] == 74.0
    assert record["seen_count"] == 1

    now["ts"] += 3600
    second_spread = main._entry_spread_audit_snapshot(
        "PULLBACK",
        "USD_CHF",
        "LONG",
        label="PULLBACK",
        bid=0.9020,
        ask=0.9022,
        context="pre_block",
        reason="portfolio_vol",
    )
    main._record_missed_opportunity(
        "PULLBACK",
        "USD_CHF",
        "LONG",
        "portfolio_vol 3.750% > cap 3.000%",
        opp=opp,
        session_name="LONDON",
        risk_amount=3.75,
        spread_snapshot=second_spread,
    )

    record = main._missed_opportunities[key]
    assert record["seen_count"] == 2
    assert record["current_pips"] == 18.0
    assert record["mfe_pips"] == 18.0
    assert record["mae_pips"] == -2.0
    assert record["horizon_pips"]["1h"] == 18.0


def test_portfolio_vol_skip_records_spread_audit_and_missed_candidate(monkeypatch, caplog) -> None:
    main = importlib.import_module("main")

    class Decision:
        allowed = False
        portfolio_vol_before = 0.0
        portfolio_vol_after = 0.0375
        cap = 0.03

    monkeypatch.setattr(main, "MISSED_OPPORTUNITY_REPORT_ENABLED", True, raising=False)
    monkeypatch.setattr(main, "_missed_opportunities", {}, raising=False)
    monkeypatch.setattr(main, "_missed_opportunities_dirty", False, raising=False)
    monkeypatch.setattr(main, "get_account_summary", lambda: {"balance": 100.0, "NAV": 100.0, "currency": "GBP"}, raising=False)
    monkeypatch.setattr(main, "get_current_session", lambda: {"name": "LONDON"}, raising=False)
    monkeypatch.setattr(main, "get_strategy_entry_block_reason", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(main, "get_trade_calibration_adjustment", lambda *a, **k: {"source": "test", "threshold_offset": 0.0, "risk_mult": 1.0, "block_reason": None}, raising=False)
    monkeypatch.setattr(main, "get_entry_risk_multiplier", lambda *a, **k: 1.0, raising=False)
    monkeypatch.setattr(main, "build_fx_budget_snapshot", lambda balance: {
        "account_balance": balance,
        "fx_sleeve_balance": balance,
        "max_trade_risk_amount": 3.75,
        "max_total_risk_amount": 6.0,
        "reserved_fx_risk": 0.0,
        "sibling_gold_reserved_risk": 0.0,
        "available_fx_risk": 6.0,
    }, raising=False)
    monkeypatch.setattr(main, "_event_signal_for_pair", lambda instrument: None, raising=False)
    monkeypatch.setattr(main, "_tier2_portfolio_vol_decision", lambda *a, **k: Decision(), raising=False)
    monkeypatch.setattr(main, "get_current_price", lambda instrument: {"bid": 0.9000, "ask": 0.9002, "spread": 0.0002}, raising=False)
    monkeypatch.setattr(main, "pip_size", lambda instrument: 0.0001, raising=False)
    monkeypatch.setattr(main, "place_order", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not place order")), raising=False)

    with caplog.at_level("INFO"):
        trade = main.open_trade_entry(
            {
                "instrument": "USD_CHF",
                "direction": "LONG",
                "score": 74.0,
                "selection_score": 74.0,
                "sl_pips": 12.0,
                "tp_pips": 30.0,
                "spread_pips": 2.0,
            },
            "PULLBACK",
            100.0,
        )

    assert trade is None
    assert "PULLBACK:USD_CHF:LONG" in main._missed_opportunities
    assert main._missed_opportunities["PULLBACK:USD_CHF:LONG"]["portfolio_vol_after"] == 0.0375
    assert main._missed_opportunities["PULLBACK:USD_CHF:LONG"]["theoretical_entry_price"] == 0.9002
    assert any("[ENTRY_SPREAD]" in rec.message and "context=pre_block" in rec.message for rec in caplog.records)
    assert any("[MISSED_OPPORTUNITY]" in rec.message for rec in caplog.records)