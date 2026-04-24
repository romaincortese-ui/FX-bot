"""Unit tests for scripts/analyse_live_log.py (Tier 1v2 V1 gate runner)."""
from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "analyse_live_log.py"
    spec = importlib.util.spec_from_file_location("analyse_live_log", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["analyse_live_log"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mod():
    return _load_module()


def _entry(ts: datetime, severity: str, message: str) -> dict:
    return {
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "severity": severity,
        "message": message,
    }


def test_gate_no_reconcile_storm_passes_on_clean_log(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts, "WARNING", "Trade 999 already closed at broker — reconciling locally"),
        _entry(ts, "INFO", "unrelated line"),
    ]
    result = mod.gate_no_reconcile_storm(entries)
    assert result.passed is True


def test_gate_no_reconcile_storm_fails_on_retry_storm(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts + timedelta(seconds=i), "ERROR", "OANDA PUT /trades/999/close error 404: TRADE_DOESNT_EXIST")
        for i in range(10)
    ]
    result = mod.gate_no_reconcile_storm(entries)
    assert result.passed is False
    assert result.counts.get("TRADE_DOESNT_EXIST", 0) == 10


def test_gate_calibration_loaded_detects_successful_load(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts, "INFO", "[CALIBRATION] Loaded trade calibration from Redis key trade_calibration: 12 strategy/pair entries, 65 trades"),
    ]
    result = mod.gate_calibration_loaded(entries)
    assert result.passed is True
    assert result.counts["max_entries"] == 12


def test_gate_calibration_loaded_fails_on_ignoring_only(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts, "INFO", "[CALIBRATION] Ignoring Redis calibration from key trade_calibration: insufficient sample (40 trades < 50)"),
    ]
    result = mod.gate_calibration_loaded(entries)
    assert result.passed is False


def test_gate_spread_log_quiet_counts_warnings(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts + timedelta(hours=i), "WARNING", "📊 spread_gate rejections=8 (max=2.50p) sample=[]")
        for i in range(3)
    ]
    # Three WARNINGs in a 24h window → pass (< 24 ceiling).
    result = mod.gate_spread_log_quiet(entries)
    assert result.passed is True
    assert result.counts["warn"] == 3


def test_gate_spread_log_quiet_fails_on_storm(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts + timedelta(seconds=i), "WARNING", "📊 spread_gate rejections=8 (max=2.50p) sample=[]")
        for i in range(30)
    ]
    result = mod.gate_spread_log_quiet(entries)
    assert result.passed is False


def test_gate_entry_or_explanation_passes_on_gate_block_telemetry(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts, "INFO", '[GATE_BLOCK] strategy=SCALPER instrument=EUR_USD direction=LONG category=net_rr_fail score=55.0 reason="net_rr 1.20 < 1.80"'),
    ]
    result = mod.gate_london_entry_or_explanation(entries)
    assert result.passed is True
    assert result.counts["net_rr_fail"] == 1


def test_gate_entry_or_explanation_passes_on_london_entry(mod):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)  # 10:00 UTC = London
    entries = [
        _entry(ts, "INFO", "[SCALPER] Placing MARKET LONG EUR_USD units=100 priceBound=1.1020 TP=1.1035 SL=1.1010 trail=10.0"),
    ]
    result = mod.gate_london_entry_or_explanation(entries)
    assert result.passed is True


def test_run_exit_code_zero_when_all_gates_pass(mod, tmp_path):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts, "INFO", "[CALIBRATION] Loaded trade calibration from Redis key trade_calibration: 12 strategy/pair entries, 65 trades"),
        _entry(ts, "WARNING", "Trade 100 already closed at broker — reconciling locally"),
        _entry(ts, "INFO", '[GATE_BLOCK] strategy=TREND instrument=EUR_USD direction=LONG category=regime_veto score=68.0 reason="chop regime blocks TREND"'),
    ]
    log_file = tmp_path / "log.json"
    log_file.write_text(json.dumps(entries), encoding="utf-8")
    assert mod.run(log_file) == 0


def test_run_exit_code_one_on_gate_failure(mod, tmp_path):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    entries = [
        _entry(ts, "ERROR", "OANDA PUT /trades/1/close error 404: TRADE_DOESNT_EXIST"),
    ]
    log_file = tmp_path / "log.json"
    log_file.write_text(json.dumps(entries), encoding="utf-8")
    assert mod.run(log_file) == 1


def test_newline_delimited_json_supported(mod, tmp_path):
    ts = datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc)
    lines = [
        json.dumps(_entry(ts, "INFO", "[CALIBRATION] Loaded trade calibration from Redis key trade_calibration: 5 strategy/pair entries, 30 trades")),
        json.dumps(_entry(ts, "INFO", '[GATE_BLOCK] strategy=SCALPER instrument=EUR_USD direction=LONG category=spread_wide score=52.0 reason="spread 2.7 > 1.8"')),
    ]
    log_file = tmp_path / "log.ndjson"
    log_file.write_text("\n".join(lines), encoding="utf-8")
    assert mod.run(log_file) == 0
