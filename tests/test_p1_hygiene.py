"""P1 hygiene fixes from FX_BOT_UPDATED_ASSESSMENT.md.

Covers:

* #6 — pair-cooldown Redis persistence (`_persist_pair_cooldowns_to_redis`,
  `_merge_pair_cooldowns_from_redis`).
* #7 — calibration backtest-seed fallback (`_load_trade_calibration_seed`,
  `load_trade_calibration` chain).
* #11 — exit-failure escalation (`schedule_close_retry`,
  `process_pending_close_retries`).
"""
from __future__ import annotations

import importlib
import json
import time
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def main(monkeypatch):
    mod = importlib.import_module("main")
    monkeypatch.setattr(mod, "PAPER_TRADE", False, raising=False)
    monkeypatch.setattr(mod, "OANDA_ACCOUNT_ID", "TEST-ACC", raising=False)
    mod._pending_close_retries.clear()
    mod._pair_cooldowns.clear()
    return mod


# ─────────────────────────────────────────────────────────────────────────
#  P1 #6 — pair-cooldown Redis persistence
# ─────────────────────────────────────────────────────────────────────────


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value
        return True


def test_persist_pair_cooldowns_writes_live_entries_only(main, monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(main, "REDIS_CLIENT", fake, raising=False)
    monkeypatch.setattr(main, "REDIS_PAIR_COOLDOWNS_KEY", "fxbot:test_cooldowns", raising=False)
    now = time.time()
    main._pair_cooldowns.clear()
    main._pair_cooldowns["EUR_USD"] = now + 600.0
    main._pair_cooldowns["GBP_USD"] = now - 60.0  # already expired

    main._persist_pair_cooldowns_to_redis()

    raw = fake.store["fxbot:test_cooldowns"]
    payload = json.loads(raw)
    assert "EUR_USD" in payload["cooldowns"]
    assert "GBP_USD" not in payload["cooldowns"]
    assert "persisted_at" in payload


def test_merge_pair_cooldowns_restores_from_redis(main, monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(main, "REDIS_CLIENT", fake, raising=False)
    monkeypatch.setattr(main, "REDIS_PAIR_COOLDOWNS_KEY", "fxbot:test_cooldowns", raising=False)
    future = time.time() + 1200.0
    fake.store["fxbot:test_cooldowns"] = json.dumps({
        "cooldowns": {"AUD_USD": future, "USD_JPY": time.time() - 5.0},
        "persisted_at": "2026-01-01T00:00:00+00:00",
    })

    main._pair_cooldowns.clear()
    main._merge_pair_cooldowns_from_redis()

    assert main._pair_cooldowns.get("AUD_USD") == pytest.approx(future)
    # Expired entries must NOT be restored — they would falsely block re-entry.
    assert "USD_JPY" not in main._pair_cooldowns


def test_persist_pair_cooldowns_noop_without_redis(main, monkeypatch):
    monkeypatch.setattr(main, "REDIS_CLIENT", None, raising=False)
    main._pair_cooldowns["EUR_USD"] = time.time() + 600.0
    # Must not raise.
    main._persist_pair_cooldowns_to_redis()
    main._merge_pair_cooldowns_from_redis()


# ─────────────────────────────────────────────────────────────────────────
#  P1 #7 — calibration backtest-seed fallback
# ─────────────────────────────────────────────────────────────────────────


def _make_seed_payload(total_trades: int, *, generated_at: str | None = None) -> dict:
    payload = {
        "total_trades": total_trades,
        "by_strategy": {"TREND": {"trades": total_trades}},
        "strategies": {"TREND": {"pairs": {"EUR_USD": {"trades": total_trades}}}},
    }
    if generated_at is not None:
        payload["generated_at"] = generated_at
    return payload


def test_load_trade_calibration_falls_back_to_seed(main, monkeypatch, tmp_path):
    """When Redis is empty and the live file is missing, the seed must load."""
    monkeypatch.setattr(main, "REDIS_CLIENT", None, raising=False)
    monkeypatch.setattr(main, "TIER4_CALIBRATION_REDIS_ONLY", False, raising=False)
    monkeypatch.setattr(main, "TRADE_CALIBRATION_FILE", str(tmp_path / "missing.json"), raising=False)
    seed_path = tmp_path / "seed.json"
    seed_path.write_text(json.dumps(_make_seed_payload(60)), encoding="utf-8")
    monkeypatch.setattr(main, "CALIBRATION_SEED_FILE", str(seed_path), raising=False)
    monkeypatch.setattr(main, "CALIBRATION_MIN_TOTAL_TRADES", 30, raising=False)

    main.trade_calibration = {}
    main.load_trade_calibration()

    assert main._count_calibration_trades(main.trade_calibration) >= 30


def test_seed_payload_below_floor_is_rejected(main, monkeypatch, tmp_path):
    """A seed with fewer than CALIBRATION_MIN_TOTAL_TRADES samples must NOT load."""
    monkeypatch.setattr(main, "REDIS_CLIENT", None, raising=False)
    monkeypatch.setattr(main, "TIER4_CALIBRATION_REDIS_ONLY", False, raising=False)
    monkeypatch.setattr(main, "TRADE_CALIBRATION_FILE", str(tmp_path / "missing.json"), raising=False)
    seed_path = tmp_path / "seed.json"
    seed_path.write_text(json.dumps(_make_seed_payload(5)), encoding="utf-8")
    monkeypatch.setattr(main, "CALIBRATION_SEED_FILE", str(seed_path), raising=False)
    monkeypatch.setattr(main, "CALIBRATION_MIN_TOTAL_TRADES", 30, raising=False)

    main.trade_calibration = {"sentinel": True}
    main.load_trade_calibration()

    assert main.trade_calibration == {}


def test_seed_skips_staleness_check(main, monkeypatch, tmp_path):
    """The seed validator must accept old `generated_at` values (snapshot semantics)."""
    monkeypatch.setattr(main, "REDIS_CLIENT", None, raising=False)
    monkeypatch.setattr(main, "TIER4_CALIBRATION_REDIS_ONLY", False, raising=False)
    monkeypatch.setattr(main, "TRADE_CALIBRATION_FILE", str(tmp_path / "missing.json"), raising=False)
    stale = (datetime.now(timezone.utc) - timedelta(days=180)).isoformat()
    seed_path = tmp_path / "seed.json"
    seed_path.write_text(json.dumps(_make_seed_payload(60, generated_at=stale)), encoding="utf-8")
    monkeypatch.setattr(main, "CALIBRATION_SEED_FILE", str(seed_path), raising=False)
    monkeypatch.setattr(main, "CALIBRATION_MIN_TOTAL_TRADES", 30, raising=False)
    monkeypatch.setattr(main, "CALIBRATION_MAX_AGE_HOURS", 72.0, raising=False)

    main.trade_calibration = {}
    main.load_trade_calibration()

    # Stale generated_at would fail the live validator, but the seed validator
    # is intentionally relaxed.
    assert main._count_calibration_trades(main.trade_calibration) >= 30


# ─────────────────────────────────────────────────────────────────────────
#  P1 #11 — exit-failure escalation
# ─────────────────────────────────────────────────────────────────────────


def _stub_telegram(monkeypatch, main):
    sent: list[str] = []
    monkeypatch.setattr(main, "telegram", lambda msg, *a, **k: sent.append(msg), raising=False)
    return sent


def _trade(trade_id: str = "TRD-1") -> dict:
    return {
        "id": trade_id,
        "instrument": "EUR_USD",
        "label": "RESTORED",
        "direction": "LONG",
        "entry_price": 1.0,
        "units": 1,
        "opened_ts": 0,
    }


def test_escalation_telegram_fires_at_alert_threshold(main, monkeypatch):
    monkeypatch.setattr(main, "EXIT_RETRY_ALERT_AFTER", 3, raising=False)
    monkeypatch.setattr(main, "EXIT_RETRY_GIVE_UP_AFTER", 10, raising=False)
    sent = _stub_telegram(monkeypatch, main)
    trade = _trade()

    # Two prior failures, the third should trip the alert (attempts=3).
    main._pending_close_retries[trade["id"]] = {
        "trade_id": trade["id"],
        "attempts": 2,
        "escalated": False,
        "broker_unreachable": False,
    }
    main.schedule_close_retry(trade, "MARKET_HALTED")

    assert any("Exit retry escalation" in m for m in sent)
    rec = main._pending_close_retries[trade["id"]]
    assert rec["attempts"] == 3
    assert rec["escalated"] is True
    assert rec["broker_unreachable"] is False


def test_escalation_telegram_only_fires_once(main, monkeypatch):
    monkeypatch.setattr(main, "EXIT_RETRY_ALERT_AFTER", 3, raising=False)
    monkeypatch.setattr(main, "EXIT_RETRY_GIVE_UP_AFTER", 10, raising=False)
    sent = _stub_telegram(monkeypatch, main)
    trade = _trade("TRD-2")

    # Already escalated at attempts=3; attempt 4 must not duplicate the alert.
    main._pending_close_retries[trade["id"]] = {
        "trade_id": trade["id"],
        "attempts": 3,
        "escalated": True,
        "broker_unreachable": False,
    }
    main.schedule_close_retry(trade, "MARKET_HALTED")

    escalations = [m for m in sent if "Exit retry escalation" in m]
    assert escalations == []


def test_give_up_marks_broker_unreachable(main, monkeypatch):
    monkeypatch.setattr(main, "EXIT_RETRY_ALERT_AFTER", 3, raising=False)
    monkeypatch.setattr(main, "EXIT_RETRY_GIVE_UP_AFTER", 10, raising=False)
    sent = _stub_telegram(monkeypatch, main)
    trade = _trade("TRD-3")

    main._pending_close_retries[trade["id"]] = {
        "trade_id": trade["id"],
        "attempts": 9,
        "escalated": True,
        "broker_unreachable": False,
    }
    main.schedule_close_retry(trade, "MARKET_HALTED")

    rec = main._pending_close_retries[trade["id"]]
    assert rec["attempts"] == 10
    assert rec["broker_unreachable"] is True
    assert any("broker_unreachable" in m for m in sent)


def test_process_pending_close_retries_skips_unreachable(main, monkeypatch):
    """A trade flagged broker_unreachable must not trigger a fresh close attempt."""
    fired: list[str] = []
    monkeypatch.setattr(
        main, "close_trade_exit",
        lambda trade, label: fired.append(trade["id"]) or True,
        raising=False,
    )
    trade_id = "TRD-4"
    main._pending_close_retries[trade_id] = {
        "trade_id": trade_id,
        "instrument": "EUR_USD",
        "label": "RESTORED",
        "attempts": 11,
        "next_retry_at": time.time() - 1.0,  # due immediately
        "broker_unreachable": True,
    }
    # An open_trade matching the pending id, otherwise the retry loop's
    # missing-trade pruning would short-circuit before our gate.
    main.open_trades.append({"id": trade_id, "instrument": "EUR_USD", "label": "RESTORED"})

    try:
        main.process_pending_close_retries()
    finally:
        main.open_trades[:] = [t for t in main.open_trades if t.get("id") != trade_id]

    assert fired == []
