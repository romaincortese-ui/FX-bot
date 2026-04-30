import importlib
import json
from datetime import datetime, timedelta, timezone

import pandas as pd

from fxbot.event_intelligence import FeedItem, build_event_intelligence_state, event_signal_for_instrument


def _event_state(now: datetime | None = None) -> dict:
    published = now or (datetime.now(timezone.utc) - timedelta(minutes=20))
    item = FeedItem(
        title="Japan MOF intervention warning: yen surges after emergency statement",
        summary="Bank of Japan and Ministry of Finance officials signal emergency intervention risk.",
        url="https://example.com/jpy-intervention",
        published_at=published,
        source="Reuters official market feed",
        source_tier="official",
    )
    return build_event_intelligence_state(
        [item],
        previous_state={},
        now=published + timedelta(minutes=5),
        lookback_minutes=90,
        ttl_minutes=180,
        min_score=0.0,
    )


def test_event_intelligence_maps_jpy_strength_to_usdjpy_short() -> None:
    state = _event_state()

    signal = event_signal_for_instrument(state, "USD_JPY", min_score=0.5)

    assert signal is not None
    assert signal["event_currency"] == "JPY"
    assert signal["direction_hint"] == "SHORT"
    assert signal["event_risk_score"] >= 0.5


def test_event_virtual_window_uses_headline_publication_time(monkeypatch) -> None:
    main = importlib.import_module("main")
    published = datetime.now(timezone.utc) - timedelta(minutes=20)
    now = published + timedelta(minutes=20)

    monkeypatch.setattr(main, "event_intelligence_state", _event_state(published))
    monkeypatch.setattr(main, "EVENT_INTEL_MIN_SCORE", 0.5)
    monkeypatch.setattr(main, "EVENT_INTEL_STALE_GRACE_SECS", 0)

    event = main._event_virtual_post_news_event("USD_JPY", now)

    assert event is not None
    assert event["source"] == "event_intelligence"
    assert event["time"] == published.isoformat()
    assert event["pause_end"] == published.isoformat()


def test_event_market_confirmation_allows_event_spread_cap(monkeypatch) -> None:
    main = importlib.import_module("main")
    published = datetime.now(timezone.utc) - timedelta(minutes=20)
    closes = [160.70] * 40 + [160.10, 159.20, 158.30, 157.20, 156.10]
    frame = pd.DataFrame(
        {
            "open": closes,
            "high": [price + 0.05 for price in closes],
            "low": [price - 0.05 for price in closes],
            "close": closes,
            "volume": [100] * len(closes),
        }
    )

    monkeypatch.setattr(main, "event_intelligence_state", _event_state(published))
    monkeypatch.setattr(main, "EVENT_INTEL_MIN_SCORE", 0.5)
    monkeypatch.setattr(main, "EVENT_INTEL_STRONG_SCORE", 0.5)
    monkeypatch.setattr(main, "EVENT_INTEL_CONFIRM_LOOKBACK_M5_BARS", 30)
    monkeypatch.setattr(main, "EVENT_INTEL_CONFIRM_MIN_MOVE_PIPS", 25.0)
    monkeypatch.setattr(main, "EVENT_INTEL_CONFIRM_MIN_ATR_MULT", 1.0)
    monkeypatch.setattr(main, "EVENT_INTEL_MAX_SPREAD_PIPS", 4.0)
    monkeypatch.setattr(main, "fetch_candles", lambda *args, **kwargs: frame)
    monkeypatch.setattr(main, "get_spread_pips", lambda instrument: 3.8)

    signal = main._event_signal_for_pair("USD_JPY")
    confirmation = main._event_market_confirmation("USD_JPY", signal)

    assert signal is not None
    assert confirmation["confirmed"] is True
    assert confirmation["move_pips"] < -250
    assert main._event_spread_cap_for_pair("USD_JPY", 2.5) == 4.0


def test_event_risk_reserve_spares_confirmed_event_pair(monkeypatch) -> None:
    main = importlib.import_module("main")
    published = datetime.now(timezone.utc) - timedelta(minutes=20)
    budget = {"available_fx_risk": 10.0, "max_total_risk_amount": 20.0}

    monkeypatch.setattr(main, "event_intelligence_state", _event_state(published))
    monkeypatch.setattr(main, "EVENT_INTEL_MIN_SCORE", 0.5)
    monkeypatch.setattr(main, "EVENT_INTEL_RISK_RESERVE_PCT", 0.25)
    monkeypatch.setattr(main, "_event_market_confirmation", lambda *args, **kwargs: {"confirmed": True})

    assert main._event_risk_reserve_amount(budget, "EUR_GBP", "LONG") == 5.0
    assert main._event_risk_reserve_amount(budget, "USD_JPY", "SHORT") == 0.0


def test_portfolio_cap_fit_downsizes_confirmed_event_trade(monkeypatch) -> None:
    main = importlib.import_module("main")

    monkeypatch.setattr(main, "TIER2_PORTFOLIO_VOL_ENABLED", True)
    monkeypatch.setattr(main, "TIER2_PORTFOLIO_VOL_CAP_PCT", 0.03)
    monkeypatch.setattr(main, "EVENT_INTEL_CAP_FIT_ENABLED", True)
    monkeypatch.setattr(main, "EVENT_INTEL_CAP_FIT_MIN_RATIO", 0.15)
    monkeypatch.setattr(main, "EVENT_INTEL_CAP_FIT_MIN_RISK_AMOUNT", 0.25)
    monkeypatch.setattr(
        main,
        "open_trades",
        [{"instrument": "USD_CAD", "direction": "SHORT", "risk_amount": 2.81}],
    )

    requested = 2.74
    fitted, reason = main._tier2_cap_fit_risk_amount("USD_JPY", "SHORT", requested, 104.19)

    assert 0.25 <= fitted < requested
    assert reason is not None and "cap-fit" in reason
    assert main._tier2_portfolio_vol_breach("USD_JPY", "SHORT", fitted, 104.19) is None
    assert main._tier2_portfolio_vol_breach("USD_JPY", "SHORT", requested, 104.19) is not None


def test_event_worker_publishes_redis_state(monkeypatch) -> None:
    worker = importlib.import_module("run_event_intelligence")
    published = datetime.now(timezone.utc) - timedelta(minutes=10)
    feed_item = FeedItem(
        title="Japan MOF intervention warning: yen surges after emergency statement",
        summary="Bank of Japan and Ministry of Finance officials signal emergency intervention risk.",
        url="https://example.com/jpy-intervention",
        published_at=published,
        source="Official/market RSS",
        source_tier="official",
    )

    class FakeRedis:
        def __init__(self):
            self.store = {}

        def get(self, key):
            return self.store.get(key)

        def set(self, key, value):
            self.store[key] = value
            return True

    client = FakeRedis()
    monkeypatch.setattr(worker, "EVENT_INTEL_FEEDS", "")
    monkeypatch.setattr(worker, "parse_feed_config", lambda raw: [{"name": "fixture", "url": "https://example.com/rss", "tier": "official"}])
    monkeypatch.setattr(worker, "_fetch_feed", lambda session, feed, now: [feed_item])
    monkeypatch.setattr(worker, "publish_event_runtime_state", lambda *args, **kwargs: True)

    assert worker.run_once(client) == 0

    state = json.loads(client.store[worker.EVENT_INTEL_STATE_KEY])
    assert "JPY" in state["currencies"]
    assert state["item_count"] == 1