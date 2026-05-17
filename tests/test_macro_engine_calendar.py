from datetime import datetime, timezone

import macro_engine


class _Response:
    content = b"""
    <weeklyevents>
      <event>
        <title>BoJ policy statement</title>
        <country>JPY</country>
        <impact>High</impact>
        <date>05-18-2026</date>
        <time>12:00am</time>
        <forecast></forecast>
        <previous></previous>
        <actual></actual>
        <url>https://example.test/boj</url>
      </event>
    </weeklyevents>
    """

    def raise_for_status(self) -> None:
        return None


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 5, 17, 22, 30, tzinfo=timezone.utc)


def test_calendar_news_includes_next_utc_date_for_tokyo_open(monkeypatch, tmp_path):
    monkeypatch.setattr(macro_engine, "datetime", _FixedDateTime)
    monkeypatch.setattr(macro_engine.requests, "get", lambda *args, **kwargs: _Response())
    monkeypatch.setattr(macro_engine, "MACRO_NEWS_CACHE_FILE", str(tmp_path / "macro_news_cache.json"))
    monkeypatch.delenv("NEWS_LOOKAHEAD_DAYS", raising=False)

    events = macro_engine.load_forex_factory_news()

    assert len(events) == 1
    assert events[0]["currency"] == "JPY"
    assert events[0]["event"] == "BoJ policy statement"