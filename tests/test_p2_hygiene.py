"""P2 hygiene fixes from FX_BOT_UPDATED_ASSESSMENT.md.

Covers:

* #12 — pricing-stream reconnect skip when the resolved instrument set is
  unchanged (`_start_price_stream`).
* #14 — DST-aware session boundaries (`_hour_in_local_band`,
  `get_current_session` under `SESSION_DST_AWARE=1`).
"""
from __future__ import annotations

import importlib
import threading
from datetime import datetime, timezone

import pytest


@pytest.fixture
def main(monkeypatch):
    mod = importlib.import_module("main")
    return mod


# ─────────────────────────────────────────────────────────────────────────
#  P2 #14 — DST-aware session boundaries
# ─────────────────────────────────────────────────────────────────────────


def test_hour_in_local_band_handles_dst_shift(main):
    """London 08:00 BST = 07:00 UTC in summer; in winter it's 08:00 UTC.

    With a UTC-only comparator, the same env-var (`LONDON_OPEN_UTC=8`) would
    say London "opens" at 08:00 UTC year-round, which is one hour late in
    summer. The DST-aware comparator must report the band open at 07:00 UTC
    in late June and at 08:00 UTC in late January.
    """
    pytest.importorskip("zoneinfo")

    summer_07_utc = datetime(2026, 6, 30, 7, 30, tzinfo=timezone.utc)
    winter_07_utc = datetime(2026, 1, 30, 7, 30, tzinfo=timezone.utc)
    winter_08_utc = datetime(2026, 1, 30, 8, 30, tzinfo=timezone.utc)

    # Treat the env constants as **local** hours: London opens 8 close 17.
    assert main._hour_in_local_band(summer_07_utc, "Europe/London", 8, 17) is True
    assert main._hour_in_local_band(winter_07_utc, "Europe/London", 8, 17) is False
    assert main._hour_in_local_band(winter_08_utc, "Europe/London", 8, 17) is True


def test_hour_in_local_band_supports_wraparound(main):
    """Some sessions cross midnight (e.g. Sydney 22..06)."""
    pytest.importorskip("zoneinfo")
    midnight_utc = datetime(2026, 6, 30, 0, 0, tzinfo=timezone.utc)
    # 22..06 wrap-around band must include 00:00 UTC (== midnight UTC).
    assert main._hour_in_local_band(midnight_utc, "Etc/UTC", 22, 6) is True
    # 02:00 UTC should still be inside.
    assert main._hour_in_local_band(midnight_utc.replace(hour=2), "Etc/UTC", 22, 6) is True
    # 10:00 UTC should be outside.
    assert main._hour_in_local_band(midnight_utc.replace(hour=10), "Etc/UTC", 22, 6) is False


def test_session_dst_aware_flag_default_off_preserves_utc_behaviour(main, monkeypatch):
    """With `SESSION_DST_AWARE=0` (default) `get_current_session` keeps the
    pre-existing UTC comparison so production behaviour does not silently
    shift on this deploy."""
    monkeypatch.setattr(main, "SESSION_DST_AWARE", False, raising=False)
    monkeypatch.setattr(main, "TOKYO_OPEN_UTC", 0, raising=False)
    monkeypatch.setattr(main, "TOKYO_CLOSE_UTC", 9, raising=False)
    monkeypatch.setattr(main, "LONDON_OPEN_UTC", 7, raising=False)
    monkeypatch.setattr(main, "LONDON_CLOSE_UTC", 16, raising=False)
    monkeypatch.setattr(main, "NY_OPEN_UTC", 12, raising=False)
    monkeypatch.setattr(main, "NY_CLOSE_UTC", 21, raising=False)

    # 13:00 UTC → London + NY both active → overlap.
    fixed = datetime(2026, 6, 30, 13, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(main, "datetime", _frozen_datetime(fixed), raising=False)
    s = main.get_current_session()
    assert s["name"] == "LONDON_NY_OVERLAP"


def _frozen_datetime(fixed: datetime):
    """Build a `datetime` shim whose `.now(tz)` returns `fixed`."""
    real = datetime

    class _D(real):  # type: ignore[misc, valid-type]
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return fixed if tz is None else fixed.astimezone(tz)

    return _D


# ─────────────────────────────────────────────────────────────────────────
#  P2 #12 — pricing-stream restart skip
# ─────────────────────────────────────────────────────────────────────────


class _DummyThread:
    def __init__(self) -> None:
        self._alive = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout=None):  # noqa: D401 — match Thread API
        self._alive = False

    def start(self):  # noqa: D401
        self._alive = True


def test_start_price_stream_skips_when_pair_set_unchanged(main, monkeypatch):
    """A second `_start_price_stream` call with the same resolved pair set
    must NOT tear down the running thread (the 24h log dump showed 91 such
    no-op reconnects in 24h)."""
    monkeypatch.setattr(main, "filter_supported_pairs", lambda pairs, _label: list(pairs), raising=False)
    monkeypatch.setattr(main, "open_trades", [], raising=False)
    monkeypatch.setattr(main, "DYNAMIC_PAIRS", ["EUR_USD", "GBP_USD"], raising=False)
    monkeypatch.setattr(main, "STATIC_ALL_PAIRS", ["EUR_USD", "GBP_USD"], raising=False)

    started: list[tuple] = []

    class _CountingThread:
        def __init__(self, *a, **kw) -> None:  # noqa: D401
            self._args = (a, kw)
            self._alive = False

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout=None):
            self._alive = False

        def start(self):
            self._alive = True
            started.append(self._args)

    monkeypatch.setattr(main.threading, "Thread", _CountingThread, raising=False)
    main._stream_thread = None
    main._streamed_pairs = ()
    main._stop_stream_event = threading.Event()

    main._start_price_stream()
    assert len(started) == 1, "first call must start the stream"
    assert main._streamed_pairs == ("EUR_USD", "GBP_USD")

    # Second call with identical resolved set → must short-circuit.
    main._start_price_stream()
    assert len(started) == 1, "no-op reconnect must be suppressed"


def test_start_price_stream_restarts_when_pair_set_changes(main, monkeypatch):
    """When the watchlist changes, the stream must reconnect with the new set."""
    pairs_state = {"current": ["EUR_USD", "GBP_USD"]}
    monkeypatch.setattr(main, "filter_supported_pairs", lambda pairs, _label: sorted(set(pairs)), raising=False)
    monkeypatch.setattr(main, "open_trades", [], raising=False)
    monkeypatch.setattr(main, "STATIC_ALL_PAIRS", ["EUR_USD", "GBP_USD", "USD_JPY"], raising=False)

    started: list[tuple] = []

    class _CountingThread:
        def __init__(self, *a, **kw) -> None:
            self._args = (a, kw)
            self._alive = False

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout=None):
            self._alive = False

        def start(self):
            self._alive = True
            started.append(self._args)

    monkeypatch.setattr(main.threading, "Thread", _CountingThread, raising=False)
    main._stream_thread = None
    main._streamed_pairs = ()
    main._stop_stream_event = threading.Event()

    main._start_price_stream(pairs_state["current"])
    assert len(started) == 1
    main._start_price_stream(["EUR_USD", "GBP_USD", "USD_JPY"])
    assert len(started) == 2, "different pair set must trigger a reconnect"
    assert main._streamed_pairs == ("EUR_USD", "GBP_USD", "USD_JPY")
