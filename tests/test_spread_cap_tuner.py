"""Tier 2v2 E1 — SpreadSampler / recommend_spread_cap tests."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from fxbot.spread_cap_tuner import (
    SpreadSampler,
    recommend_spread_cap,
    session_for_hour,
)


# ---------- recommend_spread_cap (pure) ----------

def test_recommend_returns_none_below_min_samples():
    assert recommend_spread_cap([1.0, 2.0, 3.0], min_samples=30) is None


def test_recommend_p75_clipped_to_floor():
    samples = [0.1] * 40
    # P75 = 0.1 → floor kicks in.
    assert recommend_spread_cap(samples, percentile=0.75, floor_pips=0.5) == pytest.approx(0.5)


def test_recommend_p75_clipped_to_ceiling():
    samples = [50.0] * 40
    assert recommend_spread_cap(samples, percentile=0.75, ceiling_pips=10.0) == pytest.approx(10.0)


def test_recommend_linear_interp_matches_expected():
    # 10 samples 1..10, P75 by linear interpolation = 7.75.
    samples = [float(i) for i in range(1, 11)]
    assert recommend_spread_cap(samples, percentile=0.75, min_samples=10) == pytest.approx(7.75)


def test_recommend_ignores_negative_and_none():
    samples = [None, -1.0] + [2.0] * 30
    # negative / None filtered; P75 of thirty 2.0s = 2.0
    assert recommend_spread_cap(samples, percentile=0.75, min_samples=30) == pytest.approx(2.0)


# ---------- session_for_hour ----------

@pytest.mark.parametrize(
    "hour,expected",
    [
        (0, "tokyo"), (6, "tokyo"), (23, "tokyo"),
        (7, "london"), (11, "london"),
        (12, "london_ny"), (15, "london_ny"),
        (16, "ny"), (20, "ny"),
        (21, "late_ny"), (22, "late_ny"),
    ],
)
def test_session_for_hour(hour, expected):
    assert session_for_hour(hour) == expected


# ---------- SpreadSampler ----------

def test_sampler_records_and_recommends_per_session():
    s = SpreadSampler(max_samples=200)
    london = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)
    ny = datetime(2026, 4, 24, 17, 0, tzinfo=timezone.utc)

    for _ in range(40):
        s.record(instrument="EUR_USD", spread_pips=0.4, now_utc=london)
    for _ in range(40):
        s.record(instrument="EUR_USD", spread_pips=1.2, now_utc=ny)

    lon_cap = s.recommend(instrument="EUR_USD", session="london")
    ny_cap = s.recommend(instrument="EUR_USD", session="ny")

    # London is tighter, NY is wider — tuner must reflect that.
    assert lon_cap is not None and ny_cap is not None
    assert ny_cap > lon_cap


def test_sampler_rejects_invalid_input():
    s = SpreadSampler()
    assert s.record(instrument="", spread_pips=1.0) is None
    assert s.record(instrument="EUR_USD", spread_pips=-0.5) is None
    assert s.record(instrument="EUR_USD", spread_pips=None) is None  # type: ignore[arg-type]


def test_sampler_blended_cap_never_tighter_than_static():
    s = SpreadSampler()
    london = datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)
    for _ in range(40):
        s.record(instrument="EUR_USD", spread_pips=0.3, now_utc=london)
    # Auto-recommendation would be 0.5 (floor), but static cap of 1.8 must win.
    cap = s.blended_cap(instrument="EUR_USD", session="london", static_cap_pips=1.8)
    assert cap == pytest.approx(1.8)


def test_sampler_blended_cap_relaxes_when_data_is_wider():
    s = SpreadSampler()
    ny = datetime(2026, 4, 24, 17, 0, tzinfo=timezone.utc)
    # Heavy practice-style widening 3.5p on NY close.
    for _ in range(40):
        s.record(instrument="NZD_USD", spread_pips=3.5, now_utc=ny)
    cap = s.blended_cap(instrument="NZD_USD", session="ny", static_cap_pips=1.5)
    # Static cap would reject; auto should relax to ~3.5.
    assert cap >= 3.4


def test_sampler_blended_cap_falls_back_when_insufficient():
    s = SpreadSampler()
    s.record(instrument="EUR_USD", spread_pips=5.0)  # 1 sample only
    cap = s.blended_cap(instrument="EUR_USD", session="london", static_cap_pips=2.0)
    assert cap == pytest.approx(2.0)
