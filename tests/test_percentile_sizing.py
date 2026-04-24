"""Tests for Tier 2 §12 — score-percentile sizing."""
from __future__ import annotations

from fxbot.percentile_sizing import score_percentile, size_by_percentile


def test_score_percentile_empty_history_returns_midpoint():
    assert score_percentile(50.0, []) == 0.5


def test_score_percentile_monotonic_in_rank():
    history = [10.0, 20.0, 30.0, 40.0, 50.0]
    low = score_percentile(15.0, history)
    mid = score_percentile(30.0, history)
    high = score_percentile(55.0, history)
    assert low < mid < high
    assert high == 1.0


def test_size_by_percentile_insufficient_samples_no_op():
    decision = size_by_percentile(score=50.0, history=[40.0], min_samples=20)
    assert decision.multiplier == 1.0
    assert decision.percentile is None
    assert "insufficient" in decision.reason


def test_size_by_percentile_high_score_caps_at_cap():
    history = [float(i) for i in range(40)]
    decision = size_by_percentile(
        score=10_000.0,
        history=history,
        min_samples=20,
        floor=0.5,
        cap=2.0,
    )
    assert decision.multiplier == 2.0
    assert decision.percentile == 1.0


def test_size_by_percentile_low_score_clamps_to_floor():
    history = [float(i) for i in range(40)]
    decision = size_by_percentile(
        score=-1_000.0,
        history=history,
        min_samples=20,
        floor=0.5,
        cap=2.0,
    )
    assert decision.multiplier == 0.5


def test_size_by_percentile_median_score_close_to_unit():
    history = [float(i) for i in range(40)]
    decision = size_by_percentile(
        score=19.0,
        history=history,
        min_samples=20,
        floor=0.5,
        cap=2.0,
    )
    # Median percentile maps to ~1.0 under centre=0.5.
    assert 0.9 <= decision.multiplier <= 1.1
