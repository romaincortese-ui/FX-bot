from datetime import datetime, timedelta, timezone

from fxbot.bayesian_weighting import (
    allocate_weights,
    expected_win_rate,
    new_posterior,
    pick_live_strategy,
    posterior_edge,
    update_posterior,
)


def test_new_posterior_is_fair():
    p = new_posterior("SCALPER")
    assert abs(expected_win_rate(p) - 0.5) < 1e-9
    assert p.trades == 0


def test_update_win_raises_alpha():
    p = new_posterior("TREND")
    p2 = update_posterior(p, win=True)
    assert p2.alpha == p.alpha + 1
    assert p2.beta == p.beta
    assert p2.trades == 1
    assert p2.last_update_utc is not None


def test_update_loss_raises_beta():
    p = new_posterior("TREND")
    p2 = update_posterior(p, win=False)
    assert p2.beta == p.beta + 1


def test_posterior_edge_winner():
    p = new_posterior("TREND")
    for _ in range(20):
        p = update_posterior(p, win=True)
    assert posterior_edge(p, avg_win_r=2.0, avg_loss_r=1.0) > 1.0


def test_posterior_edge_loser():
    p = new_posterior("REVERSAL")
    for _ in range(20):
        p = update_posterior(p, win=False)
    assert posterior_edge(p, avg_win_r=1.0, avg_loss_r=1.0) < 0.0


def test_allocate_weights_proportional():
    winner = new_posterior("TREND")
    loser = new_posterior("REVERSAL")
    for _ in range(20):
        winner = update_posterior(winner, win=True)
    for _ in range(20):
        loser = update_posterior(loser, win=False)
    w = allocate_weights([winner, loser])
    assert w["TREND"] > w["REVERSAL"]
    assert abs(sum(w.values()) - 1.0) < 1e-9


def test_allocate_weights_all_equal_on_zero_edge():
    losers = [new_posterior("A"), new_posterior("B"), new_posterior("C")]
    for p in list(losers):
        for _ in range(10):
            pass  # no-op; keep fair priors
    w = allocate_weights(losers, avg_win_r=1.0, avg_loss_r=2.0)
    # All edges are negative → equal weight fallback.
    for v in w.values():
        assert abs(v - 1 / 3) < 1e-9


def test_dark_rescue_applies_floor():
    now = datetime(2026, 4, 21, tzinfo=timezone.utc)
    stale = new_posterior("DARK")
    # Emulate an old "last_update" 60 days ago by rebuilding:
    from dataclasses import replace
    stale = replace(stale, last_update_utc=now - timedelta(days=60))
    fresh = new_posterior("FRESH")
    for _ in range(10):
        fresh = update_posterior(fresh, win=True, now_utc=now)
    w = allocate_weights([stale, fresh], now_utc=now, min_weight_floor=0.1)
    assert w["DARK"] >= 0.09   # floor applied then renormalised
    assert w["FRESH"] > w["DARK"]


def test_pick_live_strategy_highest_edge():
    winner = new_posterior("A")
    loser = new_posterior("B")
    for _ in range(10):
        winner = update_posterior(winner, win=True)
        loser = update_posterior(loser, win=False)
    assert pick_live_strategy([winner, loser]) == "A"


def test_pick_live_strategy_empty_is_none():
    assert pick_live_strategy([]) is None
