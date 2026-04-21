"""Bayesian strategy weighting (FX-bot Q3 §4.4).

Maintain a Beta-Bernoulli posterior over each strategy's win probability.
After every closed trade, update the posterior; allocate per-strategy
risk proportional to a score derived from the posterior expected edge,
with a floor so no strategy disappears for more than 30 days.

Pure functions and a small ``StrategyPosterior`` dataclass. The caller
is responsible for persistence (JSON on disk, etc.) — this module only
exposes the maths.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class StrategyPosterior:
    strategy: str
    alpha: float                     # Beta alpha (wins + prior)
    beta: float                      # Beta beta (losses + prior)
    trades: int
    last_update_utc: datetime | None


def new_posterior(
    strategy: str,
    *,
    prior_alpha: float = 5.0,
    prior_beta: float = 5.0,
) -> StrategyPosterior:
    """Create a fresh posterior with a weakly-informative Beta(5, 5)
    prior (expected win rate 0.5, equivalent to 10 prior trades).
    """
    return StrategyPosterior(
        strategy=strategy.upper(),
        alpha=float(prior_alpha),
        beta=float(prior_beta),
        trades=0,
        last_update_utc=None,
    )


def update_posterior(
    posterior: StrategyPosterior,
    *,
    win: bool,
    now_utc: datetime | None = None,
) -> StrategyPosterior:
    """Single-trade Bernoulli update."""
    ts = now_utc or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if win:
        return replace(
            posterior,
            alpha=posterior.alpha + 1.0,
            trades=posterior.trades + 1,
            last_update_utc=ts,
        )
    return replace(
        posterior,
        beta=posterior.beta + 1.0,
        trades=posterior.trades + 1,
        last_update_utc=ts,
    )


def expected_win_rate(p: StrategyPosterior) -> float:
    total = p.alpha + p.beta
    if total <= 0:
        return 0.5
    return p.alpha / total


def posterior_edge(
    p: StrategyPosterior,
    *,
    avg_win_r: float = 1.0,
    avg_loss_r: float = 1.0,
) -> float:
    """Expected R-multiple per trade under the posterior.

    edge = p(win) * avg_win_r - (1 - p(win)) * avg_loss_r
    """
    wr = expected_win_rate(p)
    return wr * float(avg_win_r) - (1.0 - wr) * float(avg_loss_r)


def allocate_weights(
    posteriors: list[StrategyPosterior],
    *,
    now_utc: datetime | None = None,
    min_weight_floor: float = 0.05,
    dark_rescue_days: int = 30,
    avg_win_r: float = 1.0,
    avg_loss_r: float = 1.0,
) -> dict[str, float]:
    """Allocate weights in [0, 1], summing to 1.0.

    * Raw allocation ∝ max(0, posterior_edge).
    * Any strategy not traded in ``dark_rescue_days`` is forced to at
      least ``min_weight_floor`` so nothing goes dark permanently.
    * If all edges are ≤ 0, fall back to equal-weight.
    """
    if not posteriors:
        return {}
    ts = now_utc or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    edges = {
        p.strategy: max(0.0, posterior_edge(p, avg_win_r=avg_win_r, avg_loss_r=avg_loss_r))
        for p in posteriors
    }
    total = sum(edges.values())
    if total <= 0:
        w = 1.0 / len(posteriors)
        weights = {p.strategy: w for p in posteriors}
    else:
        weights = {s: e / total for s, e in edges.items()}
    # Dark-rescue floor.
    for p in posteriors:
        if p.last_update_utc is None:
            days_since = None
        else:
            days_since = (ts - p.last_update_utc).days
        if days_since is None or days_since >= int(dark_rescue_days):
            if weights[p.strategy] < float(min_weight_floor):
                weights[p.strategy] = float(min_weight_floor)
    # Renormalise.
    s = sum(weights.values())
    if s > 0:
        weights = {k: v / s for k, v in weights.items()}
    return weights


def pick_live_strategy(
    posteriors: list[StrategyPosterior],
    *,
    avg_win_r: float = 1.0,
    avg_loss_r: float = 1.0,
) -> str | None:
    """Return the strategy with the highest posterior edge, or None."""
    if not posteriors:
        return None
    best = max(
        posteriors,
        key=lambda p: posterior_edge(p, avg_win_r=avg_win_r, avg_loss_r=avg_loss_r),
    )
    return best.strategy
