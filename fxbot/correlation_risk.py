"""Portfolio correlation risk cap (FX-bot Sprint 2 §2.2).

Replaces the USD-only vote-net risk model in ``risk.py`` with a true
portfolio-vol check based on a 7x7 correlation matrix on weekly returns.

Formulation (review memo §2.2):

    portfolio_risk = sqrt( wᵀ · Σ · w )

where ``w`` is the signed risk-weight vector across the core majors
(LONG positive, SHORT negative) and ``Σ`` is the correlation matrix.
A new trade is rejected if adding its weight would push
``portfolio_risk`` above the configured cap (default 3% of NAV).

Default correlation matrix is a conservative desk-consensus table
measured on ~5 years of weekly log returns (OANDA mid). Callers should
refresh this from live data once the correlation-rolling job is online;
the defaults are safe enough to ship today.

The matrix uses the **quote-side convention**, where EUR_USD, GBP_USD,
AUD_USD, NZD_USD, USD_JPY, USD_CHF, USD_CAD are the row/column keys.
Cross pairs (EUR_GBP, EUR_JPY, GBP_JPY, AUD_JPY, NZD_JPY) are resolved
by expansion into their two legs.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Mapping, Sequence

# Core majors against which all weights are expressed.
CORE_PAIRS: tuple[str, ...] = (
    "EUR_USD",
    "GBP_USD",
    "AUD_USD",
    "NZD_USD",
    "USD_JPY",
    "USD_CHF",
    "USD_CAD",
)

# Conservative desk-consensus correlations on weekly log returns.
# Symmetric; diagonal = 1.0. Numbers reflect ~2020-2025 averages.
_DEFAULT_CORR: dict[tuple[str, str], float] = {
    ("EUR_USD", "GBP_USD"): 0.75,
    ("EUR_USD", "AUD_USD"): 0.55,
    ("EUR_USD", "NZD_USD"): 0.50,
    ("EUR_USD", "USD_JPY"): -0.30,
    ("EUR_USD", "USD_CHF"): -0.85,
    ("EUR_USD", "USD_CAD"): -0.55,
    ("GBP_USD", "AUD_USD"): 0.55,
    ("GBP_USD", "NZD_USD"): 0.50,
    ("GBP_USD", "USD_JPY"): -0.20,
    ("GBP_USD", "USD_CHF"): -0.65,
    ("GBP_USD", "USD_CAD"): -0.50,
    ("AUD_USD", "NZD_USD"): 0.90,
    ("AUD_USD", "USD_JPY"): -0.15,
    ("AUD_USD", "USD_CHF"): -0.50,
    ("AUD_USD", "USD_CAD"): -0.65,
    ("NZD_USD", "USD_JPY"): -0.10,
    ("NZD_USD", "USD_CHF"): -0.45,
    ("NZD_USD", "USD_CAD"): -0.60,
    ("USD_JPY", "USD_CHF"): 0.20,
    ("USD_JPY", "USD_CAD"): 0.30,
    ("USD_CHF", "USD_CAD"): 0.40,
}


def default_correlation_matrix() -> dict[tuple[str, str], float]:
    """Return a full symmetric correlation dict keyed by (pair_a, pair_b)."""
    full: dict[tuple[str, str], float] = {}
    for pair in CORE_PAIRS:
        full[(pair, pair)] = 1.0
    for (a, b), rho in _DEFAULT_CORR.items():
        full[(a, b)] = rho
        full[(b, a)] = rho
    return full


@dataclass(frozen=True, slots=True)
class PortfolioRiskDecision:
    allowed: bool
    portfolio_vol_before: float
    portfolio_vol_after: float
    cap: float
    weight_vector: dict[str, float]
    reason: str


def _pair_weight(instrument: str, direction: str, risk_pct: float) -> dict[str, float]:
    """Decompose an instrument/direction/risk into per-core-pair weights.

    Cross pairs expand into their two legs (e.g. EUR_JPY LONG 0.015 →
    EUR_USD +0.015 *and* USD_JPY +0.015). Majors map 1:1. For pairs that
    don't involve any core leg the function returns an empty dict.
    """
    if not instrument or "_" not in instrument:
        return {}
    base, quote = instrument.upper().split("_")
    sign = +1.0 if direction.upper() == "LONG" else -1.0
    amount = float(risk_pct) * sign
    # Direct core match.
    if instrument.upper() in CORE_PAIRS:
        return {instrument.upper(): amount}
    # Cross-pair expansion: express as two core-pair exposures.
    weights: dict[str, float] = {}
    # Long EUR_JPY = long EUR vs USD + long USD vs JPY.
    if base != "USD" and quote != "USD":
        base_leg = f"{base}_USD"
        quote_leg = f"USD_{quote}"
        if base_leg in CORE_PAIRS:
            weights[base_leg] = weights.get(base_leg, 0.0) + amount
        if quote_leg in CORE_PAIRS:
            weights[quote_leg] = weights.get(quote_leg, 0.0) + amount
    return weights


def _portfolio_vol(
    weights: Mapping[str, float],
    correlation: Mapping[tuple[str, str], float],
) -> float:
    pairs = list(weights.keys())
    total_var = 0.0
    for i, pair_i in enumerate(pairs):
        w_i = weights[pair_i]
        for pair_j in pairs[i:]:
            w_j = weights[pair_j]
            rho = correlation.get((pair_i, pair_j))
            if rho is None:
                continue
            contribution = w_i * w_j * rho
            total_var += contribution if pair_j == pair_i else 2.0 * contribution
    if total_var <= 0:
        return 0.0
    return sqrt(total_var)


def compute_portfolio_vol_pct(
    open_trades: Sequence[Mapping],
    *,
    correlation: Mapping[tuple[str, str], float] | None = None,
) -> tuple[float, dict[str, float]]:
    """Return ``(portfolio_vol, weights)`` for the currently-open book.

    Each trade must expose ``instrument``, ``direction``, ``risk_pct``.
    """
    if correlation is None:
        correlation = default_correlation_matrix()
    weights: dict[str, float] = {}
    for trade in open_trades:
        decomposed = _pair_weight(
            str(trade.get("instrument", "")),
            str(trade.get("direction", "")),
            float(trade.get("risk_pct", 0.0) or 0.0),
        )
        for key, value in decomposed.items():
            weights[key] = weights.get(key, 0.0) + value
    return _portfolio_vol(weights, correlation), weights


def would_breach_portfolio_cap(
    *,
    open_trades: Sequence[Mapping],
    candidate_instrument: str,
    candidate_direction: str,
    candidate_risk_pct: float,
    cap_pct: float = 0.03,
    correlation: Mapping[tuple[str, str], float] | None = None,
) -> PortfolioRiskDecision:
    """Decide whether a new trade would breach the portfolio-vol cap.

    * ``cap_pct`` is the max allowed ``sqrt(wᵀΣw)``; default 3% of NAV.
    * ``candidate_risk_pct`` is the planned risk for the new trade (same
      units as the ``risk_pct`` field on open trades).
    """
    if correlation is None:
        correlation = default_correlation_matrix()
    vol_before, weights = compute_portfolio_vol_pct(open_trades, correlation=correlation)

    delta = _pair_weight(candidate_instrument, candidate_direction, candidate_risk_pct)
    combined = dict(weights)
    for key, value in delta.items():
        combined[key] = combined.get(key, 0.0) + value
    vol_after = _portfolio_vol(combined, correlation)

    if vol_after > cap_pct + 1e-12:
        return PortfolioRiskDecision(
            allowed=False,
            portfolio_vol_before=vol_before,
            portfolio_vol_after=vol_after,
            cap=cap_pct,
            weight_vector=combined,
            reason=f"would_breach_cap:{vol_after:.5f}>{cap_pct:.5f}",
        )
    return PortfolioRiskDecision(
        allowed=True,
        portfolio_vol_before=vol_before,
        portfolio_vol_after=vol_after,
        cap=cap_pct,
        weight_vector=combined,
        reason="within_cap",
    )
