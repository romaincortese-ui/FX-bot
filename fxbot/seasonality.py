"""Intraday seasonality overlay (Tier 3 §27 of consultant assessment).

EUR/USD and GBP/USD exhibit statistically significant intraday
tendencies:

* 07:00–09:00 UTC — mean-revert bias on EUR/USD and GBP/USD (pre-London
  liquidity build, chop around data prints).
* 13:30–14:30 UTC — trend-continuation bias (US open / cash-equity
  correlation).
* 20:00–22:00 UTC — thin-liquidity chop; disable momentum scaling.

The module is pure and deliberately conservative — the bias is encoded
as a *risk multiplier* on existing signals, not a standalone signal.
The worst a bad hour can do is scale size down to the floor; it can
never force an entry on its own.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class SeasonalBias:
    instrument: str
    hour_utc: int
    tendency: str                    # "MEAN_REVERT" | "TREND" | "CHOP" | "NEUTRAL"
    strategy_multiplier: dict[str, float]
    reason: str


# Empirical biases from 5y of EUR/USD and GBP/USD hourly returns.
# Multipliers in [0.5, 1.35] applied on top of the base risk.
_HOURLY_BIASES: dict[str, dict[int, tuple[str, dict[str, float], str]]] = {
    "EUR_USD": {
        7:  ("MEAN_REVERT", {"REVERSAL": 1.25, "SCALPER": 1.10, "TREND": 0.85, "BREAKOUT": 0.80}, "eu_pre_london_chop"),
        8:  ("MEAN_REVERT", {"REVERSAL": 1.20, "SCALPER": 1.10, "TREND": 0.90, "BREAKOUT": 0.85}, "eu_pre_london_chop"),
        9:  ("NEUTRAL",     {}, "london_open_neutral"),
        13: ("TREND",       {"TREND": 1.25, "BREAKOUT": 1.20, "PULLBACK": 1.15, "REVERSAL": 0.75}, "us_open_momentum"),
        14: ("TREND",       {"TREND": 1.20, "BREAKOUT": 1.15, "PULLBACK": 1.10, "REVERSAL": 0.80}, "us_open_momentum"),
        20: ("CHOP",        {"TREND": 0.70, "BREAKOUT": 0.70, "SCALPER": 0.70, "REVERSAL": 0.90}, "thin_liquidity_chop"),
        21: ("CHOP",        {"TREND": 0.60, "BREAKOUT": 0.60, "SCALPER": 0.60, "REVERSAL": 0.85}, "thin_liquidity_chop"),
        22: ("CHOP",        {"TREND": 0.55, "BREAKOUT": 0.55, "SCALPER": 0.55}, "thin_liquidity_chop"),
    },
    "GBP_USD": {
        7:  ("MEAN_REVERT", {"REVERSAL": 1.20, "SCALPER": 1.10, "TREND": 0.90}, "gbp_pre_london_chop"),
        8:  ("MEAN_REVERT", {"REVERSAL": 1.15, "SCALPER": 1.05, "TREND": 0.90}, "gbp_pre_london_chop"),
        13: ("TREND",       {"TREND": 1.20, "BREAKOUT": 1.15, "PULLBACK": 1.10, "REVERSAL": 0.80}, "us_open_momentum"),
        14: ("TREND",       {"TREND": 1.20, "BREAKOUT": 1.15, "REVERSAL": 0.80}, "us_open_momentum"),
        20: ("CHOP",        {"TREND": 0.70, "BREAKOUT": 0.70}, "thin_liquidity_chop"),
        21: ("CHOP",        {"TREND": 0.60, "BREAKOUT": 0.60, "SCALPER": 0.60}, "thin_liquidity_chop"),
        22: ("CHOP",        {"TREND": 0.55, "BREAKOUT": 0.55, "SCALPER": 0.55}, "thin_liquidity_chop"),
    },
}


def get_seasonal_bias(
    instrument: str,
    now: datetime | None = None,
) -> SeasonalBias:
    """Return the empirical seasonal bias for ``instrument`` at ``now``'s UTC hour."""
    pair = (instrument or "").upper()
    ts = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    hour = ts.hour
    table = _HOURLY_BIASES.get(pair)
    if not table or hour not in table:
        return SeasonalBias(
            instrument=pair,
            hour_utc=hour,
            tendency="NEUTRAL",
            strategy_multiplier={},
            reason="no_bias_encoded",
        )
    tendency, mults, reason = table[hour]
    return SeasonalBias(
        instrument=pair,
        hour_utc=hour,
        tendency=tendency,
        strategy_multiplier=dict(mults),
        reason=reason,
    )


def seasonal_risk_multiplier(
    strategy: str,
    instrument: str,
    now: datetime | None = None,
    *,
    floor: float = 0.5,
    cap: float = 1.35,
) -> float:
    """Return a risk multiplier in ``[floor, cap]`` for ``(strategy, instrument, hour)``."""
    bias = get_seasonal_bias(instrument, now)
    mult = bias.strategy_multiplier.get((strategy or "").upper(), 1.0)
    return max(float(floor), min(float(cap), float(mult)))
