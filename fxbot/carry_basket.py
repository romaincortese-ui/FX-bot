"""Professional carry basket (FX-bot Tier 3 §4.1).

Replaces the 5-day spread trade with a ranked carry book:

* Rank 8-10 liquid currencies by 3-month deposit rate.
* Long top N, short bottom N (equal risk weights).
* Weekly rebalance.
* Kill-switch: scale exposure down when USD/JPY 1w IV > 11%, zero
  above 13% — carry unwinds happen in FX vol, not equity vol.
* Target portfolio vol ~7-8% annualised.

Pure-function module. ``build_carry_basket`` takes currency rates +
vols and returns a list of ``CarryLeg``s that the caller translates to
OANDA instruments.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CurrencyRate:
    currency: str
    deposit_rate_3m_pct: float
    annualised_vol_pct: float        # realised 20d vol of trade-weighted index
    liquid: bool = True              # filters out illiquid EM


@dataclass(frozen=True, slots=True)
class CarryLeg:
    currency: str
    direction: str                   # "LONG" | "SHORT"
    rank: int                        # 1 = top carry, 1 = bottom for shorts
    weight: float                    # risk weight (fraction of basket)
    deposit_rate_3m_pct: float


@dataclass(frozen=True, slots=True)
class CarryBasket:
    legs: tuple[CarryLeg, ...]
    expected_annual_carry_pct: float
    target_portfolio_vol_pct: float
    exposure_multiplier: float       # 0..1 after kill-switch scaling
    reason: str


def compute_exposure_multiplier(
    *,
    usdjpy_1w_iv_pct: float | None,
    scale_down_iv: float = 11.0,
    zero_iv: float = 13.0,
) -> float:
    """Kill-switch scaler in [0, 1] based on USD/JPY 1-week implied vol.

    Linear ramp from 1.0 at ``scale_down_iv`` to 0.5 at the midpoint to
    0.0 at ``zero_iv``.
    """
    if usdjpy_1w_iv_pct is None:
        return 1.0
    iv = float(usdjpy_1w_iv_pct)
    if iv <= float(scale_down_iv):
        return 1.0
    if iv >= float(zero_iv):
        return 0.0
    span = float(zero_iv) - float(scale_down_iv)
    if span <= 0:
        return 0.0
    return 1.0 - (iv - float(scale_down_iv)) / span


def build_carry_basket(
    *,
    rates: list[CurrencyRate],
    top_n: int = 3,
    bottom_n: int = 3,
    usdjpy_1w_iv_pct: float | None = None,
    target_portfolio_vol_pct: float = 8.0,
    scale_down_iv: float = 11.0,
    zero_iv: float = 13.0,
) -> CarryBasket:
    """Construct an equal-risk-weight carry basket.

    ``rates`` should contain 8-10 liquid currencies. The top ``top_n``
    by deposit rate go LONG; the bottom ``bottom_n`` go SHORT. Weights
    are equal within each side and sum to 1.0 across the basket.
    """
    liquid = [r for r in rates if r.liquid and r.annualised_vol_pct > 0]
    if len(liquid) < top_n + bottom_n:
        return CarryBasket(
            legs=(),
            expected_annual_carry_pct=0.0,
            target_portfolio_vol_pct=float(target_portfolio_vol_pct),
            exposure_multiplier=0.0,
            reason="insufficient_liquid_currencies",
        )
    sorted_desc = sorted(liquid, key=lambda r: r.deposit_rate_3m_pct, reverse=True)
    longs = sorted_desc[:top_n]
    shorts = sorted_desc[-bottom_n:]
    # Equal risk weight — normalise by vol so each leg contributes
    # roughly equal P&L variance.
    long_total_inv_vol = sum(1.0 / r.annualised_vol_pct for r in longs)
    short_total_inv_vol = sum(1.0 / r.annualised_vol_pct for r in shorts)
    half = 0.5  # long side weight sum = short side weight sum = 0.5
    legs: list[CarryLeg] = []
    expected_carry = 0.0
    for i, r in enumerate(longs, start=1):
        w = half * (1.0 / r.annualised_vol_pct) / long_total_inv_vol
        legs.append(
            CarryLeg(
                currency=r.currency.upper(),
                direction="LONG",
                rank=i,
                weight=w,
                deposit_rate_3m_pct=r.deposit_rate_3m_pct,
            )
        )
        expected_carry += w * r.deposit_rate_3m_pct
    for i, r in enumerate(reversed(shorts), start=1):
        w = half * (1.0 / r.annualised_vol_pct) / short_total_inv_vol
        legs.append(
            CarryLeg(
                currency=r.currency.upper(),
                direction="SHORT",
                rank=i,
                weight=w,
                deposit_rate_3m_pct=r.deposit_rate_3m_pct,
            )
        )
        expected_carry -= w * r.deposit_rate_3m_pct
    mult = compute_exposure_multiplier(
        usdjpy_1w_iv_pct=usdjpy_1w_iv_pct,
        scale_down_iv=scale_down_iv,
        zero_iv=zero_iv,
    )
    reason = (
        f"carry_basket_top{top_n}_bot{bottom_n}_expcarry={expected_carry:+.2f}%"
        f"_mult={mult:.2f}"
    )
    return CarryBasket(
        legs=tuple(legs),
        expected_annual_carry_pct=expected_carry,
        target_portfolio_vol_pct=float(target_portfolio_vol_pct),
        exposure_multiplier=mult,
        reason=reason,
    )


def should_rebalance(
    *,
    last_rebalance_days_ago: int | None,
    rebalance_interval_days: int = 7,
) -> bool:
    """Weekly rebalance by default. None → yes (first build)."""
    if last_rebalance_days_ago is None:
        return True
    return int(last_rebalance_days_ago) >= int(rebalance_interval_days)


def drawdown_kill(
    *,
    basket_drawdown_pct: float,
    expected_annual_carry_pct: float,
    holding_days: int,
    kill_ratio: float = 1.5,
) -> bool:
    """Kill a carry leg/basket when drawdown exceeds ``kill_ratio`` ×
    the carry accrued so far. Per memo: "carry trades that draw down
    more than 1.5× the annualised carry historically do not recover."
    """
    # Accrued carry over the holding period.
    if expected_annual_carry_pct <= 0 or holding_days <= 0:
        return False
    accrued = expected_annual_carry_pct * (holding_days / 365.0)
    return abs(float(basket_drawdown_pct)) > float(kill_ratio) * abs(accrued)
