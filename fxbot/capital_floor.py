"""Capital-floor safety gate (Tier 2v2 E2).

Third-memo §4 + §8 E2 diagnosis: at an operating balance of £194.75, every
sizing function (percentile sizer, Kelly multiplier, ATR-R sizing) rounds
*up* to OANDA's £0.10/pip minimum spread-bet stake. Per-trade risk is
therefore structurally 5–10× what the code thinks it is.

Memo's preferred mitigation is option (a): "paper-trade only until balance
> £10 k, flagged in README and Telegram status." Option (b) — a fractional
pip accumulator — is deferred.

This module is a pure policy evaluator. It never touches OANDA state and
never mutates global config. The caller composes the result with its own
``PAPER_TRADE`` env flag; below the floor we *force* paper mode so an
operator cannot accidentally ship real orders on an under-capitalised
account.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CapitalFloorDecision:
    below_floor: bool                    # balance strictly < min_balance
    force_paper_trade: bool              # effective paper-trade state
    reason: str                          # human-readable for Telegram / logs
    account_balance: float
    min_balance: float


def evaluate_capital_floor(
    *,
    account_balance: float,
    min_balance: float,
    paper_trade: bool = False,
    enabled: bool = True,
) -> CapitalFloorDecision:
    """Decide whether live order submission is safe at this balance.

    * ``enabled=False`` is a hard bypass (legacy behaviour, dev runs).
    * ``paper_trade=True`` keeps the forced-paper flag true regardless
      (explicit intent always wins).
    * ``min_balance <= 0`` disables the floor silently.
    * Below floor: ``force_paper_trade=True`` with a reason string so the
      caller can surface it in ``runtime_status`` / Telegram.
    """
    balance = float(account_balance) if account_balance is not None else 0.0
    floor = float(min_balance) if min_balance and min_balance > 0 else 0.0

    if not enabled or floor <= 0.0:
        return CapitalFloorDecision(
            below_floor=False,
            force_paper_trade=bool(paper_trade),
            reason="capital_floor_disabled" if not enabled else "no_floor_configured",
            account_balance=balance,
            min_balance=floor,
        )

    below = balance < floor
    if paper_trade:
        reason = "paper_trade_explicit"
    elif below:
        reason = (
            f"capital_floor_breached: balance={balance:.2f} < min={floor:.2f} "
            f"(memo E2: paper-only below floor)"
        )
    else:
        reason = "above_capital_floor"

    return CapitalFloorDecision(
        below_floor=below,
        force_paper_trade=bool(paper_trade) or below,
        reason=reason,
        account_balance=balance,
        min_balance=floor,
    )


def capital_floor_status_fields(decision: CapitalFloorDecision) -> dict:
    """Flatten a decision into runtime-status / Telegram friendly fields."""
    return {
        "capital_floor_enabled": decision.min_balance > 0.0,
        "capital_floor_min_balance": round(decision.min_balance, 2),
        "capital_floor_balance": round(decision.account_balance, 2),
        "capital_floor_below": bool(decision.below_floor),
        "capital_floor_force_paper": bool(decision.force_paper_trade),
        "capital_floor_reason": decision.reason,
    }
