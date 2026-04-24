"""Net-of-cost reward/risk gate (Tier 1 §9 of consultant assessment).

On a spread-cost venue (OANDA retail spread-bet) the reward side of the
R:R calculation must be net of all round-trip transaction costs, not
gross of them. This module provides a single ``net_rr`` helper that
strategies call right before submitting an entry.

The default floor is ``1.8`` — i.e. for every £1 at risk on the stop
we want to see £1.80 of *net* reward once round-trip spread, expected
slippage, and expected financing are subtracted from the TP distance.

All quantities are expressed in *pips* for readability. Callers can
pass ``pip_size`` if they want the helper to compute the monetary
equivalent, but for the gate itself pips are enough.
"""
from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MIN_NET_RR = 1.8


@dataclass(frozen=True, slots=True)
class NetRRBreakdown:
    sl_pips: float
    gross_tp_pips: float
    entry_spread_pips: float
    exit_spread_pips: float
    slippage_pips: float
    financing_pips: float
    net_tp_pips: float
    net_rr: float
    passed: bool
    required_rr: float


def compute_net_rr(
    *,
    sl_pips: float,
    tp_pips: float,
    entry_spread_pips: float,
    exit_spread_pips: float | None = None,
    slippage_pips: float = 0.3,
    financing_pips: float = 0.0,
    min_net_rr: float = DEFAULT_MIN_NET_RR,
) -> NetRRBreakdown:
    """Compute the net-of-cost reward/risk and whether it clears ``min_net_rr``.

    * ``sl_pips`` and ``tp_pips`` are the gross distances set on the order
      (ATR-derived, pre-cost).
    * ``entry_spread_pips`` is the expected crossing cost on entry. If the
      bot places a mid-spread limit entry, callers should pass ~half the
      quoted spread (see ``plan_limit_entry``); on market entries pass the
      full quoted spread.
    * ``exit_spread_pips`` defaults to ``entry_spread_pips``.
    * ``slippage_pips`` is the expected execution slippage on fills.
    * ``financing_pips`` is the expected overnight/carry cost over the
      holding period (positive = cost, negative = credit).
    """
    sl = max(0.0, float(sl_pips))
    tp = max(0.0, float(tp_pips))
    entry = max(0.0, float(entry_spread_pips))
    exit_ = entry if exit_spread_pips is None else max(0.0, float(exit_spread_pips))
    slip = max(0.0, float(slippage_pips))
    fin = float(financing_pips)
    net_tp = tp - entry - exit_ - slip - fin
    if sl <= 0:
        rr = 0.0
    else:
        rr = net_tp / sl
    return NetRRBreakdown(
        sl_pips=sl,
        gross_tp_pips=tp,
        entry_spread_pips=entry,
        exit_spread_pips=exit_,
        slippage_pips=slip,
        financing_pips=fin,
        net_tp_pips=net_tp,
        net_rr=rr,
        required_rr=float(min_net_rr),
        passed=rr >= float(min_net_rr),
    )


def net_rr_passes(
    *,
    sl_pips: float,
    tp_pips: float,
    entry_spread_pips: float,
    exit_spread_pips: float | None = None,
    slippage_pips: float = 0.3,
    financing_pips: float = 0.0,
    min_net_rr: float = DEFAULT_MIN_NET_RR,
) -> bool:
    """Boolean convenience wrapper around :func:`compute_net_rr`."""
    return compute_net_rr(
        sl_pips=sl_pips,
        tp_pips=tp_pips,
        entry_spread_pips=entry_spread_pips,
        exit_spread_pips=exit_spread_pips,
        slippage_pips=slippage_pips,
        financing_pips=financing_pips,
        min_net_rr=min_net_rr,
    ).passed
