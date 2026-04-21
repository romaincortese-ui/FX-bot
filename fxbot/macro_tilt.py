"""Continuous macro tilt (FX-bot Sprint 3 §3.3).

The existing ``macro_logic.py`` uses DXY / VIX / rate-spread / commodity
/ ESI data as **binary** LONG_ONLY / SHORT_ONLY gates. That throws away
most of the information: a DXY at +2σ vs its mean is a much stronger
bias than DXY at +0.2σ, but the binary gate treats them identically.

This module produces a continuous macro tilt in ``[-1, +1]`` per the
memo formula:

    macro_score = 0.35·dxy_tilt + 0.25·rate_spread_z + 0.20·esi_z
                 + 0.10·commodity_tilt + 0.10·vix_regime

The returned tilt is USD-relative (positive = USD bid, negative = USD
sold). Callers should flip the sign for pairs where USD is the base
currency (USD/JPY, USD/CHF, USD/CAD) vs where USD is the quote
(EUR/USD, GBP/USD, AUD/USD, NZD/USD). For non-USD crosses (EUR/GBP,
AUD/NZD, etc.) the USD tilt has no direct sign — the module provides
a ``pair_tilt_multiplier`` helper that returns the correct per-pair
multiplier.

Every input is optional. Missing inputs are simply skipped and the
remaining weights renormalise, matching the convention in
``direction_score.py`` from Sprint 1.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import tanh


_COMPONENT_WEIGHTS: dict[str, float] = {
    "dxy_tilt": 0.35,
    "rate_spread_z": 0.25,
    "esi_z": 0.20,
    "commodity_tilt": 0.10,
    "vix_regime": 0.10,
}


@dataclass(frozen=True, slots=True)
class MacroTilt:
    score: float                       # USD-relative tilt in [-1, +1]
    components: dict[str, float]       # each in [-1, +1]
    contributing_weight: float         # sum of weights for non-None components


def _clamp(v: float, lo: float = -1.0, hi: float = +1.0) -> float:
    return max(lo, min(hi, float(v)))


def compute_dxy_tilt(dxy_z: float | None) -> float | None:
    """Map DXY z-score (20-day) to ``tanh(z / 1.5)``; clamped."""
    if dxy_z is None:
        return None
    return _clamp(tanh(float(dxy_z) / 1.5))


def compute_rate_spread_tilt(us_minus_other_2y_bp: float | None) -> float | None:
    """2Y spread in bp → tanh(bp / 75). +USD if US 2Y > counter-currency."""
    if us_minus_other_2y_bp is None:
        return None
    return _clamp(tanh(float(us_minus_other_2y_bp) / 75.0))


def compute_esi_tilt(us_esi_z: float | None) -> float | None:
    """US Economic Surprise Index z-score → tanh(z / 1.0)."""
    if us_esi_z is None:
        return None
    return _clamp(tanh(float(us_esi_z) / 1.0))


def compute_commodity_tilt(commodity_20d_return_pct: float | None) -> float | None:
    """Commodities rising → USD softer (negative tilt)."""
    if commodity_20d_return_pct is None:
        return None
    # -tanh because commodity strength is inversely related to USD.
    return _clamp(-tanh(float(commodity_20d_return_pct) / 5.0))


def compute_vix_regime_tilt(vix_percentile_60d: float | None) -> float | None:
    """High VIX → USD bid (safe haven)."""
    if vix_percentile_60d is None:
        return None
    # Map percentile [0, 100] to [-1, +1].
    return _clamp((float(vix_percentile_60d) - 50.0) / 50.0)


def compute_macro_tilt(
    *,
    dxy_z: float | None = None,
    us_minus_other_2y_bp: float | None = None,
    us_esi_z: float | None = None,
    commodity_20d_return_pct: float | None = None,
    vix_percentile_60d: float | None = None,
) -> MacroTilt:
    """Compute the continuous USD-relative macro tilt in ``[-1, +1]``."""
    components: dict[str, float] = {}
    mapping: dict[str, float | None] = {
        "dxy_tilt": compute_dxy_tilt(dxy_z),
        "rate_spread_z": compute_rate_spread_tilt(us_minus_other_2y_bp),
        "esi_z": compute_esi_tilt(us_esi_z),
        "commodity_tilt": compute_commodity_tilt(commodity_20d_return_pct),
        "vix_regime": compute_vix_regime_tilt(vix_percentile_60d),
    }
    weighted_sum = 0.0
    contributing_weight = 0.0
    for key, value in mapping.items():
        if value is None:
            continue
        w = _COMPONENT_WEIGHTS[key]
        weighted_sum += value * w
        contributing_weight += w
        components[key] = value
    if contributing_weight <= 0:
        return MacroTilt(score=0.0, components={}, contributing_weight=0.0)
    score = _clamp(weighted_sum / contributing_weight)
    return MacroTilt(score=score, components=components, contributing_weight=contributing_weight)


def pair_tilt_multiplier(instrument: str, usd_tilt: float) -> float:
    """Translate a USD-tilt into a per-pair directional multiplier.

    * USD as quote (EUR_USD, GBP_USD, AUD_USD, NZD_USD): positive USD
      tilt → pair bias SHORT, so multiplier = ``-usd_tilt``.
    * USD as base (USD_JPY, USD_CHF, USD_CAD): positive USD tilt →
      pair bias LONG, so multiplier = ``+usd_tilt``.
    * Non-USD crosses: return 0.0 (no direct USD exposure).
    """
    if not instrument or "_" not in instrument:
        return 0.0
    base, quote = instrument.upper().split("_", 1)
    if quote == "USD":
        return -float(usd_tilt)
    if base == "USD":
        return +float(usd_tilt)
    return 0.0
