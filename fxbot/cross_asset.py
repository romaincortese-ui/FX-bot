"""Cross-asset bias overlays (FX-bot Q3 §4.3).

Three lightweight hourly-refreshed macro indicators feeding the per-pair
score as a 10-15-point directional adjustment:

1. **Risk-on score** — SPY vs 20d EMA, VIX vs 60d median, HYG vs IG
   credit spread proxy. Range [-1, +1].
2. **USD-bias score** — DXY vs 20d EMA, 2s10s US curve steepness,
   10Y US yield change. Range [-1, +1].
3. **EUR-bias score** — German 2Y vs US 2Y spread, EUR/CHF vs 20d EMA,
   DAX vs SPX relative. Range [-1, +1].

All functions are pure. Each overlay is computed independently; the
caller maps them to pair-level biases via ``cross_asset_pair_bias``.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import tanh


@dataclass(frozen=True, slots=True)
class CrossAssetOverlay:
    risk_on_score: float | None
    usd_bias_score: float | None
    eur_bias_score: float | None
    reason: str


def _ema(values: list[float], period: int) -> float | None:
    if not values or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = float(values[0])
    for v in values[1:]:
        ema = float(v) * k + ema * (1 - k)
    return ema


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def _safe_ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den == 0:
        return None
    return num / den


def compute_risk_on_score(
    *,
    spy_closes: list[float] | None = None,
    vix_history: list[float] | None = None,
    hyg_ig_ratio_history: list[float] | None = None,
) -> float | None:
    """Aggregate a risk-on score in [-1, +1].

    +1 = risk-on (buy carry / risky FX), -1 = risk-off.
    """
    parts: list[float] = []
    if spy_closes is not None and len(spy_closes) >= 20:
        ema20 = _ema(spy_closes, 20)
        if ema20:
            ratio = spy_closes[-1] / ema20 - 1.0
            parts.append(tanh(ratio / 0.01))     # 1% above EMA = ~0.76
    if vix_history is not None and len(vix_history) >= 60:
        med = _median(vix_history[-60:])
        if med and med > 0:
            # Low VIX vs median → risk-on; high VIX → risk-off.
            rel = (med - vix_history[-1]) / med
            parts.append(tanh(rel / 0.10))
    if hyg_ig_ratio_history is not None and len(hyg_ig_ratio_history) >= 20:
        ema20 = _ema(hyg_ig_ratio_history, 20)
        if ema20:
            ratio = hyg_ig_ratio_history[-1] / ema20 - 1.0
            parts.append(tanh(ratio / 0.01))
    if not parts:
        return None
    score = sum(parts) / len(parts)
    return max(-1.0, min(1.0, score))


def compute_usd_bias_score(
    *,
    dxy_closes: list[float] | None = None,
    curve_2s10s_bp_history: list[float] | None = None,
    us10y_yield_history: list[float] | None = None,
) -> float | None:
    """Aggregate USD-directional score in [-1, +1]. +1 = USD long."""
    parts: list[float] = []
    if dxy_closes is not None and len(dxy_closes) >= 20:
        ema20 = _ema(dxy_closes, 20)
        if ema20:
            ratio = dxy_closes[-1] / ema20 - 1.0
            parts.append(tanh(ratio / 0.005))    # 0.5% above EMA = ~0.76
    if curve_2s10s_bp_history is not None and len(curve_2s10s_bp_history) >= 20:
        # Steepening 2s10s → USD risk-on / weaker USD; flattening → stronger.
        delta = curve_2s10s_bp_history[-1] - curve_2s10s_bp_history[-20]
        parts.append(-tanh(delta / 15.0))
    if us10y_yield_history is not None and len(us10y_yield_history) >= 5:
        # Rising 10Y vs a week ago → USD long.
        delta = us10y_yield_history[-1] - us10y_yield_history[-5]
        parts.append(tanh(delta / 0.20))          # 20bp move = ~0.76
    if not parts:
        return None
    score = sum(parts) / len(parts)
    return max(-1.0, min(1.0, score))


def compute_eur_bias_score(
    *,
    german_us_2y_spread_bp_history: list[float] | None = None,
    eurchf_closes: list[float] | None = None,
    dax_spx_ratio_history: list[float] | None = None,
) -> float | None:
    """Aggregate EUR-directional score in [-1, +1]. +1 = EUR long."""
    parts: list[float] = []
    if (
        german_us_2y_spread_bp_history is not None
        and len(german_us_2y_spread_bp_history) >= 5
    ):
        # Positive spread widening (German > US) → EUR long.
        delta = (
            german_us_2y_spread_bp_history[-1]
            - german_us_2y_spread_bp_history[-5]
        )
        parts.append(tanh(delta / 25.0))
    if eurchf_closes is not None and len(eurchf_closes) >= 20:
        ema20 = _ema(eurchf_closes, 20)
        if ema20:
            ratio = eurchf_closes[-1] / ema20 - 1.0
            parts.append(tanh(ratio / 0.005))
    if dax_spx_ratio_history is not None and len(dax_spx_ratio_history) >= 20:
        ema20 = _ema(dax_spx_ratio_history, 20)
        if ema20:
            ratio = dax_spx_ratio_history[-1] / ema20 - 1.0
            parts.append(tanh(ratio / 0.01))
    if not parts:
        return None
    score = sum(parts) / len(parts)
    return max(-1.0, min(1.0, score))


def build_cross_asset_overlay(
    *,
    spy_closes: list[float] | None = None,
    vix_history: list[float] | None = None,
    hyg_ig_ratio_history: list[float] | None = None,
    dxy_closes: list[float] | None = None,
    curve_2s10s_bp_history: list[float] | None = None,
    us10y_yield_history: list[float] | None = None,
    german_us_2y_spread_bp_history: list[float] | None = None,
    eurchf_closes: list[float] | None = None,
    dax_spx_ratio_history: list[float] | None = None,
) -> CrossAssetOverlay:
    ro = compute_risk_on_score(
        spy_closes=spy_closes,
        vix_history=vix_history,
        hyg_ig_ratio_history=hyg_ig_ratio_history,
    )
    usd = compute_usd_bias_score(
        dxy_closes=dxy_closes,
        curve_2s10s_bp_history=curve_2s10s_bp_history,
        us10y_yield_history=us10y_yield_history,
    )
    eur = compute_eur_bias_score(
        german_us_2y_spread_bp_history=german_us_2y_spread_bp_history,
        eurchf_closes=eurchf_closes,
        dax_spx_ratio_history=dax_spx_ratio_history,
    )
    return CrossAssetOverlay(
        risk_on_score=ro,
        usd_bias_score=usd,
        eur_bias_score=eur,
        reason=f"risk_on={ro}_usd={usd}_eur={eur}",
    )


# -- Per-pair mapping ----------------------------------------------------

_RISK_FX = {"AUD", "NZD", "CAD", "MXN", "ZAR", "BRL", "NOK", "SEK"}
_SAFE_FX = {"JPY", "CHF"}


def _currency_bias_from_risk(currency: str, risk_on: float) -> float:
    c = currency.upper()
    if c in _RISK_FX:
        return risk_on
    if c in _SAFE_FX:
        return -risk_on
    return 0.0


def cross_asset_pair_bias(
    instrument: str,
    overlay: CrossAssetOverlay,
    *,
    score_points: float = 12.0,
) -> float:
    """Translate the overlay into a single pair-level score adjustment
    in roughly ``[-score_points, +score_points]``.

    Rules:
    * USD bias: +pair if USD is base, -pair if USD is quote.
    * EUR bias: +pair if EUR is base, -pair if EUR is quote.
    * Risk-on: maps risk-FX long / safe-FX short. Mid-EUR/GBP pairs get
      a milder tilt via the quote leg only.
    """
    if not instrument or "_" not in instrument:
        return 0.0
    base, quote = instrument.upper().split("_", 1)
    tilt = 0.0
    if overlay.usd_bias_score is not None:
        if base == "USD":
            tilt += overlay.usd_bias_score
        elif quote == "USD":
            tilt -= overlay.usd_bias_score
    if overlay.eur_bias_score is not None:
        if base == "EUR":
            tilt += overlay.eur_bias_score
        elif quote == "EUR":
            tilt -= overlay.eur_bias_score
    if overlay.risk_on_score is not None:
        tilt += _currency_bias_from_risk(base, overlay.risk_on_score)
        tilt -= _currency_bias_from_risk(quote, overlay.risk_on_score)
    # Average the components that contributed (soft cap via tanh).
    scaled = tanh(tilt / 2.0)
    return float(score_points) * scaled
