"""Interest-rate futures / policy-surprise signal (FX-bot Q2 §3.6).

Fed funds futures (ZQ), ESTR, SONIA imply the market-implied probability
of a central-bank rate change at the next meeting. The memo asks:

* If the market prices a > 80% hike probability and the currency is
  mispriced (e.g. EUR rallying into a near-certain Fed hike), defer the
  trade — it's noise.
* Each policy-surprise signal (actual central-bank move vs implied
  probability) feeds the macro score.

This module is deliberately lightweight: it does NOT scrape CME. It
exposes pure helpers the caller wraps around whatever probability input
it has (FedWatch JSON, ESTR futures, etc.).
"""
from __future__ import annotations

from dataclasses import dataclass
from math import tanh


@dataclass(frozen=True, slots=True)
class PolicySurprise:
    currency: str
    implied_prob_hike: float | None   # 0..1
    actual_move_bp: float | None      # positive = hike, negative = cut
    expected_move_bp: float | None    # derived from implied prob
    surprise_bp: float | None         # actual − expected
    bias_direction: str               # "LONG" | "SHORT" | "NONE"
    score_multiplier: float           # [0, 1.5] via tanh saturation
    reason: str


def expected_move_bp(
    *,
    implied_prob_hike: float,
    hike_size_bp: float = 25.0,
    implied_prob_cut: float = 0.0,
    cut_size_bp: float = 25.0,
) -> float:
    """Expected policy move in basis points.

    ``implied_prob_hike`` is probability of hike at next meeting.
    ``implied_prob_cut`` is probability of cut (optional; default 0).
    """
    p_hike = max(0.0, min(1.0, float(implied_prob_hike)))
    p_cut = max(0.0, min(1.0, float(implied_prob_cut)))
    return p_hike * float(hike_size_bp) - p_cut * float(cut_size_bp)


def classify_policy_surprise(
    *,
    currency: str,
    actual_move_bp: float,
    implied_prob_hike: float,
    hike_size_bp: float = 25.0,
    implied_prob_cut: float = 0.0,
    cut_size_bp: float = 25.0,
    min_bp_for_signal: float = 8.0,
) -> PolicySurprise:
    """Surprise = actual move − expected move.

    A positive surprise (hawkish) biases the currency LONG.
    A negative surprise (dovish) biases it SHORT.
    """
    exp_bp = expected_move_bp(
        implied_prob_hike=implied_prob_hike,
        hike_size_bp=hike_size_bp,
        implied_prob_cut=implied_prob_cut,
        cut_size_bp=cut_size_bp,
    )
    surprise = float(actual_move_bp) - exp_bp
    if abs(surprise) < float(min_bp_for_signal):
        return PolicySurprise(
            currency=currency.upper(),
            implied_prob_hike=float(implied_prob_hike),
            actual_move_bp=float(actual_move_bp),
            expected_move_bp=exp_bp,
            surprise_bp=surprise,
            bias_direction="NONE",
            score_multiplier=0.0,
            reason=f"small_surprise_{surprise:+.1f}bp",
        )
    direction = "LONG" if surprise > 0 else "SHORT"
    # tanh saturates: 25 bp surprise → ~0.76, 50 bp → ~1.12, 100 bp → 1.49.
    multiplier = float(1.5 * tanh(abs(surprise) / 25.0))
    return PolicySurprise(
        currency=currency.upper(),
        implied_prob_hike=float(implied_prob_hike),
        actual_move_bp=float(actual_move_bp),
        expected_move_bp=exp_bp,
        surprise_bp=surprise,
        bias_direction=direction,
        score_multiplier=multiplier,
        reason=f"surprise_{surprise:+.1f}bp",
    )


def should_defer_counter_trend(
    *,
    implied_prob_hike: float,
    price_direction: str,
    high_prob_threshold: float = 0.80,
) -> bool:
    """Defer LONG entries when hikes are near-certain but price is
    diverging, and vice-versa. Matches the memo rule: "market pricing a
    > 80% hike and EUR/USD rallying into it — likely noise, defer".

    ``price_direction`` is the candidate trade direction on the USD-base
    instrument (i.e. USD is the currency being hiked — LONG means
    long-USD, SHORT means short-USD).
    """
    p = max(0.0, min(1.0, float(implied_prob_hike)))
    side = price_direction.upper()
    if side == "SHORT" and p >= float(high_prob_threshold):
        # Price says short-USD but market is pricing a hike → defer.
        return True
    if side == "LONG" and p <= (1.0 - float(high_prob_threshold)):
        # Price says long-USD but market is pricing a cut → defer.
        return True
    return False


def policy_bias_for_pair(
    instrument: str,
    base_surprise: PolicySurprise | None,
    quote_surprise: PolicySurprise | None,
) -> str:
    """Combine two policy surprises into a per-pair directional bias.

    Hawkish base vs dovish quote → LONG pair.
    Hawkish quote vs dovish base → SHORT pair.
    Matching signs or missing → NONE.
    """
    if not instrument or "_" not in instrument:
        return "NONE"
    base_bias = base_surprise.bias_direction if base_surprise else "NONE"
    quote_bias = quote_surprise.bias_direction if quote_surprise else "NONE"
    if base_bias == "LONG" and quote_bias != "LONG":
        return "LONG"
    if base_bias == "SHORT" and quote_bias != "SHORT":
        return "SHORT"
    if quote_bias == "LONG" and base_bias != "LONG":
        return "SHORT"
    if quote_bias == "SHORT" and base_bias != "SHORT":
        return "LONG"
    return "NONE"
