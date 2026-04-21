"""News-surprise signal (FX-bot Sprint 3 §3.9).

Replaces the legacy "calendar blackout" with a post-release surprise
score. Given ``actual``, ``consensus`` and the event's historical
release standard deviation, compute:

    surprise_z = (actual - consensus) / historical_std

The memo prescribes:

* POST_NEWS trades should fire only when ``|surprise_z| > 0.5``.
* A positive US-data surprise produces a USD-long bias for 2-4 hours.
* The directional effect inverts for inflation-style events where a
  hot print has the *opposite* sign (higher CPI → currency firms if
  it implies hawkish policy, so still USD-positive — this module takes
  ``direction_on_beat`` as a parameter rather than hardcoding).

This module exposes:

* ``SurpriseSignal`` dataclass.
* ``compute_surprise`` — safe, clamped z-score.
* ``classify_surprise_bias`` — given a currency and a surprise,
  produces a ``(direction, decay_seconds, reason)`` tuple.
* ``surprise_score_multiplier`` — convert a surprise into a
  ``[0, 1.5]`` multiplier for existing strategy scores.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import tanh


@dataclass(frozen=True, slots=True)
class SurpriseSignal:
    currency: str
    surprise_z: float                    # (actual - consensus) / std
    bias_currency: str | None            # which currency is favoured
    bias_direction: str                  # "LONG" | "SHORT" | "NONE"
    score_multiplier: float              # [0, 1.5]
    decay_seconds: int
    reason: str


def compute_surprise_z(
    *,
    actual: float,
    consensus: float,
    historical_std: float,
) -> float | None:
    """Safe surprise z-score. Returns None if std is non-positive."""
    if historical_std is None or historical_std <= 0:
        return None
    return float((actual - consensus) / historical_std)


def surprise_score_multiplier(surprise_z: float | None) -> float:
    """Map surprise magnitude to a score multiplier in ``[0, 1.5]``.

    * |z| < 0.5 → 0.0 (below the fire threshold).
    * |z| = 1.0 → ~0.76.
    * |z| >= 2 → ~1.45 (saturated).
    """
    if surprise_z is None:
        return 0.0
    z = abs(float(surprise_z))
    if z < 0.5:
        return 0.0
    # tanh saturates at ~1.5 for large z.
    return float(1.5 * tanh((z - 0.5) / 1.0))


def classify_surprise_bias(
    *,
    event_currency: str,
    actual: float,
    consensus: float,
    historical_std: float,
    direction_on_beat: str = "LONG",
    decay_seconds: int = 3 * 3600,   # 3 hours per memo
    min_fire_z: float = 0.5,
) -> SurpriseSignal:
    """Classify a post-release surprise into a directional signal.

    * ``direction_on_beat`` — what the event currency should do when
      ``actual > consensus``. Default "LONG" (the standard case for
      growth/jobs/inflation beats). For disinflation-type events use
      ``"SHORT"`` at the call site.
    """
    z = compute_surprise_z(
        actual=actual, consensus=consensus, historical_std=historical_std
    )
    if z is None:
        return SurpriseSignal(
            currency=event_currency.upper(),
            surprise_z=0.0,
            bias_currency=None,
            bias_direction="NONE",
            score_multiplier=0.0,
            decay_seconds=0,
            reason="no_historical_std",
        )
    if abs(z) < float(min_fire_z):
        return SurpriseSignal(
            currency=event_currency.upper(),
            surprise_z=z,
            bias_currency=None,
            bias_direction="NONE",
            score_multiplier=0.0,
            decay_seconds=0,
            reason=f"below_threshold_|z|={abs(z):.3f}<{min_fire_z}",
        )
    up = direction_on_beat.upper()
    if up not in {"LONG", "SHORT"}:
        up = "LONG"
    # A positive z with direction_on_beat=LONG → currency bias LONG.
    # A negative z flips it.
    beat_sign = +1 if z > 0 else -1
    long_side = (up == "LONG")
    bias_is_long = long_side if beat_sign > 0 else not long_side
    return SurpriseSignal(
        currency=event_currency.upper(),
        surprise_z=z,
        bias_currency=event_currency.upper(),
        bias_direction="LONG" if bias_is_long else "SHORT",
        score_multiplier=surprise_score_multiplier(z),
        decay_seconds=int(decay_seconds),
        reason=f"fired_z={z:+.3f}",
    )


def pair_bias_from_surprise(instrument: str, signal: SurpriseSignal) -> str:
    """Translate a currency-level surprise into a per-pair direction.

    * If the event currency is the pair's BASE, pair direction = signal direction.
    * If QUOTE, pair direction = inverted signal direction.
    * Otherwise "NONE".
    """
    if signal.bias_currency is None or signal.bias_direction == "NONE":
        return "NONE"
    if not instrument or "_" not in instrument:
        return "NONE"
    base, quote = instrument.upper().split("_", 1)
    ccy = signal.bias_currency
    if base == ccy:
        return signal.bias_direction
    if quote == ccy:
        return "SHORT" if signal.bias_direction == "LONG" else "LONG"
    return "NONE"
