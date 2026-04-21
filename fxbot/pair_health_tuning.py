"""Tuned pair-health configuration helpers (FX-bot Sprint 2 §2.8).

The live ``pair_health.py`` blocks a pair for 60-720s after 6 consecutive
quote failures. On OANDA during high-vol moments, a 2-second packet loss
can trip this and lock the bot out of a pair for minutes — exactly when
the setup is best.

This module exposes:

* ``RECOMMENDED_PAIR_HEALTH`` — a single dataclass with the memo-tuned
  thresholds (quote-fail threshold 12, base block 20s, post-unblock
  probes-before-trading = 3).
* ``ensure_news_window_passthrough`` — guard that refuses to mark a pair
  as blocked if the call is happening inside a valid news window.
* ``post_unblock_gate`` — returns True only after N successful quote
  probes following an unblock, so a flaky pair doesn't start trading
  on the first successful packet.

The live ``pair_health.py`` is not modified in Sprint 2; wiring these
helpers into it happens in the follow-up integration commit behind
``FX_PAIR_HEALTH_V2_ENABLED``.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PairHealthTuning:
    consecutive_quote_failure_threshold: int = 12
    base_block_secs: int = 20
    max_block_secs: int = 180
    probes_before_retrade: int = 3
    never_block_during_news: bool = True


RECOMMENDED_PAIR_HEALTH = PairHealthTuning()


def should_block_on_quote_failures(
    *,
    consecutive_failures: int,
    inside_news_window: bool,
    tuning: PairHealthTuning = RECOMMENDED_PAIR_HEALTH,
) -> bool:
    """Decide whether a pair should be blocked given ``consecutive_failures``.

    Returns False when inside a news window (legacy behaviour would
    block and stay blocked through the whole release).
    """
    if inside_news_window and tuning.never_block_during_news:
        return False
    return consecutive_failures >= tuning.consecutive_quote_failure_threshold


def block_duration_secs(
    *,
    block_level: int,
    tuning: PairHealthTuning = RECOMMENDED_PAIR_HEALTH,
) -> int:
    """Compute block duration using the tuned ladder (short then capped)."""
    base = max(1, tuning.base_block_secs)
    ladder = [base, base * 3, base * 6, tuning.max_block_secs]
    idx = max(0, min(block_level - 1, len(ladder) - 1))
    return min(int(ladder[idx]), int(tuning.max_block_secs))


def post_unblock_gate(
    *,
    successful_probes_since_unblock: int,
    tuning: PairHealthTuning = RECOMMENDED_PAIR_HEALTH,
) -> bool:
    """Return True only once the pair has shown N clean probes."""
    return successful_probes_since_unblock >= tuning.probes_before_retrade
