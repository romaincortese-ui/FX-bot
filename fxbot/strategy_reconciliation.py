"""Strategy reconciliation (Tier 2 §16 of consultant assessment).

SCALPER / REVERSAL / ASIAN_FADE can score opposing directions on the
same instrument on the same M15 bar. Without reconciliation the bot
can open a LONG on one strategy while the SHORT-biased strategy on the
same pair is still within its cooldown — classic double-counting of
the same price move.

This module tracks, per ``(instrument, bar_timestamp)``, the most
recent non-blocked score and direction each strategy produced. New
entries on the same bar for the same instrument in the *opposite*
direction are refused.

State is in-memory — crash-restart loses at most one bar of
reconciliation, which is acceptable given ``state.json`` also resets.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict


@dataclass(frozen=True, slots=True)
class Signal:
    strategy: str
    instrument: str
    direction: str            # "LONG" | "SHORT"
    score: float
    bar_ts_utc: datetime


@dataclass(frozen=True, slots=True)
class ReconciliationDecision:
    allowed: bool
    reason: str


class StrategyReconciliation:
    """Per-instrument, per-bar signal log with opposite-direction veto."""

    def __init__(self) -> None:
        # key: (instrument, bar_ts_iso) → list[Signal]
        self._log: Dict[tuple[str, str], list[Signal]] = {}
        self._max_bars = 64

    @staticmethod
    def _bar_ts(ts: datetime, bar_minutes: int) -> datetime:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        minute = (ts.minute // bar_minutes) * bar_minutes
        return ts.replace(minute=minute, second=0, microsecond=0)

    def record(self, signal: Signal) -> None:
        key = (signal.instrument.upper(), signal.bar_ts_utc.isoformat())
        self._log.setdefault(key, []).append(signal)
        if len(self._log) > self._max_bars:
            # Drop oldest bar keys to keep memory bounded.
            oldest = sorted(self._log.keys(), key=lambda k: k[1])[: len(self._log) - self._max_bars]
            for k in oldest:
                self._log.pop(k, None)

    def check(
        self,
        *,
        strategy: str,
        instrument: str,
        direction: str,
        score: float,
        now_utc: datetime | None = None,
        bar_minutes: int = 15,
    ) -> ReconciliationDecision:
        """Return whether ``strategy`` may enter ``direction`` on ``instrument``.

        Refuses if another strategy already fired the opposite direction
        on the same instrument within the current bar with a score that
        is comparable (within 25% of the candidate's score or greater).
        """
        ts = now_utc or datetime.now(timezone.utc)
        bar = self._bar_ts(ts, bar_minutes)
        key = (instrument.upper(), bar.isoformat())
        existing = self._log.get(key, [])
        cand = Signal(
            strategy=strategy.upper(),
            instrument=instrument.upper(),
            direction=direction.upper(),
            score=float(score),
            bar_ts_utc=bar,
        )
        for prior in existing:
            if prior.strategy == cand.strategy:
                continue
            if prior.direction == cand.direction:
                continue
            # Opposite direction. Veto if the prior signal was strong
            # enough to be credible (>= 75% of the candidate's score).
            if prior.score >= 0.75 * cand.score:
                return ReconciliationDecision(
                    allowed=False,
                    reason=(
                        f"opposite_signal_same_bar:{prior.strategy}_"
                        f"{prior.direction}_score_{prior.score:.1f}"
                    ),
                )
        self.record(cand)
        return ReconciliationDecision(allowed=True, reason="no_conflict")

    def reset(self) -> None:
        self._log.clear()


_global_reconciliation = StrategyReconciliation()


def get_default_reconciliation() -> StrategyReconciliation:
    return _global_reconciliation
