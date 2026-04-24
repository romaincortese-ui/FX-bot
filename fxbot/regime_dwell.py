"""Regime state minimum-dwell filter (Tier 4 §4 of consultant second assessment).

The raw regime classifier can flip between neighbouring states on every
bar when the underlying macro scalars sit on a threshold. This thrash
cascades into strategy eligibility and causes the bot to open and then
immediately block positions.

``RegimeDwellFilter`` holds the current *effective* regime constant
until the raw signal has held a new label for ``min_dwell_bars``
consecutive observations.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RegimeDwellFilter:
    min_dwell_bars: int = 4
    _effective: Any = None
    _candidate: Any = None
    _candidate_count: int = 0

    def observe(self, raw: Any) -> Any:
        """Feed a raw classifier output and return the effective regime."""
        if self._effective is None:
            # First observation bootstraps immediately.
            self._effective = raw
            self._candidate = raw
            self._candidate_count = 1
            return self._effective
        if raw == self._effective:
            self._candidate = raw
            self._candidate_count = max(1, self._candidate_count)
            return self._effective
        # Different from effective — count consecutive matches of the new candidate.
        if raw == self._candidate:
            self._candidate_count += 1
        else:
            self._candidate = raw
            self._candidate_count = 1
        if self._candidate_count >= max(1, int(self.min_dwell_bars)):
            self._effective = self._candidate
        return self._effective

    def current(self) -> Any:
        return self._effective

    def reset(self) -> None:
        self._effective = None
        self._candidate = None
        self._candidate_count = 0
