"""Per-trade slippage attribution (Tier 2 §19 of consultant assessment).

Captures `(signal_mid, fill_price, slip_pips, slip_bps, session,
strategy, instrument, direction)` on every fill so slippage can be
surfaced per strategy / per session. A CSV log is the persistent
audit record; an in-memory deque is surfaced via ``recent_slippage``
for Telegram summaries.
"""
from __future__ import annotations

import csv
import os
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Deque, Iterable


@dataclass(frozen=True, slots=True)
class SlippageEvent:
    ts_utc: str
    instrument: str
    strategy: str
    direction: str
    signal_mid: float
    fill_price: float
    slip_pips: float
    slip_bps: float
    session: str
    label: str


class SlippageLogger:
    def __init__(self, *, csv_path: str = "slippage.csv", max_memory: int = 500):
        self._csv_path = csv_path
        self._lock = threading.Lock()
        self._recent: Deque[SlippageEvent] = deque(maxlen=max(10, int(max_memory)))
        self._header_written = False

    @staticmethod
    def compute_slip_pips(
        *,
        signal_mid: float,
        fill_price: float,
        direction: str,
        pip_size: float,
    ) -> float:
        """Positive = adverse slippage (cost); negative = price improvement."""
        if pip_size <= 0 or signal_mid <= 0 or fill_price <= 0:
            return 0.0
        d = (direction or "").upper()
        raw = fill_price - signal_mid
        if d == "LONG":
            slip = raw
        elif d == "SHORT":
            slip = -raw
        else:
            return 0.0
        return slip / pip_size

    def log(
        self,
        *,
        instrument: str,
        strategy: str,
        direction: str,
        signal_mid: float,
        fill_price: float,
        pip_size: float,
        session: str = "",
        label: str = "",
    ) -> SlippageEvent:
        pips = self.compute_slip_pips(
            signal_mid=signal_mid,
            fill_price=fill_price,
            direction=direction,
            pip_size=pip_size,
        )
        bps = 0.0
        if signal_mid > 0:
            raw = fill_price - signal_mid
            if (direction or "").upper() == "SHORT":
                raw = -raw
            bps = (raw / signal_mid) * 10000.0
        event = SlippageEvent(
            ts_utc=datetime.now(timezone.utc).isoformat(),
            instrument=instrument,
            strategy=(strategy or "").upper(),
            direction=(direction or "").upper(),
            signal_mid=float(signal_mid),
            fill_price=float(fill_price),
            slip_pips=float(pips),
            slip_bps=float(bps),
            session=session,
            label=label,
        )
        with self._lock:
            self._recent.append(event)
            self._append_csv(event)
        return event

    def _append_csv(self, event: SlippageEvent) -> None:
        try:
            exists = os.path.exists(self._csv_path) and os.path.getsize(self._csv_path) > 0
            with open(self._csv_path, "a", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(asdict(event).keys()))
                if not exists or not self._header_written:
                    writer.writeheader()
                    self._header_written = True
                writer.writerow(asdict(event))
        except OSError:
            # Filesystem failure must never break the hot path.
            pass

    def recent_slippage(self, n: int = 20) -> list[SlippageEvent]:
        with self._lock:
            return list(self._recent)[-n:]

    def aggregate_by_strategy(self) -> dict[str, dict[str, float]]:
        with self._lock:
            events = list(self._recent)
        buckets: dict[str, list[float]] = {}
        for e in events:
            buckets.setdefault(e.strategy, []).append(e.slip_pips)
        out: dict[str, dict[str, float]] = {}
        for strat, pips in buckets.items():
            if not pips:
                continue
            pips.sort()
            n = len(pips)
            mean = sum(pips) / n
            median = pips[n // 2] if n % 2 == 1 else 0.5 * (pips[n // 2 - 1] + pips[n // 2])
            out[strat] = {
                "count": float(n),
                "mean_slip_pips": mean,
                "median_slip_pips": median,
                "p95_slip_pips": pips[min(n - 1, int(0.95 * n))],
            }
        return out


_global_logger = SlippageLogger()


def get_default_logger() -> SlippageLogger:
    return _global_logger
