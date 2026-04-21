"""Atomic shared-budget ledger (FX-bot Sprint 1 §2.10).

The current ``shared_budget_state.json`` is written by both FX-bot and
Gold-bot without coordination. If both processes read the same balance
at the same millisecond, they both reserve the same dollar — the
``shared`` file stops being a ledger.

This module provides a **file-lock-based** atomic reservation API that
works on Windows (``msvcrt.locking``) and POSIX (``fcntl.flock``). It is
deliberately free of any Redis dependency; a Redis-backed backend can be
plugged in later without changing the call sites.

Public API:

* ``read_state(path)`` — read current JSON state (returns empty dict
  when missing / unparseable).
* ``atomic_reserve(path, key, amount, max_total, as_of, requested_by)``
  — under an exclusive OS file lock: read state, check
  ``sum(existing_reservations) + amount <= max_total``, if so append the
  reservation and write back, then release. Returns a
  ``ReservationResult``.
* ``atomic_release(path, reservation_id)`` — under the same lock,
  remove the reservation by id.

The file format is a dict:

    {
      "reservations": [
        {"id": "...", "key": "fx_trade_EUR_USD", "amount": 0.015,
         "reserved_by": "fx-bot", "as_of": "...ISO..."},
        ...
      ],
      "last_updated": "...ISO..."
    }
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

# In-process lock map keyed by normalised absolute path. Necessary on
# Windows because ``msvcrt.locking`` raises "Resource deadlock avoided"
# when multiple threads in the SAME process contend on the same byte
# range. Cross-process coordination still relies on the OS file lock
# below.
_PROCESS_LOCK_MAP: dict[str, threading.Lock] = {}
_PROCESS_LOCK_MAP_GUARD = threading.Lock()


def _process_lock_for(path: str) -> threading.Lock:
    key = os.path.abspath(path)
    with _PROCESS_LOCK_MAP_GUARD:
        lock = _PROCESS_LOCK_MAP.get(key)
        if lock is None:
            lock = threading.Lock()
            _PROCESS_LOCK_MAP[key] = lock
        return lock


@dataclass(frozen=True, slots=True)
class ReservationResult:
    accepted: bool
    reservation_id: str | None
    total_after: float
    max_total: float
    reason: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_windows() -> bool:
    return sys.platform.startswith("win")


@contextmanager
def _locked_rw(path: str) -> Iterator:
    """Open ``path`` for read+write with an exclusive OS-level lock.

    Creates the file (with empty JSON ``{}``) if it does not yet exist.
    On Windows uses ``msvcrt.locking`` (LK_LOCK, LK_UNLCK). On POSIX
    uses ``fcntl.flock`` with ``LOCK_EX``.
    """
    parent = Path(path).parent
    parent.mkdir(parents=True, exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{}")
    # Serialise within the process FIRST (Windows msvcrt.locking raises
    # "Resource deadlock avoided" for same-process contention), then
    # take the OS-level lock for cross-process safety.
    proc_lock = _process_lock_for(path)
    proc_lock.acquire()
    fh = open(path, "r+", encoding="utf-8")
    try:
        if _is_windows():
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)
        else:  # pragma: no cover - POSIX path, exercised in CI
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield fh
    finally:
        try:
            if _is_windows():
                import msvcrt
                fh.seek(0)
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:  # pragma: no cover
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()
            proc_lock.release()


def _read_state_from_handle(fh) -> dict:
    fh.seek(0)
    raw = fh.read()
    if not raw.strip():
        return {"reservations": [], "last_updated": None}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"reservations": [], "last_updated": None}
    if not isinstance(payload, dict):
        return {"reservations": [], "last_updated": None}
    payload.setdefault("reservations", [])
    if not isinstance(payload["reservations"], list):
        payload["reservations"] = []
    return payload


def _write_state_to_handle(fh, state: dict) -> None:
    state["last_updated"] = _now_iso()
    fh.seek(0)
    fh.truncate()
    json.dump(state, fh, indent=2, sort_keys=True)
    fh.flush()
    try:
        os.fsync(fh.fileno())
    except OSError:
        pass


def read_state(path: str) -> dict:
    """Read the current ledger without taking a lock.

    Safe for observability / logging; not safe for mutation.
    """
    if not os.path.exists(path):
        return {"reservations": [], "last_updated": None}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {"reservations": [], "last_updated": None}
    if not isinstance(payload, dict):
        return {"reservations": [], "last_updated": None}
    payload.setdefault("reservations", [])
    return payload


def atomic_reserve(
    *,
    path: str,
    key: str,
    amount: float,
    max_total: float,
    requested_by: str = "fx-bot",
) -> ReservationResult:
    """Atomically reserve ``amount`` against ``max_total``.

    The reservation is accepted only if
    ``sum(existing.amount) + amount <= max_total``.
    """
    amount = float(amount)
    if amount <= 0:
        return ReservationResult(
            accepted=False,
            reservation_id=None,
            total_after=0.0,
            max_total=max_total,
            reason="non_positive_amount",
        )
    with _locked_rw(path) as fh:
        state = _read_state_from_handle(fh)
        existing_total = sum(float(r.get("amount", 0.0) or 0.0) for r in state["reservations"])
        new_total = existing_total + amount
        if new_total > max_total + 1e-12:
            return ReservationResult(
                accepted=False,
                reservation_id=None,
                total_after=existing_total,
                max_total=max_total,
                reason=f"would_exceed_cap:{existing_total:.6f}+{amount:.6f}>{max_total:.6f}",
            )
        reservation_id = str(uuid.uuid4())
        state["reservations"].append({
            "id": reservation_id,
            "key": str(key),
            "amount": amount,
            "reserved_by": str(requested_by),
            "as_of": _now_iso(),
        })
        _write_state_to_handle(fh, state)
        return ReservationResult(
            accepted=True,
            reservation_id=reservation_id,
            total_after=new_total,
            max_total=max_total,
            reason="reserved",
        )


def atomic_release(*, path: str, reservation_id: str) -> bool:
    """Remove a previously-created reservation by id.

    Returns True if a matching reservation was found and removed.
    """
    if not reservation_id:
        return False
    with _locked_rw(path) as fh:
        state = _read_state_from_handle(fh)
        before = len(state["reservations"])
        state["reservations"] = [
            r for r in state["reservations"]
            if str(r.get("id")) != str(reservation_id)
        ]
        if len(state["reservations"]) == before:
            return False
        _write_state_to_handle(fh, state)
        return True


def total_reserved(path: str) -> float:
    """Utility: sum of all outstanding reservations (unlocked read)."""
    state = read_state(path)
    return sum(float(r.get("amount", 0.0) or 0.0) for r in state.get("reservations", []))
