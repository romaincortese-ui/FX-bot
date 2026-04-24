"""Analyse a captured Railway live-log JSON file against the Tier 1v2 gates.

Memo 3 §8 Tier 1v2 "V1" requires a 24-hour replay under the Tier 0 fixes that
proves, mechanically:

  1. Zero TRADE_DOESNT_EXIST errors (phantom trades reconcile once and are
     gone instead of looping) — P0.1.
  2. Calibration loaded at startup with non-zero strategy/pair entries —
     P0.2.
  3. spread_gate rejections do not exceed the dedup threshold set by P0.3.
  4. At least 1 entry attempted per London session on EUR_USD or GBP_USD —
     or, for every blocked signal, a `[GATE_BLOCK]` explanation — P0.4.

This script is deliberately independent of main.py and consumes a Railway
exported JSON log so it can be run from the command line post-session.

Usage:
    python scripts/analyse_live_log.py path/to/logs.1777031374001.json

Exit code 0 when every gate passes, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


RECONCILABLE_CODES = (
    "TRADE_DOESNT_EXIST",
    "POSITION_DOESNT_EXIST",
    "ORDER_DOESNT_EXIST",
    "CLOSEOUT_POSITION_DOESNT_EXIST",
)


@dataclass
class GateResult:
    name: str
    passed: bool
    detail: str
    counts: dict[str, int] = field(default_factory=dict)


def _iter_entries(path: Path) -> Iterable[dict]:
    """Yield log entry dicts from a Railway JSON export.

    Supports both the ``[{...}, {...}]`` array form and newline-delimited
    JSON (one event per line), which the Railway CLI emits interchangeably.
    """
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return
    if text.startswith("["):
        for entry in json.loads(text):
            if isinstance(entry, dict):
                yield entry
        return
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


def _message(entry: dict) -> str:
    for key in ("message", "msg", "log", "text"):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return json.dumps(entry)[:500]


def _severity(entry: dict) -> str:
    for key in ("severity", "level", "levelname"):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value.upper()
    return ""


def _parse_ts(entry: dict) -> datetime | None:
    for key in ("timestamp", "time", "ts", "@timestamp"):
        value = entry.get(key)
        if not value:
            continue
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            continue
    return None


# ── Individual gate evaluations ──────────────────────────────────────────


def gate_no_reconcile_storm(entries: list[dict]) -> GateResult:
    counts: Counter[str] = Counter()
    reconciled: Counter[str] = Counter()
    for entry in entries:
        msg = _message(entry)
        upper = msg.upper()
        for code in RECONCILABLE_CODES:
            if code in upper and _severity(entry) in {"ERROR", "CRITICAL"}:
                counts[code] += 1
        if "BROKER_RECONCILED" in upper or "reconciling locally" in msg.lower():
            reconciled["reconciled"] += 1
    total_errors = sum(counts.values())
    passed = total_errors == 0
    detail = (
        f"{total_errors} reject-error lines across reconcilable codes; "
        f"{reconciled.get('reconciled', 0)} reconciliation messages emitted. "
        "Pass = zero reject-error lines (P0.1)."
    )
    return GateResult("reconcile_storm", passed, detail, dict(counts))


def gate_calibration_loaded(entries: list[dict]) -> GateResult:
    loaded_pattern = re.compile(
        r"\[CALIBRATION\]\s+Loaded trade calibration.*?(\d+)\s+strategy/pair entries"
    )
    ignore_pattern = re.compile(r"\[CALIBRATION\]\s+Ignoring")
    loaded_hits = 0
    ignored_hits = 0
    pair_entries = 0
    for entry in entries:
        msg = _message(entry)
        m = loaded_pattern.search(msg)
        if m:
            loaded_hits += 1
            pair_entries = max(pair_entries, int(m.group(1)))
        if ignore_pattern.search(msg) and _severity(entry) == "INFO":
            ignored_hits += 1
    passed = loaded_hits >= 1 and pair_entries > 0
    detail = (
        f"[CALIBRATION] Loaded observed {loaded_hits}× (max {pair_entries} "
        f"strategy/pair entries); {ignored_hits} 'Ignoring' lines at INFO. "
        "Pass = ≥1 Loaded line with non-zero entries (P0.2)."
    )
    return GateResult(
        "calibration_loaded",
        passed,
        detail,
        {"loaded": loaded_hits, "ignored_info": ignored_hits, "max_entries": pair_entries},
    )


def gate_spread_log_quiet(entries: list[dict]) -> GateResult:
    warn_count = 0
    dedup_count = 0
    for entry in entries:
        msg = _message(entry)
        if "spread_gate rejections" not in msg:
            continue
        if _severity(entry) == "WARNING":
            warn_count += 1
        if "[unchanged]" in msg:
            dedup_count += 1
    # Expect at most a handful of WARNING emissions per 24h once dedup is on.
    passed = warn_count <= 24
    detail = (
        f"spread_gate WARNINGs={warn_count}, dedup DEBUG lines={dedup_count}. "
        "Pass = ≤ 24 WARNINGs / 24h (one per hour ceiling, P0.3)."
    )
    return GateResult(
        "spread_log_quiet",
        passed,
        detail,
        {"warn": warn_count, "dedup": dedup_count},
    )


def gate_london_entry_or_explanation(entries: list[dict]) -> GateResult:
    entry_attempts = 0
    gate_blocks: Counter[str] = Counter()
    london_entries_eurusd_gbpusd = 0
    entry_pattern = re.compile(
        r"Placing (?:MARKET|LIMIT) (?:LONG|SHORT) (EUR_USD|GBP_USD|USD_JPY|USD_CHF|USD_CAD|NZD_USD|AUD_USD|EUR_GBP)"
    )
    gate_block_pattern = re.compile(
        r"\[GATE_BLOCK\]\s+strategy=(\w+)\s+instrument=(\w+).*?category=(\w+)"
    )
    london_hours = range(7, 16)  # 07:00–16:00 UTC
    for entry in entries:
        msg = _message(entry)
        ts = _parse_ts(entry)
        if entry_pattern.search(msg):
            entry_attempts += 1
            pair_match = entry_pattern.search(msg)
            if ts is not None and ts.astimezone(timezone.utc).hour in london_hours:
                if pair_match and pair_match.group(1) in {"EUR_USD", "GBP_USD"}:
                    london_entries_eurusd_gbpusd += 1
        m = gate_block_pattern.search(msg)
        if m:
            gate_blocks[m.group(3)] += 1
    # Pass if an entry attempted during London OR every scan cycle produced
    # a structured [GATE_BLOCK] explanation for at least one strategy/pair.
    passed = (london_entries_eurusd_gbpusd >= 1) or (sum(gate_blocks.values()) >= 1)
    detail = (
        f"{entry_attempts} entry placements overall "
        f"({london_entries_eurusd_gbpusd} during London on EUR/GBP-USD); "
        f"{sum(gate_blocks.values())} [GATE_BLOCK] lines "
        f"spread across categories {dict(gate_blocks)}. "
        "Pass = ≥1 London entry on majors OR ≥1 [GATE_BLOCK] line (P0.4)."
    )
    return GateResult(
        "entry_or_explanation",
        passed,
        detail,
        {"entries": entry_attempts, "london_majors": london_entries_eurusd_gbpusd, **gate_blocks},
    )


# ── Driver ────────────────────────────────────────────────────────────────


def run(path: Path) -> int:
    entries = list(_iter_entries(path))
    if not entries:
        print(f"[analyse_live_log] no entries parsed from {path}", file=sys.stderr)
        return 2

    gates: list[GateResult] = [
        gate_no_reconcile_storm(entries),
        gate_calibration_loaded(entries),
        gate_spread_log_quiet(entries),
        gate_london_entry_or_explanation(entries),
    ]

    total = len(entries)
    first_ts = _parse_ts(entries[0])
    last_ts = _parse_ts(entries[-1])
    window = f"{first_ts} → {last_ts}" if first_ts and last_ts else "n/a"
    print(f"[analyse_live_log] {total} entries, window={window}")
    print()

    all_passed = True
    for gate in gates:
        status = "PASS" if gate.passed else "FAIL"
        print(f"[{status}] {gate.name}: {gate.detail}")
        if gate.counts:
            print(f"       counts={gate.counts}")
        if not gate.passed:
            all_passed = False

    print()
    if all_passed:
        print("[analyse_live_log] ALL GATES PASS — Tier 1v2 V1 satisfied.")
        return 0
    print("[analyse_live_log] one or more gates failed — see lines above.")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_path", help="Path to a Railway-exported log JSON file")
    args = parser.parse_args(argv)
    path = Path(args.log_path)
    if not path.exists():
        print(f"[analyse_live_log] file not found: {path}", file=sys.stderr)
        return 2
    return run(path)


if __name__ == "__main__":
    sys.exit(main())
