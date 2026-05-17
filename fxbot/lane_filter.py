from __future__ import annotations

from typing import Iterable, NamedTuple


class TradeLane(NamedTuple):
    strategy: str
    instrument: str
    direction: str


def _clean_part(value: str) -> str:
    part = value.strip().upper()
    return "*" if part in {"", "ALL", "ANY"} else part


def parse_trade_lanes(raw: str | Iterable[str] | None) -> tuple[TradeLane, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        entries = raw.replace(";", ",").split(",")
    else:
        entries = list(raw)

    lanes: list[TradeLane] = []
    for entry in entries:
        parts = [part.strip() for part in str(entry).split(":")]
        if not parts or not parts[0]:
            continue
        if len(parts) == 1:
            strategy, instrument, direction = parts[0], "*", "*"
        elif len(parts) == 2:
            strategy, instrument, direction = parts[0], parts[1], "*"
        else:
            strategy, instrument, direction = parts[0], parts[1], parts[2]
        lanes.append(TradeLane(_clean_part(strategy), _clean_part(instrument), _clean_part(direction)))
    return tuple(lanes)


def lane_matches(lane: TradeLane, strategy: str, instrument: str, direction: str) -> bool:
    strategy = strategy.upper()
    instrument = instrument.upper()
    direction = direction.upper()
    return (
        lane.strategy in {"*", strategy}
        and lane.instrument in {"*", instrument}
        and lane.direction in {"*", direction}
    )


def trade_lane_block_reason(
    strategy: str,
    instrument: str,
    direction: str,
    allowlist: tuple[TradeLane, ...] = (),
    blocklist: tuple[TradeLane, ...] = (),
) -> str | None:
    if any(lane_matches(lane, strategy, instrument, direction) for lane in blocklist):
        return "trade lane blocked"
    if allowlist and not any(lane_matches(lane, strategy, instrument, direction) for lane in allowlist):
        return "trade lane not allowlisted"
    return None
