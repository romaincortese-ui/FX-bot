def default_pair_health() -> dict:
    return {
        "status": "healthy",
        "health_score": 100.0,
        "blocked_until": 0.0,
        "block_level": 0,
        "next_probe_at": 0.0,
        "clean_probes": 0,
        "last_failure_reason": "",
        "last_failure_at": 0.0,
        "last_recovery_at": 0.0,
        "last_quote_ok_at": 0.0,
        "last_order_ok_at": 0.0,
        "last_close_ok_at": 0.0,
        "last_spread_ok_at": 0.0,
        "last_candle_ok_at": {},
        "consecutive_quote_failures": 0,
        "consecutive_order_failures": 0,
        "consecutive_close_failures": 0,
        "consecutive_spread_failures": 0,
        "consecutive_candle_failures": {},
        "last_failure_buckets": {},
        "last_success_buckets": {},
    }


def pair_health_block_seconds(block_level: int, base_secs: int, max_secs: int) -> int:
    ladder = [base_secs, base_secs * 4, base_secs * 12, max_secs]
    idx = max(0, min(block_level - 1, len(ladder) - 1))
    return min(ladder[idx], max_secs)


def can_count_pair_health_event(
    rec: dict,
    bucket: str,
    success: bool,
    now: float,
    success_cooldown: int,
    failure_cooldown: int,
) -> bool:
    store = rec["last_success_buckets"] if success else rec["last_failure_buckets"]
    cooldown = success_cooldown if success else failure_cooldown
    last_at = float(store.get(bucket, 0.0))
    if now - last_at < cooldown:
        return False
    store[bucket] = now
    return True


def apply_pair_failure(
    rec: dict,
    *,
    reason: str,
    source: str,
    severity: str,
    timeframe: str,
    now: float,
    block_base_secs: int,
    block_max_secs: int,
    probe_interval_secs: int,
) -> dict:
    prev_status = rec["status"]
    reason_text = (reason or "").lower()
    rec["last_failure_reason"] = reason
    rec["last_failure_at"] = now
    rec["clean_probes"] = 0
    penalty = 35.0 if source == "close" and severity == "hard" else 25.0 if severity == "hard" else 10.0
    rec["health_score"] = max(0.0, float(rec.get("health_score", 100.0)) - penalty)

    degrade = False
    block = False

    if source == "quote":
        rec["consecutive_quote_failures"] = int(rec.get("consecutive_quote_failures", 0)) + 1
        degrade = rec["consecutive_quote_failures"] >= 3
        block = rec["consecutive_quote_failures"] >= 6
    elif source == "candle":
        candle_failures = rec.setdefault("consecutive_candle_failures", {})
        key = timeframe or "UNKNOWN"
        candle_failures[key] = int(candle_failures.get(key, 0)) + 1
        current = candle_failures[key]
        important = timeframe in {"M15", "H1", "H4"}
        degrade = current >= (2 if important else 3)
        block = current >= (4 if important else 6)
    elif source == "spread":
        rec["consecutive_spread_failures"] = int(rec.get("consecutive_spread_failures", 0)) + 1
        degrade = rec["consecutive_spread_failures"] >= 5
        block = rec["consecutive_spread_failures"] >= 10
    elif source == "order":
        rec["consecutive_order_failures"] = int(rec.get("consecutive_order_failures", 0)) + 1
        hard_terms = ("close-only", "close only", "tradeable", "tradable", "instrument", "liquidity", "market halted")
        if severity == "hard" or any(term in reason_text for term in hard_terms):
            block = True
        else:
            degrade = rec["consecutive_order_failures"] >= 2
            block = rec["consecutive_order_failures"] >= 4
    elif source == "close":
        rec["consecutive_close_failures"] = int(rec.get("consecutive_close_failures", 0)) + 1
        hard_terms = ("market_halted", "market halted", "close-only", "close only", "tradeable", "tradable")
        if severity == "hard" or any(term in reason_text for term in hard_terms):
            block = True
        else:
            degrade = rec["consecutive_close_failures"] >= 1
            block = rec["consecutive_close_failures"] >= 2

    if block:
        rec["status"] = "blocked"
        rec["block_level"] = int(rec.get("block_level", 0)) + (2 if source == "close" else 1)
        rec["blocked_until"] = now + pair_health_block_seconds(rec["block_level"], block_base_secs, block_max_secs)
        rec["next_probe_at"] = rec["blocked_until"]
    elif degrade and rec["status"] == "healthy":
        rec["status"] = "degraded"
        rec["next_probe_at"] = now + probe_interval_secs

    return {
        "previous_status": prev_status,
        "current_status": rec["status"],
        "status_changed": rec["status"] != prev_status,
    }


def apply_pair_success(
    rec: dict,
    *,
    source: str,
    timeframe: str,
    now: float,
    probe_interval_secs: int,
    recovery_successes: int,
) -> dict:
    prev_status = rec["status"]

    if source == "quote":
        rec["last_quote_ok_at"] = now
        rec["consecutive_quote_failures"] = 0
    elif source == "candle":
        key = timeframe or "UNKNOWN"
        rec.setdefault("last_candle_ok_at", {})[key] = now
        rec.setdefault("consecutive_candle_failures", {})[key] = 0
    elif source == "order":
        rec["last_order_ok_at"] = now
        rec["consecutive_order_failures"] = 0
    elif source == "close":
        rec["last_close_ok_at"] = now
        rec["consecutive_close_failures"] = 0
    elif source == "spread":
        rec["last_spread_ok_at"] = now
        rec["consecutive_spread_failures"] = 0

    rec["health_score"] = min(100.0, float(rec.get("health_score", 100.0)) + 5.0)

    if rec["status"] == "blocked" and now >= float(rec.get("blocked_until", 0.0)):
        rec["status"] = "degraded"
        rec["clean_probes"] = 1
        rec["next_probe_at"] = now + probe_interval_secs
        rec["last_recovery_at"] = now
    elif rec["status"] == "degraded":
        rec["clean_probes"] = int(rec.get("clean_probes", 0)) + 1
        rec["next_probe_at"] = now + probe_interval_secs
        if rec["clean_probes"] >= recovery_successes:
            rec["status"] = "healthy"
            rec["clean_probes"] = 0
            rec["next_probe_at"] = 0.0
            rec["last_recovery_at"] = now
            rec["last_failure_reason"] = ""
            rec["block_level"] = max(0, int(rec.get("block_level", 0)) - 1)
            rec["health_score"] = max(80.0, rec["health_score"])
    else:
        rec["clean_probes"] = 0

    if prev_status == "blocked" and rec["status"] == "blocked":
        rec["next_probe_at"] = max(float(rec.get("next_probe_at", 0.0)), float(rec.get("blocked_until", 0.0)))

    return {
        "previous_status": prev_status,
        "current_status": rec["status"],
        "status_changed": rec["status"] != prev_status,
    }
