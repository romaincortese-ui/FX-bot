from fxbot.pair_health import apply_pair_failure, apply_pair_success, default_pair_health


def test_pair_health_blocks_after_repeated_quote_failures():
    record = default_pair_health()
    for idx in range(6):
        event = apply_pair_failure(
            record,
            reason="quote timeout",
            source="quote",
            severity="soft",
            timeframe="",
            now=1000 + idx,
            block_base_secs=60,
            block_max_secs=3600,
            probe_interval_secs=120,
        )
    assert event["current_status"] == "blocked"
    assert record["blocked_until"] > 1000


def test_pair_health_recovers_after_clean_probes():
    record = default_pair_health()
    record["status"] = "degraded"
    for idx in range(3):
        event = apply_pair_success(
            record,
            source="quote",
            timeframe="",
            now=2000 + idx,
            probe_interval_secs=120,
            recovery_successes=3,
        )
    assert event["current_status"] == "healthy"
    assert record["last_failure_reason"] == ""