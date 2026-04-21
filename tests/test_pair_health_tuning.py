from fxbot.pair_health_tuning import (
    RECOMMENDED_PAIR_HEALTH,
    PairHealthTuning,
    block_duration_secs,
    post_unblock_gate,
    should_block_on_quote_failures,
)


def test_defaults_match_memo():
    t = RECOMMENDED_PAIR_HEALTH
    assert t.consecutive_quote_failure_threshold == 12
    assert t.base_block_secs == 20
    assert t.probes_before_retrade == 3
    assert t.never_block_during_news is True


def test_block_threshold_raised_from_six_to_twelve():
    assert should_block_on_quote_failures(
        consecutive_failures=6, inside_news_window=False
    ) is False
    assert should_block_on_quote_failures(
        consecutive_failures=12, inside_news_window=False
    ) is True


def test_news_window_suppresses_block():
    # Even with many failures, inside a news window the pair should
    # NOT get blocked (memo §2.8 explicit).
    assert should_block_on_quote_failures(
        consecutive_failures=50, inside_news_window=True
    ) is False


def test_block_duration_ladder_starts_short():
    # Level 1 uses the base_block_secs (20), not the 60 legacy value.
    assert block_duration_secs(block_level=1) == 20
    assert block_duration_secs(block_level=2) == 60
    assert block_duration_secs(block_level=3) == 120
    # Level >= 4 capped.
    assert block_duration_secs(block_level=4) == 180
    assert block_duration_secs(block_level=99) == 180


def test_block_duration_respects_custom_tuning():
    t = PairHealthTuning(base_block_secs=10, max_block_secs=60)
    assert block_duration_secs(block_level=1, tuning=t) == 10
    assert block_duration_secs(block_level=4, tuning=t) == 60


def test_post_unblock_gate_requires_clean_probes():
    assert post_unblock_gate(successful_probes_since_unblock=0) is False
    assert post_unblock_gate(successful_probes_since_unblock=2) is False
    assert post_unblock_gate(successful_probes_since_unblock=3) is True
    assert post_unblock_gate(successful_probes_since_unblock=100) is True


def test_custom_tuning_disables_news_skip():
    tuning = PairHealthTuning(never_block_during_news=False)
    assert should_block_on_quote_failures(
        consecutive_failures=12, inside_news_window=True, tuning=tuning
    ) is True
