from fxbot.rate_futures import (
    classify_policy_surprise,
    expected_move_bp,
    policy_bias_for_pair,
    should_defer_counter_trend,
)


def test_expected_move_pure_hike():
    assert expected_move_bp(implied_prob_hike=1.0) == 25.0


def test_expected_move_no_move_expected():
    assert expected_move_bp(implied_prob_hike=0.0) == 0.0


def test_expected_move_with_cut_probability():
    em = expected_move_bp(implied_prob_hike=0.0, implied_prob_cut=0.5)
    assert em == -12.5


def test_small_surprise_is_none():
    s = classify_policy_surprise(
        currency="USD", actual_move_bp=20, implied_prob_hike=0.9
    )
    assert s.bias_direction == "NONE"


def test_hawkish_surprise_biases_long():
    s = classify_policy_surprise(
        currency="USD", actual_move_bp=50, implied_prob_hike=0.5
    )
    assert s.bias_direction == "LONG"
    assert s.surprise_bp > 0


def test_dovish_surprise_biases_short():
    s = classify_policy_surprise(
        currency="USD", actual_move_bp=0, implied_prob_hike=0.9
    )
    assert s.bias_direction == "SHORT"
    assert s.surprise_bp < 0


def test_defer_short_when_hike_priced_in():
    # Market pricing 85% hike; trader wants to short USD → defer.
    assert should_defer_counter_trend(
        implied_prob_hike=0.85, price_direction="SHORT"
    ) is True


def test_defer_long_when_cut_priced_in():
    # Market pricing cut (implied_prob_hike low) but trader wants long USD.
    assert should_defer_counter_trend(
        implied_prob_hike=0.10, price_direction="LONG"
    ) is True


def test_do_not_defer_aligned_trade():
    assert should_defer_counter_trend(
        implied_prob_hike=0.85, price_direction="LONG"
    ) is False


def test_pair_bias_combines_base_quote():
    hawkish_usd = classify_policy_surprise(
        currency="USD", actual_move_bp=50, implied_prob_hike=0.5
    )
    dovish_eur = classify_policy_surprise(
        currency="EUR", actual_move_bp=-25, implied_prob_hike=0.5
    )
    # EUR_USD: base dovish, quote hawkish → pair SHORT (EUR weak vs USD).
    assert policy_bias_for_pair("EUR_USD", dovish_eur, hawkish_usd) == "SHORT"


def test_pair_bias_none_when_missing():
    assert policy_bias_for_pair("EUR_USD", None, None) == "NONE"
