from fxbot.regime_dwell import RegimeDwellFilter


def test_first_observation_bootstraps():
    f = RegimeDwellFilter(min_dwell_bars=4)
    assert f.observe("TREND_UP") == "TREND_UP"


def test_same_state_persists():
    f = RegimeDwellFilter(min_dwell_bars=3)
    f.observe("CHOP")
    assert f.observe("CHOP") == "CHOP"
    assert f.observe("CHOP") == "CHOP"


def test_single_contrary_bar_does_not_flip():
    f = RegimeDwellFilter(min_dwell_bars=4)
    f.observe("CHOP")
    # one flash of TREND_UP should NOT flip the effective regime
    assert f.observe("TREND_UP") == "CHOP"
    assert f.observe("CHOP") == "CHOP"


def test_flip_requires_consecutive_observations():
    f = RegimeDwellFilter(min_dwell_bars=4)
    f.observe("CHOP")
    assert f.observe("TREND_UP") == "CHOP"
    assert f.observe("TREND_UP") == "CHOP"
    assert f.observe("TREND_UP") == "CHOP"
    assert f.observe("TREND_UP") == "TREND_UP"


def test_candidate_resets_on_break():
    f = RegimeDwellFilter(min_dwell_bars=4)
    f.observe("CHOP")
    f.observe("TREND_UP")
    f.observe("TREND_UP")
    # breaks the streak
    assert f.observe("RISK_OFF") == "CHOP"
    # now need 4 consecutive TREND_UP from scratch
    f.observe("TREND_UP")
    f.observe("TREND_UP")
    f.observe("TREND_UP")
    assert f.observe("TREND_UP") == "TREND_UP"


def test_min_dwell_one_means_no_dwell():
    f = RegimeDwellFilter(min_dwell_bars=1)
    f.observe("CHOP")
    assert f.observe("TREND_UP") == "TREND_UP"


def test_reset_clears_state():
    f = RegimeDwellFilter(min_dwell_bars=3)
    f.observe("CHOP")
    f.reset()
    assert f.current() is None
    assert f.observe("RISK_ON") == "RISK_ON"
