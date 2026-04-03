import pandas as pd

from fxbot.strategies.direction import determine_direction


def test_determine_direction_prefers_long_when_short_term_and_higher_timeframes_align():
    prices = list(range(1, 61))
    df_5m = pd.DataFrame({"close": prices})
    df_1h = pd.DataFrame({"close": prices})
    df_4h = pd.DataFrame({"close": prices})

    direction = determine_direction("EUR_USD", df_5m, df_1h, df_4h)

    assert direction == "LONG"