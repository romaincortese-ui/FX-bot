import pandas as pd

from fxbot.indicators import calc_atr


def test_calc_atr_returns_expected_value_for_simple_series():
    df = pd.DataFrame(
        {
            "high": [10, 12, 13, 14, 15],
            "low": [9, 10, 11, 12, 13],
            "close": [9.5, 11, 12, 13, 14],
        }
    )
    atr = calc_atr(df, period=3)
    assert round(atr, 4) == 2.3333
