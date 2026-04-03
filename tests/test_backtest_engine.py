from datetime import datetime, timezone

import pandas as pd
import requests

from backtest.config import BacktestConfig
from backtest.engine import BacktestEngine
from backtest.macro_sim import MacroReplay


class StubHistoricalDataProvider:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame
        self.calls = 0

    def get_candles(self, instrument, granularity, start, end, price="M"):
        self.calls += 1
        return self.frame

    def get_pair_spread_profile(self, instrument, granularity, start, end):
        return {"default": 0.8}


def test_engine_load_series_accepts_dataframe_from_provider():
    frame = pd.DataFrame(
        [
            {
                "time": pd.Timestamp("2023-01-01T00:00:00Z"),
                "open": 1.10,
                "high": 1.11,
                "low": 1.09,
                "close": 1.105,
                "volume": 100,
            }
        ]
    ).set_index("time")
    provider = StubHistoricalDataProvider(frame)
    config = BacktestConfig(
        start=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end=datetime(2023, 1, 2, tzinfo=timezone.utc),
        instruments=["EUR_USD"],
    )
    engine = BacktestEngine(config, provider, MacroReplay({}))

    loaded = engine._load_series("EUR_USD", "M15")

    assert loaded is not None
    assert loaded.equals(frame)
    assert provider.calls == 1


def test_conversion_rate_falls_back_to_inverse_pair_when_direct_pair_is_invalid():
    frame = pd.DataFrame(
        [
            {
                "time": pd.Timestamp("2023-01-01T00:00:00Z"),
                "open": 130.0,
                "high": 130.1,
                "low": 129.9,
                "close": 130.0,
                "volume": 100,
            }
        ]
    ).set_index("time")
    provider = StubHistoricalDataProvider(frame)
    config = BacktestConfig(
        start=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end=datetime(2023, 1, 2, tzinfo=timezone.utc),
        instruments=["USD_JPY"],
    )
    engine = BacktestEngine(config, provider, MacroReplay({}))

    def fake_bar_at(instrument: str, granularity: str, now: datetime):
        if instrument == "JPY_USD":
            raise requests.HTTPError("400 Client Error")
        if instrument == "USD_JPY":
            return {"close": 130.0}
        return None

    engine._bar_at = fake_bar_at  # type: ignore[method-assign]

    rate = engine._conversion_rate("JPY", "USD", datetime(2023, 1, 1, tzinfo=timezone.utc))

    assert rate == 1.0 / 130.0