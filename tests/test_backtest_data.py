from datetime import datetime, timedelta, timezone

import requests

from backtest.data import HistoricalDataProvider


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)
        return None

    def json(self):
        return {"candles": []}


class RecordingSession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def get(self, url, params=None, timeout=30):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        return FakeResponse()


class FlakySession(RecordingSession):
    def __init__(self, responses):
        super().__init__()
        self.responses = list(responses)

    def get(self, url, params=None, timeout=30):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse()


def test_get_candles_chunks_m5_requests_under_oanda_limit(tmp_path):
    provider = HistoricalDataProvider(oanda_api_key="token", cache_dir=str(tmp_path))
    provider.session = RecordingSession()

    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=31)

    provider.get_candles("USD_JPY", "M5", start, end, price="MBA")

    assert len(provider.session.calls) >= 2
    max_span = timedelta(minutes=5 * 4500)
    for call in provider.session.calls:
        chunk_start = datetime.fromisoformat(call["params"]["from"].replace("Z", "+00:00"))
        chunk_end = datetime.fromisoformat(call["params"]["to"].replace("Z", "+00:00"))
        assert chunk_end - chunk_start <= max_span


def test_get_candles_clamps_future_end_before_oanda_request(tmp_path):
    provider = HistoricalDataProvider(oanda_api_key="token", cache_dir=str(tmp_path))
    provider.session = RecordingSession()

    start = datetime.now(timezone.utc) - timedelta(days=2)
    requested_end = datetime.now(timezone.utc) + timedelta(days=3)

    provider.get_candles("EUR_USD", "M15", start, requested_end, price="MBA")

    assert provider.session.calls
    latest_allowed = datetime.now(timezone.utc)
    for call in provider.session.calls:
        chunk_end = datetime.fromisoformat(call["params"]["to"].replace("Z", "+00:00"))
        assert chunk_end <= latest_allowed


def test_get_candles_retries_transient_oanda_errors(tmp_path, monkeypatch):
    provider = HistoricalDataProvider(oanda_api_key="token", cache_dir=str(tmp_path))
    provider.session = FlakySession([FakeResponse(status_code=502), FakeResponse(status_code=200)])
    monkeypatch.setattr("backtest.data.time.sleep", lambda *_args, **_kwargs: None)

    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=4)

    provider.get_candles("GBP_USD", "H4", start, end, price="MBA")

    assert len(provider.session.calls) == 2