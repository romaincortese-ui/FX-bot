from fxbot.usdjpy_iv_feed import fetch_usdjpy_1w_iv


class _FakeRedis:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[str] = []

    def get(self, key):
        self.calls.append(key)
        return self.payload


def test_returns_none_when_no_sources():
    assert fetch_usdjpy_1w_iv(redis_client=None) is None


def test_parses_bare_number_string():
    r = _FakeRedis(b"9.75")
    q = fetch_usdjpy_1w_iv(redis_client=r, redis_key="k")
    assert q is not None
    assert q.atm_iv_pct == 9.75
    assert q.source == "redis"
    assert q.instrument == "USD_JPY"


def test_parses_json_dict_iv_key():
    r = _FakeRedis('{"iv": 12.1, "ts": "2026-04-01"}')
    q = fetch_usdjpy_1w_iv(redis_client=r, redis_key="k")
    assert q is not None
    assert q.atm_iv_pct == 12.1


def test_rejects_non_positive():
    r = _FakeRedis("0")
    assert fetch_usdjpy_1w_iv(redis_client=r, redis_key="k") is None


def test_rejects_garbage():
    r = _FakeRedis("not-a-number")
    assert fetch_usdjpy_1w_iv(redis_client=r, redis_key="k") is None


def test_http_takes_precedence():
    r = _FakeRedis("9.0")
    q = fetch_usdjpy_1w_iv(
        redis_client=r,
        redis_key="k",
        http_fetcher=lambda: {"atm_iv_pct": 11.5},
    )
    assert q is not None
    assert q.atm_iv_pct == 11.5
    assert q.source == "http"
    # Redis must NOT be consulted when HTTP succeeded.
    assert r.calls == []


def test_http_failure_falls_back_to_redis():
    def _boom():
        raise RuntimeError("net")

    r = _FakeRedis("9.5")
    q = fetch_usdjpy_1w_iv(redis_client=r, redis_key="k", http_fetcher=_boom)
    assert q is not None
    assert q.atm_iv_pct == 9.5
    assert q.source == "redis"


def test_redis_error_is_swallowed():
    class _BoomRedis:
        def get(self, key):
            raise RuntimeError("redis down")

    assert fetch_usdjpy_1w_iv(redis_client=_BoomRedis(), redis_key="k") is None
