from fxbot.runtime_status import build_runtime_status, publish_runtime_status


class FakeRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.calls.append((key, value, ex or 0))


def test_build_runtime_status_includes_core_fields() -> None:
    payload = build_runtime_status("bot", "running", open_trades=2)

    assert payload["service"] == "bot"
    assert payload["state"] == "running"
    assert payload["open_trades"] == 2
    assert "generated_at" in payload


def test_publish_runtime_status_writes_json_with_ttl() -> None:
    client = FakeRedis()
    payload = build_runtime_status("macro", "idle", filter_count=7)

    published = publish_runtime_status(client, "macro_runtime_status", payload, 120)

    assert published is True
    assert len(client.calls) == 1
    key, value, ttl = client.calls[0]
    assert key == "macro_runtime_status"
    assert '"service": "macro"' in value
    assert ttl == 120


def test_publish_runtime_status_rejects_invalid_inputs() -> None:
    payload = build_runtime_status("calibration", "completed")

    assert publish_runtime_status(None, "key", payload, 120) is False
    assert publish_runtime_status(FakeRedis(), "", payload, 120) is False
    assert publish_runtime_status(FakeRedis(), "key", payload, 0) is False