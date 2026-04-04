from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def build_runtime_status(service: str, state: str, **fields: Any) -> dict[str, Any]:
    payload = {
        "service": service,
        "state": state,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload.update(fields)
    return payload


def publish_runtime_status(client: Any, key: str, payload: dict[str, Any], ttl_seconds: int) -> bool:
    if client is None or not key or ttl_seconds <= 0:
        return False
    client.set(key, json.dumps(payload), ex=ttl_seconds)
    return True