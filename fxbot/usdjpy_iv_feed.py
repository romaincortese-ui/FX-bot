"""USD/JPY 1-week implied-volatility ingestion (FX-bot Tier 5 §9).

The existing ``fxbot.carry_basket.compute_exposure_multiplier`` reads
``usdjpy_1w_iv_pct`` and ramps basket exposure to zero between 11% and
13% IV — the historical FX carry-unwind signature. OANDA does not
publish an FX options feed, so this module provides two ingestion
paths that are light enough to run on the same daily cadence as
financing refresh:

1. **Redis key** (preferred, zero network cost). Ops writes the
   latest JPY 1w ATM IV (percent, not vol points) to
   ``fxbot:usdjpy_1w_iv`` either manually or from an external cron.
2. **HTTP JSON endpoint** (optional). If ``USDJPY_IV_HTTP_URL`` is
   configured, we GET it and parse the first numeric value found
   under common keys. Any failure silently falls back to Redis.

The module is intentionally pure — it returns ``float | None`` and does
not mutate state. The caller decides whether to trust the value.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable, Optional


@dataclass(frozen=True, slots=True)
class ImpliedVolQuote:
    instrument: str       # "USD_JPY"
    atm_iv_pct: float     # 1-week ATM, percent (e.g. 9.25)
    source: str           # "redis" | "http" | "override"


_CANDIDATE_KEYS = ("iv", "atm_iv", "atm_iv_pct", "value", "v", "usdjpy_1w_iv")


def _coerce_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if val <= 0 or val != val:  # NaN check
        return None
    return val


def _extract_from_payload(payload: Any) -> float | None:
    """Pull a numeric IV reading out of a Redis/HTTP payload."""
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(payload, str):
        s = payload.strip()
        if not s:
            return None
        # Try bare-number first.
        direct = _coerce_float(s)
        if direct is not None:
            return direct
        try:
            payload = json.loads(s)
        except ValueError:
            return None
    if isinstance(payload, dict):
        for key in _CANDIDATE_KEYS:
            if key in payload:
                v = _coerce_float(payload[key])
                if v is not None:
                    return v
        return None
    if isinstance(payload, (list, tuple)) and payload:
        return _coerce_float(payload[0])
    return _coerce_float(payload)


def fetch_usdjpy_1w_iv(
    *,
    redis_client: Any = None,
    redis_key: str = "fxbot:usdjpy_1w_iv",
    http_fetcher: Callable[[], Any] | None = None,
) -> Optional[ImpliedVolQuote]:
    """Return the latest USD/JPY 1-week ATM IV or ``None``.

    Precedence: HTTP (if provided) → Redis. Any unexpected error or
    non-numeric payload is treated as "no data" rather than raised —
    the caller must handle ``None`` as "run without kill-switch".
    """
    if http_fetcher is not None:
        try:
            payload = http_fetcher()
        except Exception:
            payload = None
        iv = _extract_from_payload(payload)
        if iv is not None:
            return ImpliedVolQuote("USD_JPY", iv, "http")
    if redis_client is not None and redis_key:
        try:
            payload = redis_client.get(redis_key)
        except Exception:
            payload = None
        iv = _extract_from_payload(payload)
        if iv is not None:
            return ImpliedVolQuote("USD_JPY", iv, "redis")
    return None
