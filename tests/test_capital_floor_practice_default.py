"""Capital-floor default must be *off* on practice (demo) accounts.

Memo 4 follow-up: the user reported that paper-* synthetic trades were
appearing for a £190 demo account. Root cause was the capital floor
(Tier 2v2 E2) being ON by default regardless of environment. Its
rationale — "protect real money from minimum-stake rounding" — does not
apply on a demo/fxPractice account where there is no real capital at
risk, so the floor should default OFF on practice and ON on live, with
the env var always winning.
"""
from __future__ import annotations

import importlib
import os
import sys


def _reload_main(monkeypatch, *, environment: str, floor_env: str | None):
    monkeypatch.setenv("OANDA_ENVIRONMENT", environment)
    if floor_env is None:
        monkeypatch.delenv("CAPITAL_FLOOR_ENABLED", raising=False)
    else:
        monkeypatch.setenv("CAPITAL_FLOOR_ENABLED", floor_env)
    sys.modules.pop("main", None)
    return importlib.import_module("main")


def test_capital_floor_defaults_off_on_practice(monkeypatch):
    mod = _reload_main(monkeypatch, environment="practice", floor_env=None)
    assert mod.CAPITAL_FLOOR_ENABLED is False


def test_capital_floor_defaults_on_on_live(monkeypatch):
    mod = _reload_main(monkeypatch, environment="live", floor_env=None)
    assert mod.CAPITAL_FLOOR_ENABLED is True


def test_env_override_wins_on_practice(monkeypatch):
    mod = _reload_main(monkeypatch, environment="practice", floor_env="1")
    assert mod.CAPITAL_FLOOR_ENABLED is True


def test_env_override_wins_on_live(monkeypatch):
    mod = _reload_main(monkeypatch, environment="live", floor_env="0")
    assert mod.CAPITAL_FLOOR_ENABLED is False
