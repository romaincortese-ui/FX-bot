"""Tests for Tier 2 §19 — slippage attribution."""
from __future__ import annotations

import os
import tempfile

from fxbot.slippage import SlippageLogger


def test_compute_slip_pips_long_adverse():
    slip = SlippageLogger.compute_slip_pips(
        signal_mid=1.1000,
        fill_price=1.1002,
        direction="LONG",
        pip_size=0.0001,
    )
    # 2 pips adverse for a LONG.
    assert round(slip, 3) == 2.0


def test_compute_slip_pips_short_adverse():
    slip = SlippageLogger.compute_slip_pips(
        signal_mid=1.1000,
        fill_price=1.0998,
        direction="SHORT",
        pip_size=0.0001,
    )
    # Price dropped → SHORT adverse slippage positive (fill worse than mid).
    # For SHORT we sell: mid 1.1000, fill 1.0998 means we sold lower → adverse
    # (negative price-improvement for a seller).
    assert round(slip, 3) == 2.0


def test_logger_appends_csv_and_memory(tmp_path):
    csv_path = tmp_path / "slippage.csv"
    logger = SlippageLogger(csv_path=str(csv_path), max_memory=50)
    logger.log(
        instrument="EUR_USD",
        strategy="SCALPER",
        direction="LONG",
        signal_mid=1.1000,
        fill_price=1.1001,
        pip_size=0.0001,
        session="LONDON",
        label="SCALPER",
    )
    logger.log(
        instrument="USD_JPY",
        strategy="SCALPER",
        direction="LONG",
        signal_mid=150.00,
        fill_price=150.02,
        pip_size=0.01,
        session="LONDON",
    )
    assert len(logger.recent_slippage()) == 2
    text = csv_path.read_text(encoding="utf-8")
    assert "EUR_USD" in text
    assert "USD_JPY" in text
    agg = logger.aggregate_by_strategy()
    assert "SCALPER" in agg
    assert agg["SCALPER"]["count"] == 2.0


def test_invalid_inputs_return_zero():
    assert SlippageLogger.compute_slip_pips(
        signal_mid=0.0, fill_price=1.0, direction="LONG", pip_size=0.0001
    ) == 0.0
    assert SlippageLogger.compute_slip_pips(
        signal_mid=1.0, fill_price=1.0, direction="NEITHER", pip_size=0.0001
    ) == 0.0
