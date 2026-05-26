from backtest.config import BacktestConfig


def test_backtest_config_uses_scalper_only_by_default(monkeypatch):
    monkeypatch.delenv("BACKTEST_STRATEGIES", raising=False)

    config = BacktestConfig.from_env()

    assert config.strategies == ["SCALPER"]


def test_backtest_defaults_use_validated_scalper_lanes(monkeypatch):
    for name in ("BACKTEST_INSTRUMENTS", "SCALPER_THRESHOLD", "TRADE_LANE_ALLOWLIST"):
        monkeypatch.delenv(name, raising=False)

    config = BacktestConfig.from_env()
    settings = config.strategy_settings()

    assert config.instruments == [
        "AUD_USD",
        "EUR_GBP",
        "EUR_USD",
        "GBP_USD",
        "NZD_USD",
        "USD_CAD",
        "USD_CHF",
        "USD_JPY",
    ]
    assert settings["SCALPER_THRESHOLD"] == 70
    assert settings["TRADE_LANE_ALLOWLIST"] == "SCALPER:AUD_USD:SHORT,SCALPER:EUR_USD:LONG,SCALPER:USD_CHF:LONG"