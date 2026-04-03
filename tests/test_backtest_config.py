from backtest.config import BacktestConfig


def test_backtest_config_excludes_asian_fade_by_default(monkeypatch):
    monkeypatch.delenv("BACKTEST_STRATEGIES", raising=False)

    config = BacktestConfig.from_env()

    assert "ASIAN_FADE" not in config.strategies
    assert config.strategies == [
        "SCALPER",
        "TREND",
        "REVERSAL",
        "BREAKOUT",
        "CARRY",
        "POST_NEWS",
        "PULLBACK",
    ]