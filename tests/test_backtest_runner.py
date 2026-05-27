from datetime import datetime, timezone
from types import SimpleNamespace

import backtest.run_backtest as runner


def _config(tmp_path):
    return SimpleNamespace(
        start=datetime(2026, 4, 17, tzinfo=timezone.utc),
        end=datetime(2026, 5, 17, tzinfo=timezone.utc),
        cache_dir=str(tmp_path / "cache"),
        macro_state_dir=str(tmp_path / "macro"),
        output_dir=str(tmp_path / "out"),
        generate_macro_states=False,
        macro_rates_file="",
        macro_momentum_file="",
        macro_esi_file="",
        macro_liquidity_file="",
        macro_news_file="",
        dxy_history_file="",
        vix_history_file="",
    )


def _patch_runner(monkeypatch, report):
    monkeypatch.setattr(runner, "HistoricalDataProvider", lambda **_kwargs: object())
    monkeypatch.setattr(runner.MacroReplay, "from_directory", lambda *_args, **_kwargs: object())

    class FakeBacktestEngine:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def run(self):
            return [], []

    monkeypatch.setattr(runner, "BacktestEngine", FakeBacktestEngine)
    monkeypatch.setattr(runner, "build_backtest_report", lambda *_args, **_kwargs: report)
    monkeypatch.setattr(runner, "build_trade_calibration", lambda _trades: {"total_trades": report.get("total_trades", 0)})
    monkeypatch.setattr(runner, "export_backtest_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "env_str", lambda _name, default="": default)


def test_run_backtest_does_not_publish_invalid_calibration(monkeypatch, tmp_path):
    published = []
    _patch_runner(monkeypatch, {"total_trades": 0, "total_pnl": 0.0})
    monkeypatch.setattr(runner, "publish_trade_calibration", lambda *args, **_kwargs: published.append(args) or True)

    report = runner.run_backtest(_config(tmp_path))

    assert report["total_trades"] == 0
    assert published == []


def test_run_backtest_publishes_positive_calibration(monkeypatch, tmp_path):
    published = []
    _patch_runner(monkeypatch, {"total_trades": 3, "total_pnl": 42.0})
    monkeypatch.setattr(runner, "publish_trade_calibration", lambda *args, **_kwargs: published.append(args) or True)

    report = runner.run_backtest(_config(tmp_path))

    assert report["total_pnl"] == 42.0
    assert len(published) == 1