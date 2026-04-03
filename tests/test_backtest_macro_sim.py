import json
from datetime import datetime, timezone
from pathlib import Path

from backtest.macro_sim import generate_daily_macro_snapshots


def test_generate_daily_macro_snapshots_writes_states(tmp_path: Path):
    rates = tmp_path / "rates.csv"
    rates.write_text(
        "date,US_2Y,UK_2Y,EU_2Y,JP_2Y\n"
        "2024-01-01T00:00:00Z,4.5,4.8,3.2,0.4\n",
        encoding="utf-8",
    )
    dxy = tmp_path / "dxy.csv"
    dxy.write_text(
        "date,dxy_gap\n"
        "2024-01-01T00:00:00Z,0.01\n"
        "2024-01-02T00:00:00Z,-0.002\n",
        encoding="utf-8",
    )
    vix = tmp_path / "vix.csv"
    vix.write_text(
        "date,vix_value\n"
        "2024-01-01T00:00:00Z,19\n"
        "2024-01-02T00:00:00Z,27\n",
        encoding="utf-8",
    )
    news = tmp_path / "news.json"
    news.write_text(
        json.dumps(
            {
                "news_events": [
                    {
                        "currency": "USD",
                        "event": "NFP",
                        "time": "2024-01-02T13:30:00+00:00",
                        "pause_start": "2024-01-02T13:15:00+00:00",
                        "pause_end": "2024-01-02T13:45:00+00:00",
                        "impact": "high",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "states"

    states = generate_daily_macro_snapshots(
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 2, tzinfo=timezone.utc),
        output_dir=str(output_dir),
        rates_file=str(rates),
        news_file=str(news),
        dxy_history_file=str(dxy),
        vix_history_file=str(vix),
    )

    assert len(states) == 2
    assert (output_dir / "2024-01-01.json").exists()
    assert (output_dir / "2024-01-02.json").exists()
    assert states[datetime(2024, 1, 1, tzinfo=timezone.utc).date()].filters["GBP_USD"] == "LONG_ONLY"
    assert states[datetime(2024, 1, 2, tzinfo=timezone.utc).date()].vix_value == 27.0
    assert len(states[datetime(2024, 1, 2, tzinfo=timezone.utc).date()].news_events) == 1