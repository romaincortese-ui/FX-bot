import json

import pandas as pd

from backtest.build_macro_inputs import write_macro_input_files


def test_write_macro_input_files_emits_expected_shapes(tmp_path):
    outputs = write_macro_input_files(
        str(tmp_path),
        rates=pd.DataFrame([
            {"date": "2024-01-01T00:00:00Z", "US_2Y": 4.5, "UK_2Y": 4.8, "TED_SPREAD": 0.2}
        ]),
        momentum=pd.DataFrame([
            {"date": "2024-01-01T00:00:00Z", "OIL": 0.01, "COPPER": 0.02, "DXY": -0.003, "VIX": 0.04}
        ]),
        dxy=pd.DataFrame([
            {"date": "2024-01-01T00:00:00Z", "dxy": 102.5, "dxy_gap": -0.003, "value": 102.5}
        ]),
        vix=pd.DataFrame([
            {"date": "2024-01-01T00:00:00Z", "vix": 18.0, "vix_value": 18.0, "value": 18.0}
        ]),
        news_events=[{"currency": "USD", "event": "CPI"}],
    )

    rates = pd.read_csv(outputs["rates"])
    momentum = pd.read_csv(outputs["momentum"])
    dxy = pd.read_csv(outputs["dxy"])
    vix = pd.read_csv(outputs["vix"])
    news = json.loads((tmp_path / "news.json").read_text(encoding="utf-8"))

    assert set(outputs) == {"rates", "momentum", "esi", "liquidity", "dxy", "vix", "news"}
    assert list(rates.columns) == ["date", "US_2Y", "US_10Y", "UK_2Y", "EU_2Y", "JP_2Y", "TED_SPREAD"]
    assert list(momentum.columns) == ["date", "OIL", "COPPER", "DAIRY", "DXY", "VIX"]
    assert list(dxy.columns) == ["date", "dxy", "dxy_gap", "value"]
    assert list(vix.columns) == ["date", "vix", "vix_value", "value"]
    assert news["news_events"][0]["event"] == "CPI"