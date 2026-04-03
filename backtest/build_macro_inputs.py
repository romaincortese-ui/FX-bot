from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from fxbot.config import env_str

try:
    from fredapi import Fred
except ImportError:
    Fred = None  # type: ignore

try:
    import yfinance as yf
except ImportError:
    yf = None  # type: ignore


DEFAULT_FRED_SERIES = {
    "US_2Y": "DGS2",
    "US_10Y": "DGS10",
    "TED_SPREAD": "TEDRATE",
}

DEFAULT_MARKET_TICKERS = {
    "OIL": env_str("YFINANCE_OIL_TICKER", "CL=F"),
    "COPPER": env_str("YFINANCE_COPPER_TICKER", "HG=F"),
    "DXY": env_str("DXY_TICKER", "DX-Y.NYB"),
    "VIX": env_str("VIX_TICKER", "^VIX"),
}


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    normalized = df.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], utc=True, errors="coerce")
    normalized = normalized.dropna(subset=["date"]).sort_values("date")
    normalized["date"] = normalized["date"].dt.floor("D")
    normalized = normalized.drop_duplicates(subset=["date"], keep="last")
    return normalized


def _load_optional_frame(path: str) -> pd.DataFrame:
    file_path = Path(path)
    if not path or not file_path.exists():
        return pd.DataFrame()
    if file_path.suffix.lower() == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            records = payload.get("records") if isinstance(payload.get("records"), list) else [payload]
            frame = pd.DataFrame(records)
        elif isinstance(payload, list):
            frame = pd.DataFrame(payload)
        else:
            return pd.DataFrame()
    else:
        frame = pd.read_csv(file_path)
    if "date" not in frame.columns:
        return pd.DataFrame()
    return _normalize_frame(frame)


def _merge_frames(base: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return _normalize_frame(incoming)
    if incoming.empty:
        return _normalize_frame(base)
    merged = pd.merge(base, incoming, on="date", how="outer")
    return _normalize_frame(merged)


def _fetch_fred_history(series_id: str, column: str, start: datetime, end: datetime, api_key: str) -> pd.DataFrame:
    if not series_id or not api_key or Fred is None:
        return pd.DataFrame()
    try:
        fred = Fred(api_key=api_key)
        series = fred.get_series(series_id, observation_start=start.date(), observation_end=end.date())
    except Exception:
        return pd.DataFrame()
    if series is None or series.empty:
        return pd.DataFrame()
    frame = series.rename(column).reset_index()
    frame.columns = ["date", column]
    return _normalize_frame(frame)


def _fetch_yfinance_history(ticker: str, value_column: str, start: datetime, end: datetime) -> pd.DataFrame:
    if not ticker or yf is None:
        return pd.DataFrame()
    try:
        history = yf.Ticker(ticker).history(
            start=start.date().isoformat(),
            end=(end + timedelta(days=1)).date().isoformat(),
            interval="1d",
            auto_adjust=False,
        )
    except Exception:
        return pd.DataFrame()
    if history.empty or "Close" not in history.columns:
        return pd.DataFrame()
    frame = history[["Close"]].rename(columns={"Close": value_column}).reset_index()
    frame.columns = ["date", value_column]
    return _normalize_frame(frame)


def _with_pct_change(frame: pd.DataFrame, source_column: str, target_column: str) -> pd.DataFrame:
    if frame.empty or source_column not in frame.columns:
        return pd.DataFrame()
    result = frame[["date", source_column]].copy()
    result[target_column] = pd.to_numeric(result[source_column], errors="coerce").pct_change()
    return _normalize_frame(result[["date", target_column]])


def _write_frame(path: Path, frame: pd.DataFrame, columns: list[str]) -> None:
    if frame.empty:
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        return
    output = frame.copy()
    for column in columns:
        if column not in output.columns:
            output[column] = pd.NA
    output = output[columns]
    output["date"] = pd.to_datetime(output["date"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    output.to_csv(path, index=False)


def write_macro_input_files(
    output_dir: str,
    *,
    rates: pd.DataFrame | None = None,
    momentum: pd.DataFrame | None = None,
    esi: pd.DataFrame | None = None,
    liquidity: pd.DataFrame | None = None,
    dxy: pd.DataFrame | None = None,
    vix: pd.DataFrame | None = None,
    news_events: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    base = Path(output_dir)
    base.mkdir(parents=True, exist_ok=True)

    rate_columns = ["date", "US_2Y", "US_10Y", "UK_2Y", "EU_2Y", "JP_2Y", "TED_SPREAD"]
    momentum_columns = ["date", "OIL", "COPPER", "DAIRY", "DXY", "VIX"]
    esi_columns = ["date", "US", "UK", "EU", "JP"]
    liquidity_columns = ["date", "TED_SPREAD", "FRA_OIS_SPREAD"]
    dxy_columns = ["date", "dxy", "dxy_gap", "value"]
    vix_columns = ["date", "vix", "vix_value", "value"]

    _write_frame(base / "rates.csv", _normalize_frame(rates if rates is not None else pd.DataFrame()), rate_columns)
    _write_frame(base / "momentum.csv", _normalize_frame(momentum if momentum is not None else pd.DataFrame()), momentum_columns)
    _write_frame(base / "esi.csv", _normalize_frame(esi if esi is not None else pd.DataFrame()), esi_columns)
    _write_frame(base / "liquidity.csv", _normalize_frame(liquidity if liquidity is not None else pd.DataFrame()), liquidity_columns)
    _write_frame(base / "dxy.csv", _normalize_frame(dxy if dxy is not None else pd.DataFrame()), dxy_columns)
    _write_frame(base / "vix.csv", _normalize_frame(vix if vix is not None else pd.DataFrame()), vix_columns)
    (base / "news.json").write_text(json.dumps({"news_events": list(news_events or [])}, indent=2), encoding="utf-8")

    return {
        "rates": str(base / "rates.csv"),
        "momentum": str(base / "momentum.csv"),
        "esi": str(base / "esi.csv"),
        "liquidity": str(base / "liquidity.csv"),
        "dxy": str(base / "dxy.csv"),
        "vix": str(base / "vix.csv"),
        "news": str(base / "news.json"),
    }


def build_historical_macro_inputs(
    start: datetime,
    end: datetime,
    output_dir: str,
    *,
    fred_api_key: str = "",
    uk_rates_file: str = "",
    eu_rates_file: str = "",
    jp_rates_file: str = "",
    esi_file: str = "",
    liquidity_file: str = "",
    dairy_file: str = "",
    news_file: str = "",
    oil_ticker: str = DEFAULT_MARKET_TICKERS["OIL"],
    copper_ticker: str = DEFAULT_MARKET_TICKERS["COPPER"],
    dxy_ticker: str = DEFAULT_MARKET_TICKERS["DXY"],
    vix_ticker: str = DEFAULT_MARKET_TICKERS["VIX"],
    uk_fred_series: str = "",
    eu_fred_series: str = "",
    jp_fred_series: str = "",
    fra_ois_fred_series: str = "",
) -> dict[str, str]:
    rates = pd.DataFrame()
    for column, series_id in DEFAULT_FRED_SERIES.items():
        rates = _merge_frames(rates, _fetch_fred_history(series_id, column, start, end, fred_api_key))
    if uk_fred_series:
        rates = _merge_frames(rates, _fetch_fred_history(uk_fred_series, "UK_2Y", start, end, fred_api_key))
    if eu_fred_series:
        rates = _merge_frames(rates, _fetch_fred_history(eu_fred_series, "EU_2Y", start, end, fred_api_key))
    if jp_fred_series:
        rates = _merge_frames(rates, _fetch_fred_history(jp_fred_series, "JP_2Y", start, end, fred_api_key))
    rates = _merge_frames(rates, _load_optional_frame(uk_rates_file))
    rates = _merge_frames(rates, _load_optional_frame(eu_rates_file))
    rates = _merge_frames(rates, _load_optional_frame(jp_rates_file))

    oil_prices = _fetch_yfinance_history(oil_ticker, "oil_close", start, end)
    copper_prices = _fetch_yfinance_history(copper_ticker, "copper_close", start, end)
    dxy_prices = _fetch_yfinance_history(dxy_ticker, "dxy", start, end)
    vix_prices = _fetch_yfinance_history(vix_ticker, "vix", start, end)
    dairy = _load_optional_frame(dairy_file)

    momentum = _merge_frames(_with_pct_change(oil_prices, "oil_close", "OIL"), _with_pct_change(copper_prices, "copper_close", "COPPER"))
    momentum = _merge_frames(momentum, _with_pct_change(dxy_prices, "dxy", "DXY"))
    momentum = _merge_frames(momentum, _with_pct_change(vix_prices, "vix", "VIX"))
    momentum = _merge_frames(momentum, dairy)

    dxy = _merge_frames(dxy_prices, _with_pct_change(dxy_prices, "dxy", "dxy_gap"))
    if not dxy.empty:
        dxy["value"] = dxy.get("dxy")
    vix = vix_prices.copy()
    if not vix.empty:
        vix["vix_value"] = vix.get("vix")
        vix["value"] = vix.get("vix")

    liquidity = _merge_frames(_load_optional_frame(liquidity_file), pd.DataFrame())
    if fra_ois_fred_series:
        liquidity = _merge_frames(liquidity, _fetch_fred_history(fra_ois_fred_series, "FRA_OIS_SPREAD", start, end, fred_api_key))
    ted = rates[["date", "TED_SPREAD"]].copy() if not rates.empty and "TED_SPREAD" in rates.columns else pd.DataFrame()
    liquidity = _merge_frames(liquidity, ted)

    esi = _load_optional_frame(esi_file)
    news_events: list[dict[str, Any]] = []
    news_payload_path = Path(news_file)
    if news_file and news_payload_path.exists():
        payload = json.loads(news_payload_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            maybe_events = payload.get("news_events", [])
            if isinstance(maybe_events, list):
                news_events = [event for event in maybe_events if isinstance(event, dict)]
        elif isinstance(payload, list):
            news_events = [event for event in payload if isinstance(event, dict)]

    return write_macro_input_files(
        output_dir,
        rates=rates,
        momentum=momentum,
        esi=esi,
        liquidity=liquidity,
        dxy=dxy,
        vix=vix,
        news_events=news_events,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build historical macro input files for the FX backtester")
    parser.add_argument("--start", required=True, help="UTC ISO start datetime")
    parser.add_argument("--end", required=True, help="UTC ISO end datetime")
    parser.add_argument("--output-dir", default="backtest_macro_inputs", help="Directory for generated CSV/JSON macro inputs")
    parser.add_argument("--fred-api-key", default=os.getenv("FRED_API_KEY", ""), help="FRED API key used for rate and liquidity series")
    parser.add_argument("--uk-rates-file", default="", help="Optional CSV or JSON with date and UK_2Y columns")
    parser.add_argument("--eu-rates-file", default="", help="Optional CSV or JSON with date and EU_2Y columns")
    parser.add_argument("--jp-rates-file", default="", help="Optional CSV or JSON with date and JP_2Y columns")
    parser.add_argument("--esi-file", default="", help="Optional CSV or JSON with historical ESI values")
    parser.add_argument("--liquidity-file", default="", help="Optional CSV or JSON with liquidity stress values")
    parser.add_argument("--dairy-file", default="", help="Optional CSV or JSON with date and DAIRY columns")
    parser.add_argument("--news-file", default="", help="Optional JSON news payload to copy into news.json")
    parser.add_argument("--oil-ticker", default=DEFAULT_MARKET_TICKERS["OIL"], help="Yahoo Finance ticker for oil")
    parser.add_argument("--copper-ticker", default=DEFAULT_MARKET_TICKERS["COPPER"], help="Yahoo Finance ticker for copper")
    parser.add_argument("--dxy-ticker", default=DEFAULT_MARKET_TICKERS["DXY"], help="Yahoo Finance ticker for DXY")
    parser.add_argument("--vix-ticker", default=DEFAULT_MARKET_TICKERS["VIX"], help="Yahoo Finance ticker for VIX")
    parser.add_argument("--uk-fred-series", default="", help="Optional FRED series id for UK 2Y yields")
    parser.add_argument("--eu-fred-series", default="", help="Optional FRED series id for EU 2Y yields")
    parser.add_argument("--jp-fred-series", default="", help="Optional FRED series id for JP 2Y yields")
    parser.add_argument("--fra-ois-fred-series", default="", help="Optional FRED series id for FRA/OIS spread")
    args = parser.parse_args()

    outputs = build_historical_macro_inputs(
        _parse_datetime(args.start),
        _parse_datetime(args.end),
        args.output_dir,
        fred_api_key=args.fred_api_key,
        uk_rates_file=args.uk_rates_file,
        eu_rates_file=args.eu_rates_file,
        jp_rates_file=args.jp_rates_file,
        esi_file=args.esi_file,
        liquidity_file=args.liquidity_file,
        dairy_file=args.dairy_file,
        news_file=args.news_file,
        oil_ticker=args.oil_ticker,
        copper_ticker=args.copper_ticker,
        dxy_ticker=args.dxy_ticker,
        vix_ticker=args.vix_ticker,
        uk_fred_series=args.uk_fred_series,
        eu_fred_series=args.eu_fred_series,
        jp_fred_series=args.jp_fred_series,
        fra_ois_fred_series=args.fra_ois_fred_series,
    )
    print(json.dumps(outputs, indent=2))


if __name__ == "__main__":
    main()