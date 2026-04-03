# Forex Trading Bot

This repository now uses a lighter modular structure around the existing OANDA FX bot and macro engine.

## What changed

The monolithic bot logic in `main.py` was partially decomposed into a shared package:

- `fxbot/config.py`: environment parsing helpers and runtime validation
- `fxbot/fx_math.py`: pip sizing and pip value helpers
- `fxbot/indicators.py`: ATR, RSI, MACD, Bollinger, Keltner, EMA
- `fxbot/pair_health.py`: pair-health scoring and recovery transitions
- `fxbot/risk.py`: correlation exposure checks
- `fxbot/macro_logic.py`: rate, commodity, market, ESI, and liquidity bias logic
- `fxbot/news.py`: economic-news timestamp parsing and feed-cache fallback helpers
- `fxbot/strategies/`: extracted direction and strategy scoring modules

The live bot still uses `main.py` as the orchestrator, but its core reusable logic now lives outside the runtime loop and can be tested independently.

## Current structure

- `main.py`: live OANDA FX bot runtime
- `macro_engine.py`: macro and news state builder
- `backtest/`: integrated backtest engine, simulator, reporter, and CLI
- `fxbot/`: shared modules extracted from the monolith
- `fxbot/strategies/`: extracted direction and scoring layer for strategies
- `tests/`: unit tests for extracted core logic
- `.github/workflows/python-tests.yml`: CI test workflow
- `Dockerfile`: container build for Railway or other worker deployments
- `Procfile`: worker startup command

## Review outcome

High-impact improvements from the review that are now implemented:

- Shared pure logic extracted from the main runtime into importable modules
- Runtime config validation added for both bot and macro engine
- Core unit tests added for FX math, ATR, macro bias merging, and pair-health transitions
- Macro engine made more resilient with cached-news fallback when live XML sources fail
- CI and Docker scaffolding added so the project is easier to ship cleanly

Still intentionally deferred:

- Full asyncio rewrite of the bot runtime
- Full strategy-class extraction for every scoring and execution path
- Proper historical backtesting engine with OANDA data replay
- Portfolio optimizer and walk-forward layer

## Local setup

1. Install Python 3.12.
2. Create and activate a virtual environment.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Copy `.env.sample` to `.env` in the repository root and set your OANDA, Redis, and macro values.
	Local runs of `main.py`, `macro_engine.py`, and `python -m backtest.run_backtest` now auto-load that file.
5. Run tests:

```bash
pytest -q
```

6. Run the macro engine:

```bash
python macro_engine.py
```

7. Run the bot:

```bash
python main.py
```

## Backtesting

The repository now includes a lightweight backtester that reuses the existing scoring and direction logic instead of requiring a second strategy implementation.

Files:

- `backtest/config.py`: backtest runtime settings and strategy thresholds
- `backtest/data.py`: cached historical candle loader for OANDA
- `backtest/build_macro_inputs.py`: helper CLI to build historical macro CSV/JSON inputs
- `backtest/macro_sim.py`: macro/news replay plus daily macro snapshot generation
- `backtest/simulator.py`: fills, slippage, spread, TP/SL, timeout, partial TP, and optional bid/ask execution
- `backtest/engine.py`: bar-by-bar strategy evaluation using the shared `StrategyScoringContext`
- `backtest/reporter.py`: summary metrics and artifact export
- `backtest/run_backtest.py`: CLI entrypoint

Example:

```bash
python -m backtest.run_backtest --start 2023-01-01T00:00:00Z --end 2023-06-01T00:00:00Z --instruments EUR_USD,GBP_USD,USD_JPY --granularity M15
```

If you want the backtest to fetch historical candles from OANDA locally, add `OANDA_API_KEY` and `OANDA_API_URL` to the root `.env` before running it.

Useful environment variables:

- `OANDA_API_KEY`, `OANDA_API_URL`
- `REDIS_URL`, `REDIS_MACRO_STATE_KEY`, `REDIS_TRADE_CALIBRATION_KEY`
- `CALIBRATION_MAX_AGE_HOURS`, `CALIBRATION_MIN_TOTAL_TRADES`
- `BACKTEST_START`, `BACKTEST_END`
- `BACKTEST_INSTRUMENTS`
- `BACKTEST_GRANULARITY`
- `BACKTEST_CACHE_DIR`
- `BACKTEST_MACRO_STATE_DIR`
- `BACKTEST_OUTPUT_DIR`
- `BACKTEST_GENERATE_MACRO_STATES`
- `BACKTEST_USE_BID_ASK_DATA`
- `BACKTEST_MACRO_RATES_FILE`, `BACKTEST_MACRO_MOMENTUM_FILE`
- `BACKTEST_MACRO_ESI_FILE`, `BACKTEST_MACRO_LIQUIDITY_FILE`
- `BACKTEST_MACRO_NEWS_FILE`, `BACKTEST_DXY_HISTORY_FILE`, `BACKTEST_VIX_HISTORY_FILE`

Historical macro input helper:

```bash
python -m backtest.build_macro_inputs --start 2023-01-01T00:00:00Z --end 2025-01-01T00:00:00Z --output-dir backtest_macro_inputs
```

What it does:

- Builds `rates.csv`, `momentum.csv`, `esi.csv`, `liquidity.csv`, `dxy.csv`, `vix.csv`, and `news.json` in one directory.
- Pulls US Treasury and TED spread history from FRED when `FRED_API_KEY` is available.
- Pulls oil, copper, DXY, and VIX daily history from Yahoo Finance and derives momentum series for the snapshot generator.
- Accepts optional override files for UK/EU/JP yields, ESI, liquidity, dairy, and news so you can fill gaps in sources without hand-editing the generated files.

Artifacts are written to the configured output directory as:

- `equity_curve.csv`
- `trade_journal.csv`
- `summary.json`
- `calibration.json`

`calibration.json` contains grouped backtest stats by strategy, strategy/pair, and strategy/pair/session.
The live bot can read that file through `TRADE_CALIBRATION_FILE` or, when `REDIS_URL` is configured, from `REDIS_TRADE_CALIBRATION_KEY` on the same Redis used by the macro engine.

Historical realism notes:

- If `BACKTEST_GENERATE_MACRO_STATES=true`, the backtester writes one macro snapshot per day into `BACKTEST_MACRO_STATE_DIR` using the supplied historical macro input files.
- If `BACKTEST_USE_BID_ASK_DATA=true`, OANDA candles are requested with bid/ask components and the simulator uses those for trade entry, stop-loss, take-profit, and timeout exits when available.
- When bid/ask candles are not available, the engine falls back to pair-specific spread profiles built from cached bid/ask history and only then to ATR-based spread estimation.

## GitHub setup

1. Create a new GitHub repository.
2. Put these files in the repository root.
3. Commit everything:

```bash
git init
git add .
git commit -m "Refactor FX bot into shared modules"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main
```

4. Confirm GitHub Actions is enabled so `.github/workflows/python-tests.yml` runs on push and pull request.

## Railway setup

Recommended layout:

1. Create one Railway service for the trading bot.
2. Create a second Railway service or scheduled job for `macro_engine.py`.
3. Use the same repository for both services.

Bot service:

1. Connect the GitHub repository to Railway.
2. Set the start command to:

```text
python main.py
```

3. Add all required environment variables in Railway.
4. Mount Redis if you want macro state shared through `REDIS_URL`.

Macro service:

1. Reuse the same repository.
2. Set the start command to:

```text
python macro_engine.py
```

3. If you want it to run once per day, use a scheduled job or a separate worker process pattern.

## Docker option

You can also deploy with the included `Dockerfile`.

```bash
docker build -t fx-bot .
docker run --env-file .env fx-bot
```

For Railway Docker deployments, Railway can build directly from the repository.

## Notes

- `main.py` is the live entrypoint in this repository.
- The CI workflow only runs unit tests for extracted core logic, not live broker integration.
- The macro engine now falls back to `macro_news_cache.json` if all economic-calendar sources fail.
- Before meaningful live capital, add a proper backtesting harness and a controlled forward-test checklist.