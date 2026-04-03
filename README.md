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

4. Copy `.env.sample` to `.env` and set your OANDA, Telegram, Redis, and macro values.
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
- `backtest/macro_sim.py`: macro/news replay from daily snapshots or static files
- `backtest/simulator.py`: fills, slippage, spread, TP/SL, timeout, and partial TP simulation
- `backtest/engine.py`: bar-by-bar strategy evaluation using the shared `StrategyScoringContext`
- `backtest/reporter.py`: summary metrics and artifact export
- `backtest/run_backtest.py`: CLI entrypoint

Example:

```bash
python -m backtest.run_backtest --start 2023-01-01T00:00:00Z --end 2023-06-01T00:00:00Z --instruments EUR_USD,GBP_USD,USD_JPY --granularity M15
```

Useful environment variables:

- `BACKTEST_START`, `BACKTEST_END`
- `BACKTEST_INSTRUMENTS`
- `BACKTEST_GRANULARITY`
- `BACKTEST_CACHE_DIR`
- `BACKTEST_MACRO_STATE_DIR`
- `BACKTEST_OUTPUT_DIR`

Artifacts are written to the configured output directory as:

- `equity_curve.csv`
- `trade_journal.csv`
- `summary.json`

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