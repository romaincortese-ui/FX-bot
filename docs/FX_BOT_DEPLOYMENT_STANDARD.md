# FX-bot — Railway Deployment Standard

**Last updated:** 29 April 2026 (P2 #15)
**Audience:** operator running multiple OANDA-fronted bots (FX, gold, future
metals/energies sleeves) on a shared Railway project.
**Source assessment:** [`docs/FX_BOT_UPDATED_ASSESSMENT.md`](../../docs/FX_BOT_UPDATED_ASSESSMENT.md)
§2.4 / §4.4 / P2 #15.

---

## 1. One OANDA sub-account per bot — non-negotiable

OANDA permits multiple sub-accounts under a single login at no charge. Each
bot in the estate **must** be pointed at its own sub-account via
`OANDA_ACCOUNT_ID`. Sharing one account between bots produces three
permanent problems that no amount of code can clean up:

1. **P&L attribution becomes impossible per-bot** — every bot's
   `/v3/accounts/{id}/openTrades` enumerates the *other* bots' positions and
   has to filter them by instrument/allowlist. The 24-hour log dump
   (28-Apr-2026) showed FX-bot's restore loop logging a gold-priced trade
   (`@ 4593.41`) because the gold-bot opened it on the shared account.
2. **Margin and exposure caps interact in unintended ways** — every bot's
   `MAX_TOTAL_EXPOSURE` is computed from the account NAV, which includes
   margin reserved by sibling bots. A gold-bot drawdown silently shrinks
   FX-bot's available risk envelope.
3. **Capital-floor circuit-breaker collisions** — on a £200 account a £100
   sibling drawdown is a 50% balance shock that may unintentionally trigger
   or release the FX-bot's capital floor.

### Provisioning checklist (~30 minutes)

| Step | Where | Notes |
|---|---|---|
| 1. Create FX sub-account | OANDA web dashboard → My Accounts → Sub-account | Name it `fx-bot` (or similar). Keep parent funded; sub-accounts inherit the credential. |
| 2. Create Gold sub-account | same flow | Name it `gold-bot`. |
| 3. Fund each sub-account | OANDA dashboard → Transfer | Allocate the per-bot budget independently. Do **not** share. |
| 4. Capture `OANDA_ACCOUNT_ID` for each | OANDA dashboard → Manage Funds shows the `nnn-nnn-nnnnnnnn-nnn` id | One id per bot. |
| 5. Set `OANDA_ACCOUNT_ID` per Railway service | Railway → service → Variables | `FX-bot` service gets the FX id; `Gold-bot` service gets the gold id. |
| 6. Restart each Railway service | Railway → service → Deployments → Redeploy | Required for the env-var change to take effect; the new boot banner (P1 #8) prints the resolved `OANDA_ACCOUNT_ID`. |
| 7. Smoke-test | Telegram `/balance` (or equivalent) | Each bot now reports only its own NAV. |
| 8. Update `FX_BUDGET_ALLOCATION` if previously sharing | Railway env | With separate accounts, default to `1.00` (whole sub-account) per bot. |

### What stays shared

* **Redis** is still shared across all bots — the killswitch state, calibration
  blob, and shared-budget keys (`fxbot:*`, `goldbot:*`, `shared_budget_state`)
  live there. Each bot's keys are namespaced; collisions are not possible.
* **The OANDA login itself** — only the *account id* changes per bot.

---

## 2. Per-service Railway env-var contract

Variables that **must** differ per bot:

| Variable | FX-bot value | Gold-bot value | Notes |
|---|---|---|---|
| `OANDA_ACCOUNT_ID` | FX sub-account id | Gold sub-account id | from §1 step 4 |
| `REDIS_BOT_STATUS_KEY` | `fxbot_runtime_status` | `goldbot_runtime_status` | per-bot status blob |
| `REDIS_KILLSWITCH_STATE_KEY` | `fxbot:drawdown_state` | `goldbot:drawdown_state` | per-bot drawdown |
| `REDIS_PAIR_COOLDOWNS_KEY` | `fxbot:pair_cooldowns` | `goldbot:pair_cooldowns` | per-bot cooldowns (P1 #6) |
| `TELEGRAM_BOT_TOKEN` | FX bot Telegram token | Gold bot Telegram token | separate Telegram bots so notifications are filterable |

Variables that **should** match (shared budget coordination):

| Variable | Value | Notes |
|---|---|---|
| `SHARED_BUDGET_KEY` | `shared_budget_state` | both bots read/write |
| `FX_BUDGET_ALLOCATION` | `1.00` | FX-bot side |
| `GOLD_BUDGET_ALLOCATION` | `1.00` | Gold-bot side |
| `REDIS_URL` | shared Railway-Redis URL | one Redis instance |

---

## 3. P0/P1/P2 hygiene flags introduced in this round

| Flag | Default | When to enable |
|---|---|---|
| `MAX_RISK_AMOUNT_PER_TRADE` | `0` (disabled) | Set to a £-cap (e.g. `3.0` on a £200 sleeve) to enforce an absolute-£ ceiling regardless of `MAX_RISK_PER_TRADE * balance`. Required for small-account safety. |
| `MIN_LIVE_BALANCE` | `10000` | **Lower** to match your sleeve size (e.g. `75`) — otherwise the capital-floor circuit-breaker forces paper mode silently. |
| `EXIT_RETRY_ALERT_AFTER` | `3` | Telegram escalation after N consecutive close failures on a single trade. Lower for tighter monitoring; raise for noisier brokers. |
| `EXIT_RETRY_GIVE_UP_AFTER` | `10` | Trade marked `broker_unreachable`; retry loop halts. Operator must restart or clear pending-close state to resume. |
| `CALIBRATION_SEED_FILE` | `backtest_output/calibration_seed.json` | Path to a long-lived backtest snapshot that loads when the live calibration blob has < `CALIBRATION_MIN_TOTAL_TRADES` samples. Generate via the `backtest/` runner; commit the JSON if you want it to survive container rotations. |
| `SESSION_DST_AWARE` | `0` (off) | Flip to `1` after confirming the `*_OPEN_UTC` / `*_CLOSE_UTC` env-vars represent the intended **local** exchange hours (Asia/Tokyo, Europe/London, America/New_York). The current production values match standard winter-time UTC; flipping the flag without updating the values would slip session detection by an hour. |

---

## 4. Provisioning a new bot in this estate (template)

When adding a third bot (e.g. metals, energies, indices):

1. Create a new OANDA sub-account; capture its id.
2. Add a new Railway service from a fresh repo.
3. Copy the env-var contract from §2 and substitute new namespaced Redis
   keys (e.g. `metalsbot:drawdown_state`).
4. Funded sub-account, separate Telegram bot, separate Redis namespace.
5. Verify the boot banner (P1 #8) prints the correct account id and risk
   envelope on the first deploy.
6. Run for ≥30 days at minimum stake before scaling capital.

This contract is the cheapest insurance against the "shared-account leakage"
class of bugs that has cost the most operator time across the FX/Gold pair.
