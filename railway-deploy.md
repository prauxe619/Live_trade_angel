# Railway deployment runbook — Nifty AI Trader

## Important status
This kit connects collector snapshots to PostgreSQL and patches the signal engine to read them. It includes a guarded Angel One adapter and an idempotent order-intent ledger. **It does not automatically turn strategy signals into broker orders.** Do not enable real order submission until the signal-to-contract mapping and exit lifecycle have been reviewed against your exact strategy objects and current Angel One account/API requirements.

## 1. Put files into your existing repository
Copy `trading_live/` into the repository root. Replace the engine with the patched `daily_signal_engine_v11.py` only after reviewing the diff. Replace each collector with the corresponding file in `collectors/`. Keep your existing `scripts/`, model/data assets, and collector dependencies.

## 2. Railway PostgreSQL
Create/attach a PostgreSQL database. Set `DATABASE_URL` on both collector and engine services to the same private connection URL. Set `SNAPSHOT_SOURCE=postgres`, `SNAPSHOT_MAX_AGE_SECONDS=60`, `TRADING_MODE=paper`, `TRADING_ENABLED=false`, `ALLOW_ORDER_SUBMISSION=false`.

## 3. Collector service
Use a separate Railway Worker service from the GitHub repo. The start command should start each collector as its own process, or use a supervisor that restarts each process. Example individual commands:
- `python collectors/nifty50_collector_v4.py`
- `python collectors/banknifty_collector_v1.py`
- `python collectors/finnifty_collector_v1.py`
- `python collectors/sensex_collector_v1.py`

These are independent workers if you create four services. If you use one service, create a proper process supervisor; do not chain them with `&&` because the first long-running collector prevents later collectors starting. Ensure the code's relative paths and `scripts` imports resolve from repository root. Check all collector-specific env names before deploying.

## 4. Engine service
Separate Railway Worker service, start command: `python daily_signal_engine_v11.py`. Add all existing bot/Telegram/model variables used by the engine plus shared `DATABASE_URL`. This service should not be configured with the collector start command.

## 5. Verify shared data before any broker action
Run `python -m trading_live.healthcheck`. Check Railway logs for PostgreSQL publish/read errors. Confirm all four snapshots become fresh during market hours. If the market is closed, missing/stale snapshots are expected. Test a paper scan and verify no real order API is called.

## 6. Angel One adapter checks
Use `ANGEL_API_KEY`, `ANGEL_CLIENT_ID`, `ANGEL_PASSWORD`, `ANGEL_TOTP` only after confirming the currently supported Angel One SmartAPI auth flow, account restrictions, allowed static IP, and current order API requirements. Never paste credentials into chat or commit `.env`. Download the official current instrument master through an approved Angel One source and verify its field names and expiry format; contract lookup deliberately fails if it cannot identify exactly one matching contract.

## 7. Live order release gate
Keep `TRADING_MODE=paper`, `TRADING_ENABLED=false`, `ALLOW_ORDER_SUBMISSION=false`. Before changing them, implement and test: (a) strategy signal -> exact contract/token/expiry/lot mapping; (b) durable order intent and broker order ID handling; (c) partial-fill reconciliation; (d) actual broker-position-based target/SL exits; (e) restart reconciliation; (f) duplicate prevention across multiple engine instances; (g) daily loss/P&L from actual fills; (h) emergency disable and square-off procedure. An API timeout must be treated as UNKNOWN and reconciled against order book before any retry.

## Railway caveats
Separate services do not share their local filesystem. PostgreSQL stores only the latest snapshot per symbol; historical candles and raw option-chain archives remain in each collector's own storage unless separately uploaded to durable storage. Do not assume Railway local files survive redeploys. Ensure the broker's currently applicable static IP/network requirements are satisfied before order placement.
