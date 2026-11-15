NIFTY TRADING AGENT — CORRECTED SHARED MODULES

This ZIP contains ONLY the five shared modules requested. It intentionally does
NOT contain any of the four collectors, because the user already has corrected
collector files.

Files:
  collector_common.py
  instrument_master_store.py
  market_store.py
  ohlcv_stream.py
  shared_snapshots.py

Important fixes applied in this package:
1. SmartWebSocketV2 import is inside the reconnect loop's exception boundary,
   so an import/connection failure is logged and retried instead of silently
   terminating the daemon thread.
2. OHLCV shutdown flush persists a current bar only after its minute has ended.
   A still-open partial minute is discarded rather than mislabeled as complete.
3. OHLCV persistence calls are serialized to prevent concurrent callbacks from
   racing while submitting and removing the pending queue prefix.
4. Snapshot and OHLCV table initialization use separate PostgreSQL transaction
   advisory locks to serialize first-time setup across EC2 services.
5. market_store boolean conversion explicitly parses true/false strings and
   does not convert the string "false" to True.

Install/placement:
  Put these files under collectors/scripts/ in the existing project, preserving
  the package structure and __init__.py files already present in your project.

Dependency note:
  shared_snapshots.py imports psycopg 3 directly and uses psycopg.types.json.Jsonb.
  Install psycopg[binary]>=3.2. A psycopg2-only environment is NOT sufficient.
  Other project dependencies used by these modules (including pandas, requests,
  scipy, and SmartAPI) must also be installed as required by the application.

Safety:
  These are shared data/market modules, not an instruction to enable order
  submission. Keep paper/observation mode and order submission disabled while
  performing integration tests.

Validation performed in this environment:
  - Python compilation for all five modules.
  - Mock tests for partial/completed candle shutdown behavior.
  - Mock test that failed candle persistence retains the pending queue and can
    retry successfully.
  - Boolean parsing tests.
  - Static checks for schema advisory locks and retry-protected SmartAPI import.

Not performed:
  - Live Angel One authentication or WebSocket subscription.
  - Live EC2/PostgreSQL DDL, writes, read-back, or multi-service race test.
  - Full trading-session test or EC2 restart test.

Therefore this package has passed local/static and mocked checks, but it is not
certified crash-free in a live market until deployed and integration-tested.
