# EC2 Trading Agent Patch

This ZIP contains repo-relative replacement files for `prauxe619/Live_trade_angel`.
It is based on the latest EC2 source uploads supplied in this conversation. Some of
those files are newer than the current GitHub `main` copies; use this ZIP as a
coherent patch set and do not mix it with older collector/OHLCV copies.

## Files in this patch

- `daily_signal_engine_v11.py`
- `collectors/nifty50_collector_v4.py`
- `collectors/banknifty_collector_v1.py`
- `collectors/finnifty_collector_v1.py`
- `collectors/sensex_collector_v1.py`
- `collectors/scripts/ohlcv_stream.py`
- `collectors/scripts/shared_snapshots.py`
- `collectors/scripts/collector_common.py` (included as the exact dependency baseline; no logic change)
- `collectors/validate_daily_data.py`
- `tests/test_ec2_pipeline_regressions.py`

No `.env`, broker credentials, Telegram credentials, database URLs, or local databases
are included. No GitHub push or EC2 deployment was performed.

## Corrections made

1. Moved `SignalDataFetcher.get_option_premium()` into the class. This fixes the
   production `AttributeError` that was blocking scheduled SNIPER/LIVE_SNIPER,
   ZERO_HERO/JACKPOT, swing premium lookups, and close-price lookups.
2. Fixed the missing `_ic_fired_today` initialization and persisted its once-per-day
   state. This fixes the `DailySignalEngine` `AttributeError` seen in the logs.
3. Kept the `scan_1400` schedule key in the replacement engine. The earlier EC2
   version in the logs raised `KeyError: 'scan_1400'`.
4. Added futures SNAP_QUOTE volume subscriptions for BANKNIFTY, FINNIFTY, and SENSEX.
   NIFTY's existing futures subscription is retained. Exchange types are NFO=2 and
   BFO=4.
5. Refreshes the running WebSocket's JWT/feed credentials after the 09:14 collector
   login, so it reconnects using the refreshed credentials.
6. Corrected FINNIFTY's token-refresh script invocation to use `--account C`.
7. Added a separate PostgreSQL reader for today's active futures contract and joined
   its real volume to spot OHLC by exact minute. Spot index volume remains NULL;
   futures volume is never written into the spot candle volume column.
8. Strong-breakout confirmation now consumes `futures_volume`, not synthetic or
   missing spot volume. It uses completed-candle metadata when available, avoiding
   an unnecessary one-candle delay for PostgreSQL's completed-only bars.
9. Prevented cumulative futures volume accumulated during a long WebSocket reconnect
   gap from being incorrectly assigned to one minute.
10. Prefer PostgreSQL candles in PostgreSQL snapshot mode. Angel historical-candle
    API fallback is disabled by default to avoid rate-limit retries and to prevent
    signal generation from API-only data with default PCR/GEX values. It can be
    explicitly enabled with `ALLOW_ANGEL_CANDLE_API_FALLBACK=true`; this is not
    recommended for normal EC2 operation.
11. Ollama alert-copy generation is disabled by default on EC2. The engine uses its
    built-in message fallbacks. Enable with `OLLAMA_ENABLED=true` and set
    `OLLAMA_BASE_URL` only if Ollama is actually running and reachable.
12. Replaced the old parquet-only daily validator with PostgreSQL validation for
    snapshots, spot candles, futures volume, option rows, and spot-volume integrity.

## Keep paper-only safeguards

Keep these settings in the EC2 `.env`:

```env
TRADE_LOTS=1
TRADING_MODE=paper
PAPER_TRADING=1
TRADING_ENABLED=false
ALLOW_ORDER_SUBMISSION=false
LIVE_SHADOW=true
PAPER_LEDGER_ENABLED=true
```

The patch does not enable broker order submission and does not change broker order
logic, risk limits, or the live bridge.

## EC2 replacement and verification

From the repository root, back up the current files first, then extract this ZIP
into the project root so the folder structure is preserved. Do not overwrite `.env`.

Recommended verification:

```bash
python -m py_compile daily_signal_engine_v11.py \
  collectors/nifty50_collector_v4.py \
  collectors/banknifty_collector_v1.py \
  collectors/finnifty_collector_v1.py \
  collectors/sensex_collector_v1.py \
  collectors/scripts/ohlcv_stream.py \
  collectors/scripts/shared_snapshots.py \
  collectors/validate_daily_data.py

python -m unittest discover -s tests -v
python -m collectors.validate_daily_data
```

The PostgreSQL validator creates the futures candle table idempotently. It requires
the existing EC2 `DATABASE_URL` and the dependencies in `requirements.txt`.

After copying and verifying the files, restart the existing EC2 systemd service
during a controlled maintenance window. This ZIP does not restart services.

## EC2 log directory permissions

The supplied EC2 logs also showed an early `Permission denied` for
`/opt/live-trade-angel/app/logs/2026-11-16/app.log`. That is an EC2 filesystem
ownership issue, not a Python syntax issue. Before restarting, ensure the service
user owns the writable log/validation directories. For an `ubuntu` service user:

```bash
sudo install -d -o ubuntu -g ubuntu /opt/live-trade-angel/app/logs
sudo install -d -o ubuntu -g ubuntu /opt/live-trade-angel/app/data/validation
```

Do not recursively change ownership of the whole application or database storage
without checking the service user and mount permissions first.

## Strategy behavior intentionally preserved

This patch fixes the data/engine defects; it does not loosen strategy thresholds or
reactivate strategies explicitly disabled in the current engine. In particular,
standard scheduled SNIPER remains disabled by the existing explicit user setting.
ZERO_HERO/JACKPOT remain conditional classifications, LIVE_SNIPER retains its
existing logic, and STRONG_BREAKOUT retains its current ATR/body/volume/retest
thresholds. A `volatility_too_low` rejection remains a valid no-trade outcome.
