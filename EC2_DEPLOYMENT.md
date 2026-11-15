# EC2 deployment — Railway-free architecture

This checkout is based on the uploaded repository at commit `9e3eb83ccd4cdff985a0f5c1baa8979243be08a8`.
The runtime source is retained; Railway-specific deployment instructions are replaced with this EC2 runbook.

## Target layout

- One EC2 instance runs PostgreSQL locally, all four index collectors, `daily_signal_engine_v11.py`, and the supervisor.
- `supervisor.py` remains the single process supervisor and launches the four collectors plus signal engine.
- PostgreSQL listens locally. Do not expose port 5432 to the internet.
- No Railway service is required after the cutover is validated.
- The systemd service is installed/enabled by the installer, but is deliberately **not started** by it.

## Important safety defaults

The generated environment uses paper mode:
`TRADING_MODE=paper`, `TRADING_ENABLED=false`, `ALLOW_ORDER_SUBMISSION=false`,
`LIVE_SHADOW=true`, `PAPER_LEDGER_ENABLED=true`.

Do not change these during migration. Do not enable live order submission until a separate
execution audit has passed, including partial fills, broker-position reconciliation,
restart recovery, duplicate-order prevention, actual-fill daily-loss controls, and emergency
disable/square-off testing.

## 1. Prepare the EC2 checkout

The expected path is `/opt/live-trade-angel/app` and the service user is `ubuntu`.

On EC2:

```bash
sudo mkdir -p /opt/live-trade-angel
sudo chown ubuntu:ubuntu /opt/live-trade-angel
cd /opt/live-trade-angel
git clone https://github.com/prauxe619/Live_trade_angel.git app
cd app
git checkout main
git pull --ff-only
```

If the repository is already present, do not clone over it. Back it up first, then update it
with `git pull --ff-only`. Check that this updated ZIP is the version you commit and push to
GitHub before pulling it on EC2.

Upload this ZIP's contents to GitHub (excluding `.env`, `.git`, logs, caches and runtime state).
Never commit credentials.

## 2. Install PostgreSQL and the Python runtime

From the repository root on EC2:

```bash
sudo bash scripts/ec2_install.sh
```

The installer:
- installs PostgreSQL and Python build dependencies;
- creates/updates a local PostgreSQL role and database;
- creates `.venv` and installs `requirements.txt`;
- writes `/etc/live-trade-angel.env` with a local loopback `DATABASE_URL`;
- installs/enables `live-trade-angel.service`.

It does **not** start the service. The generated environment file is root-readable only.
The installer generates a new DB password each time it is run, so avoid rerunning it after
the database has been populated unless you intend to rotate the password and update the
environment in a controlled manner.

## 3. Export the Railway PostgreSQL database

Do this while Railway PostgreSQL is still running. Use the Railway PostgreSQL **public/external
connection URL** from its dashboard, not an internal hostname that your Windows PC cannot reach.
Do not paste the URL into chat or commit it.

On Windows PowerShell, after installing PostgreSQL client tools and making `pg_dump` available:

```powershell
$env:PGPASSWORD = "<RAILWAY_DATABASE_PASSWORD>"
pg_dump --format=custom --no-owner --no-acl --verbose --file ".\railway_live_trade.dump" "<RAILWAY_EXTERNAL_DATABASE_URL>"
Remove-Item Env:PGPASSWORD
```

If your provider URL already includes the password, you do not need `PGPASSWORD`; protect the
terminal history and do not share the URL. Verify that the dump file exists and is non-empty.

Copy the dump to EC2:

```powershell
scp -i "C:\Users\pc\Documents\Website\Angel_one_live_trader_agent\Algo-master-trader-key.pem" ".\railway_live_trade.dump" ubuntu@15.207.255.58:/home/ubuntu/railway_live_trade.dump
```

## 4. Restore into EC2 PostgreSQL

On EC2:

```bash
cd /opt/live-trade-angel/app
sudo bash scripts/ec2_restore_database.sh /home/ubuntu/railway_live_trade.dump
```

The restore script stops the service, asks for the exact confirmation `RESTORE-EC2`,
backs up the current EC2 database if one exists, and restores into the **EC2 target database**.
It does not modify or delete Railway data.

After restore, check that expected tables and recent rows exist:

```bash
sudo -u postgres psql -d live_trade -c "\dt"
sudo -u postgres psql -d live_trade -c "SELECT symbol, updated_at FROM market_snapshots ORDER BY symbol;"
sudo -u postgres psql -d live_trade -c "SELECT symbol, MAX(timestamp) AS latest_candle, COUNT(*) AS rows FROM market_ohlcv_candles GROUP BY symbol ORDER BY symbol;"
```

Some tables may not exist in an older dump; investigate rather than assuming a successful migration.
Compare row counts and latest timestamps on Railway and EC2 for the important tables before cutover.

## 5. Add broker and notification credentials

Edit the protected environment file:

```bash
sudo nano /etc/live-trade-angel.env
sudo chmod 600 /etc/live-trade-angel.env
```

Copy the current long-lived credentials from your private local environment file, not from GitHub.
The uploaded archive contained a `.env`; it has intentionally been excluded from this ZIP because
it contains secrets. Treat any credentials previously committed or shared in a ZIP as exposed
and rotate them.

The uploaded `.env` used these variable names (names only; no values are included here):

```text
ANGEL_API_KEY
ANGEL_CLIENT_ID
ANGEL_PASSWORD
ANGEL_TOTP
ANGEL_API_KEY_B
ANGEL_CLIENT_ID_B
ANGEL_PASSWORD_B
ANGEL_TOTP_B
ANGEL_API_KEY_C
ANGEL_CLIENT_ID_C
ANGEL_PASSWORD_C
ANGEL_TOTP_C
ANGEL_API_KEY_D
ANGEL_CLIENT_ID_D
ANGEL_PASSWORD_D
ANGEL_TOTP_D
BOT_TOKEN
CHAT_ID
VIP_BOT_TOKEN
SUPPORT_BOT
LIVE_BOT_CHANNEL_ID
LIVE_BOT_TOKEN
SIGNAL_BOT_TOKEN
TRADING_TELEGRAM_CHAT_ID
GEMINI_API_KEY
```

Only add variables actually used by your chosen runtime configuration. Do not copy stale session
values such as `ANGEL_JWT_TOKEN`, `ANGEL_FEED_TOKEN`, or `TOKEN_NIFTY`; allow the token-refresh
flow to obtain fresh sessions. Keep the generated local `DATABASE_URL`. Do not set
`LOCAL_DATABASE_URL`/`USE_LOCAL_DATABASE=true` for production.

Keep these migration settings in the file:

```dotenv
MARKET_TIMEZONE=Asia/Kolkata
SNAPSHOT_SOURCE=postgres
SNAPSHOT_MAX_AGE_SECONDS=60
OPTION_RETENTION_DAYS=0
TRADING_MODE=paper
TRADING_ENABLED=false
ALLOW_ORDER_SUBMISSION=false
LIVE_SHADOW=true
PAPER_LEDGER_ENABLED=true
DISABLE_SYSTEM_SHUTDOWN=true
```

`OPTION_RETENTION_DAYS=0` disables automatic history deletion in the collector's retention
routine. Monitor disk usage and arrange EBS expansion/backups; keeping all option ticks forever
can grow storage quickly.

## 6. Preflight and controlled start

Before starting, verify:
- PostgreSQL is active and the restored tables/data are present.
- `/etc/live-trade-angel.env` contains the local EC2 `DATABASE_URL` and required credentials.
- no Railway database URL remains in the EC2 environment;
- the four collectors and engine use `Asia/Kolkata`;
- all four latest snapshots and genuine OHLCV candles pass the existing health checks;
- paper-only order safeguards remain false/disabled.

Then:

```bash
sudo systemctl start live-trade-angel
sudo systemctl status live-trade-angel --no-pager
sudo journalctl -u live-trade-angel -f
```

The supervisor schedules around the Indian market session. Outside the session, a healthy service
may be active without launching the market processes immediately.

Useful operations:

```bash
sudo systemctl stop live-trade-angel
sudo systemctl restart live-trade-angel
sudo journalctl -u live-trade-angel --since today --no-pager
sudo systemctl status postgresql --no-pager
```

## 7. Backups

Create a local PostgreSQL backup:

```bash
sudo bash scripts/ec2_backup_database.sh
```

Backups are stored in `/var/backups/live-trade-angel` with 14-day local retention.
Copy backups off the EC2 instance regularly (for example, to a private S3 bucket). An EC2-only
backup does not protect against instance or volume loss.

## 8. Cutover and Railway shutdown gate

Do **not** delete Railway or stop its database until all checks below pass:

1. A fresh Railway database dump restores without errors.
2. Table counts and latest timestamps match for key tables (option ticks, index candles,
   option OHLCV candles, market OHLCV candles, snapshots, instrument master, paper trades,
   signal/position/risk state, and order intents).
3. EC2 collectors successfully authenticate and publish all four fresh snapshots.
4. Genuine one-minute OHLCV bars are being persisted to EC2 PostgreSQL.
5. Signal engine reads EC2 snapshots/candles and paper ledger/state survives a service restart.
6. Telegram/health alerts work.
7. No process on EC2 connects to Railway PostgreSQL.
8. At least one full market session has been observed in paper/shadow mode and logs reviewed.

Only after this gate should Railway workers and PostgreSQL be stopped. Keep a final Railway dump
and a separate off-instance EC2 backup before deleting anything.

## 9. Scope and known boundaries

This ZIP includes the application source and deployment overlay, not a live Railway database
export, EC2 secrets, AWS key, or EC2 filesystem. PostgreSQL contents must be exported/restored
separately. Any files stored only on Railway service filesystems (models, caches, logs, parquet
archives) must be identified and copied separately; a PostgreSQL dump does not include them.

The included historical parquet files and local paper-ledger SQLite file are retained from the
uploaded source archive. They are not a substitute for the Railway PostgreSQL migration.
