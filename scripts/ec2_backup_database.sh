#!/usr/bin/env bash
set -euo pipefail
ENV_FILE="${ENV_FILE:-/etc/live-trade-angel.env}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/live-trade-angel}"
[[ -r "$ENV_FILE" ]] || { echo "Cannot read $ENV_FILE"; exit 1; }
set -a; source "$ENV_FILE"; set +a
: "${DATABASE_URL:?DATABASE_URL is missing}"
mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
pg_dump "$DATABASE_URL" --format=custom --no-owner --no-acl --file "$BACKUP_DIR/live_trade_${STAMP}.dump"
find "$BACKUP_DIR" -type f -name 'live_trade_*.dump' -mtime +14 -delete
echo "Backup created: $BACKUP_DIR/live_trade_${STAMP}.dump"
