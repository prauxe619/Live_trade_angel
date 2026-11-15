#!/usr/bin/env bash
set -euo pipefail
ENV_FILE="${ENV_FILE:-/etc/live-trade-angel.env}"
DUMP_FILE="${1:-}"
[[ -n "$DUMP_FILE" && -f "$DUMP_FILE" ]] || { echo "Usage: sudo bash scripts/ec2_restore_database.sh /path/to/railway.dump"; exit 1; }
[[ -r "$ENV_FILE" ]] || { echo "Cannot read $ENV_FILE"; exit 1; }
set -a; source "$ENV_FILE"; set +a
: "${DATABASE_URL:?DATABASE_URL is missing}"
echo "WARNING: this replaces objects in the TARGET EC2 database only."
read -r -p "Type RESTORE-EC2 to continue: " CONFIRM
[[ "$CONFIRM" == "RESTORE-EC2" ]] || { echo "Cancelled."; exit 1; }
systemctl stop live-trade-angel 2>/dev/null || true
BACKUP_DIR="/var/backups/live-trade-angel"
mkdir -p "$BACKUP_DIR"; chmod 700 "$BACKUP_DIR"
if pg_dump "$DATABASE_URL" --format=custom --no-owner --no-acl --file "$BACKUP_DIR/pre_restore_$(date -u +%Y%m%dT%H%M%SZ).dump"; then
  echo "Existing EC2 database backed up."
fi
pg_restore --dbname="$DATABASE_URL" --clean --if-exists --no-owner --no-acl --exit-on-error "$DUMP_FILE"
echo "Restore finished. Validate row counts and latest timestamps before starting service."
