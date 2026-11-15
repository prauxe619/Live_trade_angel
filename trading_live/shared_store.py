"""PostgreSQL handoff for snapshots between separate Railway services."""
from __future__ import annotations
import json, os, time
from typing import Any
_SCHEMA_READY = False


def _connect():
    use_local = os.getenv("USE_LOCAL_DATABASE", "").strip().lower() in ("1", "true", "yes")
    url = os.getenv("LOCAL_DATABASE_URL" if use_local else "DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("Database URL is required for shared snapshots")
    try:
        import psycopg
        return psycopg.connect(url, connect_timeout=5)
    except ImportError:
        try:
            import psycopg2
            return psycopg2.connect(url, connect_timeout=5)
        except ImportError as exc:
            raise RuntimeError("Install psycopg[binary]>=3.2") from exc


def init_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY: return
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS market_snapshots (
                symbol TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                source_timestamp TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("""
                ALTER TABLE market_snapshots
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            """)
            cur.execute("""
                ALTER TABLE market_snapshots
                ADD COLUMN IF NOT EXISTS source_timestamp TEXT
            """)
            cur.execute("""CREATE TABLE IF NOT EXISTS live_order_intents (
                intent_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued', payload JSONB NOT NULL,
                broker_order_id TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), error TEXT,
                action TEXT, attempt_key TEXT
            )""")
            # Upgrade older installations without dropping or rewriting intent history.
            cur.execute("ALTER TABLE live_order_intents ADD COLUMN IF NOT EXISTS action TEXT")
            cur.execute("ALTER TABLE live_order_intents ADD COLUMN IF NOT EXISTS attempt_key TEXT")
            cur.execute("""UPDATE live_order_intents
                SET action = UPPER(COALESCE(payload->>'action', 'ENTRY')),
                    attempt_key = COALESCE(payload->>'attempt_key', 'legacy')
                WHERE action IS NULL OR attempt_key IS NULL""")
            # Fail visibly rather than silently choosing a winner if legacy data already
            # contains more than one unresolved STOP/EXIT for a position.
            cur.execute("""SELECT signal_id, action, COUNT(*)
                FROM live_order_intents
                WHERE action IN ('STOP','EXIT')
                  AND status IN ('submitting','unknown','open','partial')
                GROUP BY signal_id, action HAVING COUNT(*) > 1 LIMIT 10""")
            conflicts = cur.fetchall()
            if conflicts:
                raise RuntimeError(
                    'Cannot install live-order uniqueness guard: multiple unresolved '
                    f'STOP/EXIT intents already exist: {conflicts}. Reconcile these orders first.'
                )
            cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_live_order_one_unresolved_action
                ON live_order_intents(signal_id, action)
                WHERE action IN ('STOP','EXIT')
                  AND status IN ('submitting','unknown','open','partial')""")
            cur.execute("""CREATE TABLE IF NOT EXISTS live_positions (
                position_key TEXT PRIMARY KEY, signal_id TEXT NOT NULL,
                status TEXT NOT NULL, payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS trading_risk_state (
                state_key TEXT PRIMARY KEY, payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        conn.commit()
    _SCHEMA_READY = True


def publish_snapshot(symbol: str, payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict) or not payload:
        raise ValueError("snapshot must be a non-empty dict")
    init_schema()
    source_ts = str(payload.get("timestamp", ""))
    safe_payload = json.loads(json.dumps(payload, default=str, allow_nan=False))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO market_snapshots(symbol,payload,source_timestamp,updated_at)
                VALUES (%s,%s::jsonb,%s,NOW())
                ON CONFLICT(symbol) DO UPDATE SET payload=EXCLUDED.payload,
                source_timestamp=EXCLUDED.source_timestamp, updated_at=NOW()""",
                (symbol.upper(), json.dumps(safe_payload, allow_nan=False), source_ts))
        conn.commit()


def read_snapshot(symbol: str, max_age_s: int = 120) -> dict[str, Any]:
    init_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT payload, EXTRACT(EPOCH FROM (NOW()-updated_at)) FROM market_snapshots WHERE symbol=%s", (symbol.upper(),))
            row = cur.fetchone()
    if not row:
        return {}
    payload, age = row
    age_s = float(age or 0)
    if age_s > max_age_s:
        return {"_stale": True, "_age_s": age_s}
    if isinstance(payload, str):
        payload = json.loads(payload)
    result = dict(payload)
    result["_age_s"] = age_s
    result["_updated_at_epoch"] = time.time() - age_s
    return result
