"""Durable state for live positions (PostgreSQL `live_positions` table, created by shared_store).

One row per signal_id. `payload` holds the full position record (order ids, SL trigger, fills, P&L).
MemoryStore is used by tests and as an explicit opt-in; production uses PgStore.
"""
from __future__ import annotations
import json, threading, copy
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
ACTIVE = {"SUBMITTING", "UNKNOWN_ENTRY", "OPEN", "STOP_UNKNOWN", "EXITING", "EXIT_PENDING", "EXIT_BLOCKED"}


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


class MemoryStore:
    def __init__(self):
        self._rows: dict = {}
        self._flags: dict = {}
        self._lock = threading.Lock()

    def get(self, signal_id):
        with self._lock:
            r = self._rows.get(signal_id)
            return copy.deepcopy(r) if r else None

    def put(self, pos: dict):
        with self._lock:
            self._rows[pos["signal_id"]] = copy.deepcopy(pos)

    def list_all(self):
        with self._lock:
            return [copy.deepcopy(v) for v in self._rows.values()]

    def get_flag(self, key):
        return self._flags.get(key)

    def set_flag(self, key, value):
        self._flags[key] = value


class PgStore:
    def __init__(self):
        from .shared_store import _connect, init_schema
        self._connect = _connect
        init_schema()

    def get(self, signal_id):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM live_positions WHERE position_key=%s", (signal_id,))
            row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0]) if isinstance(row[0], str) else dict(row[0])

    def put(self, pos: dict):
        body = json.dumps(pos, default=str, allow_nan=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO live_positions(position_key,signal_id,status,payload,updated_at)
                       VALUES (%s,%s,%s,%s::jsonb,NOW())
                       ON CONFLICT(position_key) DO UPDATE SET status=EXCLUDED.status,
                       payload=EXCLUDED.payload, updated_at=NOW()""",
                    (pos["signal_id"], pos["signal_id"], pos["status"], body))
            conn.commit()

    def list_all(self):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("""SELECT payload FROM live_positions
                           WHERE status IN ('SUBMITTING','UNKNOWN_ENTRY','OPEN','STOP_UNKNOWN',
                                            'EXITING','EXIT_PENDING','EXIT_BLOCKED')
                              OR updated_at > NOW() - INTERVAL '3 days'""")
            rows = cur.fetchall()
        return [json.loads(r[0]) if isinstance(r[0], str) else dict(r[0]) for r in rows]

    def get_flag(self, key):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM trading_risk_state WHERE state_key=%s", (key,))
            row = cur.fetchone()
        if not row:
            return None
        p = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        return p.get("value")

    def set_flag(self, key, value):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO trading_risk_state(state_key,payload,updated_at) VALUES (%s,%s::jsonb,NOW())
                       ON CONFLICT(state_key) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()""",
                    (key, json.dumps({"value": value})))
            conn.commit()


# helpers that work on any store
def list_active(store):
    return [p for p in store.list_all() if p.get("status") in ACTIVE]


def realized_pnl_today(store) -> float:
    """Closed trades today. Unpriced closes count as their PLANNED loss (conservative for the daily cap)."""
    d, total = today_ist(), 0.0
    for p in store.list_all():
        if p.get("closed_date") != d:
            continue
        if p.get("status") == "CLOSED":
            total += float(p.get("pnl") or 0)
        elif p.get("status") == "CLOSED_UNPRICED":
            total -= float((p.get("planned") or {}).get("risk_rupees") or 0)
    return round(total, 2)
