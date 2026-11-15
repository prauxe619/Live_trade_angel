"""PostgreSQL-backed paper-trade ledger. Never submits broker orders."""
from __future__ import annotations
import json
import math
from typing import Any
from .shared_store import _connect, init_schema


def init_paper_schema() -> None:
    """Create dedicated paper-ledger tables; leave existing tables untouched."""
    init_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    signal_id TEXT PRIMARY KEY,
                    index_key TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    strike NUMERIC NOT NULL CHECK (strike > 0),
                    expiry TEXT NOT NULL DEFAULT '',
                    entry_price NUMERIC NOT NULL CHECK (entry_price > 0),
                    quantity INTEGER NOT NULL CHECK (quantity > 0),
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'closed')),
                    exit_price NUMERIC,
                    pnl_rupees NUMERIC,
                    exit_reason TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    closed_at TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paper_trade_events (
                    event_id BIGSERIAL PRIMARY KEY,
                    signal_id TEXT NOT NULL REFERENCES paper_trades(signal_id),
                    event_type TEXT NOT NULL CHECK (event_type IN ('ENTRY', 'EXIT')),
                    event_key TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (signal_id, event_key)
                )
            """)
        conn.commit()


def create_paper_trade(signal: dict[str, Any], *, quantity: int,
                       entry_price: float, metadata: dict[str, Any] | None = None) -> bool:
    """Create one simulated long-option trade; False means signal ID already exists."""
    if not isinstance(signal, dict):
        raise TypeError("signal must be a dict")
    signal_id = str(signal.get("signal_id", "")).strip()
    index_key = str(signal.get("index", signal.get("index_key", ""))).strip().upper()
    direction = str(signal.get("direction", "")).strip().upper()
    expiry = str(signal.get("expiry", "") or "")
    try:
        strike, entry = float(signal["strike"]), float(entry_price)
        qty_raw = float(quantity)
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("signal strike, quantity and entry_price must be valid numbers") from exc
    if not all(math.isfinite(v) for v in (strike, entry, qty_raw)):
        raise ValueError("strike, quantity and entry_price must be finite")
    if not qty_raw.is_integer():
        raise ValueError("quantity must be a whole number; fractional quantities are not supported")
    qty = int(qty_raw)
    if not signal_id or not index_key:
        raise ValueError("signal_id and index/index_key are required")
    if direction not in {"CE", "PE"}:
        raise ValueError("direction must be CE or PE")
    if strike <= 0 or entry <= 0 or qty <= 0:
        raise ValueError("strike, entry_price and quantity must be positive")

    safe_metadata = json.dumps(metadata or {}, allow_nan=False, default=str)
    event_payload = json.dumps({
        "entry_price": entry, "quantity": qty, "index": index_key,
        "direction": direction, "strike": strike, "expiry": expiry
    }, allow_nan=False)

    init_paper_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO paper_trades
                    (signal_id,index_key,direction,strike,expiry,entry_price,quantity,status,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'open',%s::jsonb)
                ON CONFLICT (signal_id) DO NOTHING RETURNING signal_id
            """, (signal_id,index_key,direction,strike,expiry,entry,qty,safe_metadata))
            if not cur.fetchone():
                return False
            cur.execute("""
                INSERT INTO paper_trade_events(signal_id,event_type,event_key,payload)
                VALUES (%s,'ENTRY',%s,%s::jsonb) ON CONFLICT (signal_id,event_key) DO NOTHING
            """, (signal_id, f"ENTRY:{signal_id}", event_payload))
        conn.commit()
    return True


def close_paper_trade(signal_id: str, *, exit_price: float, exit_reason: str) -> dict[str, Any] | None:
    """Close an open simulated trade once; None means missing/already closed."""
    sid, reason = str(signal_id).strip(), str(exit_reason).strip()
    try:
        exit_value = float(exit_price)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("exit_price must be numeric") from exc
    if not math.isfinite(exit_value):
        raise ValueError("exit_price must be finite")
    if not sid or not reason or exit_value <= 0:
        raise ValueError("signal_id, positive exit_price and exit_reason are required")
    init_paper_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE paper_trades
                SET status='closed', exit_price=%s,
                    pnl_rupees=ROUND(((%s-entry_price)*quantity)::numeric,2),
                    exit_reason=%s, closed_at=NOW()
                WHERE signal_id=%s AND status='open'
                RETURNING entry_price,quantity,pnl_rupees,closed_at
            """, (exit_value,exit_value,reason,sid))
            row = cur.fetchone()
            if not row:
                return None
            entry, qty, pnl, closed_at = row
            result = {"signal_id":sid, "entry_price":float(entry),
                      "exit_price":exit_value, "quantity":int(qty),
                      "pnl_rupees":float(pnl), "exit_reason":reason,
                      "closed_at":closed_at.isoformat() if closed_at else None}
            cur.execute("""
                INSERT INTO paper_trade_events(signal_id,event_type,event_key,payload)
                VALUES (%s,'EXIT',%s,%s::jsonb) ON CONFLICT (signal_id,event_key) DO NOTHING
            """, (sid, f"EXIT:{sid}", json.dumps(result,allow_nan=False)))
        conn.commit()
    return result


def list_open_paper_trades() -> list[dict[str, Any]]:
    """Read open trades for recovery/reporting; places no orders."""
    init_paper_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signal_id,index_key,direction,strike,expiry,entry_price,
                       quantity,status,metadata,opened_at
                FROM paper_trades WHERE status='open' ORDER BY opened_at,signal_id
            """)
            names = [col.name for col in cur.description]
            rows = cur.fetchall()
    result = []
    for row in rows:
        item = dict(zip(names,row))
        item["strike"], item["entry_price"] = float(item["strike"]), float(item["entry_price"])
        item["quantity"] = int(item["quantity"])
        if item.get("opened_at"):
            item["opened_at"] = item["opened_at"].isoformat()
        result.append(item)
    return result
