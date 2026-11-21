"""PostgreSQL persistence for daily signal-engine recovery."""
from __future__ import annotations

import json
import math
from typing import Any

from .shared_store import _connect, init_schema

STATE_KEY = "daily_signal_engine_v11"

def _finite_positive(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{label} must be finite and positive")
    return number


def init_signal_state_schema() -> None:
    """Create an isolated table for paper/signal-engine state."""
    init_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS engine_signal_states (
                    signal_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL
                        CHECK (status IN ('open', 'closed')),
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_engine_signal_states_status
                ON engine_signal_states(status, updated_at)
            """)
        conn.commit()


def signal_to_payload(sig: Any) -> dict:
    """Capture mutable signal state needed for accurate recovery."""
    numeric_values = (
        float(sig.strike), float(sig.entry_premium), float(sig.sl_price),
        float(sig.tp_price), float(sig.trail_trigger), float(sig.tp_pts),
        float(sig.created_at), float(getattr(sig, "peak", getattr(sig, "entry_premium", 0.0))),
    )
    if not all(math.isfinite(value) for value in numeric_values):
        raise ValueError("signal state contains a non-finite numeric value")
    if any(value <= 0 for value in numeric_values[:5]) or numeric_values[5] <= 0:
        raise ValueError("signal state prices/strike/target points must be positive")
    return {
        "signal_id": str(sig.signal_id),
        "index_key": str(sig.index_key),
        "direction": str(sig.direction),
        "strike": int(sig.strike),
        "expiry": str(sig.expiry),
        "entry_premium": float(sig.entry_premium),
        "sl_price": float(sig.sl_price),
        "tp_price": float(sig.tp_price),
        "trail_trigger": float(sig.trail_trigger),
        "tp_pts": float(sig.tp_pts),
        "lot_size": int(sig.lot_size),
        "signal_type": str(sig.signal_type),
        "is_free_signal": bool(sig.is_free_signal),
        "trailed": bool(sig.trailed),
        "active": bool(sig.active),
        "created_at": float(sig.created_at),
        "targets": [
            {
                "name": str(t["name"]),
                "price": _finite_positive(t["price"], "target price"),
                "hit": bool(t["hit"]),
            }
            for t in sig.targets
        ],
                "moonbag": bool(getattr(sig, "moonbag", False)),
                "peak": float(getattr(sig, "peak", getattr(sig, "entry_premium", 0.0))),
                "moonbag_last_sl_trigger_premium": _finite_positive(
                    getattr(
                        sig,
                        "moonbag_last_sl_trigger_premium",
                        getattr(sig, "peak", sig.entry_premium),
                    ),
                    "moonbag_last_sl_trigger_premium",
                ),

                "moonbag_atr": (
                    None
                    if getattr(sig, "moonbag_atr", None) is None
                    else _finite_positive(
                        getattr(sig, "moonbag_atr"), "moonbag_atr"
                    )
                ),

                "moonbag_swing_low": (
                    None
                    if getattr(sig, "moonbag_swing_low", None) is None
                    else _finite_positive(
                        getattr(sig, "moonbag_swing_low"), "moonbag_swing_low"
                    )
                ),

                "moonbag_last_candle_ts": (
                    getattr(sig, "moonbag_last_candle_ts").isoformat()
                    if hasattr(getattr(sig, "moonbag_last_candle_ts", None), "isoformat")
                    else (
                        str(getattr(sig, "moonbag_last_candle_ts"))
                        if getattr(sig, "moonbag_last_candle_ts", None) is not None
                        else None
                    )
                ),
           }


def save_signal_state(sig: Any) -> None:
    """Insert/update one signal's full mutable state."""
    payload = signal_to_payload(sig)
    body = json.dumps(payload, allow_nan=False)
    init_signal_state_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO engine_signal_states
                    (signal_id, status, payload, updated_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (signal_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
            """, (
                payload["signal_id"],
                "open" if payload["active"] else "closed",
                body,
            ))
        conn.commit()


def list_open_signal_states() -> list[dict]:
    """Return saved open signals; database failures deliberately propagate."""
    init_signal_state_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT payload
                FROM engine_signal_states
                WHERE status = 'open'
                ORDER BY created_at, signal_id
            """)
            rows = cur.fetchall()

    result = []
    for (payload,) in rows:
        if isinstance(payload, str):
            payload = json.loads(payload)
        result.append(dict(payload))
    return result



def close_signal_state(signal_id: str, exit_data: dict | None = None) -> None:
    """Close a persisted signal; fail if the signal row does not exist."""
    init_signal_state_schema()
    body = json.dumps(
        {"active": False, **(exit_data or {})},
        allow_nan=False,
    )
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE engine_signal_states
                SET status = 'closed',
                    payload = payload || %s::jsonb,
                    updated_at = NOW()
                WHERE signal_id = %s AND status = 'open'
                RETURNING signal_id
            """, (body, str(signal_id)))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(
                    f"Cannot close PostgreSQL signal {signal_id!r}: "
                    "no matching open row"
                )
        conn.commit()


def save_engine_state(state: dict) -> None:
    """Save counters under this engine's dedicated risk-state key."""
    body = json.dumps(state, allow_nan=False)
    init_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trading_risk_state(state_key, payload, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (state_key) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
            """, (STATE_KEY, body))
        conn.commit()


def load_engine_state() -> dict | None:
    """Load this engine's counters; connection errors propagate."""
    init_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT payload FROM trading_risk_state
                WHERE state_key = %s
            """, (STATE_KEY,))
            row = cur.fetchone()

    if not row:
        return None
    payload = row[0]
    return json.loads(payload) if isinstance(payload, str) else dict(payload)


def close_signal_and_paper_trade(signal_id: str, exit_data: dict, exit_reason: str) -> dict:
    """Atomically close persisted engine state and its paper-ledger row in PostgreSQL."""
    import math
    from .paper_ledger import init_paper_schema

    sid = str(signal_id).strip()
    reason = str(exit_reason).strip()
    try:
        exit_price = float(exit_data["exit_price"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("exit_data.exit_price must be numeric") from exc
    if not sid or not reason or not math.isfinite(exit_price) or exit_price <= 0:
        raise ValueError("signal_id, reason and finite positive exit_price are required")

    body = json.dumps({"active": False, **exit_data}, allow_nan=False)
    init_paper_schema()
    init_signal_state_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE engine_signal_states
                SET status='closed', payload=payload || %s::jsonb, updated_at=NOW()
                WHERE signal_id=%s AND status='open'
                RETURNING signal_id
            """, (body, sid))
            if cur.fetchone() is None:
                raise RuntimeError(f"No open persisted signal state for {sid}")

            cur.execute("""
                UPDATE paper_trades
                SET status='closed', exit_price=%s,
                    pnl_rupees=ROUND(((%s-entry_price)*quantity)::numeric,2),
                    exit_reason=%s, closed_at=NOW()
                WHERE signal_id=%s AND status='open'
                RETURNING entry_price, quantity, pnl_rupees, closed_at
            """, (exit_price, exit_price, reason, sid))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"No open paper-ledger trade for {sid}; transaction rolled back")
            entry, quantity, pnl, closed_at = row
            event = {
                "signal_id": sid, "entry_price": float(entry),
                "exit_price": exit_price, "quantity": int(quantity),
                "pnl_rupees": float(pnl), "exit_reason": reason,
                "closed_at": closed_at.isoformat() if closed_at else None,
            }
            cur.execute("""
                INSERT INTO paper_trade_events(signal_id,event_type,event_key,payload)
                VALUES (%s,'EXIT',%s,%s::jsonb)
                ON CONFLICT (signal_id,event_key) DO NOTHING
            """, (sid, f"EXIT:{sid}", json.dumps(event, allow_nan=False)))
        conn.commit()
    return event


def create_signal_and_paper_trade(sig: Any, *, quantity: int, metadata: dict | None = None) -> bool:
    """Atomically create the persisted signal state and its paper-ledger entry."""
    import math
    from .paper_ledger import init_paper_schema

    payload = signal_to_payload(sig)
    body = json.dumps(payload, allow_nan=False)
    try:
        qty_value = float(quantity)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("quantity must be a whole positive number") from exc
    if not math.isfinite(qty_value) or qty_value <= 0 or not qty_value.is_integer():
        raise ValueError("quantity must be a whole positive number")
    qty = int(qty_value)
    safe_metadata = json.dumps(metadata or {}, allow_nan=False, default=str)
    event = json.dumps({
        "entry_price": float(payload["entry_premium"]), "quantity": qty,
        "index": payload["index_key"], "direction": payload["direction"],
        "strike": float(payload["strike"]), "expiry": payload["expiry"],
    }, allow_nan=False)

    init_paper_schema()
    init_signal_state_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO paper_trades
                    (signal_id,index_key,direction,strike,expiry,entry_price,quantity,status,metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'open',%s::jsonb)
                ON CONFLICT (signal_id) DO NOTHING RETURNING signal_id
            """, (payload["signal_id"], payload["index_key"], payload["direction"],
                  float(payload["strike"]), payload["expiry"], float(payload["entry_premium"]),
                  qty, safe_metadata))
            if cur.fetchone() is None:
                raise RuntimeError(f"Paper-ledger signal already exists: {payload['signal_id']}")
            cur.execute("""
                INSERT INTO paper_trade_events(signal_id,event_type,event_key,payload)
                VALUES (%s,'ENTRY',%s,%s::jsonb)
                ON CONFLICT (signal_id,event_key) DO NOTHING
            """, (payload["signal_id"], f"ENTRY:{payload['signal_id']}", event))
            cur.execute("""
                INSERT INTO engine_signal_states(signal_id,status,payload,updated_at)
                VALUES (%s,'open',%s::jsonb,NOW())
                ON CONFLICT (signal_id) DO NOTHING RETURNING signal_id
            """, (payload["signal_id"], body))
            if cur.fetchone() is None:
                raise RuntimeError(f"Persisted signal state already exists: {payload['signal_id']}")
        conn.commit()
    return True
