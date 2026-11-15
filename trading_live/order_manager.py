"""PostgreSQL order-intent ledger with atomic, fail-closed entry claims.

Claiming an ENTRY intent and persisting its SUBMITTING position happen in one
PostgreSQL transaction. A conflict never grants permission to call the broker.
Enqueueing/claiming never places a broker order.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .shared_store import _connect, init_schema


def stable_intent_id(signal_id: str, action: str) -> str:
    if not signal_id:
        raise ValueError("signal_id required")
    if not action:
        raise ValueError("action required")
    return hashlib.sha256(f"{signal_id}|{action}".encode()).hexdigest()


def claim_entry_intent(signal_id: str, payload: dict, position: dict) -> dict[str, Any]:
    """Atomically claim one ENTRY for a signal and persist its recovery row.

    Only the transaction that inserts BOTH the unique intent and position row
    receives ``claimed=True``. Existing intents or positions fail closed. The
    intent is committed as ``submitting`` BEFORE the caller invokes place_order;
    after a crash, the supervisor must reconcile the broker order tag and must
    never blindly submit the same intent again.
    """
    if not signal_id:
        raise ValueError("signal_id required")
    if not isinstance(payload, dict) or not isinstance(position, dict):
        raise TypeError("payload and position must be dictionaries")
    if str(position.get("signal_id")) != str(signal_id):
        raise ValueError("position.signal_id must match signal_id")

    init_schema()
    action = "ENTRY"
    intent_id = stable_intent_id(signal_id, action)
    intent_json = json.dumps(payload, allow_nan=False, default=str)
    position_copy = dict(position)
    position_copy["status"] = "SUBMITTING"
    position_json = json.dumps(position_copy, allow_nan=False, default=str)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO live_order_intents
                       (intent_id, signal_id, status, payload, action, attempt_key)
                   VALUES (%s, %s, 'submitting', %s::jsonb, 'ENTRY', 'ENTRY')
                   ON CONFLICT (intent_id) DO NOTHING
                   RETURNING intent_id""",
                (intent_id, str(signal_id), intent_json),
            )
            inserted = cur.fetchone()
            if not inserted:
                cur.execute(
                    """SELECT status, broker_order_id, error
                       FROM live_order_intents WHERE intent_id=%s""",
                    (intent_id,),
                )
                existing = cur.fetchone()
                conn.commit()
                if existing:
                    return {
                        "claimed": False,
                        "intent_id": intent_id,
                        "status": existing[0],
                        "broker_order_id": existing[1],
                        "error": existing[2],
                        "reason": "ENTRY_INTENT_ALREADY_EXISTS",
                    }
                return {
                    "claimed": False,
                    "intent_id": intent_id,
                    "status": "UNKNOWN",
                    "reason": "ENTRY_INTENT_CONFLICT_BUT_ROW_NOT_FOUND",
                }

            cur.execute(
                """INSERT INTO live_positions
                       (position_key, signal_id, status, payload, updated_at)
                   VALUES (%s, %s, 'SUBMITTING', %s::jsonb, NOW())
                   ON CONFLICT (position_key) DO NOTHING
                   RETURNING position_key""",
                (str(signal_id), str(signal_id), position_json),
            )
            position_inserted = cur.fetchone()
            if not position_inserted:
                # Roll back the newly inserted intent too. A pre-existing
                # position means this signal cannot acquire another entry.
                conn.rollback()
                return {
                    "claimed": False,
                    "intent_id": intent_id,
                    "status": "POSITION_EXISTS",
                    "reason": "LIVE_POSITION_ALREADY_EXISTS",
                }
        conn.commit()

    return {
        "claimed": True,
        "intent_id": intent_id,
        "status": "submitting",
        "broker_order_id": None,
        "reason": None,
    }



def claim_order_intent(signal_id: str, action: str, attempt_key: str, payload: dict) -> dict[str, Any]:
    """Atomically claim one unresolved STOP or EXIT per signal/action.

    PostgreSQL's partial unique index is the cross-process arbiter. A new
    attempt is permitted only after the prior intent reaches a terminal status.
    """
    if not signal_id or not action or not attempt_key:
        raise ValueError("signal_id, action and attempt_key are required")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")
    action = action.upper()
    if action not in {"STOP", "EXIT"}:
        raise ValueError("claim_order_intent only supports STOP and EXIT")
    init_schema()
    intent_id = hashlib.sha256(
        f"{signal_id}|{action}|{attempt_key}".encode()
    ).hexdigest()
    body = dict(payload)
    body.update({"action": action, "attempt_key": attempt_key})
    intent_json = json.dumps(body, allow_nan=False, default=str)
    unresolved = ("submitting", "unknown", "open", "partial")
    with _connect() as conn:
        with conn.cursor() as cur:
            # First return any unresolved prior attempt, even if the caller
            # generated a new attempt key. The unique index closes the race
            # between this read and competing inserts from other processes.
            cur.execute(
                """SELECT intent_id, status, broker_order_id, error
                   FROM live_order_intents
                   WHERE signal_id=%s AND action=%s
                     AND status IN ('submitting','unknown','open','partial')
                   ORDER BY created_at LIMIT 1""",
                (str(signal_id), action),
            )
            prior = cur.fetchone()
            if prior:
                conn.commit()
                return {"claimed": False, "intent_id": prior[0], "status": prior[1],
                        "broker_order_id": prior[2], "error": prior[3],
                        "reason": "UNRESOLVED_ACTION_INTENT_EXISTS"}
            cur.execute(
                """INSERT INTO live_order_intents
                       (intent_id, signal_id, status, payload, action, attempt_key)
                   VALUES (%s, %s, 'submitting', %s::jsonb, %s, %s)
                   ON CONFLICT DO NOTHING
                   RETURNING intent_id""",
                (intent_id, str(signal_id), intent_json, action, attempt_key),
            )
            inserted = cur.fetchone()
            if inserted:
                conn.commit()
                return {"claimed": True, "intent_id": intent_id, "status": "submitting"}
            # A concurrent process won the partial unique-index race or this
            # exact attempt already exists. Read the winning unresolved row.
            cur.execute(
                """SELECT intent_id, status, broker_order_id, error
                   FROM live_order_intents
                   WHERE signal_id=%s AND action=%s
                     AND status IN ('submitting','unknown','open','partial')
                   ORDER BY created_at LIMIT 1""",
                (str(signal_id), action),
            )
            existing = cur.fetchone()
            conn.commit()
    return {
        "claimed": False,
        "intent_id": existing[0] if existing else intent_id,
        "status": existing[1] if existing else "unknown",
        "broker_order_id": existing[2] if existing else None,
        "error": existing[3] if existing else None,
        "reason": "UNRESOLVED_ACTION_INTENT_EXISTS" if existing else "ORDER_INTENT_CONFLICT_UNRESOLVED",
    }


def enqueue_intent(signal_id: str, payload: dict, action: str = "ENTRY") -> str:
    """Create a non-submitting intent for legacy/non-entry callers.

    ENTRY submission code must use claim_entry_intent, never this helper.
    """
    if not signal_id:
        raise ValueError("signal_id required")
    init_schema()
    intent_id = stable_intent_id(signal_id, action)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO live_order_intents(intent_id,signal_id,status,payload,action,attempt_key)
                   VALUES(%s,%s,'queued',%s::jsonb,%s,'legacy')
                   ON CONFLICT(intent_id) DO NOTHING""",
                (intent_id, str(signal_id), json.dumps(payload, allow_nan=False, default=str), action.upper()),
            )
        conn.commit()
    return intent_id


def update_intent(
    intent_id: str,
    status: str,
    broker_order_id: str | None = None,
    error: str | None = None,
) -> None:
    allowed = {
        "queued", "validated", "submitting", "unknown", "open", "partial",
        "complete", "rejected", "cancelled", "closed",
    }
    if status not in allowed:
        raise ValueError("Invalid order status")
    init_schema()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE live_order_intents
                   SET status=%s,
                       broker_order_id=COALESCE(%s,broker_order_id),
                       error=%s,
                       updated_at=NOW()
                   WHERE intent_id=%s""",
                (status, broker_order_id, error, intent_id),
            )
            if cur.rowcount != 1:
                raise KeyError(f"Unknown intent_id {intent_id}")
        conn.commit()
