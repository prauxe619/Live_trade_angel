"""Shared PostgreSQL storage for market snapshots and genuine OHLCV candles.

Environment:
    DATABASE_URL
    USE_LOCAL_DATABASE=true
    LOCAL_DATABASE_URL

Snapshot API:
    publish_snapshot(symbol, snapshot)
    get_snapshot(symbol, max_age_seconds=None)

OHLCV API:
    publish_candles(symbol, candles, strict=True)
    get_recent_candles(symbol, limit=120, max_age_seconds=None)
    get_latest_candle(symbol, max_age_seconds=None)

Only genuine OHLCV bars should be passed to publish_candles().
Do not create synthetic OHLC from individual spot-price samples.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

_SNAPSHOT_TABLE_READY = False
_CANDLE_TABLE_READY = False

# Separate transaction-scoped locks prevent concurrent Railway services from
# racing while creating the two shared tables.
_SNAPSHOT_SCHEMA_LOCK = 727101
_CANDLE_SCHEMA_LOCK = 727102


def _connect():
    """Open a short-lived PostgreSQL connection."""
    use_local = os.getenv("USE_LOCAL_DATABASE", "").strip().lower() in {
        "1", "true", "yes"
    }
    env_name = "LOCAL_DATABASE_URL" if use_local else "DATABASE_URL"
    database_url = os.getenv(env_name, "").strip()

    if not database_url:
        raise RuntimeError(f"{env_name} is not configured")

    return psycopg.connect(database_url, connect_timeout=5)


def _json_safe(value: Any) -> Any:
    """Convert datetimes, NumPy values and non-finite floats to JSON-safe data."""
    if value is None or isinstance(value, (str, bool, int)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (ValueError, TypeError):
            pass

    if hasattr(value, "tolist"):
        try:
            return _json_safe(value.tolist())
        except (ValueError, TypeError):
            pass

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable"
    )


def _normalise_symbol(symbol: str) -> str:
    value = str(symbol).strip().upper()
    if not value:
        raise ValueError("symbol must not be empty")
    return value


def _as_datetime(value: Any) -> datetime:
    """Parse a timestamp and return a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, (int, float)) and math.isfinite(float(value)):
        result = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError("timestamp must not be empty")
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        result = datetime.fromisoformat(raw)
    else:
        raise ValueError(f"Unsupported timestamp value: {value!r}")

    # Naive project timestamps are interpreted as IST.
    if result.tzinfo is None:
        result = result.replace(tzinfo=IST)

    return result.astimezone(timezone.utc)


# ---------------------------------------------------------------------
# LATEST MARKET SNAPSHOTS
# ---------------------------------------------------------------------

def init_snapshot_table() -> None:
    """Create the latest-snapshot table if necessary."""
    global _SNAPSHOT_TABLE_READY

    if _SNAPSHOT_TABLE_READY:
        return

    with _connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SNAPSHOT_SCHEMA_LOCK,))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                symbol TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    _SNAPSHOT_TABLE_READY = True


def publish_snapshot(symbol: str, snapshot: dict) -> None:
    """Atomically replace the latest snapshot for a symbol."""
    if not isinstance(snapshot, dict):
        raise TypeError("snapshot must be a dict")

    key = _normalise_symbol(symbol)
    init_snapshot_table()
    payload = _json_safe(snapshot)

    with _connect() as conn:
        conn.execute("""
            INSERT INTO market_snapshots (symbol, payload, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = EXCLUDED.updated_at
        """, (key, Jsonb(payload)))


def get_snapshot(
    symbol: str,
    max_age_seconds: float | None = None,
) -> dict:
    """Return the latest snapshot and its age, or {} if missing/stale.

    Existing callers remain compatible: without max_age_seconds, stale
    snapshots are returned with their age rather than automatically rejected.
    """
    key = _normalise_symbol(symbol)
    init_snapshot_table()

    with _connect() as conn:
        row = conn.execute("""
            SELECT payload, updated_at
            FROM market_snapshots
            WHERE symbol = %s
        """, (key,)).fetchone()

    if not row:
        return {}

    payload, updated_at = row

    if isinstance(payload, str):
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        logger.warning("Snapshot payload for %s is not a JSON object", key)
        return {}

    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    age_seconds = max(
        0.0,
        (
            datetime.now(timezone.utc)
            - updated_at.astimezone(timezone.utc)
        ).total_seconds(),
    )

    if (
        max_age_seconds is not None
        and age_seconds > max(0.0, float(max_age_seconds))
    ):
        logger.warning(
            "Snapshot for %s is stale: %.1fs", key, age_seconds
        )
        return {}

    result = dict(payload)
    result["_shared_age_s"] = age_seconds
    result["_shared_updated_at"] = (
        updated_at.astimezone(timezone.utc).isoformat()
    )
    return result


# ---------------------------------------------------------------------
# GENUINE OHLCV CANDLE STORAGE
# ---------------------------------------------------------------------

def init_candle_table() -> None:
    """Create a separate table for genuine OHLCV bars."""
    global _CANDLE_TABLE_READY

    if _CANDLE_TABLE_READY:
        return

    with _connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_CANDLE_SCHEMA_LOCK,))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_ohlcv_candles (
                symbol TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                trade_date DATE NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION,
                source TEXT,
                tick_count INTEGER NOT NULL DEFAULT 0,
                is_complete BOOLEAN NOT NULL DEFAULT FALSE,
                quality_status TEXT NOT NULL DEFAULT 'LEGACY_UNVERIFIED',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                PRIMARY KEY (symbol, ts),

                CHECK (open > 0),
                CHECK (high > 0),
                CHECK (low > 0),
                CHECK (close > 0),

                CHECK (high >= low),
                CHECK (high >= open AND high >= close),
                CHECK (low <= open AND low <= close)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS market_ohlcv_candles_day_idx
            ON market_ohlcv_candles (symbol, trade_date, ts DESC)
        """)

        # Backward-compatible migration for tables created by older deployments.
        conn.execute("""
            ALTER TABLE market_ohlcv_candles
            ADD COLUMN IF NOT EXISTS tick_count INTEGER NOT NULL DEFAULT 0
        """)
        conn.execute("""
            ALTER TABLE market_ohlcv_candles
            ADD COLUMN IF NOT EXISTS is_complete BOOLEAN NOT NULL DEFAULT FALSE
        """)
        conn.execute("""
            ALTER TABLE market_ohlcv_candles
            ADD COLUMN IF NOT EXISTS quality_status TEXT NOT NULL DEFAULT 'LEGACY_UNVERIFIED'
        """)

    _CANDLE_TABLE_READY = True


def _validate_candle(row: dict) -> tuple:
    """Validate and prepare a genuine OHLCV candle."""
    if not isinstance(row, dict):
        raise TypeError("Each candle must be a dict")

    timestamp = row.get("timestamp", row.get("ts"))
    ts = _as_datetime(timestamp)

    values = {}

    for field in ("open", "high", "low", "close"):
        try:
            value = float(row[field])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"Candle is missing a valid {field}"
            ) from exc

        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f"Candle {field} must be a finite positive number"
            )

        values[field] = value

    open_price = values["open"]
    high_price = values["high"]
    low_price = values["low"]
    close_price = values["close"]

    if high_price < max(open_price, low_price, close_price):
        raise ValueError("Candle high is inconsistent with OHLC values")

    if low_price > min(open_price, high_price, close_price):
        raise ValueError("Candle low is inconsistent with OHLC values")

    volume = row.get("volume")

    if volume is not None:
        try:
            volume = float(volume)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Candle volume must be numeric or None"
            ) from exc

        if not math.isfinite(volume) or volume < 0:
            raise ValueError(
                "Candle volume must be finite and non-negative"
            )

    source = row.get("source")
    if source is not None:
        source = str(source)[:100]

    try:
        tick_count = int(row.get("tick_count", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError("Candle tick_count must be an integer") from exc
    if tick_count < 0:
        raise ValueError("Candle tick_count cannot be negative")

    is_complete = row.get("is_complete")
    if not isinstance(is_complete, bool):
        raise ValueError("Candle is_complete must be a boolean")
    if not is_complete:
        raise ValueError("Partial candles cannot be published as completed OHLCV")

    quality_status = str(row.get("quality_status") or "").strip().upper()
    expected_status = "VALID" if tick_count >= 3 else "INVALID_LOW_TICK_COUNT"
    if quality_status != expected_status:
        raise ValueError(
            f"Candle quality_status must be {expected_status} for tick_count={tick_count}"
        )

    trade_date = ts.astimezone(IST).date()

    return (
        ts,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        source,
        tick_count,
        is_complete,
        quality_status,
    )


def publish_candles(
    symbol: str,
    candles: Iterable[dict],
    *,
    strict: bool = True,
) -> int:
    """Persist genuine OHLCV candles idempotently.

    Returns the number of newly inserted rows.
    Existing timestamps are ignored rather than overwritten.

    strict=True:
        Reject the batch if any candle is invalid.

    strict=False:
        Log and skip invalid candles.
    """
    key = _normalise_symbol(symbol)
    rows = list(candles)

    if not rows:
        return 0

    prepared = []

    for index, row in enumerate(rows):
        try:
            prepared.append(_validate_candle(row))
        except (KeyError, TypeError, ValueError) as exc:
            if strict:
                raise ValueError(
                    f"Invalid candle at batch index {index}: {exc}"
                ) from exc

            logger.warning(
                "Skipping invalid %s candle at index %s: %s",
                key,
                index,
                exc,
            )

    if not prepared:
        return 0

    init_candle_table()
    inserted = 0

    with _connect() as conn:
        with conn.cursor() as cur:
            for offset in range(0, len(prepared), 250):
                batch = prepared[offset:offset + 250]

                cur.executemany("""
                    INSERT INTO market_ohlcv_candles (
                        symbol, ts, trade_date,
                        open, high, low, close, volume, source,
                        tick_count, is_complete, quality_status
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (symbol, ts) DO NOTHING
                """, [(key, *row) for row in batch])

                inserted += max(cur.rowcount, 0)

    return inserted


def get_recent_candles(
    symbol: str,
    limit: int = 120,
    max_age_seconds: float | None = None,
) -> list[dict]:
    """Return recent OHLCV bars oldest-first.

    If max_age_seconds is provided, bars older than that threshold are
    excluded. Use this to verify freshness before generating a signal.
    """
    key = _normalise_symbol(symbol)

    try:
        limit = int(limit)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc

    if limit < 1 or limit > 10000:
        raise ValueError("limit must be between 1 and 10000")

    init_candle_table()

    params = [key]
    age_clause = ""

    if max_age_seconds is not None:
        age = max(0.0, float(max_age_seconds))
        age_clause = """
            AND ts >= NOW() - (%s * INTERVAL '1 second')
        """
        params.append(age)

    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(f"""
            SELECT
                ts, open, high, low, close, volume, source,
                tick_count, is_complete, quality_status
            FROM market_ohlcv_candles
            WHERE symbol = %s
              AND is_complete IS TRUE
              AND quality_status = 'VALID'
              AND tick_count >= 3
            {age_clause}
            ORDER BY ts DESC
            LIMIT %s
        """, params).fetchall()

    now = datetime.now(timezone.utc)
    candles = []

    for (
        ts, open_price, high, low, close, volume, source,
        tick_count, is_complete, quality_status,
    ) in reversed(rows):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        ts_utc = ts.astimezone(timezone.utc)

        candles.append({
            "timestamp": ts_utc.astimezone(IST).isoformat(),
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "source": source,
            "tick_count": int(tick_count),
            "is_complete": bool(is_complete),
            "quality_status": quality_status,
            "age_seconds": max(
                0.0, (now - ts_utc).total_seconds()
            ),
        })

    return candles


def get_latest_candle(
    symbol: str,
    max_age_seconds: float | None = None,
) -> dict:
    """Return the latest OHLCV candle, or {} if none is available."""
    candles = get_recent_candles(
        symbol,
        limit=1,
        max_age_seconds=max_age_seconds,
    )
    return candles[-1] if candles else {}


def purge_old_candles(retention_days: int = 30) -> int:
    """Delete old OHLCV rows. Retention <= 0 disables deletion."""
    retention_days = int(retention_days)

    if retention_days <= 0:
        return 0

    init_candle_table()
    cutoff = datetime.now(IST).date() - timedelta(days=retention_days)

    with _connect() as conn:
        cur = conn.execute("""
            DELETE FROM market_ohlcv_candles
            WHERE trade_date < %s
        """, (cutoff,))

        return max(cur.rowcount, 0)
