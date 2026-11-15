"""PostgreSQL persistence for collector option rows and candles.

Replaces the local parquet files (which are not a substitute for durable database storage).
Uses the same env switch as the rest of the project:
    DATABASE_URL                         (EC2 Postgres; default)
    USE_LOCAL_DATABASE=true + LOCAL_DATABASE_URL   (local development)

Design notes
  * Every insert is idempotent (PRIMARY KEY + ON CONFLICT DO NOTHING), so a restart or a retried
    batch can never create duplicate rows.
  * Connections are short-lived (one per write). Nothing is held open across the 60-second tick,
    so a dropped EC2 connection can't poison the collector.
  * Schema creation takes an advisory lock, so four collectors starting at the same second don't race.
  * NaN/inf are converted to NULL/None before they reach JSONB (Postgres rejects NaN in JSON).
  * Rows are stored in typed columns (about 0.5 KB/row) rather than JSONB (about 1.3 KB/row). Any
    field a collector adds in future that has no column yet is kept in the `extra` JSONB column.
  * Old rows are purged after OPTION_RETENTION_DAYS (default 14; 0 = keep forever). A full EC2
    volume can stop Postgres, which would also take the live snapshot table down with it.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import date, datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo

logger = logging.getLogger("market_store")
IST = ZoneInfo("Asia/Kolkata")

_SCHEMA_READY = False
_INSERT_CHUNK = 250          # rows per INSERT statement (keeps parameter count well under driver limits)

# Typed columns. (column, collector row key, kind)  kind: f = double, b = boolean, t = text
_OPTION_FIXED = ("timestamp", "symbol", "expiry", "strike", "type")       # stored as ts/symbol/expiry/strike/opt_type
_OPTION_SPEC = [
    ("ltp", "ltp", "f"), ("oi", "oi", "f"), ("volume", "volume", "f"),
    ("bid", "bid", "f"), ("ask", "ask", "f"), ("bid_qty", "bid_qty", "f"), ("ask_qty", "ask_qty", "f"),
    ("quote_valid", "quote_valid", "b"), ("spread_pct", "spread_pct", "f"),
    ("quote_entry_eligible", "quote_entry_eligible", "b"),
    ("exchange_feed_time", "exchange_feed_time", "t"), ("exchange_trade_time", "exchange_trade_time", "t"),
    ("iv_valid", "iv_valid", "b"), ("iv_solution_valid", "iv_solution_valid", "b"),
    ("iv_estimate", "iv_estimate", "f"), ("iv_price", "iv_price", "f"), ("iv_price_source", "iv_price_source", "t"),
    ("spot", "spot", "f"), ("vix", "vix", "f"),
    ("delta", "delta", "f"), ("gamma", "gamma", "f"), ("vega", "vega", "f"), ("theta", "theta", "f"),
    ("vanna", "vanna", "f"), ("charm", "charm", "f"), ("vomma", "vomma", "f"), ("speed", "speed", "f"),
    ("zomma", "zomma", "f"), ("color", "color", "f"), ("gex", "gex", "f"),
    ("vanna_exposure", "vanna_exposure", "f"), ("charm_exposure", "charm_exposure", "f"), ("iv_used", "iv_used", "f"),
    ("delta_oi", "delta_oi", "f"), ("oi_buildup", "oi_buildup", "t"), ("obi", "obi", "f"),
    ("tape_velocity", "tape_velocity", "f"), ("vpin_proxy", "vpin_proxy", "f"),
    ("market_pcr", "market_pcr", "f"), ("regime", "regime", "t"), ("max_pain", "max_pain", "f"),
]
_CANDLE_FIXED = ("timestamp",)
_CANDLE_SPEC = [
    # Genuine OHLCV fields. These remain NULL for ordinary spot-sample rows.
    ("open", "open", "f"), ("high", "high", "f"),
    ("low", "low", "f"), ("close", "close", "f"),
    ("volume", "volume", "f"),
    # Existing per-minute market features; retained for backward compatibility.
    ("spot", "spot", "f"), ("vix", "vix", "f"),
    ("futures_basis", "futures_basis", "f"),
    ("futures_basis_pct", "futures_basis_pct", "f"),
    ("realized_vol", "realized_vol", "f"),
    ("iv_rv_spread", "iv_rv_spread", "f"),
    ("tick_count", "tick_count", "f"),
    ("is_complete", "is_complete", "b"),
    ("quality_status", "quality_status", "t"),
]
_SQL_TYPE = {"f": "DOUBLE PRECISION", "b": "BOOLEAN", "t": "TEXT"}


# ── connection ───────────────────────────────────────────────────────────────
def _db_url() -> str:
    use_local = os.getenv("USE_LOCAL_DATABASE", "").strip().lower() in ("1", "true", "yes")
    url = os.getenv("LOCAL_DATABASE_URL" if use_local else "DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set (add the EC2 Postgres reference variable)")
    return url


def _connect():
    url = _db_url()
    try:
        import psycopg
        return psycopg.connect(url, connect_timeout=8)
    except ImportError:
        try:
            import psycopg2
            return psycopg2.connect(url, connect_timeout=8)
        except ImportError as exc:
            raise RuntimeError("Install psycopg[binary]>=3.2 (add it to requirements.txt)") from exc


# ── value cleaning ───────────────────────────────────────────────────────────
def _plain(o: Any) -> Any:
    """Make any value JSON-safe: numpy -> python, date/datetime -> ISO string, NaN/inf -> None."""
    if o is None or isinstance(o, (str, bool)):
        return o
    if type(o).__module__ == "numpy":
        try:
            o = o.item() if getattr(o, "ndim", 0) == 0 else o.tolist()
        except Exception:
            o = str(o)
    if isinstance(o, dict):
        return {str(k): _plain(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set)):
        return [_plain(v) for v in o]
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, (int, bytes)):
        return o if isinstance(o, int) else o.decode("utf-8", "replace")
    return str(o)


def _num(v: Any):
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _bool(v: Any):
    """Convert database-bound boolean values without treating the string 'false' as True."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        value = v.strip().lower()
        if value in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if value in {"0", "false", "f", "no", "n", "off", ""}:
            return False
    return None


def _text(v: Any):
    return None if v is None else str(v)


_CAST = {"f": _num, "b": _bool, "t": _text}


def _as_date(v: Any):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    raw = str(v).strip()
    try:
        return date.fromisoformat(raw[:10])
    except (TypeError, ValueError):
        for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(raw.upper(), fmt).date()
            except ValueError:
                continue
        return None


def _trade_date(ts: datetime) -> date:
    # collectors pass an IST-aware timestamp; keep its IST calendar date
    if isinstance(ts, datetime) and ts.tzinfo is not None:
        return ts.astimezone(IST).date()
    return ts.date()


# ── schema ───────────────────────────────────────────────────────────────────
def init_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    opt_cols = ",\n                    ".join(f"{c} {_SQL_TYPE[k]}" for c, _, k in _OPTION_SPEC)
    can_cols = ",\n                    ".join(f"{c} {_SQL_TYPE[k]}" for c, _, k in _CANDLE_SPEC)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(727001)")      # serialise concurrent first-time setup
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS option_ticks (
                    index_key  TEXT        NOT NULL,
                    ts         TIMESTAMPTZ NOT NULL,
                    trade_date DATE        NOT NULL,
                    symbol     TEXT        NOT NULL,
                    expiry     DATE,
                    strike     DOUBLE PRECISION,
                    opt_type   TEXT,
                    {opt_cols},
                    extra      JSONB,
                    PRIMARY KEY (index_key, ts, symbol)
                )""")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS index_candles (
                    index_key  TEXT        NOT NULL,
                    ts         TIMESTAMPTZ NOT NULL,
                    trade_date DATE        NOT NULL,
                    {can_cols},
                    extra      JSONB,
                    PRIMARY KEY (index_key, ts)
                )""")
            for c, _, k in _OPTION_SPEC:                              # pick up columns added in later versions
                cur.execute(f"ALTER TABLE option_ticks ADD COLUMN IF NOT EXISTS {c} {_SQL_TYPE[k]}")
            for c, _, k in _CANDLE_SPEC:
                cur.execute(f"ALTER TABLE index_candles ADD COLUMN IF NOT EXISTS {c} {_SQL_TYPE[k]}")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS option_ohlcv_candles (
                    index_key TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    trade_date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    token TEXT NOT NULL,
                    exchange_segment TEXT NOT NULL,
                    expiry DATE NOT NULL,
                    strike DOUBLE PRECISION NOT NULL,
                    opt_type TEXT NOT NULL CHECK (opt_type IN ('CE','PE')),
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION,
                    tick_count INTEGER NOT NULL DEFAULT 0,
                    is_complete BOOLEAN NOT NULL DEFAULT TRUE,
                    quality_status TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'angelone_websocket_option_ltp',
                    PRIMARY KEY (symbol, ts)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS option_ohlcv_contract_ts_idx
                ON option_ohlcv_candles(index_key, expiry, strike, opt_type, ts DESC)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS option_ohlcv_trade_date_idx
                ON option_ohlcv_candles(index_key, trade_date)
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS option_ticks_day_idx ON option_ticks (index_key, trade_date)")
            cur.execute("CREATE INDEX IF NOT EXISTS index_candles_day_idx ON index_candles (index_key, trade_date)")
        conn.commit()
        _SCHEMA_READY = True
    finally:
        conn.close()


# ── writes ───────────────────────────────────────────────────────────────────
def _bulk_insert(conn, head: str, row_tpl: str, rows: list, tail: str) -> int:
    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), _INSERT_CHUNK):
            part = rows[i:i + _INSERT_CHUNK]
            sql = f"{head} VALUES " + ",".join([row_tpl] * len(part)) + f" {tail}"
            cur.execute(sql, [v for r in part for v in r])
            inserted += max(cur.rowcount, 0)
    return inserted


def _extra(row: dict, known: set):
    rest = {k: v for k, v in row.items() if k not in known}
    return json.dumps(_plain(rest), allow_nan=False, separators=(",", ":")) if rest else None


def insert_option_rows(index_key: str, rows: Iterable[dict]) -> int:
    """Insert option-chain rows. Returns the number actually inserted (duplicates are skipped)."""
    rows = list(rows)
    if not rows:
        return 0
    init_schema()
    key = index_key.upper()
    known = set(_OPTION_FIXED) | {k for _, k, _ in _OPTION_SPEC}
    prepared = []
    for row_number, r in enumerate(rows):
        ts, sym = r.get("timestamp"), r.get("symbol")
        if ts is None or not str(sym or "").strip():
            # Do not silently discard malformed observations: raise so the
            # collector retains its pending batch and reports the DB failure.
            raise ValueError(
                f"Option row {row_number} missing required timestamp or symbol"
            )
        prepared.append((
            key, ts, _trade_date(ts), str(sym), _as_date(r.get("expiry")), _num(r.get("strike")),
            _text(r.get("type")),
            *[_CAST[kind](r.get(rk)) for _, rk, kind in _OPTION_SPEC],
            _extra(r, known),
        ))
    cols = ["index_key", "ts", "trade_date", "symbol", "expiry", "strike", "opt_type"] + \
           [c for c, _, _ in _OPTION_SPEC] + ["extra"]
    tpl = "(" + ",".join(["%s"] * (len(cols) - 1) + ["%s::jsonb"]) + ")"
    conn = _connect()
    try:
        n = _bulk_insert(conn, f"INSERT INTO option_ticks ({','.join(cols)})", tpl, prepared, "ON CONFLICT DO NOTHING")
        conn.commit()
        return n
    finally:
        conn.close()


def insert_candles(index_key: str, rows: Iterable[dict]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    init_schema()
    key = index_key.upper()
    known = set(_CANDLE_FIXED) | {k for _, k, _ in _CANDLE_SPEC} | {"symbol"}
    prepared = []
    for row_number, r in enumerate(rows):
        ts = r.get("timestamp")
        if ts is None:
            raise ValueError(f"Candle/feature row {row_number} missing required timestamp")

        # Feature-only spot samples are allowed, but they are NOT OHLC candles.
        # If any OHLC field is supplied, require all four and validate consistency.
        ohlc_names = ("open", "high", "low", "close")
        supplied_ohlc = any(r.get(name) is not None for name in ohlc_names)
        if supplied_ohlc:
            missing = [name for name in ohlc_names if r.get(name) is None]
            if missing:
                raise ValueError(
                    f"Row {row_number} has incomplete OHLC fields: {missing}"
                )
            o, h, low, c = (_num(r.get(name)) for name in ohlc_names)
            if any(value is None or value <= 0 for value in (o, h, low, c)):
                raise ValueError(f"Row {row_number} has invalid OHLC prices")
            if h < max(o, low, c) or low > min(o, h, c):
                raise ValueError(f"Row {row_number} has inconsistent OHLC values")

        prepared.append((
            key,
            ts,
            _trade_date(ts),
            *[_CAST[kind](r.get(rk)) for _, rk, kind in _CANDLE_SPEC],
            _extra(r, known),
        ))
    cols = ["index_key", "ts", "trade_date"] + [c for c, _, _ in _CANDLE_SPEC] + ["extra"]
    tpl = "(" + ",".join(["%s"] * (len(cols) - 1) + ["%s::jsonb"]) + ")"
    conn = _connect()
    try:
        n = _bulk_insert(conn, f"INSERT INTO index_candles ({','.join(cols)})", tpl, prepared, "ON CONFLICT DO NOTHING")
        conn.commit()
        return n
    finally:
        conn.close()


# ── option-premium OHLC candles (Moonbag / ATR) ───────────────────────────────
def insert_option_ohlcv_candles(index_key: str, rows: Iterable[dict]) -> int:
    """Persist completed, contract-specific option premium OHLC bars.

    Unlike option_ticks, these rows are actual OHLC aggregates from WebSocket
    price ticks. Partial bars and invalid low-tick-count bars are not stored.
    """
    rows = list(rows)
    if not rows:
        return 0
    init_schema()
    key = str(index_key).upper().strip()
    prepared = []
    for i, row in enumerate(rows):
        if row.get("is_complete") is not True:
            raise ValueError(f"Option candle {i} is not marked complete")
        if str(row.get("quality_status", "")).upper() != "VALID":
            continue
        ts = row.get("timestamp")
        if isinstance(ts, str):
            raw_ts = ts.strip()
            if raw_ts.endswith("Z"):
                raw_ts = raw_ts[:-1] + "+00:00"
            ts = datetime.fromisoformat(raw_ts)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)
        if not isinstance(ts, datetime):
            raise ValueError(f"Option candle {i} has invalid timestamp")
        symbol = str(row.get("symbol") or "").strip()
        token = str(row.get("token") or "").strip()
        exchange = str(row.get("exchange_segment") or "").strip().upper()
        expiry = _as_date(row.get("expiry"))
        strike = _num(row.get("strike"))
        opt_type = str(row.get("opt_type") or "").upper()
        o, h, low, c = (_num(row.get(k)) for k in ("open", "high", "low", "close"))
        if ts is None or not symbol or not token or not exchange or expiry is None:
            raise ValueError(f"Option candle {i} missing contract identity/timestamp")
        if strike is None or strike <= 0 or opt_type not in {"CE", "PE"}:
            raise ValueError(f"Option candle {i} has invalid strike/type")
        if any(v is None or v <= 0 for v in (o, h, low, c)):
            raise ValueError(f"Option candle {i} has invalid OHLC")
        if h < max(o, low, c) or low > min(o, h, c):
            raise ValueError(f"Option candle {i} has inconsistent OHLC")
        tick_count = int(row.get("tick_count") or 0)
        if tick_count < 3:
            continue
        prepared.append((
            key, ts, _trade_date(ts), symbol, token, exchange, expiry,
            strike, opt_type, o, h, low, c, _num(row.get("volume")),
            tick_count, True, "VALID",
            str(row.get("source") or "angelone_websocket_option_ltp"),
        ))
    if not prepared:
        return 0
    conn = _connect()
    try:
        n = _bulk_insert(
            conn,
            """INSERT INTO option_ohlcv_candles
            (index_key,ts,trade_date,symbol,token,exchange_segment,expiry,strike,opt_type,
             open,high,low,close,volume,tick_count,is_complete,quality_status,source)""",
            "(" + ",".join(["%s"] * 18) + ")",
            prepared,
            "ON CONFLICT (symbol, ts) DO NOTHING",
        )
        conn.commit()
        return n
    finally:
        conn.close()


def load_option_ohlcv_candles(
    index_key: str, expiry: str, strike: float, opt_type: str,
    limit: int = 80,
) -> list[dict]:
    """Return valid completed bars for one exact option contract, oldest first."""
    init_schema()
    key = str(index_key).upper().strip()
    exp = _as_date(expiry)
    typ = str(opt_type).upper().strip()
    if exp is None or typ not in {"CE", "PE"}:
        raise ValueError("Valid expiry and CE/PE type are required")
    limit = int(limit)
    if not 1 <= limit <= 500:
        raise ValueError("limit must be between 1 and 500")
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ts, open, high, low, close, tick_count, quality_status,
                       symbol, token, exchange_segment
                FROM option_ohlcv_candles
                WHERE index_key=%s AND expiry=%s AND strike=%s AND opt_type=%s
                  AND is_complete=TRUE AND quality_status='VALID'
                ORDER BY ts DESC LIMIT %s
            """, (key, exp, float(strike), typ, limit))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {"timestamp": r[0].astimezone(IST), "open": float(r[1]), "high": float(r[2]),
         "low": float(r[3]), "close": float(r[4]), "tick_count": int(r[5]),
         "quality_status": r[6], "symbol": r[7], "token": r[8],
         "exchange_segment": r[9], "is_complete": True}
        for r in reversed(rows)
    ]


def load_open_signal_payloads(index_key: str) -> list[dict]:
    """Read active signal identities for the option-candle subscription manager."""
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.engine_signal_states')")
            if cur.fetchone()[0] is None:
                return []
            cur.execute("""
                SELECT payload FROM engine_signal_states
                WHERE status='open'
                  AND UPPER(payload->>'index_key')=%s
                ORDER BY created_at, signal_id
            """, (str(index_key).upper(),))
            rows = cur.fetchall()
    except Exception as exc:
        # A missing signal-state table is a deployment/configuration issue.
        # Do not silently interpret it as an empty signal set.
        raise RuntimeError(f"Could not read open signal contracts: {exc}") from exc
    finally:
        conn.close()
    out = []
    for (payload,) in rows:
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(payload, dict) and payload.get("active") is True:
            out.append(dict(payload))
    return out


# ── reads (restart recovery) ─────────────────────────────────────────────────
def count_option_rows(index_key: str, trade_date: date | str) -> int:
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM option_ticks WHERE index_key=%s AND trade_date=%s",
                        (index_key.upper(), _as_date(trade_date)))
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def load_latest_option_state(index_key: str, trade_date: date | str) -> list[dict]:
    """Load the latest saved observation for each option contract on a trade date.

    Used by all four collectors to restore prev_oi, prev_ltp and prev_volume
    after a process restart. Contract fields are included in DISTINCT ON so
    the recovery key matches the tuple used by the collectors.
    """
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (symbol, expiry, strike, opt_type)
                       symbol, expiry, strike, opt_type, oi, ltp, volume
                FROM option_ticks
                WHERE index_key = %s AND trade_date = %s
                ORDER BY symbol, expiry, strike, opt_type, ts DESC
                """,
                (index_key.upper(), _as_date(trade_date)),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "symbol": row[0],
            "expiry": row[1].isoformat() if hasattr(row[1], "isoformat") else (str(row[1]) if row[1] else ""),
            "strike": float(row[2]) if row[2] is not None else 0.0,
            "type": str(row[3] or "").upper(),
            "oi": float(row[4]) if row[4] is not None else 0.0,
            "ltp": float(row[5]) if row[5] is not None else 0.0,
            "volume": float(row[6]) if row[6] is not None else 0.0,
        }
        for row in rows
    ]


def count_candles(index_key: str, trade_date: date | str) -> int:
    init_schema()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM index_candles WHERE index_key=%s AND trade_date=%s",
                        (index_key.upper(), _as_date(trade_date)))
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _extra_dict(v):
    if not v:
        return {}
    return json.loads(v) if isinstance(v, str) else dict(v)


def load_recent_candles(index_key: str, trade_date: date | str, limit: int = 240) -> list[dict]:
    """Today's most recent candles, oldest first, shaped exactly like the collectors' in-memory candles."""
    init_schema()
    cols = [c for c, _, _ in _CANDLE_SPEC]
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT ts, {','.join(cols)}, extra FROM index_candles
                            WHERE index_key=%s AND trade_date=%s ORDER BY ts DESC LIMIT %s""",
                        (index_key.upper(), _as_date(trade_date), int(limit)))
            fetched = cur.fetchall()
    finally:
        conn.close()
    out = []
    for rec in reversed(fetched):
        row = {"timestamp": rec[0].astimezone(IST), "symbol": index_key.upper()}
        row.update(dict(zip(cols, rec[1:-1])))
        row.update(_extra_dict(rec[-1]))
        out.append(row)
    return out


def load_recent_ohlcv_candles(
    index_key: str,
    limit: int = 240,
    max_age_seconds: float | None = None,
) -> list[dict]:
    """Load genuine OHLC candles from market_ohlcv_candles, oldest first.

    Market features are left-joined from index_candles on the exact timestamp.
    This keeps genuine OHLC separate from feature-only spot samples while
    preserving fields expected by existing signal-engine callers.
    """
    init_schema()
    limit = int(limit)
    if not 1 <= limit <= 10000:
        raise ValueError("limit must be between 1 and 10000")

    key = index_key.upper()
    params = [key]
    age_clause = ""
    if max_age_seconds is not None:
        age = max(0.0, float(max_age_seconds))
        age_clause = "AND m.ts >= NOW() - (%s * INTERVAL '1 second')"
        params.append(age)
    params.append(limit)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    m.ts,
                    m.open, m.high, m.low, m.close, m.volume,
                    i.spot, i.vix, i.futures_basis, i.futures_basis_pct,
                    i.realized_vol, i.iv_rv_spread,
                    m.source,
                    m.tick_count, m.is_complete, m.quality_status,
                    i.extra
                FROM market_ohlcv_candles AS m
                LEFT JOIN index_candles AS i
                    ON i.index_key = m.symbol
                   AND i.ts = m.ts
                WHERE m.symbol = %s
                  AND m.open IS NOT NULL
                  AND m.high IS NOT NULL
                  AND m.low IS NOT NULL
                  AND m.close IS NOT NULL
                  AND m.is_complete IS TRUE
                  AND m.quality_status = 'VALID'
                  AND m.tick_count >= 3
                  {age_clause}
                ORDER BY m.ts DESC
                LIMIT %s
            """, tuple(params))
            fetched = cur.fetchall()
    finally:
        conn.close()

    out = []
    for rec in reversed(fetched):
        ts = rec[0]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=IST)

        row = {
            "timestamp": ts.astimezone(IST),
            "symbol": key,
            "open": rec[1],
            "high": rec[2],
            "low": rec[3],
            "close": rec[4],
            "volume": rec[5],
            "spot": rec[6],
            "vix": rec[7],
            "futures_basis": rec[8],
            "futures_basis_pct": rec[9],
            "realized_vol": rec[10],
            "iv_rv_spread": rec[11],
            "source": rec[12],
            "tick_count": int(rec[13] or 0),
            "is_complete": bool(rec[14]),
            "quality_status": rec[15],
        }
        row.update(_extra_dict(rec[16]))
        out.append(row)

    return out


# ── maintenance ──────────────────────────────────────────────────────────────
def purge_old(retention_days: int | None = None) -> int:
    """Delete option rows (and candles) older than OPTION_RETENTION_DAYS. 0/negative = keep everything."""
    if retention_days is None:
        try:
            retention_days = int(os.getenv("OPTION_RETENTION_DAYS", "14"))
        except ValueError:
            retention_days = 14
    if retention_days <= 0:
        return 0
    init_schema()
    cutoff = datetime.now(IST).date() - timedelta(days=retention_days)
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM option_ticks WHERE trade_date < %s", (cutoff,))
            n = max(cur.rowcount, 0)
            cur.execute("DELETE FROM index_candles WHERE trade_date < %s", (cutoff,))
            cur.execute("DELETE FROM option_ohlcv_candles WHERE trade_date < %s", (cutoff,))
        conn.commit()
        return n
    finally:
        conn.close()


def export_day_parquet(index_key: str, trade_date: date | str, out_dir: str) -> dict:
    """Write one day back to parquet files (for backtests / the validate_daily_data script)."""
    import pandas as pd
    from pathlib import Path
    init_schema()
    d, key = _as_date(trade_date), index_key.upper()
    out = Path(out_dir)
    (out / "options").mkdir(parents=True, exist_ok=True)
    (out / "candles").mkdir(parents=True, exist_ok=True)
    cols = [c for c, _, _ in _OPTION_SPEC]
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT ts,symbol,expiry,strike,opt_type,{','.join(cols)},extra FROM option_ticks
                            WHERE index_key=%s AND trade_date=%s ORDER BY ts,symbol""", (key, d))
            opt = cur.fetchall()
    finally:
        conn.close()
    orows = []
    for rec in opt:
        row = {"timestamp": rec[0].astimezone(IST), "symbol": rec[1], "expiry": rec[2], "strike": rec[3], "type": rec[4]}
        row.update(dict(zip(cols, rec[5:-1])))
        row.update(_extra_dict(rec[-1]))
        orows.append(row)
    crows = load_recent_candles(key, d, limit=10_000)
    if orows:
        pd.DataFrame(orows).to_parquet(out / "options" / f"{d}.parquet", index=False)
    if crows:
        pd.DataFrame(crows).to_parquet(out / "candles" / f"{d}.parquet", index=False)
    return {"options": len(orows), "candles": len(crows)}
