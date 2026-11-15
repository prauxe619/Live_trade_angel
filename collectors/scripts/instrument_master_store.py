"""Shared Angel One instrument master, stored once per trading day in PostgreSQL.

Architecture:
- First collector to request today's master downloads and publishes it.
- NIFTY, BANKNIFTY, FINNIFTY and SENSEX read the same PostgreSQL row.
- A PostgreSQL session advisory lock serializes refreshes across Railway services.
- A collector rechecks PostgreSQL after acquiring the lock, so only one download
  occurs when multiple collectors start together.
- Missing, invalid, checksum-mismatched or wrong-date data is never returned.
- If refresh fails, an exception is raised; stale data is never silently used.

Database configuration:
- Uses the same connection helper as collectors/scripts/market_store.py.
- Railway: DATABASE_URL
- Local development: USE_LOCAL_DATABASE=true and LOCAL_DATABASE_URL

Dependencies:
    pandas, requests, and either psycopg[binary]>=3.2 or psycopg2-binary>=2.9
"""

import gzip
import hashlib
import io
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from collectors.scripts import market_store

logger = logging.getLogger("instrument_master_store")

IST = ZoneInfo("Asia/Kolkata")
MASTER_URL = (
    "https://margincalculator.angelone.in/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)
LOCK_KEY = 7421001
TABLE_NAME = "public.instrument_master"

MIN_ROWS = int(os.getenv("INSTRUMENT_MASTER_MIN_ROWS", "20000"))
DOWNLOAD_ATTEMPTS = max(1, int(os.getenv("INSTRUMENT_MASTER_DOWNLOAD_ATTEMPTS", "3")))
DOWNLOAD_TIMEOUT = max(5, int(os.getenv("INSTRUMENT_MASTER_DOWNLOAD_TIMEOUT", "60")))
REQUIRED_SEGMENTS = {"NSE", "NFO", "BSE", "BFO"}
REQUIRED_COLUMNS = {
    "token",
    "name",
    "exch_seg",
    "symbol",
    "instrumenttype",
    "expiry",
    "strike",
    "lotsize",
}


# ---------------------------------------------------------------------------
# PostgreSQL connection handling
# ---------------------------------------------------------------------------
def _conn():
    """Use the project's configured PostgreSQL driver and connection settings."""
    return market_store._connect()


@contextmanager
def _db():
    """Commit/rollback transaction and always close the short-lived connection."""
    conn = _conn()
    try:
        with conn:
            yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------
def init_table():
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    master_date DATE PRIMARY KEY,
                    downloaded_at TIMESTAMPTZ NOT NULL,
                    checksum CHAR(64) NOT NULL,
                    row_count INTEGER NOT NULL,
                    payload_gz BYTEA NOT NULL
                )
                """
            )


# ---------------------------------------------------------------------------
# Validation and checksum
# ---------------------------------------------------------------------------
def _validate(raw: bytes) -> pd.DataFrame:
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        raise ValueError("Instrument master payload is empty")

    try:
        df = pd.read_json(io.BytesIO(raw))
    except Exception as exc:
        raise ValueError(f"Instrument master JSON could not be parsed: {exc}") from exc

    if df.empty or len(df) < MIN_ROWS:
        raise ValueError(
            f"Instrument master too small: {len(df)} rows; minimum is {MIN_ROWS}"
        )

    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Instrument master missing columns: {sorted(missing_columns)}"
        )

    tokens = df["token"].astype("string").str.strip()
    invalid_tokens = tokens.isna() | tokens.eq("")
    if invalid_tokens.any():
        raise ValueError(
            f"Instrument master contains {int(invalid_tokens.sum())} missing/empty tokens"
        )

    segments = set(
        df["exch_seg"].dropna().astype(str).str.strip().unique()
    )
    missing_segments = REQUIRED_SEGMENTS - segments
    if missing_segments:
        raise ValueError(
            f"Instrument master missing exchange segments: {sorted(missing_segments)}"
        )

    return df


def _checksum(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


# ---------------------------------------------------------------------------
# Read today's published master
# ---------------------------------------------------------------------------
def _read_today(today) -> pd.DataFrame | None:
    """Return today's validated master, or None if missing/corrupt."""
    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT checksum, row_count, payload_gz
                FROM {TABLE_NAME}
                WHERE master_date = %s
                """,
                (today,),
            )
            row = cur.fetchone()

    if row is None:
        return None

    checksum, row_count, payload_gz = row

    try:
        raw = gzip.decompress(bytes(payload_gz))
        actual_checksum = _checksum(raw)

        if actual_checksum != str(checksum).strip():
            logger.error(
                "Instrument master checksum mismatch for %s; cached row rejected.",
                today,
            )
            return None

        df = _validate(raw)

        if len(df) != int(row_count):
            logger.error(
                "Instrument master row-count mismatch for %s: DB=%s payload=%s",
                today,
                row_count,
                len(df),
            )
            return None

        return df

    except Exception as exc:
        logger.exception(
            "Stored instrument master for %s is invalid: %s", today, exc
        )
        return None


# ---------------------------------------------------------------------------
# Download and publish
# ---------------------------------------------------------------------------
def _download() -> bytes:
    last_error = None

    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            logger.info(
                "Downloading Angel One instrument master (attempt %d/%d)...",
                attempt,
                DOWNLOAD_ATTEMPTS,
            )

            response = requests.get(
                MASTER_URL,
                timeout=DOWNLOAD_TIMEOUT,
                headers={"User-Agent": "NiftyTradingAgent/1.0"},
            )
            response.raise_for_status()
            raw = response.content

            # Validate before the payload is allowed into PostgreSQL.
            _validate(raw)
            return raw

        except Exception as exc:
            last_error = exc
            logger.warning(
                "Instrument master download attempt %d failed: %s",
                attempt,
                exc,
            )
            if attempt < DOWNLOAD_ATTEMPTS:
                time.sleep(min(3 * attempt, 10))

    raise RuntimeError(
        f"Angel One instrument master download failed after "
        f"{DOWNLOAD_ATTEMPTS} attempts: {last_error}"
    ) from last_error


def _publish(today, raw: bytes, row_count: int):
    compressed = gzip.compress(raw, compresslevel=6)
    checksum = _checksum(raw)

    with _db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}
                    (master_date, downloaded_at, checksum, row_count, payload_gz)
                VALUES (%s, now(), %s, %s, %s)
                ON CONFLICT (master_date) DO UPDATE SET
                    downloaded_at = EXCLUDED.downloaded_at,
                    checksum = EXCLUDED.checksum,
                    row_count = EXCLUDED.row_count,
                    payload_gz = EXCLUDED.payload_gz
                """,
                (today, checksum, row_count, compressed),
            )

            # Keep the latest seven calendar days of instrument-master snapshots.
            cur.execute(
                f"DELETE FROM {TABLE_NAME} WHERE master_date < %s::date - 7",
                (today,),
            )

    logger.info(
        "Instrument master published: date=%s rows=%d checksum=%s compressed_bytes=%d",
        today,
        row_count,
        checksum,
        len(compressed),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def get_today_master(tz=IST) -> pd.DataFrame:
    """Return today's validated master, downloading and publishing it if needed.

    First-caller-wins: whichever collector reaches the refresh path first becomes
    the publisher. Other collectors wait on the same PostgreSQL advisory lock,
    then re-read the published row rather than downloading another copy.
    """
    today = datetime.now(tz).date()
    init_table()

    # Fast path: most collector calls only read the already-published row.
    df = _read_today(today)
    if df is not None:
        logger.info(
            "Instrument master cache HIT: date=%s rows=%d source=PostgreSQL",
            today,
            len(df),
        )
        return df

    logger.warning(
        "Today's instrument master is missing or invalid; attempting serialized refresh."
    )

    # Session-level lock is held on this dedicated connection until explicit unlock.
    lock_conn = _conn()
    lock_acquired = False

    try:
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        lock_conn.commit()
        lock_acquired = True

        # Another collector may have downloaded while this collector waited.
        df = _read_today(today)
        if df is not None:
            logger.info(
                "Instrument master published by another collector: date=%s rows=%d",
                today,
                len(df),
            )
            return df

        raw = _download()
        df = _validate(raw)
        _publish(today, raw, len(df))

        # Read back and validate the exact bytes committed to PostgreSQL.
        published_df = _read_today(today)
        if published_df is None:
            raise RuntimeError(
                "Instrument master publish completed, but PostgreSQL "
                "read-back validation failed"
            )

        logger.info(
            "Instrument master refresh successful: date=%s rows=%d",
            today,
            len(published_df),
        )
        return published_df

    finally:
        if lock_acquired:
            try:
                with lock_conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
                lock_conn.commit()
            except Exception:
                logger.exception(
                    "Failed to explicitly release instrument-master advisory lock"
                )
        lock_conn.close()

def get_instrument_master(publisher=True, tz=IST) -> pd.DataFrame:
    """
    Backward-compatible API for all index collectors.

    publisher is accepted for compatibility with older collectors.
    The PostgreSQL advisory lock and first-caller-wins logic inside
    get_today_master() determine which collector publishes the master.
    """
    return get_today_master(tz=tz)
