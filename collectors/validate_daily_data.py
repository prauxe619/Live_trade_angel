"""PostgreSQL-first validation for the EC2 Angel One market-data pipeline.

Validates latest market snapshots, genuine spot OHLC candles, the separate
active-futures OHLCV stream, and option-chain persistence. Spot index volume
must remain NULL; breakout volume is supplied only by futures candles.
"""
from __future__ import annotations

import json
import logging
import math
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from collectors.scripts import market_store
from collectors.scripts.shared_snapshots import (
    get_snapshot,
    get_recent_candles,
    get_recent_futures_candles,
    init_snapshot_table,
    init_candle_table,
    init_futures_candle_table,
    _connect as _shared_connect,
)

try:
    from revenue_agent.telegram_vip_alert import send_telegram_message
except Exception:
    def send_telegram_message(message):
        print(f"Telegram unavailable: {message}")

TZ = ZoneInfo("Asia/Kolkata")
OUT_DIR = Path("data/validation")
INDICES = ("NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX")
SNAPSHOT_MAX_AGE_SECONDS = max(30, int(__import__("os").getenv("SNAPSHOT_MAX_AGE_SECONDS", "120")))
CANDLE_MAX_AGE_SECONDS = max(30, int(__import__("os").getenv("CANDLE_MAX_AGE_SECONDS", "180")))
MIN_VALID_CANDLES = max(3, int(__import__("os").getenv("VALIDATION_MIN_CANDLES", "3")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("daily_data_validation")


def _age_seconds(value) -> float | None:
    if value is None:
        return None
    ts = value
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds())


def _finite_positive(value) -> bool:
    try:
        number = float(value)
        return math.isfinite(number) and number > 0
    except (TypeError, ValueError, OverflowError):
        return False


def _validate_snapshot(index_key: str) -> dict:
    snapshot = get_snapshot(index_key, max_age_seconds=SNAPSHOT_MAX_AGE_SECONDS)
    if not snapshot:
        return {
            "ok": False,
            "issues": [f"{index_key}: missing or stale PostgreSQL snapshot"],
        }

    issues = []
    spot = snapshot.get("spot", snapshot.get("spot_val"))
    vix = snapshot.get("vix")
    pcr = snapshot.get("pcr", snapshot.get("market_pcr", snapshot.get("pcr_oi")))
    if not _finite_positive(spot):
        issues.append("spot missing/invalid")
    if not _finite_positive(vix):
        issues.append("VIX missing/invalid")
    if not _finite_positive(pcr):
        issues.append("PCR missing/invalid")

    age = float(snapshot.get("_shared_age_s", 0.0))
    return {
        "ok": not issues,
        "spot": float(spot) if _finite_positive(spot) else None,
        "vix": float(vix) if _finite_positive(vix) else None,
        "pcr": float(pcr) if _finite_positive(pcr) else None,
        "age_seconds": round(age, 1),
        "updated_at": snapshot.get("_shared_updated_at"),
        "issues": issues,
    }


def _validate_spot_candles(index_key: str, trade_date: date) -> dict:
    candles = get_recent_candles(index_key, limit=240)
    today_candles = []
    for candle in candles:
        try:
            ts = datetime.fromisoformat(str(candle["timestamp"]).replace("Z", "+00:00"))
            if ts.astimezone(TZ).date() == trade_date:
                today_candles.append(candle)
        except Exception:
            continue

    issues = []
    if len(today_candles) < MIN_VALID_CANDLES:
        issues.append(
            f"only {len(today_candles)} valid completed spot candles; "
            f"minimum={MIN_VALID_CANDLES}"
        )

    for candle in today_candles:
        try:
            o, h, low, c = (float(candle[k]) for k in ("open", "high", "low", "close"))
            if not all(math.isfinite(v) and v > 0 for v in (o, h, low, c)):
                raise ValueError("non-positive/non-finite OHLC")
            if h < max(o, low, c) or low > min(o, h, c):
                raise ValueError("inconsistent OHLC")
        except Exception as exc:
            issues.append(f"invalid spot candle {candle.get('timestamp')}: {exc}")
            break

    latest = today_candles[-1] if today_candles else {}
    age = _age_seconds(latest.get("timestamp")) if latest else None
    now = datetime.now(TZ)
    market_hours = now.weekday() < 5 and "09:15" <= now.strftime("%H:%M") < "15:35"
    if market_hours and age is not None and age > CANDLE_MAX_AGE_SECONDS:
        issues.append(f"latest spot candle stale: {age:.1f}s")

    return {
        "ok": not issues,
        "count": len(today_candles),
        "latest_timestamp": latest.get("timestamp"),
        "latest_age_seconds": round(age, 1) if age is not None else None,
        "issues": issues,
    }


def _validate_futures_volume(index_key: str, trade_date: date) -> dict:
    rows = get_recent_futures_candles(index_key, limit=500, trade_date=trade_date)
    today_rows = []
    for row in rows:
        try:
            ts = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            if ts.astimezone(TZ).date() == trade_date:
                today_rows.append(row)
        except Exception:
            continue

    issues = []
    if not today_rows:
        issues.append("no valid active-futures OHLCV candles for today")
    for row in today_rows:
        if (
            row.get("is_complete") is not True
            or str(row.get("quality_status", "")).upper() != "VALID"
            or int(row.get("tick_count") or 0) < 3
            or not math.isfinite(float(row.get("volume", -1)))
            or float(row.get("volume", -1)) < 0
        ):
            issues.append(f"invalid futures-volume candle {row.get('timestamp')}")
            break

    latest = today_rows[-1] if today_rows else {}
    age = _age_seconds(latest.get("timestamp")) if latest else None
    now = datetime.now(TZ)
    market_hours = now.weekday() < 5 and "09:15" <= now.strftime("%H:%M") < "15:35"
    if market_hours and age is not None and age > CANDLE_MAX_AGE_SECONDS:
        issues.append(f"latest futures candle stale: {age:.1f}s")

    return {
        "ok": not issues,
        "count": len(today_rows),
        "active_token": latest.get("token"),
        "expiry": latest.get("expiry"),
        "latest_timestamp": latest.get("timestamp"),
        "latest_age_seconds": round(age, 1) if age is not None else None,
        "issues": issues,
    }


def _validate_options(index_key: str, trade_date: date) -> dict:
    market_store.init_schema()
    with market_store._connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*), MAX(ts), COUNT(*) FILTER (WHERE ltp IS NOT NULL AND ltp > 0)
            FROM option_ticks
            WHERE index_key = %s AND trade_date = %s
            """,
            (index_key, trade_date),
        ).fetchone()

    count, latest_ts, valid_ltp_count = row or (0, None, 0)
    issues = []
    if int(count or 0) == 0:
        issues.append("no option-chain rows persisted today")
    if int(valid_ltp_count or 0) == 0:
        issues.append("no positive option LTP rows persisted today")
    return {
        "ok": not issues,
        "count": int(count or 0),
        "valid_ltp_count": int(valid_ltp_count or 0),
        "latest_timestamp": latest_ts.isoformat() if latest_ts else None,
        "issues": issues,
    }


def _validate_spot_volume_is_not_fabricated(index_key: str, trade_date: date) -> dict:
    with _shared_connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) FILTER (WHERE volume IS NOT NULL), COUNT(*)
            FROM market_ohlcv_candles
            WHERE symbol = %s AND trade_date = %s
            """,
            (index_key, trade_date),
        ).fetchone()
    non_null, total = row or (0, 0)
    issues = []
    if int(non_null or 0) > 0:
        issues.append(
            f"{int(non_null)} of {int(total)} spot rows have non-NULL volume; "
            "verify this is not synthetic index volume"
        )
    return {
        "ok": not issues,
        "rows_with_volume": int(non_null or 0),
        "total_rows": int(total or 0),
        "issues": issues,
    }


def calculate_status(results: dict) -> str:
    checks = [
        check
        for index_result in results.values()
        for check in index_result.values()
        if isinstance(check, dict) and "ok" in check
    ]
    if not checks or not any(check.get("count", 0) or check.get("spot") for check in checks):
        return "INCOMPLETE"
    if all(check["ok"] for check in checks):
        return "HEALTHY"
    if any(
        "missing or stale" in issue or "no valid active-futures" in issue
        for check in checks for issue in check.get("issues", [])
    ):
        return "DEGRADED"
    return "WARNING"


def run_validation(date_str: str | None = None) -> dict:
    trade_date = date.fromisoformat(date_str) if date_str else datetime.now(TZ).date()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting PostgreSQL market-data validation for %s", trade_date)

    # Schema creation is idempotent and serialized by advisory locks.
    market_store.init_schema()
    init_snapshot_table()
    init_candle_table()
    init_futures_candle_table()

    report = {
        "meta": {
            "date": trade_date.isoformat(),
            "validated_at": datetime.now(TZ).isoformat(),
            "timezone": "Asia/Kolkata",
            "version": "EC2-POSTGRES-V1",
        },
        "results": {},
    }

    for index_key in INDICES:
        report["results"][index_key] = {
            "snapshot": _validate_snapshot(index_key),
            "spot_candles": _validate_spot_candles(index_key, trade_date),
            "futures_volume": _validate_futures_volume(index_key, trade_date),
            "options": _validate_options(index_key, trade_date),
            "spot_volume_integrity": _validate_spot_volume_is_not_fabricated(index_key, trade_date),
        }

    report["status"] = calculate_status(report["results"])
    report_path = OUT_DIR / f"{trade_date.isoformat()}_report.json"
    temp_path = report_path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    temp_path.replace(report_path)

    lines = [f"📊 *EC2 DATA VALIDATION: {report['status']}*", f"Date: `{trade_date.isoformat()}`"]
    for index_key, checks in report["results"].items():
        issues = [
            f"{name}: {issue}"
            for name, result in checks.items()
            for issue in result.get("issues", [])
        ]
        counts = (
            f"spot={checks['spot_candles']['count']} "
            f"futures={checks['futures_volume']['count']} "
            f"options={checks['options']['count']}"
        )
        lines.append(f"{index_key}: {counts}" + (f" | {'; '.join(issues[:3])}" if issues else " | OK"))
    message = "\n".join(lines)
    try:
        send_telegram_message(message[:3900])
    except Exception:
        logger.exception("Telegram validation notification failed")

    logger.info("Validation status=%s report=%s", report["status"], report_path)
    return report


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    result = run_validation(target)
    print(json.dumps(result, indent=2, default=str))
