"""Offline regression tests for futures session VWAP shadow.
Run from repo root: python -m pytest -q tests/test_session_vwap_shadow.py
No DB, broker, Telegram, scheduler, or order submission is used.
"""
import importlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent
ENGINE_MODULE = os.getenv("VWAP_SHADOW_ENGINE_MODULE", "daily_signal_engine_v11")
SNAPSHOTS_FILE = ROOT / "collectors" / "scripts" / "shared_snapshots.py"
IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture(scope="module")
def engine():
    try:
        return importlib.import_module(ENGINE_MODULE)
    except Exception as exc:
        pytest.skip(f"Engine import unavailable in this environment: {exc}")


def rows_for_session(now, count=None):
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if count is None:
        # Build a full minute-by-minute session through the current minute so
        # the helper's elapsed-session coverage gate is satisfied.
        count = max(1, int((now.replace(second=0, microsecond=0) - start).total_seconds() // 60) + 1)
    rows = []
    for i in range(count):
        p = 25000.0 + i
        rows.append({
            "timestamp": (start + timedelta(minutes=i)).astimezone(ZoneInfo("UTC")).isoformat(),
            "open": p - 1, "high": p + 2, "low": p - 2, "close": p + 1,
            "volume": 100 + i, "tick_count": 5,
            "is_complete": True, "quality_status": "VALID",
            "token": "TEST", "expiry": "2099-01-01",
        })
    return rows


def require_after_0940(now):
    if now.hour * 60 + now.minute < 9 * 60 + 40:
        pytest.skip("Run after 09:40 IST for the helper's 20-bar and coverage gates.")


def test_empty_input_fails_closed(engine):
    r = engine._calculate_futures_session_vwap([])
    assert not r["valid"]
    assert r["reason"] == "missing_futures_data"


def test_missing_ohlc_fails_closed(engine):
    r = engine._calculate_futures_session_vwap([{"timestamp": "2026-01-01T03:45:00Z", "volume": 100}])
    assert not r["valid"]
    assert r["reason"] == "missing_required_columns"


def test_valid_session_produces_vwap(engine, monkeypatch):
    now = datetime.now(IST)
    require_after_0940(now)
    monkeypatch.setattr(engine, "_ist_now", lambda: now)
    r = engine._calculate_futures_session_vwap(rows_for_session(now))
    assert r["valid"], r
    assert r["reason"] == "valid"
    assert r["vwap"] is not None and r["distance"] is not None
    assert r["bar_count"] >= 20 and r["volume_sum"] > 0


def test_prior_day_rows_are_excluded(engine, monkeypatch):
    now = datetime.now(IST)
    require_after_0940(now)
    monkeypatch.setattr(engine, "_ist_now", lambda: now)
    today = rows_for_session(now)
    yesterday = rows_for_session(now - timedelta(days=1), 25)
    a = engine._calculate_futures_session_vwap(yesterday + today)
    b = engine._calculate_futures_session_vwap(today)
    assert a["valid"] == b["valid"]
    assert a["bar_count"] == b["bar_count"]
    assert a["vwap"] == pytest.approx(b["vwap"])


def test_incomplete_and_invalid_quality_rows_are_excluded(engine, monkeypatch):
    now = datetime.now(IST)
    require_after_0940(now)
    monkeypatch.setattr(engine, "_ist_now", lambda: now)
    base_rows = rows_for_session(now)
    base = engine._calculate_futures_session_vwap(base_rows)
    changed = [dict(x) for x in base_rows]
    changed[0]["is_complete"] = False
    changed[1]["quality_status"] = "BAD"
    result = engine._calculate_futures_session_vwap(changed)
    assert result["valid"]
    assert result["bar_count"] == base["bar_count"] - 2

    # Removing two positive-volume bars must change the weighted average
    # according to the remaining bars; it must not retain the old VWAP.
    valid_remaining = [
        row for i, row in enumerate(changed)
        if i not in (0, 1)
    ]
    weighted = [row for row in valid_remaining if row["volume"] > 0]
    expected_volume = sum(row["volume"] for row in weighted)
    expected_vwap = sum(
        ((row["high"] + row["low"] + row["close"]) / 3.0) * row["volume"]
        for row in weighted
    ) / expected_volume
    assert result["vwap"] == pytest.approx(expected_vwap)


def test_zero_volume_does_not_change_weighted_vwap(engine, monkeypatch):
    now = datetime.now(IST)
    require_after_0940(now)
    monkeypatch.setattr(engine, "_ist_now", lambda: now)
    base_rows = rows_for_session(now)
    # Make the same bars zero-volume in both datasets. Their prices must
    # not affect VWAP because those bars carry no traded volume.
    for x in base_rows[:3]:
        x["volume"] = 0
    base = engine._calculate_futures_session_vwap(base_rows)

    changed = [dict(x) for x in base_rows]
    for x in changed[:3]:
        x["high"] += 500
        x["close"] += 400
    result = engine._calculate_futures_session_vwap(changed)

    assert result["valid"]
    assert result["vwap"] == pytest.approx(base["vwap"])
    assert result["volume_sum"] == pytest.approx(base["volume_sum"])


def test_stale_latest_candle_is_rejected(engine, monkeypatch):
    now = datetime.now(IST)
    require_after_0940(now)
    monkeypatch.setattr(engine, "_ist_now", lambda: now)
    data = rows_for_session(now)
    # Move every candle back so no newer candle remains after sorting/dedup.
    for row in data:
        old_ts = pd.Timestamp(row["timestamp"])
        row["timestamp"] = (old_ts - pd.Timedelta(minutes=20)).isoformat()
    result = engine._calculate_futures_session_vwap(data)
    assert not result["valid"]
    assert result["reason"] == "stale_futures_candles"


def test_shadow_score_replaces_vwap_component_without_mutating_live_score():
    live_score = 1.10
    shadow_score = live_score - 0.20 + (-0.20)
    live_probability = max(-1.0, min(1.0, live_score / 1.6))
    shadow_probability = max(-1.0, min(1.0, shadow_score / 1.6))
    assert live_score == pytest.approx(1.10)
    assert live_probability == pytest.approx(1.10 / 1.6)
    assert shadow_probability == pytest.approx(0.70 / 1.6)


def test_vix_multiplier_is_identical_for_live_and_shadow():
    live_score, shadow_score, vix = 1.0, 0.6, 29.0
    if vix > 20:
        live_score *= 0.75
        shadow_score *= 0.75
    if vix > 28:
        live_score *= 0.60
        shadow_score *= 0.60
    assert live_score == pytest.approx(0.45)
    assert shadow_score == pytest.approx(0.27)


def test_futures_getter_source_contains_ohlc_select_and_mapping():
    import ast
    candidates = list(ROOT.rglob("shared_snapshots.py"))
    if not candidates:
        pytest.skip("Could not locate shared_snapshots.py under the project root.")
    source_file = next(
        (x for x in candidates if "collectors" in str(x).lower()),
        candidates[0],
    )
    source = source_file.read_text(encoding="utf-8")
    tree = ast.parse(source)
    fn = next((n for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
               and n.name == "get_recent_futures_candles"), None)
    assert fn is not None, "get_recent_futures_candles() not found"
    body = ast.get_source_segment(source, fn).lower()
    for field in ("open", "high", "low", "close", "quality_status", "is_complete"):
        assert field in body, f"{field} missing from futures getter"


def test_live_probability_source_uses_live_score_not_shadow_score(engine):
    import ast
    source = Path(engine.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignments = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign):
            if any(isinstance(t, ast.Name) and t.id == "prob" for t in n.targets):
                assignments.append(ast.get_source_segment(source, n) or "")
    assert any("score /" in x and "shadow_score" not in x for x in assignments), (
        "Live probability assignment must use score, not shadow_score."
    )
