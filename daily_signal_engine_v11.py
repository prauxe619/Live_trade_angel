"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   BLACK — daily_signal_engine.py  v11.0  (Ultimate Hybrid — merged candle validation)                 ║
║                                                                              ║
║  WHAT'S IN v11.0 (Best of v8 + v10):                                        ║
║                                                                              ║
║  FROM v8 (kept / restored):                                                  ║
║   ✅ ISOLATED MEMORY PATH — data/signal_engine_v11 (no conflicts)           ║
║   ✅ ZERO-TO-HERO DETECTOR — 0DTE expiry plays with 150% target             ║
║   ✅ JACKPOT MULTI-TARGET — 3-stage target ladder for big moves             ║
║   ✅ REVERSAL AUTO-CLOSE — old trade closed when direction flips            ║
║   ✅ CHOP IRON CONDOR advisory to VIP on ranging days                       ║
║   ✅ PERSONAL MODE — one Telegram channel, trade entry/final exit only     ║
║   ✅ FREE/PROMOTIONAL POSTS DISABLED                                       ║
║   ✅ Persistent state + open position restore on restart                    ║
║                                                                              ║
║  FROM v10 / v9 (kept / upgraded):                                            ║
║   ✅ VOLUME SURGE FILTER — signal only on >= 1.2× 20-bar avg vol            ║
║      (flat-volume bypass so index spot data never gets stuck)               ║
║   ✅ CANDLE METRICS LOGGED; DIRECTION GUARD DISABLED BY STRATEGY CHOICE      ║
║      NOW enforced inside StrategyEngine.sniper() so it applies everywhere  ║
║   ✅ PCR + GEX DOUBLED WEIGHT — 0.30 / 0.20 (institutional data matters)   ║
║   ✅ ATOMIC CLOSE GUARD — TargetMonitor, force_close & scan can never       ║
║      double-fire exit messages (v9 bug fix, now in all close paths)        ║
║   ✅ LOT SIZE ADVISORY — VIX > 20 → "trade 1 lot today" in signal msg      ║
║   ✅ LIVE CONTINUOUS SCAN — every 5 min between scheduled windows,          ║
║      IMPROVED: only runs if snapshot age < 60s (stale data guard added)    ║
║      threshold 0.55 (higher bar than scheduled 0.40)                       ║
║      labelled "⚡ LIVE DETECTION" so VIPs know algo watches all day        ║
║                                                                              ║
║  NEW IN v11.0:                                                               ║
║   ✅ CANDLE METRICS AVAILABLE — entry-direction guard intentionally off    ║
║      to avoid filtering out otherwise valid scheduled/live signals         ║
║   ✅ STALE SNAPSHOT GUARD for live scan — live check skips any index        ║
║      whose snapshot file is > 60 seconds old (prevents stale-data fires)  ║
║   ✅ SIGNAL LOG PATH FIXED — v11 uses its own isolated log file             ║
║   ✅ FREE SL HIT MESSAGE — v10 had a cleaner free_sl_hit format; kept       ║
║   ✅ CLEANER JACKPOT HIT FORMAT — v10's compact % gain display used        ║
║   ✅ CLEANER TRAIL ALERT — v10's "gain now" running pts line used          ║
║                                                                              ║
║  SCAN SCHEDULE (IST):                                                        ║
║   09:09  Pre-market bias scan (Telegram notification suppressed)           ║
║   09:25  Force close overnight swings                                       ║
║   09:45  Golden window 1 — morning breakout                                 ║
║   10:30  Golden window 2 — trend continuation                              ║
║   11:30  Pre-lunch scan                                                     ║
║   13:15  Golden window 3 — afternoon volume return                         ║
║   14:00  Final intraday push                                                ║
║   14:30  Swing window only                                                  ║
║   14:45  Pre-close checks (Telegram notification suppressed)               ║
║   15:25  Force close intraday                                               ║
║   15:30  EOD accounting (Telegram summary suppressed)                     ║
║   + Every 5 min (live sniper check between windows, prob >= 0.55)          ║
║                                                                              ║
║  ENV VARS:                                                                   ║
║   SIGNAL_BOT_TOKEN, TRADING_TELEGRAM_CHAT_ID (or VIP_CHANNEL_ID fallback) ║
║   ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD, ANGEL_TOTP      ║
║   PAYMENT_LINK_TRIAL/WEEKLY/MONTHLY/YEARLY                                  ║
║                                                                              ║
║  RUN:                                                                        ║
║   python daily_signal_engine_v11.py              # scheduler mode          ║
║   python daily_signal_engine_v11.py --scan-now   # immediate stealth scan  ║
║   python daily_signal_engine_v11.py --test       # offline self-test       ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations
import math
from xml.dom.domreg import registered
import aiohttp
import asyncio
import os, sys, json, time, random, queue, logging, threading
import warnings
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import sqlite3
from trading_live.shared_store import read_snapshot as read_shared_snapshot, _connect as _shared_db_connect

warnings.filterwarnings("ignore")
load_dotenv()
IST = ZoneInfo("Asia/Kolkata")

def _ist_now() -> datetime:
    return datetime.now(IST)

def _calculate_futures_session_vwap(futures_rows):
    """
    Shadow-only conventional session VWAP.

    Uses:
        Typical Price = (High + Low + Close) / 3

        Session VWAP =
            Sum(Typical Price * Volume) / Sum(Volume)

    Only current-session, completed, valid futures candles.
    """

    result = {
        "valid": False,
        "reason": "missing_futures_data",
        "vwap": None,
        "distance": None,
        "direction": None,
        "latest_close": None,
        "latest_timestamp": None,
        "token": None,
        "expiry": None,
        "bar_count": 0,
        "volume_sum": 0.0,
        "coverage_pct": 0.0,
        "valid_bar_count": 0,
        "positive_volume_bar_count": 0,
    }

    try:
        if not futures_rows:
            return result

        df = pd.DataFrame(futures_rows)

        required = [
            "timestamp", "open", "high", "low", "close",
            "volume", "tick_count", "is_complete", "quality_status"
        ]

        if not all(col in df.columns for col in required):
            result["reason"] = "missing_required_columns"
            return result

        df["timestamp"] = pd.to_datetime(
            df["timestamp"], errors="coerce", utc=True
        )

        for col in ["open", "high", "low", "close", "volume", "tick_count"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=required[:7])

        df = df[
            (df["open"] > 0)
            & (df["high"] > 0)
            & (df["low"] > 0)
            & (df["close"] > 0)
            & (df["volume"] >= 0)
            & (df["tick_count"] >= 3)
            & (df["high"] >= df[["open", "low", "close"]].max(axis=1))
            & (df["low"] <= df[["open", "high", "close"]].min(axis=1))
            & df["is_complete"].eq(True)
            & df["quality_status"].astype(str).str.upper().eq("VALID")
        ].copy()

        if df.empty:
            result["reason"] = "no_valid_completed_candles"
            return result

        df = df.sort_values("timestamp")
        df = df.drop_duplicates("timestamp", keep="last")

        df["timestamp_ist"] = df["timestamp"].dt.tz_convert(IST)

        now_ist = _ist_now()
        session_date = now_ist.date()

        # Current trading session only.
        session_minutes = (
            df["timestamp_ist"].dt.hour * 60
            + df["timestamp_ist"].dt.minute
        )

        df = df[
            (df["timestamp_ist"].dt.date == session_date)
            & (session_minutes >= 9 * 60 + 15)
            & (df["timestamp_ist"] <= now_ist)
        ].copy()

        if df.empty:
            result["reason"] = "no_current_session_candles"
            return result

        latest = df.iloc[-1]

        latest_ts = latest["timestamp"]
        latest_age = (
            pd.Timestamp.now(tz="UTC") - latest_ts
        ).total_seconds()

        max_age = max(
            30.0,
            float(os.getenv("CANDLE_MAX_AGE_SECONDS", "180"))
        )

        if latest_age < -5 or latest_age > max_age:
            result["reason"] = "stale_futures_candles"
            return result

        # Session history should begin close to the exchange open.
        first_time = df.iloc[0]["timestamp_ist"]

        if first_time.hour * 60 + first_time.minute > 9 * 60 + 20:
            result["reason"] = "incomplete_session_history"
            return result

        # Only positive traded volume contributes to VWAP.
        # Only positive-volume candles contribute to the VWAP calculation.
        weighted = df[df["volume"] > 0].copy()

        if len(weighted) < 20:
            result["reason"] = "insufficient_valid_volume_bars"
            return result

        # Keep the existing expected_bars calculation here.
        # It must be defined before the coverage calculation.

        valid_bar_count = len(df)
        expected_bars = max(
            1,
            int(
                (
                    latest["timestamp_ist"]
                    - pd.Timestamp(
                        now_ist.replace(
                            hour=9, minute=15, second=0, microsecond=0
                        )
                    )
                ).total_seconds() / 60
            ) + 1
        )

        valid_bar_count = len(df)
        positive_volume_bar_count = len(weighted)
        coverage = min(1.0, valid_bar_count / expected_bars)

        if coverage < 0.80:
            result["reason"] = "insufficient_session_coverage"
            return result

        typical_price = (
            weighted["high"]
            + weighted["low"]
            + weighted["close"]
        ) / 3.0

        volume_sum = float(weighted["volume"].sum())

        if not math.isfinite(volume_sum) or volume_sum <= 0:
            result["reason"] = "invalid_total_volume"
            return result

        session_vwap = float(
            (typical_price * weighted["volume"]).sum() / volume_sum
        )

        latest_close = float(latest["close"])
        distance = latest_close - session_vwap

        if not math.isfinite(session_vwap) or session_vwap <= 0:
            result["reason"] = "invalid_session_vwap"
            return result

        result.update({
            "valid": True,
            "reason": "valid",
            "vwap": session_vwap,
            "distance": distance,
            "direction": "BULLISH" if distance > 0 else "BEARISH",
            "latest_close": latest_close,
            "latest_timestamp": latest_ts.isoformat(),
            "token": str(latest.get("token", "")),
            "expiry": str(latest.get("expiry", "")),
            "bar_count": valid_bar_count,
            "valid_bar_count": valid_bar_count,
            "positive_volume_bar_count": positive_volume_bar_count,
            "volume_sum": volume_sum,
            "coverage_pct": round(coverage * 100, 2),
        })

        return result

    except Exception as exc:
        _log.exception("[VWAP SHADOW] Calculation failed: %s", exc)
        result["reason"] = "calculation_exception"
        return result


def _persist_vwap_shadow_observation(
    symbol: str,
    candles: pd.DataFrame,
    snap: dict,
    rolling_vwap: float,
    rolling_distance: float,
    live_probability: float,
    live_signal: str,
    shadow_probability,
    shadow_signal: str,
    shadow: dict,
) -> None:
    """
    Persist one rolling-VWAP vs futures-session-VWAP observation.

    Uses the existing shared PostgreSQL connection.
    Persistence failures must never change trading decisions.
    """

    def _num(value):
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (TypeError, ValueError, OverflowError):
            return None

    try:
        observed_at = _ist_now()

        # Latest spot candle timestamp.
        candle_ts = None
        if (
            isinstance(candles, pd.DataFrame)
            and not candles.empty
            and "timestamp" in candles.columns
        ):
            parsed_ts = pd.to_datetime(
                candles["timestamp"].iloc[-1],
                errors="coerce",
                utc=True,
            )
            if not pd.isna(parsed_ts):
                candle_ts = parsed_ts.to_pydatetime()

        # Existing rolling VWAP direction.
        rolling_direction = (
            "BULLISH" if rolling_distance > 0 else "BEARISH"
        )

        shadow_valid = bool(shadow.get("valid"))

        session_direction = shadow.get("direction")

        agreement = (
            rolling_direction == session_direction
            if shadow_valid and session_direction
            else None
        )

        session_vwap = _num(shadow.get("vwap"))
        session_distance = _num(shadow.get("distance"))
        futures_latest_close = _num(shadow.get("latest_close"))

        spot_price = _num(snap.get("spot"))

        # Important: retain both futures-based and spot-based distances.
        # Futures VWAP and spot index prices have a basis difference.
        session_spot_distance = (
            spot_price - session_vwap
            if spot_price is not None and session_vwap is not None
            else None
        )

        futures_latest_timestamp = None
        raw_futures_ts = shadow.get("latest_timestamp")

        if raw_futures_ts:
            parsed_futures_ts = pd.to_datetime(
                raw_futures_ts,
                errors="coerce",
                utc=True,
            )
            if not pd.isna(parsed_futures_ts):
                futures_latest_timestamp = (
                    parsed_futures_ts.to_pydatetime()
                )

        with _shared_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO vwap_shadow_observations (
                        observed_at,
                        session_date,
                        symbol,
                        spot_price,
                        spot_candle_timestamp,
                        rolling_vwap,
                        rolling_distance,
                        rolling_direction,
                        session_vwap,
                        session_distance,
                        session_direction,
                        session_spot_distance,
                        futures_latest_close,
                        futures_latest_timestamp,
                        futures_token,
                        futures_expiry,
                        futures_volume_sum,
                        futures_valid_bars,
                        futures_positive_volume_bars,
                        futures_coverage_pct,
                        shadow_valid,
                        shadow_reason,
                        vwap_agreement,
                        live_probability,
                        shadow_probability,
                        live_signal,
                        shadow_signal,
                        india_vix
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    """,
                    (
                        observed_at,
                        observed_at.date(),
                        symbol,
                        spot_price,
                        candle_ts,
                        _num(rolling_vwap),
                        _num(rolling_distance),
                        rolling_direction,
                        session_vwap,
                        session_distance,
                        session_direction,
                        _num(session_spot_distance),
                        futures_latest_close,
                        futures_latest_timestamp,
                        shadow.get("token"),
                        shadow.get("expiry"),
                        _num(shadow.get("volume_sum")),
                        shadow.get("valid_bar_count"),
                        shadow.get("positive_volume_bar_count"),
                        _num(shadow.get("coverage_pct")),
                        shadow_valid,
                        shadow.get("reason"),
                        agreement,
                        _num(live_probability),
                        _num(shadow_probability),
                        live_signal,
                        shadow_signal,
                        _num(snap.get("vix")),
                    ),
                )

        _log.info(
            "[VWAP SHADOW DB SAVED] %s | valid=%s | "
            "agreement=%s | live=%.4f | shadow=%s",
            symbol,
            shadow_valid,
            agreement,
            float(live_probability),
            (
                f"{float(shadow_probability):.4f}"
                if shadow_probability is not None
                else "UNAVAILABLE"
            ),
        )

    except Exception:
        # Historical analytics must never interrupt the signal engine.
        _log.exception(
            "[VWAP SHADOW DB SAVE FAILED] %s",
            symbol,
        )


# Number of exchange lots per strategy trade. cfg["lot"] below is the
# contract units in one lot; it is NOT the number of lots. Start with 1.
TRADE_LOTS = int(os.getenv("TRADE_LOTS", "1"))
if TRADE_LOTS < 1:
    raise ValueError("TRADE_LOTS must be an integer >= 1")



def _persist_vwap_shadow_collection(
    index_key: str,
    candles: pd.DataFrame,
    shadow: dict,
) -> None:
    """
    Persist independent futures-session VWAP collection observations.

    One row per index per minute.
    A valid observation cannot be overwritten by an invalid one.
    Analytics-only: never affects trading decisions.
    """

    def _num(value):
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (TypeError, ValueError, OverflowError):
            return None

    def _integer(value):
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError, OverflowError):
            return None

    try:
        if not isinstance(shadow, dict):
            shadow = {}

        observed_at = _ist_now()
        observation_minute = observed_at.replace(
            second=0,
            microsecond=0,
        )

        spot_price = None

        if (
            isinstance(candles, pd.DataFrame)
            and not candles.empty
            and "close" in candles.columns
        ):
            spot_price = _num(candles["close"].iloc[-1])

        futures_latest_timestamp = None
        raw_timestamp = shadow.get("latest_timestamp")

        if raw_timestamp:
            parsed = pd.to_datetime(
                raw_timestamp,
                errors="coerce",
                utc=True,
            )
            if not pd.isna(parsed):
                futures_latest_timestamp = parsed.to_pydatetime()

        shadow_valid = bool(shadow.get("valid"))

        with _shared_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.vwap_shadow_collection_observations
                    AS current (
                        observed_at,
                        session_date,
                        observation_minute,
                        symbol,
                        spot_price,
                        session_vwap,
                        session_distance,
                        session_direction,
                        futures_latest_close,
                        futures_latest_timestamp,
                        futures_token,
                        futures_expiry,
                        futures_volume_sum,
                        futures_valid_bars,
                        futures_positive_volume_bars,
                        futures_coverage_pct,
                        shadow_valid,
                        shadow_reason
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (
                        session_date,
                        symbol,
                        observation_minute
                    )
                    DO UPDATE SET
                        observed_at = EXCLUDED.observed_at,
                        spot_price = EXCLUDED.spot_price,
                        session_vwap = EXCLUDED.session_vwap,
                        session_distance = EXCLUDED.session_distance,
                        session_direction = EXCLUDED.session_direction,
                        futures_latest_close = EXCLUDED.futures_latest_close,
                        futures_latest_timestamp = EXCLUDED.futures_latest_timestamp,
                        futures_token = EXCLUDED.futures_token,
                        futures_expiry = EXCLUDED.futures_expiry,
                        futures_volume_sum = EXCLUDED.futures_volume_sum,
                        futures_valid_bars = EXCLUDED.futures_valid_bars,
                        futures_positive_volume_bars =
                            EXCLUDED.futures_positive_volume_bars,
                        futures_coverage_pct = EXCLUDED.futures_coverage_pct,
                        shadow_valid = EXCLUDED.shadow_valid,
                        shadow_reason = EXCLUDED.shadow_reason
                    WHERE
                        NOT current.shadow_valid
                        OR EXCLUDED.shadow_valid
                    """,
                    (
                        observed_at,
                        observed_at.date(),
                        observation_minute,
                        str(index_key),
                        spot_price,
                        _num(shadow.get("vwap")),
                        _num(shadow.get("distance")),
                        shadow.get("direction"),
                        _num(shadow.get("latest_close")),
                        futures_latest_timestamp,
                        shadow.get("token"),
                        shadow.get("expiry"),
                        _num(shadow.get("volume_sum")),
                        _integer(
                            shadow.get(
                                "valid_bar_count",
                                shadow.get("bar_count"),
                            )
                        ),
                        _integer(shadow.get("positive_volume_bar_count")),
                        _num(shadow.get("coverage_pct")),
                        shadow_valid,
                        shadow.get("reason"),
                    ),
                )

        _log.info(
            "[VWAP SHADOW COLLECTION SAVED] %s | valid=%s | "
            "vwap=%s | coverage=%s%%",
            index_key,
            shadow_valid,
            shadow.get("vwap"),
            shadow.get("coverage_pct"),
        )

    except Exception:
        _log.exception(
            "[VWAP SHADOW COLLECTION SAVE FAILED] %s",
            index_key,
        )


def _display_trade_lots(index_key: str) -> int:
    """
    Lot count for engine P&L estimates.
    Live mode uses per-index allocation.
    Paper mode retains the existing TRADE_LOTS setting.
    """
    if os.getenv("TRADING_MODE", "paper").strip().lower() == "live":
        from trading_live.lot_config import configured_live_lots
        return configured_live_lots(
            index_key,
            fallback=TRADE_LOTS,
        )

    return TRADE_LOTS
# ── Optional Angel One SDK ────────────────────────────────────────────────────
try:
    from SmartApi import SmartConnect
    import pyotp
    _ANGEL_OK = True
except ImportError:
    _ANGEL_OK = False

# 🔥 FIX: This saves all logs to a permanent file AND prints to your screen
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("engine_errors.log", mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
_log = logging.getLogger("black.signal")


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

def _first_valid_env(*names):
    for name in names:
        value = os.getenv(name, "").strip()
        if value and value.count(":") == 1 and not value.startswith("("):
            return value
    return ""

# Use the existing signal-engine bot and exactly the two configured destinations.
# Single Telegram destination: channel only.
SIGNAL_BOT_TOKEN = os.getenv("SIGNAL_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("LIVE_BOT_CHANNEL_ID", "").strip()
TELEGRAM_CHAT_IDS = (TELEGRAM_CHAT_ID,) if TELEGRAM_CHAT_ID else ()

TRADING_TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
VIP_CHANNEL_ID = TELEGRAM_CHAT_ID

# Compatibility aliases: the legacy single-chat name now means the channel only.
# Compatibility aliases: the legacy single-chat name now means the channel only.
TRADING_TELEGRAM_CHAT_ID = TELEGRAM_CHAT_ID
VIP_CHANNEL_ID = TELEGRAM_CHAT_ID

PAYMENT_LINKS = {
    "trial"  : os.getenv("PAYMENT_LINK_TRIAL",   "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd"),
    "weekly" : os.getenv("PAYMENT_LINK_WEEKLY",  "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd"),
    "monthly": os.getenv("PAYMENT_LINK_MONTHLY", "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd"),
    "yearly" : os.getenv("PAYMENT_LINK_YEARLY",  "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd"),
}

# ── Angel One Credentials ─────────────────────────────────────────────────────
SIGNAL_API_KEY   = os.getenv("ANGEL_API_KEY", "")
SIGNAL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID", "")
SIGNAL_PASSWORD  = os.getenv("ANGEL_PASSWORD", "")
SIGNAL_TOTP      = os.getenv("ANGEL_TOTP", "")

SNAPSHOTS = {
    "NIFTY"    : Path("data/live/live_snapshot.json"),
    "BANKNIFTY": Path("data/live/banknifty_live_snapshot.json"),
    "FINNIFTY" : Path("data/live/finnifty_live_snapshot.json"),
    "SENSEX"   : Path("data/live/sensex_live_snapshot.json"),
}

# 🔥 MULTI-INDEX HIGH-WIN-RATE CONFIGURATION
INDICES = {
    "NIFTY"    : {"token": "26000",  "exchange": "NSE", "label": "Nifty 50",   "emoji": "🔵", "strike_gap": 50,  "lot": 65, "min_prob": 0.60},
    "BANKNIFTY": {"token": "26009",  "exchange": "NSE", "label": "Bank Nifty", "emoji": "🟠", "strike_gap": 100, "lot": 30, "min_prob": 0.68},
    "FINNIFTY" : {"token": "26037",  "exchange": "NSE", "label": "Fin Nifty",  "emoji": "💜", "strike_gap": 50,  "lot": 60, "min_prob": 0.62},
    "SENSEX"   : {"token": "999901", "exchange": "BSE", "label": "Sensex",     "emoji": "🔴", "strike_gap": 100, "lot": 20, "min_prob": 0.68},
}

MOONBAG_PREMIUM_SL_STEP = {
    "BANKNIFTY": 60.0,
    "SENSEX": 70.0,
    "NIFTY": 25.0,
    "FINNIFTY": 25.0,
}
VIX_TOKEN = "26017"

# ── Per-index option premium SL/TP floors (in ₹ premium points) ───────────────
INDEX_OPTION_FLOORS = {
    #                sniper_sl  sniper_tp  scalp_sl  scalp_tp
    "NIFTY"    : {"sniper_sl": 35,  "sniper_tp": 90,  "scalp_sl": 30, "scalp_tp": 60},
    "BANKNIFTY": {"sniper_sl": 75,  "sniper_tp": 180, "scalp_sl": 60, "scalp_tp": 120},
    "FINNIFTY" : {"sniper_sl": 35,  "sniper_tp": 90,  "scalp_sl": 38, "scalp_tp": 75},
    "SENSEX"   : {"sniper_sl": 120, "sniper_tp": 300, "scalp_sl": 52, "scalp_tp": 100},
}

# Swing trades need wider buffers for overnight holds (~1.8× intraday floors)
INDEX_SWING_FLOORS = {
    "NIFTY"    : {"sl_pts": 85,  "tp_pts": 190},
    "BANKNIFTY": {"sl_pts": 170, "tp_pts": 380},
    "FINNIFTY" : {"sl_pts": 105, "tp_pts": 230},
    "SENSEX"   : {"sl_pts": 150, "tp_pts": 325},
}
SIGNAL_STATE_PG_ENABLED = os.getenv("SIGNAL_STATE_PG_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
_signal_persistence_failed = threading.Event()

# ─────────────────────────────────────────────────────────────
# STRONG BREAKOUT — STRICT RETEST CONFIGURATION
# ─────────────────────────────────────────────────────────────

STRONG_BK_MIN_BARS = 20
STRONG_BK_VOLUME_MULT = 1.70
STRONG_BK_MIN_BODY_RATIO = 0.65
STRONG_BK_MAX_EXTENSION_ATR = 0.75
STRONG_BK_RETEST_ATR = 0.20
STRONG_BK_RETEST_MAX_AGE_SEC = 900
STRONG_BK_CONFIRM_BODY_RATIO = 0.45
STRONG_BK_MIN_ATR_PCT = 0.0005
STRONG_BK_MIN_BREAKOUT_ATR = 0.15

def _vix_sl_multiplier(vix: float) -> float:
    """Return SL/TP scaling factor based on India VIX."""
    if vix >= 28: return 1.80
    if vix >= 24: return 1.55
    if vix >= 20: return 1.35
    if vix >= 17: return 1.15
    return 1.0


def _afternoon_prob_threshold() -> float:
    """
    Raises the conviction bar in the afternoon to suppress noise trades.
      09:20–12:59 → 0.55 (new base normal)
      13:00–13:59 → 0.60 (afternoon slowdown)
      14:00–14:29 → 0.65 (late day chop guard)
      14:30–15:00 → 0.70 (extreme high bar, scalp blocked)
    """
    now_h = _ist_now().hour + _ist_now().minute / 60
    if   now_h >= 14.50: return 0.70
    elif now_h >= 14.00: return 0.65
    elif now_h >= 13.00: return 0.60
    return 0.55  # <-- Matches your new SNIPER_MIN_PROB


# ── Intraday hard cutoffs ─────────────────────────────────────────────────────
NO_NEW_SCALP_HM    = "13:30"   # scalp window closes (needs 2h to run)
NO_NEW_SNIPER_HM   = "14:35"   # no new snipers after 2:30 PM
INTRADAY_CUTOFF_HM = "15:00"   # HARD BLOCK — NO new signals of any kind

# ── Strategy thresholds ────────────────────────────────────────────────────────
SNIPER_MIN_PROB     = 0.55
SCALP_MIN_PROB      = 0.35
CHOP_PROB_CEILING   = 0.30
SWING_MIN_SCORE     = 4
SWING_MIN_PROB      = 0.70
SNIPER_COOLDOWN_MIN = 45
SWING_NO_TRAIL      = 99999   # sentinel: swing trades do not use trailing SL
BREAKOUT_LOOKBACK   = 25

# ── v10/v11: Live continuous scan ─────────────────────────────────────────────
LIVE_SCAN_PROB_THRESHOLD = 0.65   # <-- Raised to 0.65 to stay stricter than the new 0.55 base
LIVE_SCAN_INTERVAL_SEC   = 300    # check every 5 minutes
LIVE_SNAPSHOT_MAX_AGE_S  = max(1, int(os.getenv("LIVE_SNAPSHOT_MAX_AGE_SECONDS", "120")))  # paper validation: 120s

# ── Scan schedule (HH:MM IST) ─────────────────────────────────────────────────
SCANS = {
    "pre_market":  "09:11",   # Ensures NSE pre-open equilibrium spot has settled
    "swing_close": "09:25",

    # Prime Window 1: Opening Range Breakout & Morning Trend (+1 min candle-commit buffer)
    "scan_936":    "09:36",   # 09:30–09:35 bar committed + 20 bars ready for session VWAP
    "scan_1006":   "10:06",   # 10:00–10:05 bar committed
    "scan_1046":   "10:46",   # 10:40–10:45 bar committed

    # Prime Window 2: Afternoon Institutional Flow & 0DTE Gamma (+1 min candle-commit buffer)
    "scan_1336":   "13:36",   # Post-European-open 13:30–13:35 bar committed
    "scan_1401":   "14:01",   # 13:55–14:00 bar committed
    "scan_1431":   "14:31",   # 14:25–14:30 bar committed (final entry scan)

    "pre_close":   "14:45",
    "force_close": "15:12",   # Exits ahead of 15:15 CAS freeze & broker MIS square-offs
    "eod":         "15:30",
}

# ── v11: Isolated memory path (no conflicts with v8/v10) ──────────────────────
DATA_DIR    = Path("data/signal_engine_v11")
DATA_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_LOG = DATA_DIR / "signals_log_v11.json"


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM & AI — TRUE ASYNC I/O ARCHITECTURE
# ══════════════════════════════════════════════════════════════════════════════

_tq: queue.Queue = queue.Queue(maxsize=500)

async def _async_tg_post(session, token: str, chat_id: str, text: str) -> bool:
    if not token or not chat_id:
        _log.error("Telegram blocked: missing token or chat ID")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }

    for attempt in range(1, 4):
        try:
            async with session.post(url, data=data, timeout=12) as response:
                result = await response.json(content_type=None)

                if response.status == 200 and result.get("ok") is True:
                    _log.info("TELEGRAM_SENT: message delivered successfully")
                    return True

                description = result.get("description", str(result))
                _log.error(
                    "TELEGRAM_FAILED attempt=%s status=%s error=%s",
                    attempt, response.status, description
                )

                # Retry only temporary failures.
                if response.status == 429 or response.status >= 500:
                    await asyncio.sleep(2 * attempt)
                    continue

                return False

        except Exception as exc:
            _log.exception("Telegram attempt %s failed: %s", attempt, exc)
            await asyncio.sleep(2 * attempt)

    return False

async def _async_tq_worker():
    """Dispatch queued Telegram messages without blocking executor threads."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                task = _tq.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            try:
                sent = await _async_tg_post(session, *task)
                if not sent:
                    _log.error("TELEGRAM_DELIVERY_FAILED: queued message was not delivered")
            except Exception as e:
                _log.error(f"TQ async worker: {e}")
            finally:
                _tq.task_done()



def _is_valid_entry_window(now_hm: str) -> bool:
    """Matches Prime Window 1 (09:36-10:46) and Prime Window 2 (13:36-14:31)."""
    return ("09:35" <= now_hm <= "11:05") or ("13:36" <= now_hm <= "14:35")

def _effective_prob_threshold(index_key: str, is_live: bool = False) -> float:
    """
    Applies time-of-day and live-scan conviction bumps on top of each index's base min_prob.
      Morning   (09:30–13:29): base (NIFTY 0.60, FINNIFTY 0.62, BNF/SENSEX 0.68)
      Afternoon (13:30–14:14): base + 0.03
      Late day  (14:15–14:35): base + 0.07
      Live 5-min ticks       : +0.03 stricter (floor at LIVE_SCAN_PROB_THRESHOLD)
    """
    base = float(INDICES.get(index_key, {}).get("min_prob", 0.60))
    now_h = _ist_now().hour + _ist_now().minute / 60

    if now_h >= 14.25:       # 14:15+
        thresh = base + 0.07
    elif now_h >= 13.50:     # 13:30–14:14
        thresh = base + 0.03
    else:
        thresh = base

    if is_live:
        thresh = max(LIVE_SCAN_PROB_THRESHOLD, thresh + 0.03)

    return round(min(0.85, thresh), 3)

def _tq_runner():
    asyncio.run(_async_tq_worker())

_tq_worker_lock = threading.Lock()
_tq_worker_started = False

def _ensure_tq_worker():
    """Start Telegram worker only when a valid notification is actually queued."""
    global _tq_worker_started
    if _tq_worker_started:
        return
    with _tq_worker_lock:
        if not _tq_worker_started:
            threading.Thread(target=_tq_runner, daemon=True, name="sig-tg-sender-async").start()
            _tq_worker_started = True

def _is_personal_trade_update(text: str) -> bool:
    """Allow only approved entries and final exit events."""
    if not isinstance(text, str) or not text.strip():
        return False

    first_line = text.strip().splitlines()[0].strip()

    allowed_headers = (
        ("JACKPOT TRADE",),
        ("LIVE DETECTION",),
        ("ZERO-TO-HERO (0DTE)",),
        ("JACKPOT", "ALL TARGETS HIT"),
        ("FINAL TARGET HIT",),
        ("REVERSAL EXIT",),
        ("STOP LOSS HIT",),
        ("TRAILED SL HIT",),
        ("INTRADAY FORCE CLOSE",),
        ("SWING FORCE CLOSE",),
        ("TRAIL SL",),
        ("MAX PROFIT LOCK",),
        ("NEXT-DAY SWING",),
        # Additional trading diagnostics
        ("STRONG BREAKOUT",),
        ("No Directional Setup",),
        ("SCAN —",),
        ("VIP END OF DAY SUMMARY",),
    )

    return any(
        all(term in first_line for term in group)
        for group in allowed_headers
    )
def _to_vip(text: str):
    """Queue every engine trade/decision notification to both configured chats."""
    if not isinstance(text, str) or not text.strip():
        return
    if not SIGNAL_BOT_TOKEN or len(TELEGRAM_CHAT_IDS) != 1:
        _log.error(
            "Trade alert blocked: SIGNAL_BOT_TOKEN and exactly one "
            "Telegram channel destination are required (found %d).",
            len(TELEGRAM_CHAT_IDS),
        )
        return

    queued = 0
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            _tq.put_nowait((SIGNAL_BOT_TOKEN, chat_id, text))
            queued += 1
        except queue.Full:
            _log.error("Telegram queue full; destination %s was not queued.", chat_id)

    if queued:
        _ensure_tq_worker()
    if queued != len(TELEGRAM_CHAT_IDS):
        _log.error(
            "Telegram fan-out incomplete: queued=%d destinations=%d",
            queued, len(TELEGRAM_CHAT_IDS),
        )


def _to_free(text: str):
    """Disable all free-channel notifications in personal-use mode."""
    return

def _snapshot_is_fresh(snap: dict, max_age_s: float) -> bool:
    try:
        age = float(snap.get("_age_s"))

        return (
            math.isfinite(age)
            and -2 <= age <= float(max_age_s)
        )

    except (TypeError, ValueError, OverflowError):
        return False

def _nearest_option_strike(spot: float, gap: int) -> int:
    """Return the nearest valid ATM strike using the index strike interval."""
    spot = float(spot)
    gap = int(gap)

    if not math.isfinite(spot) or spot <= 0:
        raise ValueError(f"Invalid spot price: {spot}")

    if gap <= 0:
        raise ValueError(f"Invalid strike gap: {gap}")

    return int(math.floor((spot / gap) + 0.5) * gap)

def _now()        -> str: return _ist_now().strftime("%d %b  %H:%M")
def _now_precise()-> str: return _ist_now().strftime("%d %b  %H:%M:%S")
def _today()      -> str: return _ist_now().strftime("%d %b %Y")
def _hhmm()       -> str: return _ist_now().strftime("%I:%M %p")

def _pay_block() -> str:
    return (
        f"\n\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"💎 <b>Join BLACK VIP Today</b>\n\n"
        f"🔹 <a href='{PAYMENT_LINKS['trial']}'>Trial ₹599 / 1 day</a>\n"
        f"🔸 <a href='{PAYMENT_LINKS['weekly']}'>Weekly ₹1,599</a>\n"
        f"🏆 <a href='{PAYMENT_LINKS['monthly']}'>Monthly ₹3,999</a>  ✦ <b>Most Popular</b>\n"
        f"👑 <a href='{PAYMENT_LINKS['yearly']}'>Yearly ₹29,999</a>\n\n"
        f"⚡ Instant Telegram access after payment\n"
        f"✅ UPI · Cards · Net Banking accepted\n"
        f"💬 Need help or payment failed? Contact @black_support6_bot\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"
    )

INSIGHTS = [
    "Discipline is the only edge that never expires.",
    "A bad day in chop is better than a forced loss.",
    "Premium selling in chop is income, not gambling.",
    "Consistency beats brilliance every single month.",
    "Your biggest loss is always the trade you forced.",
    "Risk management is not optional — it is the strategy.",
    "Chop days are savings days. Trending days are earning days.",
    "Patience is a position. Holding it is the hardest trade.",
    "The market rewards patience and punishes emotion.",
    "A trade without a stop loss is just hope with leverage.",
]

def _insight() -> str:
    return random.choice(INSIGHTS)

def _generate_ai_hook(index: str, direction: str, vix: float, alert_type: str = "target") -> str:
    fallbacks = {
        "target": "Retail provided the liquidity. VIPs took the profit. Simple math. 💰",
        "hero":   "0DTE ripped 150%. VIPs were already inside. You were still deciding. 🔥",
        "breakout": "Operators are moving right now. Don't be the one holding the bag. ⚡",
        "jackpot": "100+ points. Booked. Logged. Counted. You had the same market. 💸",
        "insight": "Patience is a position. The algo is waiting for high conviction. Don't force the trade. 🧘‍♂️"
    }

    if os.getenv("OLLAMA_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        return fallbacks.get(alert_type, fallbacks["target"])

    base_rule = (
        "Write in pure English. Tone: professional trader, confident, sharp. "
        "NO hashtags. NO cringe. Max 20 words total."
    )

    prompts = {
        "hero": f"{base_rule}\n\n{index} expiry rip in {direction}. VIPs caught it. Write a savage line mocking those who missed it. (🔥 or 🚀).",
        "breakout": f"{base_rule}\n\n{index} breakout {direction} loading. Write an urgent alert to be ready. (🚨 or ⚡).",
        "jackpot": f"{base_rule}\n\nBLACK algo bagged 100+ points on {index} {direction}. Write a flex line. (💸 or 🤑).",
        "target": f"{base_rule}\n\nBLACK algo hit target on {index} {direction}. Write a line on VIP liquidity. (🛑 or 💰).",
        "insight": f"{base_rule}\n\n{index} is in chop. Write a short, professional insight on why staying out is a trading edge."
    }

    async def fetch_hook():
        try:
            payload = {"model": "dolphin3:latest", "prompt": prompts.get(alert_type, prompts["target"]), "stream": False}
            async with aiohttp.ClientSession() as session:
                ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
            async with session.post(f"{ollama_url}/api/generate", json=payload, timeout=5) as r:
                    if r.status == 200:
                        result = await r.json()
                        text = result.get("response", "").strip().replace('"', "")
                        return text if text else fallbacks.get(alert_type, fallbacks["target"])
        except Exception as e:
            _log.warning(f"Ollama hook async failed: {e}")
        return fallbacks.get(alert_type, fallbacks["target"])

    try:
        return asyncio.run(fetch_hook())
    except Exception:
        return fallbacks.get(alert_type, fallbacks["target"])


# ══════════════════════════════════════════════════════════════════════════════
#  PERSISTENT SIGNAL LEDGER (UPGRADED TO SQLITE)
# ══════════════════════════════════════════════════════════════════════════════
import sqlite3

DB_PATH = DATA_DIR / "black_trading_ledger.db"

def _init_db():
    """Initializes the ultra-fast SQLite local database."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                data_json TEXT,
                status TEXT,
                date TEXT
            )
        """)
        # 🔥 UPGRADE: Added dedicated table for Engine State
        conn.execute("""
            CREATE TABLE IF NOT EXISTS engine_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT
            )
        """)
_init_db()

def _sync_json():
    """Silently exports the DB to your JSON file so your dashboards don't break."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("SELECT data_json FROM signals ORDER BY rowid").fetchall()
            records = [json.loads(r[0]) for r in rows]
            tmp = str(SIGNALS_LOG) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(records[-1000:], f, indent=4)
            os.replace(tmp, SIGNALS_LOG)
    except Exception as e:
        _log.warning(f"JSON Sync error: {e}")

def _append_signal_log(entry: dict) -> None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO signals (signal_id, data_json, status, date) VALUES (?, ?, ?, ?)",
                (entry["signal_id"], json.dumps(entry), entry["status"], entry["date"])
            )
        _sync_json()
    except Exception as e:
        _log.warning(f"DB insert error: {e}")

def _paper_ledger_enabled() -> bool:
    """Paper ledger is opt-in and only runs when all order-safety gates are off."""
    truthy = {"1", "true", "yes", "on"}
    return (
        os.getenv("PAPER_LEDGER_ENABLED", "false").strip().lower() in truthy
        and os.getenv("TRADING_MODE", "paper").strip().lower() == "paper"
        and os.getenv("TRADING_ENABLED", "false").strip().lower() not in truthy
        and os.getenv("ALLOW_ORDER_SUBMISSION", "false").strip().lower() not in truthy
    )

def _open_paper_ledger_trade(sig) -> None:
    """Record an engine signal as a simulated trade; never submits broker orders."""
    if not _paper_ledger_enabled():
        return
    try:
        from trading_live.paper_ledger import create_paper_trade
        quantity = int(sig.lot_size) * TRADE_LOTS
        if quantity <= 0 or float(sig.entry_premium) <= 0:
            _log.warning("Paper ledger skipped invalid entry for %s", sig.signal_id)
            return
        created = create_paper_trade(
            {
                "signal_id": sig.signal_id, "index": sig.index_key,
                "direction": sig.direction, "strike": sig.strike, "expiry": sig.expiry,
            },
            quantity=quantity, entry_price=float(sig.entry_premium),
            metadata={
                "source": "daily_signal_engine_v11", "signal_type": sig.signal_type,
                "lot_size_units": int(sig.lot_size), "trade_lots": TRADE_LOTS,
                "sl_price": float(sig.sl_price), "tp_price": float(sig.tp_price),
            },
        )
        if created:
            _log.info("Paper ledger ENTRY recorded: %s quantity=%s", sig.signal_id, quantity)
        else:
            raise RuntimeError(f"Paper ledger already contains signal_id {sig.signal_id}; refusing duplicate monitor registration")
    except Exception:
        _log.error("Paper ledger ENTRY failed for %s", sig.signal_id, exc_info=True)
        raise

def _close_paper_ledger_trade(signal_id: str, exit_price: float, outcome: str) -> None:
    """Close the matching simulated trade once; independent of SQLite log health."""
    if not _paper_ledger_enabled():
        return
    try:
        from trading_live.paper_ledger import close_paper_trade
        result = close_paper_trade(
            signal_id, exit_price=float(exit_price), exit_reason=str(outcome)
        )
        if result is None:
            raise RuntimeError(f"Paper ledger EXIT found no open trade: {signal_id}")
        else:
            _log.info(
                "Paper ledger EXIT recorded: %s pnl=₹%.2f quantity=%s",
                signal_id, result["pnl_rupees"], result["quantity"]
            )
    except Exception:
        _log.error("Paper ledger EXIT failed for %s", signal_id, exc_info=True)
        raise

def _update_signal_exit(signal_id: str, exit_price: float, pnl_pts: float, pnl_inr: float, outcome: str) -> None:
    exit_time = _ist_now().strftime("%Y-%m-%d %H:%M:%S")

    # Keep PostgreSQL signal state and paper ledger in one transaction whenever
    # both are enabled; never leave a half-closed pair on an ordinary DB failure.
    exit_data = {
        "exit_price": float(exit_price),
        "pnl_pts": round(float(pnl_pts), 2),
        "pnl_inr": round(float(pnl_inr), 2),
        "outcome": str(outcome),
        "exit_time": exit_time,
    }
    try:
        if SIGNAL_STATE_PG_ENABLED:
            if _paper_ledger_enabled():
                from trading_live.signal_state import close_signal_and_paper_trade
                close_signal_and_paper_trade(signal_id, exit_data, str(outcome))
            else:
                from trading_live.signal_state import close_signal_state
                close_signal_state(signal_id, exit_data)
        else:
            _close_paper_ledger_trade(signal_id, exit_price, outcome)
    except Exception:
        _signal_persistence_failed.set()
        _log.critical("PostgreSQL/paper-ledger close failed for %s; halting monitor for reconciliation",
                      signal_id, exc_info=True)
        raise

    # Only update local SQLite after PostgreSQL confirms the close.
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT data_json FROM signals WHERE signal_id=? AND status='open'",
                (signal_id,),
            ).fetchone()
            if row:
                data = json.loads(row[0])
                data.update({
                    "exit_price": float(exit_price),
                    "pnl_pts": round(float(pnl_pts), 2),
                    "pnl_inr": round(float(pnl_inr), 2),
                    "status": "closed",
                    "outcome": str(outcome),
                    "exit_time": exit_time,
                })
                conn.execute(
                    "UPDATE signals SET data_json=?, status='closed' WHERE signal_id=?",
                    (json.dumps(data), signal_id),
                )
        _sync_json()
    except Exception:
        if SIGNAL_STATE_PG_ENABLED:
            _signal_persistence_failed.set()
            _log.critical(
                "Local SQLite close failed for %s after PostgreSQL close",
                signal_id, exc_info=True,
            )
            raise
        _log.warning("DB update error for %s", signal_id, exc_info=True)


def _regime_label(regime: str) -> str:
    r = regime.upper()
    if "BULL" in r: return "📈 Bulls in control"
    if "BEAR" in r: return "📉 Bears in control"
    return "⚠️ Choppy — patience needed"


def _vix_label(vix: float) -> str:
    if vix < 14: return "🟢 Calm market — good for clean breakouts"
    if vix < 18: return "🟡 Moderate volatility — trade with sizing"
    return "🔴 High volatility — tighter stops today"


# ══════════════════════════════════════════════════════════════════════════════
#  FREE CHANNEL — 6 TIMED ENGAGEMENT POSTS
# ══════════════════════════════════════════════════════════════════════════════

def _free_post_morning(spots: dict, vix: float, regime: str = "—",
                       swing_open: bool = False, swing_symbol: str = "",
                       swing_side: str = "") -> None:
    swing_line = ""
    if swing_open and swing_symbol:
        de = "🟢" if "CE" in swing_side.upper() else "🔴"
        swing_line = (
            f"\n🌙 *Swing carry from yesterday:*\n"
            f"   {de} `{swing_symbol}` — VIP members holding this right now.\n"
        )

    index_lines = "\n".join(
        f"{INDICES[k]['emoji']} {INDICES[k]['label']} : `{v:.0f}`"
        for k, v in spots.items() if v > 0
    )

    today_str = _ist_now().strftime("%d %b %y").upper()
    hype_list = []
    _get_expiry("FINNIFTY", trade_type="ZERO_TO_HERO")
    _get_expiry("BANKNIFTY", trade_type="ZERO_TO_HERO")
    _get_expiry("NIFTY", trade_type="ZERO_TO_HERO")
    _get_expiry("SENSEX", trade_type="ZERO_TO_HERO")
    expiry_block = f"\n{chr(10).join(hype_list)}\n" if hype_list else "\n"

    _to_free(
        f"☀️ *Good Morning, Traders!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓️  *{_today()}* — Markets just opened!\n"
        f"{expiry_block}"
        f"{index_lines}\n"
        f"😨 India VIX  :  `{vix:.1f}`  →  {_vix_label(vix)}\n"
        f"📊 Market Feel :  {_regime_label(regime)}\n"
        f"{swing_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 BLACK is scanning every tick right now.\n"
        f"The moment a high-probability setup fires —\n"
        f"*VIP members get the alert instantly.*\n\n"
        f"Free members? They'll see it after the trade closes. 😅\n"
        f"Don't let that be you today 👇"
        + _pay_block()
    )


def _free_post_mid_morning(signals_fired: int, vip_pnl_so_far: float, spot: float) -> None:
    if signals_fired > 0 and vip_pnl_so_far > 0:
        hook = (
            f"🔥 *It's already happening.*\n\n"
            f"BLACK fired *{signals_fired} signal{'s' if signals_fired > 1 else ''}* since open.\n"
            f"VIP members are sitting on `₹{vip_pnl_so_far:,.0f}` in open profit.\n\n"
            f"You're reading this. They're counting gains. 🤑"
        )
    elif signals_fired > 0:
        hook = (
            f"⚡ *{signals_fired} signal{'s' if signals_fired > 1 else ''} fired since open.*\n\n"
            f"BLACK sent live CE/PE alerts to VIP members.\n"
            f"Entry price, stop loss, target — everything delivered.\n\n"
            f"Will you be ready for the next one, or watch from sidelines again?"
        )
    else:
        hook = (
            f"⏳ *Market is building up.*\n\n"
            f"BLACK has been watching every candle, every tick.\n"
            f"The setup hasn't fired yet — but when it does,\n"
            f"*VIP members will be the first to know.*\n\n"
            f"Will you be ready, or will you watch from the sidelines again?"
        )

    _to_free(
        f"👀 *BLACK VIP Update — {_hhmm()}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{hook}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *What VIP members get that you don't:*\n\n"
        f"⚡ Live entry alert with exact strike price\n"
        f"🛑 Stop Loss level — capital always protected\n"
        f"🎯 Two targets — book profits on the way up\n"
        f"🛡️ Trail SL alert — locks in profit mid-trade\n"
        f"📊 Real-time exit alert — no guessing when to leave\n\n"
        f"*Start with just ₹599 for a full day trial.* No risk. 👇"
        + _pay_block()
    )


def _free_post_lunch(trades_done: int, morning_pnl: float,
                     best_trade_pts: float, still_in_trade: bool) -> None:
    if trades_done > 0 and morning_pnl > 0:
        result_text = (
            f"✅ *{trades_done} trade{'s' if trades_done > 1 else ''} closed* this morning.\n"
            f"💰 VIP members pocketed `₹{morning_pnl:,.0f}` before lunch.\n"
            + (f"🏆 Best single trade: `{best_trade_pts:.0f} pts`\n\n" if best_trade_pts > 0 else "\n")
            + f"While the market rests — they've already made their day."
        )
    elif trades_done > 0 and morning_pnl <= 0:
        result_text = (
            f"🛡️ *{trades_done} trade{'s' if trades_done > 1 else ''} taken this morning.*\n"
            f"SL was hit — but capital was protected.\n\n"
            f"The algo moved on. Afternoon setups are loading.\n"
            f"*Discipline over emotion — every single day.*"
        )
    elif still_in_trade:
        result_text = (
            f"🔄 *One trade is still running.*\n"
            f"VIP members entered this morning and are trailing their stop.\n\n"
            f"The algo is managing it. They don't even have to watch."
        )
    else:
        result_text = (
            f"😴 *Slow morning — the algo stayed patient.*\n\n"
            f"Not every session needs a trade.\n"
            f"Capital protection is the #1 rule at BLACK.\n"
            f"Afternoon setups are looking interesting though 👀"
        )

    _to_free(
        f"🍽️ *Lunch Break Check-in — {_hhmm()}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{result_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💬 *A message from our VIP members:*\n\n"
        f"_\"I used to struggle finding entries. Now I just wait for the alert and follow the plan.\"_\n\n"
        f"_\"Best ₹3,999 I've ever spent. Covered in the first 2 days.\"_\n\n"
        f"_\"Finally stopped revenge trading. The algo keeps me disciplined.\"_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Afternoon session starts soon. Don't miss the next one 👇*"
        + _pay_block()
    )


def _free_post_afternoon(spot: float, regime: str, day_signals: int,
                         day_pnl_so_far: float) -> None:
    r = regime.upper()
    if "BULL" in r:   regime_line = "📈 *Bulls are pushing* — momentum setups are live"
    elif "BEAR" in r: regime_line = "📉 *Bears in control* — shorting setups on radar"
    else:             regime_line = "⚠️ *Market in chop* — BLACK is waiting for clarity"

    if day_signals > 0:
        pnl_line = f"⚡ `{day_signals}` signal{'s' if day_signals>1 else ''} sent to VIP so far. Afternoon window opening.\n"
    else:
        pnl_line = f"⚡ Fresh setups are loading for the afternoon session.\n"

    _to_free(
        f"📍 *Afternoon Update — {_hhmm()}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Nifty at *{spot:.0f}*\n"
        f"{regime_line}\n\n"
        f"{pnl_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 *Why BLACK VIP is different:*\n\n"
        f"Are you revenge trading right now to recover morning losses?\n"
        f"VIP members already booked their targets and closed the terminal. 💻\n\n"
        f"1️⃣  Every trade is shared *5–10 mins before entry*\n"
        f"2️⃣  Stop Loss is *mandatory* on every single call\n"
        f"3️⃣  Trail SL alert makes the trade risk-free automatically.\n\n"
        f"*2 hours left in today's session. Catch the close 👇*"
        + _pay_block()
    )


def _free_post_pre_close(spot: float, day_signals: int, swing_fired: bool) -> None:
    if day_signals > 0:
        summary = (
            f"📊 *Today so far for VIP members:*\n"
            f"   Signals sent :  `{day_signals}`\n"
            f"   Live alerts  :  entry, trail SL, and exit — all delivered\n\n"
        )
    else:
        summary = (
            f"⚡ *BLACK is scanning for the close setup right now.*\n\n"
            f"No signal yet — algo waiting for the right conditions.\n\n"
        )

    swing_add = ""
    if swing_fired:
        swing_add = (
            f"🌙 *Swing trade also sent to VIP tonight.*\n"
            f"Overnight position with managed risk — next-week expiry.\n\n"
        )

    _to_free(
        f"⏰ *45 Minutes to Close — Don't Blink!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{summary}"
        f"{swing_add}"
        f"The last 45 minutes of Nifty are often the most explosive.\n"
        f"Big money moves happen right before close. 📈\n\n"
        f"If BLACK fires a signal now —\n"
        f"*VIP members get it instantly.*\n"
        f"You'll see it in the free channel after market closes. 🙁\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 *Tomorrow is another day.*\n"
        f"But tomorrow's signals also go to VIP first.\n\n"
        f"Join tonight. Be ready by *9:02 AM tomorrow.*\n"
        f"Trial is just ₹599 👇"
        + _pay_block()
    )


def _free_post_eod(day_signals: int, best_signal: Optional[dict],
                   vix: float, swing_open: bool = False) -> None:
    swing_tag = "Yes ⚠️  (carried overnight)" if swing_open else "Flat ✅"
    won = (best_signal is not None and best_signal.get("tp_pts", 0) > 0)
    day_header = (
        f"📈 *Signals fired, targets delivered!*" if won
        else f"🛡️ *Algo stayed disciplined — no forced trades.*"
    )

    text = (
        f"🔔 *BLACK — End of Day Report*\n"
        f"🗓️  `{_today()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{day_header}\n\n"
        f"📡 Signals Today    :  `{day_signals}`\n"
        f"😨 India VIX        :  `{vix:.1f}`\n"
        f"🌙 Swing Status     :  `{swing_tag}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 *Today's thought:*\n"
        f"_{_insight()}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔒 *VIP members received these signals live — at entry.*\n"
        f"_By the time you're reading this, they already exited and counted profits._\n\n"
        f"Same setup fires tomorrow. Be inside the room this time 👇"
    )

    if best_signal:
        idx = best_signal.get("index", "")
        cfg = INDICES.get(idx, {})
        ep  = best_signal.get("entry_premium", 0)
        tp  = best_signal.get("tp_pts", 0)
        d   = best_signal.get("direction", "CE")
        st  = best_signal.get("strike", 0)
        lot = cfg.get("lot", 1)
        p2l = round(tp * lot * TRADE_LOTS)
        tp_str = f"₹{round(ep+tp,1)}" if ep > 0 else f"+{tp:.0f} pts"
        text += (
            f"\n\n📌 *One Signal Preview (free members see this AFTER close):*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{'🟢' if d=='CE' else '🔴'}  `{d}` — `{cfg.get('label','')} {st}`\n"
            + (f"Entry `₹{ep:.1f}` → Target `{tp_str}`\n" if ep > 0
               else f"Target: `+{tp:.0f} pts`\n") +
            f"{TRADE_LOTS}-Lot Profit: `₹{p2l:,}` 🔥\n\n"
            f"_VIP members got this alert at the moment of entry._\n"
            f"_You got it now — after it closed._ 🙁"
        )

    text += _pay_block()
    _to_free(text)


def _free_post_no_trade(vix: float, reason: str = "") -> None:
    reason_line = f"\n📋 _Reason: {reason}_\n" if reason else ""
    _to_free(
        f"⚠️ *BLACK — No Trade Signal Today*\n"
        f"📅 `{_today()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"The algo scanned all 4 indices today.\n"
        f"No high-quality setup met our strict criteria.\n"
        f"{reason_line}\n"
        f"😨 VIX: `{vix:.1f}` — {_vix_label(vix)}\n\n"
        f"*What this means:*\n"
        f"✅ Capital protected — zero forced trades today\n"
        f"✅ VIP members received key S/R levels + Iron Condor advisory\n"
        f"✅ Chop days are savings days — the real edge is staying out\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 *Tomorrow the algo resets fresh.*\n"
        f"First signal fires at 9:20 AM — VIP members get it first.\n\n"
        f"_Don't miss it 👇_"
        + _pay_block()
    )


def _free_post_chop(vix: float) -> None:
    _to_free(
        f"⚠️ *BLACK — CHOP MARKET ALERT*\n"
        f"📅 `{_today()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Market is eating premiums today. Theta decay is destroying retail buyers.\n"
        f"⚠️ *Capital bachao (Protect your capital).* \n\n"
        f"VIX: `{vix:.1f}`  →  {_vix_label(vix)}\n\n"
        f"*What VIP members are doing in chop:*\n"
        f"🎯 Iron Condor advisory — selling time decay instead of fighting it.\n"
        f"📍 Key S/R levels with breakout triggers ready.\n"
        f"🛡️ Zero forced directional trades.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Waiting patiently is also a strategy. The algo will fire the moment real direction appears.\n"
        f"Join VIP to trade like an institution. 👇"
        + _pay_block()
    )


def _free_post_friday_recap(week_signals: int, vix: float,
                             best_signal: Optional[dict] = None) -> None:
    text = (
        f"📊 *BLACK — Weekly Recap*\n"
        f"📅 Week ending `{_today()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Signals fired this week : *{week_signals}*\n"
        f"😨 VIX at close            : `{vix:.1f}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Free members received: *market updates and educational posts*\n"
        f"VIP members received : *all {week_signals} signals + key levels + swings*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if best_signal:
        idx = best_signal.get("index", "")
        cfg = INDICES.get(idx, {})
        ep  = best_signal.get("entry_premium", 0)
        tp  = best_signal.get("tp_pts", 0)
        d   = best_signal.get("direction", "CE")
        st  = best_signal.get("strike", 0)
        p2l = round(tp * cfg.get("lot", 1) * TRADE_LOTS)
        text += (
            f"🏅 *Best Trade of the Week:*\n\n"
            f"{'🟢' if d=='CE' else '🔴'}  `{d}` — `{cfg.get('label','')} {st}`\n"
            + (f"Entry `₹{ep:.1f}` → +{tp:.0f} pts\n" if ep > 0 else f"Target: +{tp:.0f} pts\n") +
            f"{TRADE_LOTS}-Lot Profit: `₹{p2l:,}` 💥\n\n"
            f"_VIP members were in this trade from entry to exit._\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        )
    text += (
        f"🚀 *Next week kicks off Monday 9:02 AM.*\n\n"
        f"Will you be inside the room this time?\n"
        f"Join over the weekend and start fresh Monday 👇"
        + _pay_block()
    )
    _to_free(text)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA FETCHER
# ══════════════════════════════════════════════════════════════════════════════

class SignalDataFetcher:

    def __init__(self):
        self.smart      = None
        self._auth_time = 0.0
        self._auth_ttl  = 3600 * 6
        if _ANGEL_OK and SIGNAL_API_KEY:
            self._auth()

    def _auth(self) -> bool:
        try:
            self.smart = SmartConnect(api_key=SIGNAL_API_KEY)
            totp       = pyotp.TOTP(SIGNAL_TOTP).now()
            resp       = self.smart.generateSession(SIGNAL_CLIENT_ID, SIGNAL_PASSWORD, totp)
            if resp.get("status"):
                self._auth_time = time.time()
                _log.info("✅ Signal Angel One auth OK")
                return True
            _log.error(f"Signal Angel auth failed: {resp}")
        except Exception as e:
            _log.error(f"Signal Angel auth error: {e}")
        self.smart = None
        return False

    def _ensure_auth(self, force=False) -> bool:
        if (
            force
            or self.smart is None
            or (time.time() - self._auth_time) > self._auth_ttl
        ):
            return self._auth()

        return True

    def _fetch_ltp_api(self, exchange: str, symbol: str, token: str) -> float:
        """API Fallback for fetching option LTP if JSON is stale/missing"""
        self._ensure_auth()
        if self.smart and symbol and token:
            try:
                resp = self.smart.ltpData(exchange, symbol, token)
                if resp and resp.get("status"):
                    return float(resp["data"]["ltp"])
            except Exception as e:
                _log.debug(f"API LTP fallback failed for {symbol}: {e}")
        return 0.0

    def read_snapshot(self, index_key: str) -> dict:
        path = SNAPSHOTS.get(index_key)
    
        try:
            raw = None
            age_s = 0.0
            source = os.getenv("SNAPSHOT_SOURCE", "postgres").strip().lower()
    
            # 1. Read PostgreSQL snapshot first
            if source == "postgres":
                try:
                    raw = read_shared_snapshot(
                        index_key,
                        max_age_s=int(os.getenv("SNAPSHOT_MAX_AGE_SECONDS", "120"))
                    )
                    if raw and raw.get("_stale"):
                        raw = None
                    if raw:
                        age_s = float(raw.get("_age_s", 0.0))
                except Exception as e:
                    _log.warning(f"PostgreSQL snapshot read failed ({index_key}): {e}")
                    raw = None
    
            # 2. Local JSON fallback must be explicitly enabled in PostgreSQL mode.
            truthy = {"1", "true", "yes", "on"}
            
            allow_local_fallback = (
                source != "postgres"
                or os.getenv(
                    "ALLOW_LOCAL_SNAPSHOT_FALLBACK", "false"
                ).strip().lower() in truthy
            )
            
            if not raw and source == "postgres" and not allow_local_fallback:
                _log.warning(
                    "%s: PostgreSQL snapshot unavailable; "
                    "local JSON fallback disabled",
                    index_key,
                )
                return {}
            
            # Local JSON fallback
            if not raw:
                if not path or not path.exists():
                    _log.warning(
                        "No usable snapshot for %s: PostgreSQL/local snapshot missing",
                        index_key,
                    )
                    return {}
    
                age_s = time.time() - path.stat().st_mtime
                if age_s > max(1, int(os.getenv("SNAPSHOT_MAX_AGE_SECONDS", "120"))):
                    _log.warning(f"Snapshot {index_key} is {age_s:.0f}s old")
                    return {}
    
                for attempt in range(3):
                    try:
                        with open(path, "r") as f:
                            raw = json.load(f)
                        break
                    except (json.JSONDecodeError, PermissionError) as e:
                        if attempt < 2:
                            time.sleep(0.1)
                        else:
                            _log.warning(
                                f"Read collision on {index_key} after 3 retries: {e}"
                            )
                            return {}
    
            # 3. Normalize snapshot structure
            item = {}
    
            if isinstance(raw, list):
                for obj in reversed(raw):
                    if isinstance(obj, dict) and (
                        "spot" in obj or "spot_val" in obj
                    ):
                        item = obj
                        break
    
                if not item and raw and isinstance(raw[0], dict):
                    item = raw[0]
    
            elif isinstance(raw, dict):
                # Root payload holds market-wide aggregates; data[] holds option contracts.
                # Never replace root PCR/GEX/OI with values from one option row.
                item = raw
                data = raw.get("data")
                if isinstance(data, list) and data and not item.get("spot") and not item.get("spot_val"):
                    last_entry = next(
                        (row for row in reversed(data)
                         if isinstance(row, dict) and
                         (row.get("spot") is not None or row.get("spot_val") is not None)),
                        None,
                    )
                    if last_entry is not None:
                        item = {**item, "spot": last_entry.get("spot", last_entry.get("spot_val", 0))}

            # 4. Build the normalized snapshot
            snap = {}
            snap["spot"] = float(item.get("spot", item.get("spot_val", 0)) or 0)
            snap["vix"] = float(item.get("vix") or 0.0)
            snap["put_oi"] = float(item.get("put_oi", 0) or 0)
            snap["call_oi"] = float(item.get("call_oi", 0) or 0)
            snap["pcr"] = float(item.get("pcr", item.get("pcr_oi", item.get("market_pcr", 1.0))) or 1.0)
            snap["net_gex"] = float(item.get("net_gex", item.get("raw_net_gex", 0)) or 0)
            snap["max_pain"] = float(item.get("max_pain", snap["spot"]) or snap["spot"])
            snap["timestamp"] = item.get("timestamp", "")
            
            snap["candles"] = self._candles_from_snapshot(raw)

            # PostgreSQL is canonical in EC2 mode: it contains completed spot
            # OHLC plus the separately persisted futures-volume stream. Prefer
            # it even when the snapshot embeds older spot-only candle samples.
            if source == "postgres":
                db_candles = self._candles_from_postgres(index_key)
                if not db_candles.empty:
                    snap["candles"] = db_candles
            
            snap["_age_s"] = age_s

    
            if snap["spot"] == 0:
                _log.warning(f"Could not find 'spot' price in {index_key} snapshot")
            else:
                _log.info(
                    f"Snapshot loaded: {index_key} | "
                    f"source={source if source == 'postgres' else 'local/fallback'} | "
                    f"spot={snap['spot']:.2f} | age={age_s:.1f}s"
                )
    
            return snap

        except Exception as e:
            _log.warning(f"Snapshot read error ({index_key}): {e}")
            return {}

    def _candles_from_snapshot(self, raw) -> pd.DataFrame:
        """Return only genuine, structurally valid OHLC candles from a snapshot.

        Collector spot samples and option-chain rows are deliberately rejected;
        they must never be converted into synthetic OHLC bars.
        """
        empty = pd.DataFrame()
        if isinstance(raw, dict):
            quality = str(raw.get("candle_quality", "")).strip().lower()
            if quality in {"spot_samples_not_true_ohlc", "spot_sample_not_true_ohlc", "invalid", "unavailable"}:
                return empty
            data_list = None
            for key in ("candles", "history", "bars", "ohlcv"):
                if isinstance(raw.get(key), list):
                    data_list = raw[key]
                    break
        elif isinstance(raw, list):
            data_list = raw
        else:
            return empty

        if not data_list or len(data_list) < 6:
            return empty
        try:
            df = pd.DataFrame([row for row in data_list if isinstance(row, dict)])
            needed = {"open", "high", "low", "close"}
            if df.empty or not needed.issubset(df.columns):
                return empty

            # Reject explicit invalid markers at row level, even if root metadata is absent.
            if "ohlc_valid" in df.columns:
                df = df[df["ohlc_valid"].map(lambda v: not (v is False or str(v).strip().lower() in {"false", "0", "no"}))]
            if "candle_quality" in df.columns:
                q = df["candle_quality"].astype(str).str.strip().str.lower()
                df = df[~q.isin({"spot_sample_not_true_ohlc", "spot_samples_not_true_ohlc", "invalid", "unavailable"})]
            if df.empty:
                return empty

            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["open", "high", "low", "close"])
            df = df[(df[["open", "high", "low", "close"]] > 0).all(axis=1)]
            df = df[(df["high"] >= df[["open", "close", "low"]].max(axis=1)) &
                    (df["low"] <= df[["open", "close", "high"]].min(axis=1))]
            if "timestamp" not in df.columns:
                _log.warning("Snapshot candles rejected: timestamp column missing")
                return empty
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")
            df = df.sort_values("timestamp")
            if df.empty:
                return empty

            # A fresh root snapshot must not make stale candle history look current.
            # During market hours, reject a candle tail older than this limit.
            try:
                from zoneinfo import ZoneInfo
                now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
                market_open = (now_ist.weekday() < 5 and
                               pd.Timestamp("09:15").time() <= now_ist.time() < pd.Timestamp("15:30").time())
                if market_open:
                    latest = df["timestamp"].iloc[-1].to_pydatetime()
                    latest_age = (datetime.now(latest.tzinfo) - latest).total_seconds()
                    max_candle_age = max(30.0, float(os.getenv("CANDLE_MAX_AGE_SECONDS", "180")))
                    if latest_age < -5 or latest_age > max_candle_age:
                        _log.warning(
                            "Snapshot candle tail stale/future: age=%.1fs limit=%.1fs",
                            latest_age, max_candle_age,
                        )
                        return empty
            except Exception as age_exc:
                _log.warning("Could not validate latest candle age: %s", age_exc)
                return empty
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
            else:
                df["volume"] = 0
            if len(df) < 6:
                return empty
            return df.tail(120).reset_index(drop=True)
        except Exception as exc:
            _log.warning(f"Rejected malformed snapshot candles: {exc}")
            return empty

    
    def _candles_from_postgres(self, index_key: str) -> pd.DataFrame:
        """
        Read genuine completed OHLC candles from market_ohlcv_candles.
        Uses the existing shared PostgreSQL connection.
        """
        try:
            with _shared_db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            ts,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            is_complete,
                            quality_status
                        FROM market_ohlcv_candles
                        WHERE symbol = %s
                          AND is_complete = TRUE
                          AND UPPER(quality_status) = 'VALID'
                        ORDER BY ts DESC
                        LIMIT 120
                    """, (index_key,))

                    rows = cur.fetchall()

                    if not rows:
                        _log.warning(
                            "%s: No completed valid PostgreSQL candles",
                            index_key
                        )
                        return pd.DataFrame()

                    columns = [col.name for col in cur.description]
                    records = [
                        dict(zip(columns, row))
                        for row in rows
                    ]

            # Convert DB timestamp to the timestamp field
            # expected by the existing validator.
            for record in records:
                ts = record.pop("ts")
                record["timestamp"] = (
                    ts.isoformat()
                    if hasattr(ts, "isoformat")
                    else str(ts)
                )

            # Existing validation remains responsible for
            # OHLC structure, timestamp and candle freshness.
            candles = self._candles_from_snapshot({
                "candles": records
            })

            if candles.empty:
                _log.warning(
                    "%s: PostgreSQL candles failed OHLC validation",
                    index_key
                )
                return pd.DataFrame()
            # Initialize shadow VWAP metadata.
            # Invalid by default: shadow must never use incomplete futures data.
            candles.attrs["session_vwap_shadow"] = {
                "valid": False,
                "reason": "futures_data_not_loaded",
            }
            candles.attrs["index_key"] = index_key

            # Spot index OHLC has no traded volume. Keep its volume field
            # untouched and attach genuine futures volume in a separate column.
            candles["timestamp"] = pd.to_datetime(
                candles["timestamp"], errors="coerce", utc=True
            )
            candles["futures_volume"] = np.nan
            try:
                from collectors.scripts.shared_snapshots import get_recent_futures_candles

                futures_rows = get_recent_futures_candles(
                    index_key,
                    limit=1000,
                    max_age_seconds=max(
                        30.0, float(os.getenv("CANDLE_MAX_AGE_SECONDS", "180"))
                    ),
                )
                if futures_rows:
                    futures = pd.DataFrame(futures_rows)
                    futures["timestamp"] = pd.to_datetime(
                        futures["timestamp"], errors="coerce", utc=True
                    )
                    futures["volume"] = pd.to_numeric(
                        futures["volume"], errors="coerce"
                    )
                    futures["tick_count"] = pd.to_numeric(
                        futures["tick_count"], errors="coerce"
                    )
                    futures = futures.dropna(
                        subset=["timestamp", "volume", "tick_count"]
                    )
                    futures = futures[
                        (futures["volume"] >= 0)
                        & (futures["tick_count"] >= 3)
                        & futures["is_complete"].eq(True)
                        & futures["quality_status"].astype(str).str.upper().eq("VALID")
                    ]
                    futures = futures[["timestamp", "volume"]].rename(
                        columns={"volume": "futures_volume"}
                    )
                    candles = candles.drop(
                        columns=["futures_volume"], errors="ignore"
                    ).merge(
                        futures,
                        on="timestamp",
                        how="left",
                        validate="one_to_one",
                    )
                    matched = int(candles["futures_volume"].notna().sum())
                    _log.info(
                        "%s: matched %d/%d spot candles with genuine active-futures volume",
                        index_key, matched, len(candles)
                    )
                else:
                    _log.warning(
                        "%s: no fresh valid active-futures volume candles; "
                        "strong-breakout volume confirmation will fail closed",
                        index_key,
                    )

                # Independent session VWAP calculation.
                # This does not modify existing index candles or volume.
                session_vwap_shadow = _calculate_futures_session_vwap(
                    futures_rows
                )

                candles.attrs["session_vwap_shadow"] = session_vwap_shadow
                candles.attrs["index_key"] = index_key

                _log.info(
                    "[VWAP SHADOW DATA] %s | valid=%s | reason=%s | "
                    "session_vwap=%s | distance=%s | direction=%s | "
                    "bars=%s | coverage=%s%%",
                    index_key,
                    session_vwap_shadow["valid"],
                    session_vwap_shadow["reason"],
                    session_vwap_shadow["vwap"],
                    session_vwap_shadow["distance"],
                    session_vwap_shadow["direction"],
                    session_vwap_shadow["bar_count"],
                    session_vwap_shadow["coverage_pct"],
                )
            except Exception as futures_exc:
                candles.attrs["session_vwap_shadow"] = {
                    "valid": False,
                    "reason": "futures_data_error",
                    "error": str(futures_exc),
                }
                candles.attrs["index_key"] = index_key

                _log.warning(
                    "%s: futures-volume read/join failed; "
                    "breakout volume/VWAP unavailable: %s",
                    index_key,
                    futures_exc,
                )
            # Independent VWAP collection persistence.
            # Runs for both valid and invalid futures observations.
            _persist_vwap_shadow_collection(
                index_key=index_key,
                candles=candles,
                shadow=candles.attrs.get(
                    "session_vwap_shadow",
                    {},
                ),
            )

            _log.info(
                "%s: Loaded %d validated PostgreSQL spot candles",
                index_key,
                len(candles)
            )
            return candles

        except Exception as exc:
            _log.exception(
                "%s: PostgreSQL candle read failed: %s",
                index_key,
                exc
            )
            return pd.DataFrame()


    def fetch_candles_api(self, token: str, exchange: str = "NSE", bars: int = 120) -> pd.DataFrame:
        self._ensure_auth()
        if self.smart:
            for attempt in range(3):
                try:
                    to_dt   = _ist_now()
                    from_dt = to_dt - timedelta(minutes=bars + 30)
                    resp = self.smart.getCandleData({
                        "exchange": exchange, "symboltoken": token,
                        "interval": "ONE_MINUTE",
                        "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
                        "todate"  : to_dt.strftime("%Y-%m-%d %H:%M"),
                    })
                    if resp and resp.get("status") and resp.get("data"):
                        df = pd.DataFrame(resp["data"],
                                          columns=["timestamp","open","high","low","close","volume"])
                        df["timestamp"] = pd.to_datetime(df["timestamp"])
                        df = df[df["timestamp"].dt.time >= pd.Timestamp("09:15").time()]
                        # 🔧 TIMEFRAME FIX: Resample 1-min API data to 5-min to match JSON lookbacks
                        for col in ("open", "high", "low", "close", "volume"):
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
                        df = df[(df[["open", "high", "low", "close"]] > 0).all(axis=1)]
                        df = df[(df["high"] >= df[["open", "close", "low"]].max(axis=1)) &
                                (df["low"] <= df[["open", "close", "high"]].min(axis=1))]
                        if df.empty:
                            _log.warning(f"Angel candle API returned no structurally valid OHLC rows for {token}")
                            break
                        df.set_index("timestamp", inplace=True)
                        df = df.resample("5min").agg({
                            "open": "first", "high": "max", "low": "min",
                            "close": "last", "volume": "sum"
                        }).dropna(subset=["open", "high", "low", "close"]).reset_index()
                        df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
                        if len(df) < 6:
                            _log.warning(f"Angel candle API returned only {len(df)} valid bars for {token}")
                            return pd.DataFrame()
                        return df.tail(bars).reset_index(drop=True)
                    elif resp and resp.get("errorcode") == "AB1021":
                        _log.warning(f"Rate limit hit for {token}. Retrying in 3s... ({attempt+1}/3)")
                        time.sleep(3)
                        continue
                    break
                except Exception as e:
                    _log.warning(f"Angel candle fetch attempt {attempt+1} ({token}): {e}")
                    time.sleep(2)
        return pd.DataFrame()

    def fetch_vix(self) -> float:
        # PostgreSQL is the primary cross-service source; local JSON is fallback only.
        for index_key in SNAPSHOTS:
            try:
                raw = read_shared_snapshot(
                    index_key,
                    max_age_s=int(os.getenv("SNAPSHOT_MAX_AGE_SECONDS", "120")),
                )
                if raw and not raw.get("_stale"):
                    value = raw.get("vix")
                    if value is None and isinstance(raw.get("data"), list):
                        value = next((r.get("vix") for r in raw["data"]
                                      if isinstance(r, dict) and r.get("vix") is not None), 0)
                    v = float(value or 0)
                    if math.isfinite(v) and v > 0:
                        return v
            except Exception as exc:
                _log.debug("PostgreSQL VIX read failed for %s: %s", index_key, exc)

        for snap_path in SNAPSHOTS.values():
            if snap_path.exists():
                try:
                    with open(snap_path, "r", encoding="utf-8") as f:
                        raw = json.load(f)
                    value = raw.get("vix", 0) if isinstance(raw, dict) else 0
                    if not value and isinstance(raw, dict) and isinstance(raw.get("data"), list):
                        value = next((r.get("vix") for r in raw["data"]
                                      if isinstance(r, dict) and r.get("vix") is not None), 0)
                    v = float(value or 0)
                    if math.isfinite(v) and v > 0:
                        return v
                except Exception as exc:
                    _log.debug("Local VIX snapshot read failed (%s): %s", snap_path, exc)
        allow_api_fallback = os.getenv(
            "ALLOW_ANGEL_VIX_API_FALLBACK", "false"
        ).strip().lower() in {"1", "true", "yes", "on"}
        if allow_api_fallback:
            df = self.fetch_candles_api(VIX_TOKEN, bars=5)
            if not df.empty:
                value = float(df["close"].iloc[-1])
                return value if math.isfinite(value) and value > 0 else 0.0
        return 0.0

    
    def get_index_data(self, index_key: str) -> dict:
        from datetime import datetime, time as dt_time
        from zoneinfo import ZoneInfo

        cfg = INDICES[index_key]
        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        in_session = (
            now.weekday() < 5
            and dt_time(9, 15) <= now.time() < dt_time(15, 30)
        )

        snap = self.read_snapshot(index_key)

        if snap and snap.get("spot", 0) > 0:
            if snap["candles"].empty:
                allow_api_fallback = os.getenv(
                    "ALLOW_ANGEL_CANDLE_API_FALLBACK", "false"
                ).strip().lower() in {"1", "true", "yes", "on"}
                if in_session and allow_api_fallback:
                    _log.warning(
                        "%s: PostgreSQL/snapshot OHLC unavailable; explicit Angel API fallback enabled",
                        index_key,
                    )
                    snap["candles"] = self.fetch_candles_api(
                        cfg["token"], cfg["exchange"]
                    )
                else:
                    _log.warning(
                        "%s: valid PostgreSQL/snapshot OHLC missing; "
                        "Angel API fallback disabled or outside session; skipping",
                        index_key,
                    )
            if snap["candles"].empty:
                _log.warning(
                    f"{index_key}: no valid OHLC candles available; "
                    "this index must be skipped for signal generation"
                )
            return snap

        if not in_session:
            _log.warning(
                f"{index_key}: no usable snapshot outside market hours; "
                "skipping API fallback"
            )
            return {
                "spot": 0.0,
                "vix": 0.0,
                "put_oi": 0,
                "call_oi": 0,
                "pcr": 1.0,
                "net_gex": 0,
                "max_pain": 0.0,
                "candles": pd.DataFrame(),
                "_age_s": None,
            }

        _log.warning(
            "%s: no fresh PostgreSQL market snapshot; refusing API-only signal "
            "generation with default PCR/GEX values",
            index_key,
        )
        return {
            "spot": 0.0,
            "vix": 0.0,
            "put_oi": 0,
            "call_oi": 0,
            "pcr": 0.0,
            "net_gex": 0,
            "max_pain": 0.0,
            "candles": pd.DataFrame(),
            "_age_s": None,
        }

    def get_option_quote(self, index_key: str, strike: int, direction: str,
                         target_expiry: str) -> dict:
        """Read a contract-specific quote. Never use LTP API fallback as entry evidence."""
        import math
        from datetime import datetime
        from pathlib import Path
        import json
        import os
        import time

        blocked = {
            "found": False, "entry_eligible": False, "reason": "quote_not_found",
            "index": index_key, "strike": strike, "direction": direction,
            "expiry": target_expiry, "ltp": 0.0, "bid": 0.0, "ask": 0.0,
            "bid_qty": 0.0, "ask_qty": 0.0, "spread_pct": None,
            "snapshot_age_s": None, "quote_valid": False,
            "quote_entry_eligible": False, "exchange_feed_time": None,
            "exchange_trade_time": None,
        }

        path = SNAPSHOTS.get(index_key)

        def parse_exp(value):
            if not value:
                return None
            value = str(value).strip().upper()
            for fmt in ("%Y-%m-%d", "%d%b%Y", "%d %b %y",
                         "%d-%b-%Y", "%d%b%y", "%d %b %Y", "%d-%m-%Y"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    pass
            return None

        if not target_expiry or parse_exp(target_expiry) is None:
            blocked["reason"] = "target_expiry_missing_or_invalid"
            return blocked

        d = str(direction).strip().upper()
        d = {"CALL": "CE", "PUT": "PE"}.get(d, d)
        if d not in ("CE", "PE"):
            blocked["reason"] = "invalid_option_type"
            return blocked

        try:
            raw = None
            snapshot_age = None
            source = os.getenv("SNAPSHOT_SOURCE", "postgres").strip().lower()

            # Canonical option data lives in option_ticks, not market_snapshots.
            # Query the exact contract and latest timestamp before considering legacy snapshot fallback.
            if source == "postgres":
                try:
                    with _shared_db_connect() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT ts, expiry, strike, opt_type, ltp, bid, ask,
                                       bid_qty, ask_qty, quote_valid, spread_pct,
                                       quote_entry_eligible, exchange_feed_time,
                                       exchange_trade_time, symbol
                                FROM option_ticks
                                WHERE index_key=%s AND strike=%s AND opt_type=%s AND expiry=%s
                                ORDER BY ts DESC LIMIT 1
                            """, (index_key, float(strike), d, parse_exp(target_expiry)))
                            db_row = cur.fetchone()
                            if db_row:
                                names = [col.name for col in cur.description]
                                matched_db = dict(zip(names, db_row))
                                ts_value = matched_db.get("ts")
                                matched_db["timestamp"] = ts_value.isoformat() if hasattr(ts_value, "isoformat") else str(ts_value)
                                matched_db["type"] = matched_db.get("opt_type")
                                matched_db["expiry"] = str(matched_db.get("expiry"))
                                raw = {"data": [matched_db]}
                                snapshot_age = time.time() - ts_value.timestamp() if hasattr(ts_value, "timestamp") else None
                except Exception as exc:
                    _log.warning("PostgreSQL option_ticks read failed (%s): %s", index_key, exc)

            # Legacy snapshot fallback is retained for compatibility, but never overrides option_ticks.
            if raw is None and source == "postgres":
                try:
                    shared = read_shared_snapshot(
                        index_key,
                        max_age_s=int(os.getenv("SNAPSHOT_MAX_AGE_SECONDS", "120")),
                    )
                    if shared and not shared.get("_stale"):
                        raw = shared
                        snapshot_age = float(shared.get("_age_s", 0.0))
                except Exception as exc:
                    _log.warning("PostgreSQL option snapshot read failed (%s): %s", index_key, exc)

            # PostgreSQL mode must not silently downgrade to a local file when the
            # canonical store has no matching contract or is unavailable.
            if raw is None and source == "postgres":
                blocked["reason"] = "postgres_option_quote_missing_or_unavailable"
                return blocked

            # Local JSON is permitted only when explicitly selected by configuration.
            if raw is None:
                if not path or not path.exists():
                    blocked["reason"] = "snapshot_missing"
                    return blocked
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                snapshot_age = max(0.0, time.time() - Path(path).stat().st_mtime)

            rows = []
            if isinstance(raw, list):
                rows = raw
            elif isinstance(raw, dict):
                for key in ("data", "option_chain", "options", "chain"):
                    if isinstance(raw.get(key), list):
                        rows = raw[key]
                        break

            matched = None
            for item in rows:
                if not isinstance(item, dict):
                    continue
                try:
                    item_strike = int(float(item.get("strike", item.get("strikePrice", -1))))
                except (TypeError, ValueError, OverflowError):
                    continue

                item_type = str(item.get("type", item.get("instrumentType", ""))).strip().upper()
                item_type = {"CALL": "CE", "PUT": "PE"}.get(item_type, item_type)
                item_expiry = parse_exp(item.get("expiry", item.get("expiryDate", "")))

                if (item_strike == int(strike)
                        and item_type == d
                        and item_expiry == parse_exp(target_expiry)):
                    matched = item
                    break

            if matched is None:
                blocked["reason"] = "exact_contract_not_found"
                return blocked

            def number(value):
                try:
                    value = float(value)
                    return value if math.isfinite(value) else 0.0
                except (TypeError, ValueError, OverflowError):
                    return 0.0

            def parse_timestamp(value):
                if isinstance(value, (int, float)):
                    return float(value)
                if not value:
                    return None
                try:
                    parsed = datetime.fromisoformat(
                        str(value).strip().replace("Z", "+00:00")
                    )
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=IST)
                    return parsed.timestamp()
                except (TypeError, ValueError, OverflowError):
                    return None

            now = time.time()
            # LTP is the paper-entry/mark price. Validate both the feed timestamp
            # and last-trade timestamp where supplied: a fresh websocket feed can
            # still carry an old LTP if the option contract has not traded recently.
            feed_ts = parse_timestamp(matched.get("exchange_feed_time"))
            trade_ts = parse_timestamp(matched.get("exchange_trade_time"))
            row_ts = parse_timestamp(matched.get("timestamp"))
            if row_ts is None and isinstance(raw, dict):
                row_ts = parse_timestamp(
                    raw.get("timestamp", raw.get("updated_at", raw.get("generated_at")))
                )

                        
            # Use the market-feed timestamp and database row timestamp
            # to measure quote freshness. Last-trade time is tracked separately:
            # an old last trade does not automatically mean the live order book is stale.
            
            feed_age = (now - feed_ts) if feed_ts is not None else None
            row_age = (now - row_ts) if row_ts is not None else None
            trade_age = (now - trade_ts) if trade_ts is not None else None
            
            freshness_ages = [
                value for value in (snapshot_age, feed_age, row_age)
                if value is not None
            ]
            
            age = max(freshness_ages) if freshness_ages else None
            
            _log.debug(
                "OPTION QUOTE AGE index=%s strike=%s type=%s "
                "db_age=%s feed_age=%s trade_age=%s effective_age=%s",
                index_key,
                strike,
                d,
                round(snapshot_age, 2) if snapshot_age is not None else None,
                round(feed_age, 2) if feed_age is not None else None,
                round(trade_age, 2) if trade_age is not None else None,
                round(age, 2) if age is not None else None,
            )


            bid = number(matched.get("bid"))
            ask = number(matched.get("ask"))
            ltp = number(matched.get("ltp"))
            bid_qty = number(matched.get("bid_qty"))
            ask_qty = number(matched.get("ask_qty"))
            spread = number(matched.get("spread_pct")) if matched.get("spread_pct") is not None else None
            quote_valid = (
                matched.get("quote_valid") is True
                and bid > 0 and ask > bid
            )
            if bid > 0 and ask > bid:
                spread = ((ask - bid) / ask) * 100.0

            max_age = max(1.0, number(os.getenv("QUOTE_MAX_AGE_SECONDS", "120")))

            # Deliberately do NOT gate LTP paper signals on bid/ask depth,
            # spread percentage, or the collector's quote_entry_eligible flag.
            # Keep those fields as diagnostics so we can audit paper fills.
            reasons = []
            if age is None:
                reasons.append("timestamp_missing_or_unparseable")
            elif age < -2:
                reasons.append("timestamp_in_future")
            elif age > max_age:
                reasons.append("stale_ltp_or_quote")
            if ltp <= 0:
                reasons.append("invalid_ltp")

            result = {
                "found": True,
                "entry_eligible": not reasons,
                "reason": "ok" if not reasons else ";".join(reasons),
                "index": index_key, "strike": int(strike), "direction": d,
                "expiry": str(matched.get("expiry", matched.get("expiryDate", ""))),
                "ltp": ltp, "bid": bid, "ask": ask,
                "bid_qty": bid_qty, "ask_qty": ask_qty,
                "spread_pct": spread, "snapshot_age_s": age,
                "quote_valid": quote_valid,
                "quote_entry_eligible": matched.get("quote_entry_eligible") is True,
                "collector_eligibility_enforced": False,
                "pricing_mode": "LTP_PAPER",
                "exchange_feed_time": matched.get("exchange_feed_time"),
                "exchange_trade_time": matched.get("exchange_trade_time"),
                "symbol": matched.get("symbol", matched.get("tradingSymbol")),
                "token": matched.get("token", matched.get("symbolToken")),
            }
            return result

        except Exception as e:
            _log.warning("Structured quote read failed for %s: %s", index_key, e)
            blocked["reason"] = "quote_read_error"
            return blocked

    def get_option_greeks(
        self,
        index_key: str,
        strike: int,
        direction: str,
        target_expiry: str
    ) -> dict:
        """
        Optional Greek lookup for adaptive ITM selection.
        Failure or missing data must never affect normal quote eligibility.
        """
        try:
            import math
            import os
            import time
            from datetime import datetime

            d = str(direction).strip().upper()
            d = {"CALL": "CE", "PUT": "PE"}.get(d, d)

            if d not in ("CE", "PE"):
                return {}

            expiry_date = None
            expiry_text = str(target_expiry).strip()

            for fmt in (
                "%Y-%m-%d", "%d%b%Y", "%d %b %y",
                "%d-%b-%Y", "%d%b%y", "%d %b %Y",
                "%d-%m-%Y"
            ):
                try:
                    expiry_date = datetime.strptime(
                        expiry_text, fmt
                    ).date()
                    break
                except ValueError:
                    continue

            if expiry_date is None:
                return {}

            with _shared_db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT delta, theta, ts
                        FROM option_ticks
                        WHERE index_key = %s
                          AND strike = %s
                          AND opt_type = %s
                          AND expiry = %s
                        ORDER BY ts DESC
                        LIMIT 1
                    """, (
                        index_key,
                        float(strike),
                        d,
                        expiry_date
                    ))

                    row = cur.fetchone()

            if not row:
                return {}

            delta, theta, ts = row

            if delta is None or theta is None or ts is None:
                return {}

            age = time.time() - ts.timestamp()

            max_age = max(
                1.0,
                float(os.getenv("QUOTE_MAX_AGE_SECONDS", "120"))
            )

            if age < -2 or age > max_age:
                return {}

            delta = float(delta)
            theta = float(theta)

            if not math.isfinite(delta) or not math.isfinite(theta):
                return {}

            return {
                "delta": delta,
                "theta": theta,
                "age_seconds": age
            }

        except Exception as exc:
            _log.debug(
                "Optional ITM Greek lookup unavailable for %s %s: %s",
                index_key, strike, exc
            )
            return {}
            

    
    def get_option_premium(self, index_key: str, strike: int, direction: str, target_expiry: str = "", *, side: str = "ask") -> float:
        """Return the requested fresh option price: ask for entry, bid for exit, LTP for marking."""
        side = str(side).strip().lower()
        if side not in {"ask", "bid", "ltp"}:
            raise ValueError("side must be 'ask', 'bid', or 'ltp'")

        quote = self.get_option_quote(index_key, strike, direction, target_expiry)
        if not quote.get("entry_eligible"):
            _log.warning("Option price blocked for %s %s %s %s side=%s: %s", index_key, strike, direction, target_expiry, side, quote.get("reason"))
            return 0.0

        if side in {"bid", "ask"}:
            if not quote.get("quote_valid"):
                _log.warning("Option %s blocked: invalid bid/ask quote for %s %s %s", side, index_key, strike, direction)
                return 0.0
            value = quote.get(side, 0.0)
        else:
            value = quote.get("ltp", 0.0)

        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0

        if not math.isfinite(value) or value <= 0:
            _log.warning("Invalid option %s price for %s %s %s: %s", side, index_key, strike, direction, value)
            return 0.0

        return value


# ══════════════════════════════════════════════════════════════════════════════
#  TECHNICAL BRAIN  (v11: doubled PCR/GEX weights from v9/v10)
# ══════════════════════════════════════════════════════════════════════════════

class TechnicalBrain:

    @staticmethod
    def analyze(candles: pd.DataFrame, snap: dict) -> dict:
        if not isinstance(snap, dict) or not math.isfinite(float(snap.get("vix") or 0.0)) or float(snap.get("vix") or 0.0) <= 0:
            return {"prob": 0.0, "direction": None, "signal_type": "HOLD",
                    "atr": 30.0, "breakdown": {"blocked": "missing_or_invalid_vix"},
                    "swing": {}, "regime": "CHOP", "vol_surge": False,
                    "bull_candle": False, "bear_candle": False}
        result = {
            "prob": 0.0, "direction": None, "signal_type": "HOLD",
            "atr": 30.0, "breakdown": {}, "swing": {}, "regime": "CHOP",
            "vol_surge": True, "bull_candle": True, "bear_candle": True,
        }
        if candles.empty or len(candles) < 20:
            result["signal_type"] = "WARMUP"
            return result

        c    = candles["close"].astype(float)
        h    = candles["high"].astype(float)
        lo_s = candles["low"].astype(float)
        spot = snap.get("spot", float(c.iloc[-1]))
        vix  = snap.get("vix", 15.0)
        pcr  = snap.get("pcr", 1.0)
        gex  = snap.get("net_gex", 0.0)

        tr  = pd.concat([h - lo_s,
                         (h - c.shift()).abs(),
                         (lo_s - c.shift()).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14, min_periods=5).mean().iloc[-1])
        result["atr"] = max(atr, 5.0)

        delta = c.diff()
        gain  = delta.where(delta > 0, 0).rolling(14, min_periods=5).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14, min_periods=5).mean()
        rsi   = float((100 - 100 / (1 + gain / (loss + 1e-9))).iloc[-1])

        ema12  = c.ewm(span=12, adjust=False).mean()
        ema26  = c.ewm(span=26, adjust=False).mean()
        macd   = ema12 - ema26
        macd_s = macd.ewm(span=9, adjust=False).mean()
        macd_h = float((macd - macd_s).iloc[-1])

        bb_mid = c.rolling(20, min_periods=5).mean()
        bb_std = c.rolling(20, min_periods=5).std() + 1e-9
        bb_pos = float(((c - (bb_mid - 2*bb_std)) / (4*bb_std)).clip(0,1).iloc[-1])
        bb_w   = float((4 * bb_std / bb_mid).iloc[-1])

        vol = candles.get("volume", pd.Series([1]*len(c))).astype(float).replace(0, 1)
        # Anchor VWAP to today's session only — use last 75 bars max (5-min × 75 = 6.25h covers full session)
        _session_len = min(len(c), 75)
        _c_s   = c.iloc[-_session_len:]
        _vol_s = vol.iloc[-_session_len:]
        vwap      = (_c_s * _vol_s).cumsum() / _vol_s.cumsum()
        vwap_dist = float((_c_s - vwap).iloc[-1])
        rolling_vwap = float(vwap.iloc[-1])

        ma5  = float(c.rolling(5).mean().iloc[-1])
        ma20 = float(c.rolling(20).mean().iloc[-1])
        slope = (c.iloc[-1] - c.iloc[-10]) / max(c.iloc[-10], 1) * 100

        pcr_bull = pcr > 1.2
        pcr_bear = pcr < 0.8
        gex_bull = gex > 0
        gex_bear = gex < 0

        # ── v9/v10/v11: Volume surge — bypass for flat/fake volume data ──────
        vol_series = candles.get("volume", pd.Series([1]*len(c))).astype(float).replace(0, 1)
        
        if vol_series.max() == vol_series.min():
            vol_surge = True   # flat volume = index spot data, bypass the check
        else:
            vol_avg20 = float(vol_series.rolling(20, min_periods=5).mean().iloc[-1])
            last_vol  = float(vol_series.iloc[-1])
            vol_surge = last_vol >= vol_avg20 * 1.2

        # ── v9/v10/v11: Candle direction confirmation ─────────────────────────
        last_open  = float(candles["open"].iloc[-1])
        last_close = float(c.iloc[-1])
        bull_candle = last_close > last_open
        bear_candle = last_close < last_open

        # ── Probability score ─────────────────────────────────────────────────
        score = 0.0
        score += 0.6 if macd_h > 0 else -0.6
        score += 0.5 if ma5 > ma20  else -0.5
        if   rsi > 58: score += 0.3
        elif rsi < 42: score -= 0.3
        if   rsi > 75: score -= 0.2
        elif rsi < 25: score += 0.2
        score += 0.2 if bb_pos > 0.6 else (-0.2 if bb_pos < 0.4 else 0)
        score += 0.2 if vwap_dist > 0 else -0.2

        # 🔧 v11: INSTITUTIONAL DATA STALENESS GUARD
        age_s = snap.get("_age_s", 0)
        if age_s <= 300:  # 5 minutes
            # PCR and GEX doubled — institutional data properly weighted
            if pcr_bull: score += 0.30
            if pcr_bear: score -= 0.30
            if gex_bull: score += 0.20
            if gex_bear: score -= 0.20
        else:
            _log.warning(f"Institutional data (PCR/GEX) > 5m old ({age_s:.0f}s). Skipping weighting.")

        # -------------------------------------------------
        # SHADOW SESSION VWAP
        # Does NOT affect existing live probability.
        # -------------------------------------------------

        vwap_shadow = candles.attrs.get(
            "session_vwap_shadow", {}
        )

        shadow_valid = bool(vwap_shadow.get("valid"))

        old_vwap_component = (
            0.2 if vwap_dist > 0 else -0.2
        )

        shadow_probability = None
        shadow_signal = "UNAVAILABLE"
        shadow_score = None

        if shadow_valid:

            session_distance = float(vwap_shadow["distance"])

            new_vwap_component = (
                0.2 if session_distance > 0 else -0.2
            )

            # Replace the existing VWAP contribution hypothetically.
            # Never add both VWAP contributions.
            shadow_score = (
                score
                - old_vwap_component
                + new_vwap_component
            )

        if vix > 28:
            score *= 0.60
            if shadow_score is not None:
                shadow_score *= 0.60
        elif vix > 20:
            score *= 0.75
            if shadow_score is not None:
                shadow_score *= 0.75

        if shadow_score is not None:

            shadow_probability = float(
                np.clip(shadow_score / 1.6, -1.0, 1.0)
            )

            shadow_is_chop = (
                bb_w < 0.015
                and abs(shadow_probability) < CHOP_PROB_CEILING
            )

            if shadow_is_chop:
                shadow_signal = "CHOP"
            elif abs(shadow_probability) >= SNIPER_MIN_PROB:
                shadow_signal = "SNIPER"
            elif abs(shadow_probability) >= SCALP_MIN_PROB:
                shadow_signal = "SCALP"
            else:
                shadow_signal = "HOLD"

        # Divisor = dominant indicator max (MACD 0.6 + MA 0.5 + RSI 0.3) = 1.4, rounded to 1.6
        # for headroom. Scores above 1.6 clip to 1.0 (extreme confluence = max conviction).
        _PROB_NORM = 1.6
        prob    = float(np.clip(score / _PROB_NORM, -1.0, 1.0))
        is_chop = bb_w < 0.015 and abs(prob) < CHOP_PROB_CEILING

        if is_chop:          regime = "CHOP"
        elif prob > 0.20:    regime = "BULL"
        elif prob < -0.20:   regime = "BEAR"
        else:                regime = "CHOP"

        # ── Swing scoring (from v8, unchanged) ───────────────────────────────
        swing = {}
        now_h = _ist_now().hour + _ist_now().minute / 60
        if 13.5 <= now_h <= 14.5:
            swing_score = 0
            if abs(prob) >= SWING_MIN_PROB:    swing_score += 2
            if abs(float(slope)) > 0.4:        swing_score += 1
            if rsi > 65 or rsi < 35:           swing_score += 1
            if pcr_bull or pcr_bear:           swing_score += 1
            is_vix_safe = vix <= 20.0
            is_allowed  = (swing_score >= SWING_MIN_SCORE) and (abs(prob) >= SWING_MIN_PROB) and is_vix_safe
            swing = {
                "swing_score"  : swing_score,
                "swing_allowed": is_allowed,
                "direction"    : "CE" if prob > 0 else "PE",
                "conviction"   : "EXTREME" if swing_score == 5 else "HIGH",
                "rationale"    : (
                    f"Score {swing_score}/5 | prob={prob:+.3f} | RSI={rsi:.0f} | "
                    f"PCR={pcr:.2f} | VIX={vix:.1f}"
                ),
            }

        if   is_chop:                      sig = "CHOP"
        elif abs(prob) >= SNIPER_MIN_PROB: sig = "SNIPER"
        elif abs(prob) >= SCALP_MIN_PROB:  sig = "SCALP"
        else:                              sig = "HOLD"

        result.update({
            "prob"       : round(prob, 4),
            "direction"  : "CE" if prob > 0 else "PE",
            "signal_type": sig,
            "regime"     : regime,
            "vol_surge"  : vol_surge,
            "bull_candle": bull_candle,
            "bear_candle": bear_candle,
            "swing"      : swing,
            "breakdown"  : {
                "rsi"      : round(rsi, 1),
                "macd_h"   : round(macd_h, 2),
                "bb_pos"   : round(bb_pos, 3),
                "bb_width" : round(bb_w, 4),
                "vwap_dist": round(vwap_dist, 2),
                "ma5_v_20" : round(ma5 - ma20, 2),
                "pcr"      : round(pcr, 2),
                "vix"      : round(vix, 1),
                "is_chop"  : is_chop,
            "vwap_shadow": {
                "valid": shadow_valid,
                "reason": vwap_shadow.get("reason"),
                "rolling_vwap_direction": (
                    "BULLISH" if vwap_dist > 0 else "BEARISH"
                ),
                "session_vwap": vwap_shadow.get("vwap"),
                "session_vwap_distance": vwap_shadow.get("distance"),
                "session_vwap_direction": vwap_shadow.get("direction"),
                "agreement": (
                    (
                        "BULLISH" if vwap_dist > 0 else "BEARISH"
                    ) == vwap_shadow.get("direction")
                    if shadow_valid else None
                ),
                "shadow_probability": shadow_probability,
                "shadow_signal": shadow_signal,
            },
            },
        })
        if shadow_valid:
            _log.info(
                "[VWAP SHADOW COMPARE] %s | "
                "ROLLING=%s | SESSION=%s | "
                "AGREEMENT=%s | LIVE_PROB=%.4f | "
                "SHADOW_PROB=%.4f | LIVE_SIGNAL=%s | "
                "SHADOW_SIGNAL=%s",
                candles.attrs.get("index_key", "UNKNOWN"),
                "BULLISH" if vwap_dist > 0 else "BEARISH",
                vwap_shadow.get("direction"),
                (
                    ("BULLISH" if vwap_dist > 0 else "BEARISH")
                    == vwap_shadow.get("direction")
                ),
                prob,
                shadow_probability,
                sig,
                shadow_signal,
            )
        
        else:
            _log.info(
                "[VWAP SHADOW INVALID] %s | reason=%s",
                candles.attrs.get("index_key", "UNKNOWN"),
                vwap_shadow.get("reason", "unavailable"),
            )

        # Persist the rolling/session VWAP comparison.
        # This is analytics-only and cannot modify live probability.
        _persist_vwap_shadow_observation(
            symbol=str(
                candles.attrs.get("index_key", "UNKNOWN")
            ),
            candles=candles,
            snap=snap,
            rolling_vwap=rolling_vwap,
            rolling_distance=vwap_dist,
            live_probability=prob,
            live_signal=sig,
            shadow_probability=shadow_probability,
            shadow_signal=shadow_signal,
            shadow=vwap_shadow,
        )

        return result



# ══════════════════════════════════════════════════════════════════════════════
#  EXPIRY HELPER — Smart Weekly + Monthly, Expiry-Day Aware
# ══════════════════════════════════════════════════════════════════════════════

import calendar as _calendar


# ══════════════════════════════════════════════════════════════════════════════
# DYNAMIC EXPIRY RESOLVER — EXCHANGE-DATE AWARE
#
# Source of truth: recently collected option_ticks in PostgreSQL.
# Normal trades: nearest expiry strictly AFTER today.
# ZERO_TO_HERO: today's expiry when available, otherwise nearest future expiry.
# No hardcoded weekday/monthly expiry fallback.
# ══════════════════════════════════════════════════════════════════════════════

def _available_expiries(
    index_key: str,
    strike: int = None,
    direction: str = None
) -> list:
    """
    Return recent expiries actually collected for this index.

    When strike and direction are supplied, return only expiries for which
    that exact option contract has a recent, positive-LTP database row.
    """
    if index_key not in INDICES:
        _log.error("Expiry resolver: unknown index %s", index_key)
        return []

    exact_contract = strike is not None and direction is not None

    if (strike is None) != (direction is None):
        _log.error(
            "[%s] Expiry resolver requires BOTH strike and direction "
            "for exact-contract lookup.",
            index_key
        )
        return []

    if exact_contract:
        opt_type = str(direction).strip().upper()
        opt_type = {"CALL": "CE", "PUT": "PE"}.get(opt_type, opt_type)

        if opt_type not in ("CE", "PE"):
            _log.error("[%s] Invalid option direction: %s", index_key, direction)
            return []

        max_age = max(
            1,
            int(float(os.getenv("QUOTE_MAX_AGE_SECONDS", "120")))
        )
    else:
        opt_type = None
        max_age = max(
            60,
            int(os.getenv("EXPIRY_DATA_MAX_AGE_SECONDS", "900"))
        )

    try:
        with _shared_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT expiry, MAX(ts) AS latest_ts
                    FROM option_ticks
                    WHERE index_key = %s
                      AND expiry IS NOT NULL
                      AND ts >= NOW() - (%s * INTERVAL '1 second')
                      AND ltp > 0
                      AND (%s::numeric IS NULL OR strike = %s::numeric)
                      AND (%s::text IS NULL OR UPPER(opt_type) = %s::text)
                    GROUP BY expiry
                    ORDER BY expiry ASC
                """, (
                    index_key,
                    max_age,
                    float(strike) if exact_contract else None,
                    float(strike) if exact_contract else None,
                    opt_type,
                    opt_type,
                ))

                rows = cur.fetchall()

        today = _ist_now().date()
        expiries = []

        for row in rows:
            expiry_value = row[0]

            if hasattr(expiry_value, "date"):
                expiry_date = expiry_value.date()
            else:
                try:
                    expiry_date = datetime.strptime(
                        str(expiry_value)[:10], "%Y-%m-%d"
                    ).date()
                except (TypeError, ValueError):
                    continue

            if expiry_date >= today and expiry_date not in expiries:
                expiries.append(expiry_date)

        expiries.sort()

        _log.info(
            "[%s] %s expiries: %s",
            index_key,
            "Exact-contract" if exact_contract else "Index-wide",
            [d.isoformat() for d in expiries]
        )

        return expiries

    except Exception:
        _log.exception(
            "[%s] Could not read expiry data from PostgreSQL",
            index_key
        )
        return []


def _get_expiry(
    index_key: str,
    trade_type: str = "INTRADAY",
    force_next: bool = False,
    next_week: bool = False,
    strike: int = None,
    direction: str = None,
    same_day_only: bool = False
) -> str:
    """
    Resolve expiry from recently collected contracts.

    If strike and direction are supplied, expiry selection is exact-contract
    aware. No expiry is returned unless that contract has recent positive LTP.

    ZERO_TO_HERO can use today's expiry. With same_day_only=True it will
    NOT fall back to a future expiry.
    """
    trade_type = str(trade_type or "INTRADAY").strip().upper()
    zero_to_hero = trade_type in {"ZERO_TO_HERO", "ZEROHERO", "0DTE"}

    today = _ist_now().date()

    expiries = _available_expiries(
        index_key,
        strike=strike,
        direction=direction
    )

    if not expiries:
        _log.error(
            "[%s] EXPIRY BLOCKED: no recent matching contract data. "
            "trade_type=%s strike=%s direction=%s",
            index_key, trade_type, strike, direction
        )
        return ""

    if zero_to_hero:
        eligible = [d for d in expiries if d >= today]

        if same_day_only:
            eligible = [d for d in eligible if d == today]
    else:
        eligible = [d for d in expiries if d > today]

    if force_next or next_week:
        eligible = eligible[1:]

    if not eligible:
        _log.warning(
            "[%s] EXPIRY BLOCKED: no eligible exact contract. "
            "today=%s trade_type=%s strike=%s direction=%s "
            "same_day_only=%s available=%s",
            index_key,
            today,
            trade_type,
            strike,
            direction,
            same_day_only,
            [d.isoformat() for d in expiries]
        )
        return ""

    selected = eligible[0]
    result = selected.strftime("%d %b %y").upper()

    _log.info(
        "[%s] EXPIRY SELECTED: type=%s strike=%s direction=%s "
        "expiry=%s same_day_only=%s",
        index_key,
        trade_type,
        strike,
        direction,
        result,
        same_day_only
    )

    return result

def select_optional_itm_strike(
    fetcher,
    index_key: str,
    base_strike: int,
    direction: str,
    expiry: str
) -> dict:
    """
    1-step ITM is always the default.

    2-step ITM is an optional upgrade only when:
      - Both exact contracts have fresh quotes.
      - The 2-step contract has meaningfully stronger Delta.
      - Its Theta decay per rupee of premium is no worse.
      - Its extra premium is within the configured limit.

    Any error or missing data returns the 1-step ITM strike.
    Never blocks a trade.
    """
    import math
    import os

    result = {
        "strike": int(base_strike),
        "selected_step": 1,
        "reason": "default_1_step_itm"
    }

    try:
        if os.getenv("ITM_TWO_STEP_ENABLED", "true").lower() not in (
            "1", "true", "yes", "on"
        ):
            result["reason"] = "feature_disabled"
            return result

        direction = str(direction).strip().upper()
        if direction not in ("CE", "PE"):
            return result

        gap = int(INDICES[index_key]["strike_gap"])

        two_step_strike = (
            base_strike - gap
            if direction == "CE"
            else base_strike + gap
        )

        # Both quotes must be available for comparison.
        # Failure simply preserves the default strike.
        one_quote = fetcher.get_option_quote(
            index_key, base_strike, direction, expiry
        )

        two_quote = fetcher.get_option_quote(
            index_key, two_step_strike, direction, expiry
        )

        if not one_quote.get("entry_eligible"):
            result["reason"] = "baseline_quote_not_available"
            return result

        if not two_quote.get("entry_eligible"):
            result["reason"] = "upgrade_quote_not_available"
            return result

        one_premium = float(one_quote.get("ltp", 0) or 0)
        two_premium = float(two_quote.get("ltp", 0) or 0)

        if one_premium <= 0 or two_premium <= 0:
            result["reason"] = "invalid_comparison_premium"
            return result

        one_greeks = fetcher.get_option_greeks(
            index_key, base_strike, direction, expiry
        )

        two_greeks = fetcher.get_option_greeks(
            index_key, two_step_strike, direction, expiry
        )

        if not one_greeks or not two_greeks:
            result["reason"] = "greeks_unavailable"
            return result

        one_delta = abs(float(one_greeks["delta"]))
        two_delta = abs(float(two_greeks["delta"]))

        one_theta = abs(float(one_greeks["theta"]))
        two_theta = abs(float(two_greeks["theta"]))

        if not all(math.isfinite(v) for v in (
            one_delta, two_delta, one_theta, two_theta
        )):
            result["reason"] = "invalid_greeks"
            return result

        delta_gain = two_delta - one_delta

        min_delta_gain = float(
            os.getenv("ITM_TWO_STEP_MIN_DELTA_GAIN", "0.05")
        )

        premium_uplift_pct = (
            (two_premium / one_premium) - 1
        ) * 100

        max_premium_uplift = float(
            os.getenv("ITM_TWO_STEP_MAX_PREMIUM_UPLIFT_PCT", "30")
        )

        # Compare daily Theta decay relative to the premium paid.
        one_theta_drag = one_theta / one_premium
        two_theta_drag = two_theta / two_premium

        max_theta_drag_ratio = float(
            os.getenv("ITM_TWO_STEP_MAX_THETA_DRAG_RATIO", "1.0")
        )

        if delta_gain < min_delta_gain:
            result["reason"] = "insufficient_delta_improvement"
            return result

        if two_theta_drag > one_theta_drag * max_theta_drag_ratio:
            result["reason"] = "theta_efficiency_not_better"
            return result

        if premium_uplift_pct > max_premium_uplift:
            result["reason"] = "premium_uplift_too_high"
            return result

        result.update({
            "strike": int(two_step_strike),
            "selected_step": 2,
            "reason": "two_step_itm_greek_upgrade",
            "one_step_delta": round(one_delta, 4),
            "two_step_delta": round(two_delta, 4),
            "delta_gain": round(delta_gain, 4),
            "one_step_theta": round(one_theta, 4),
            "two_step_theta": round(two_theta, 4),
            "premium_uplift_pct": round(premium_uplift_pct, 2),
        })

        _log.info(
            "[%s] ITM upgrade: %s -> %s | Delta gain=%.3f | "
            "Premium uplift=%.1f%%",
            index_key,
            base_strike,
            two_step_strike,
            delta_gain,
            premium_uplift_pct
        )

        return result

    except Exception as exc:
        _log.warning(
            "[%s] Optional ITM selector failed; retaining 1-step ITM: %s",
            index_key, exc
        )
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  STRATEGY ENGINE
#  Candle-direction guard intentionally DISABLED; metrics remain diagnostic.
#  Volume-surge and existing strategy filters remain unchanged.
# ══════════════════════════════════════════════════════════════════════════════

class StrategyEngine:

    @staticmethod
    def sniper(analysis: dict, snap: dict, index_key: str, is_reversal: bool = False) -> dict:
        if analysis["signal_type"] != "SNIPER":
            return {"active": False}

        # v9/v11: Volume surge filter — skip if no institutional activity
        if not analysis.get("vol_surge", True):
            _log.info(f"[{index_key}] SNIPER blocked — volume below 1.2× avg")
            return {"active": False}

        # Intentionally do not filter by last-candle direction; user chose to
        # keep this guard disabled because it filtered out too many valid setups.
        #direction = analysis["direction"]
        #if direction == "CE" and not analysis.get("bull_candle", True):
        #    _log.info(f"[{index_key}] SNIPER CE blocked — last candle is bearish")
        #    return {"active": False}
        #if direction == "PE" and not analysis.get("bear_candle", True):
        #    _log.info(f"[{index_key}] SNIPER PE blocked — last candle is bullish")
        #    return {"active": False}

        cfg    = INDICES[index_key]
        floors = INDEX_OPTION_FLOORS.get(index_key, {"sniper_sl": 45, "sniper_tp": 110})
        spot   = snap["spot"]
        atr    = analysis["atr"]
        vix    = snap.get("vix", analysis.get("breakdown", {}).get("vix", 15.0))
        gap    = cfg["strike_gap"]
        atm = _nearest_option_strike(spot, gap)
        d      = analysis["direction"]
        strike = atm - gap if d == "CE" else atm + gap
        mul    = _vix_sl_multiplier(vix)

        sl_raw    = max(floors["sniper_sl"], round(atr * 1.2 / 5) * 5)
        tp_raw    = max(floors["sniper_tp"], round(atr * 3.0 / 5) * 5)
        sl_pts    = round(sl_raw * mul / 5) * 5
        tp_pts    = round(tp_raw * mul / 5) * 5
        trail_pts = round(tp_pts * 0.5 / 5) * 5

        # 🔧 REVERSAL LOGIC: Tighter SL, delayed trail-to-breakeven
        if is_reversal:
            sl_pts = max(20, round(sl_pts * 0.75 / 5) * 5) # 25% tighter
            trail_pts = round(tp_pts * 0.8 / 5) * 5        # Delay trailing until 80% to target
        return {
            "active"    : True, "type": "SNIPER",
            "direction" : d,
            "strike"    : strike,
            "sl_pts"    : sl_pts,
            "tp_pts"    : tp_pts,
            "trail_pts" : trail_pts,
            "expiry"    : _get_expiry(index_key, trade_type="SNIPER"),
            "confidence": "HIGH" if abs(analysis["prob"]) >= 0.60 else "MEDIUM",
            "vix_mul"   : mul,
        }

    @staticmethod
    def scalp(analysis: dict, snap: dict, index_key: str) -> dict:
        if analysis["signal_type"] not in ("SNIPER", "SCALP"):
            return {"active": False}
        cfg    = INDICES[index_key]
        floors = INDEX_OPTION_FLOORS.get(index_key, {"scalp_sl": 30, "scalp_tp": 60})
        spot   = snap["spot"]
        vix    = snap.get("vix", analysis.get("breakdown", {}).get("vix", 15.0))
        gap    = cfg["strike_gap"]
        atm = _nearest_option_strike(spot, gap)
        d      = analysis["direction"]
        strike = atm - gap if d == "CE" else atm + gap
        mul    = _vix_sl_multiplier(vix)
        sl_pts = round(floors["scalp_sl"] * mul / 5) * 5
        tp_pts = round(floors["scalp_tp"] * mul / 5) * 5
        return {
            "active"    : True, "type": "SCALP",
            "direction" : d,
            "strike"    : strike,
            "sl_pts"    : sl_pts,
            "tp_pts"    : tp_pts,
            "trail_pts" : round(tp_pts * 0.5 / 5) * 5,
            "expiry"    : _get_expiry(index_key, trade_type="SCALP"),
            "confidence": "HIGH" if abs(analysis["prob"]) >= 0.60 else "MEDIUM",
        }

    @staticmethod
    def swing(analysis: dict, snap: dict, index_key: str) -> dict:
        sw = analysis.get("swing", {})
        if not sw.get("swing_allowed"):
            return {"active": False}
        now_t    = _ist_now().time()
        earliest = pd.Timestamp("13:30").time()
        latest   = pd.Timestamp("14:45").time()
        if not (earliest <= now_t <= latest):
            return {"active": False}
        cfg    = INDICES[index_key]
        floors = INDEX_SWING_FLOORS.get(index_key, {"sl_pts": 85, "tp_pts": 190})
        spot   = snap["spot"]
        vix    = snap.get("vix", analysis.get("breakdown", {}).get("vix", 15.0))
        gap    = cfg["strike_gap"]
        atm    = round(spot / gap) * gap
        mul    = _vix_sl_multiplier(vix)
        return {
            "active"     : True, "type": "SWING",
            "direction"  : sw["direction"],
            "strike"     : atm,
            "expiry"     : _get_expiry(index_key, trade_type="SWING"),
            "sl_pts"     : round(floors["sl_pts"] * mul / 5) * 5,
            "tp_pts"     : round(floors["tp_pts"] * mul / 5) * 5,
            "swing_score": sw["swing_score"],
            "conviction" : sw["conviction"],
            "rationale"  : sw["rationale"],
        }

    @staticmethod
    def chop_sell(analysis: dict, snap: dict, index_key: str) -> dict:
        cfg  = INDICES[index_key]
        spot = snap["spot"]
        atr  = analysis["atr"]
        gap  = cfg["strike_gap"]
        def ts(p): return round(p / gap) * gap
        return {
            "active"     : True, "type": "CHOP_SELL",
            "atm"        : ts(spot),
            "short_call" : ts(spot + max(1, round(1.5*atr/gap)) * gap), "short_put": ts(spot - max(1, round(1.5*atr/gap)) * gap),
            "long_call"  : ts(spot + max(2, round(2.5*atr/gap)) * gap), "long_put" : ts(spot - max(2, round(2.5*atr/gap)) * gap),
            "profit_zone": f"{ts(spot - 1.5*atr)} — {ts(spot + 1.5*atr)}",
            "range_pts"  : ts(spot + 1.5 * atr) - ts(spot - 1.5 * atr),
            "expiry"     : _get_expiry(index_key, trade_type="CHOP"),
            "atr"        : round(atr, 1),
        }

    @staticmethod
    def breakout_watch(candles: pd.DataFrame, spot: float, index_key: str) -> dict:
        if candles.empty or len(candles) < 10:
            return {"active": False}
        cfg = INDICES[index_key]
        gap = cfg["strike_gap"]
        lb  = min(BREAKOUT_LOOKBACK, len(candles))
        hi  = float(candles["high"].tail(lb).max())
        lo  = float(candles["low"].tail(lb).min())
        pivot     = round((hi + lo + spot) / 3 / gap) * gap
        atr_proxy = (hi - lo) / lb
        ri        = max(gap * 4, 100)
        return {
            "active"     : True, "type": "BREAKOUT",
            "pivot"      : pivot,
            "r1"         : round((pivot + atr_proxy) / gap) * gap,
            "s1"         : round((pivot - atr_proxy) / gap) * gap,
            "period_high": round(hi, 1),
            "period_low" : round(lo, 1),
            "bull_break" : max(hi, (round(spot / ri) + 1) * ri),
            "bear_break" : min(lo, round(spot / ri) * ri),
        }
    @staticmethod
    def strong_breakout_retest(
        candles: pd.DataFrame,
        spot: float,
        index_key: str,
        pending: dict,
        now_ts: float,
    ) -> dict:

        result = {
            "active": False,
            "stage": "NO_SETUP",
            "direction": None,
            "level": None,
            "reason": None,
        }

        required = {"open", "high", "low", "close", "volume"}

        if candles is None or candles.empty:
            result["reason"] = "no_candles"
            return result

        if not required.issubset(candles.columns):
            result["reason"] = "missing_ohlcv_columns"
            return result

        minimum_rows = STRONG_BK_MIN_BARS + (
            1 if "is_complete" in candles.columns else 2
        )
        if len(candles) < minimum_rows:
            result["reason"] = "insufficient_history"
            return result

        try:
            data = candles.copy()

            for col in required:
                data[col] = pd.to_numeric(data[col], errors="coerce")

            data = data.replace([np.inf, -np.inf], np.nan)
            data = data.dropna(subset=list(required))

            if len(data) < minimum_rows:
                result["reason"] = "insufficient_valid_bars"
                return result

            # PostgreSQL candle readers return completed bars only. When
            # completion metadata exists, use all explicitly completed rows;
            # otherwise retain the legacy safety rule and exclude the final
            # potentially-forming row.
            if "is_complete" in data.columns:
                complete_mask = data["is_complete"].map(
                    lambda value: value is True
                    or str(value).strip().lower() in {"true", "1", "yes"}
                )
                closed = data.loc[complete_mask].copy()
            else:
                closed = data.iloc[:-1].copy()

            if len(closed) < STRONG_BK_MIN_BARS + 1:
                result["reason"] = "insufficient_closed_bars"
                return result

            last = closed.iloc[-1]
            history = closed.iloc[-(STRONG_BK_MIN_BARS + 1):-1]

            if len(history) < STRONG_BK_MIN_BARS:
                result["reason"] = "insufficient_breakout_history"
                return result

            atr = (
                closed["high"] - closed["low"]
            ).rolling(14).mean().iloc[-1]

            if not math.isfinite(float(atr)) or atr <= 0:
                result["reason"] = "invalid_atr"
                return result

            atr_pct = float(atr) / float(spot)
            result["atr"] = round(float(atr), 2)
            result["atr_pct"] = round(atr_pct, 6)
            if atr_pct < STRONG_BK_MIN_ATR_PCT:
                result["reason"] = "volatility_too_low"
                return result

            candle_range = float(last["high"] - last["low"])

            if candle_range <= 0:
                result["reason"] = "invalid_candle_range"
                return result

            body_ratio = abs(
                float(last["close"] - last["open"])
            ) / candle_range

            avg_volume = float(history["volume"].tail(20).mean())
            last_volume = float(last["volume"])

            if avg_volume <= 0 or last_volume <= 0:
                result["reason"] = "invalid_volume"
                return result

            volume_ratio = last_volume / avg_volume

            resistance = float(history["high"].max())
            support = float(history["low"].min())

            # Check whether a breakout is already waiting for a retest.
            if pending:
                age = now_ts - float(pending["created_at"])

                if age > STRONG_BK_RETEST_MAX_AGE_SEC:
                    pending.clear()
                    result["reason"] = "retest_expired"
                    return result

                direction = pending["direction"]
                level = float(pending["level"])
                tolerance = atr * STRONG_BK_RETEST_ATR

                if direction == "UP":
                    touched = (
                        float(last["low"]) <= level + tolerance
                        and float(last["low"]) >= level - tolerance
                    )

                    held = float(last["close"]) > level
                    rejection = (
                        float(last["close"]) > float(last["open"])
                        and body_ratio >= STRONG_BK_CONFIRM_BODY_RATIO
                    )

                else:
                    touched = (
                        float(last["high"]) >= level - tolerance
                        and float(last["high"]) <= level + tolerance
                    )

                    held = float(last["close"]) < level
                    rejection = (
                        float(last["close"]) < float(last["open"])
                        and body_ratio >= STRONG_BK_CONFIRM_BODY_RATIO
                    )

                if touched and held and rejection:
                    pending.clear()

                    result.update({
                        "active": True,
                        "stage": "RETEST_CONFIRMED",
                        "direction": direction,
                        "level": level,
                        "atr": round(float(atr), 2),
                        "volume_ratio": round(volume_ratio, 2),
                        "body_ratio": round(body_ratio, 2),
                    })
                    return result

                # Do not replace an existing pending setup with another one.
                result["stage"] = "WAITING_FOR_RETEST"
                result["direction"] = direction
                result["level"] = level
                result["reason"] = "retest_not_confirmed"
                return result

            # New bullish breakout.
            breakout_buffer = atr * STRONG_BK_MIN_BREAKOUT_ATR

            # New bullish breakout.
            bullish = (
                float(last["close"]) > resistance + breakout_buffer
                and float(last["close"]) > float(last["open"])
                and body_ratio >= STRONG_BK_MIN_BODY_RATIO
                and volume_ratio >= STRONG_BK_VOLUME_MULT
            )
            
            # New bearish breakout.
            bearish = (
                float(last["close"]) < support - breakout_buffer
                and float(last["close"]) < float(last["open"])
                and body_ratio >= STRONG_BK_MIN_BODY_RATIO
                and volume_ratio >= STRONG_BK_VOLUME_MULT
            )

            if not bullish and not bearish:
                result["reason"] = "breakout_filters_not_met"
                return result

            direction = "UP" if bullish else "DOWN"
            level = resistance if bullish else support

            extension = (
                float(last["close"]) - level
                if bullish
                else level - float(last["close"])
            )

            if extension < 0 or extension > atr * STRONG_BK_MAX_EXTENSION_ATR:
                result["reason"] = "breakout_extension_invalid"
                return result

            pending.update({
                "direction": direction,
                "level": float(level),
                "created_at": float(now_ts),
            })

            result.update({
                "active": True,
                "stage": "BREAKOUT_DETECTED",
                "direction": direction,
                "level": float(level),
                "atr": round(float(atr), 2),
                "volume_ratio": round(volume_ratio, 2),
                "body_ratio": round(body_ratio, 2),
            })

            return result

        except (TypeError, ValueError, KeyError, IndexError, OverflowError):
            _log.exception("[%s] Strong breakout evaluation failed", index_key)
            result["reason"] = "evaluation_error"
            return result


# ══════════════════════════════════════════════════════════════════════════════
#  VIP MESSAGE FORMATTER
#  v11: uses v10's cleaner compact formats for jackpot, trail, and sl_hit
# ══════════════════════════════════════════════════════════════════════════════

class Fmt:

    @staticmethod
    def vip_jackpot_hit(cfg: dict, strike: int, direction: str, expiry: str,
                        entry_premium: float, tp_price: float, tp_pts: float,
                        trade_profit: int) -> str:
        de = "🟢" if direction == "CE" else "🔴"
        gain_pct = round(((tp_price - entry_premium) / entry_premium) * 100)
        return (
            f"🎰 <b>JACKPOT — ALL TARGETS HIT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{cfg['label']}  {strike} {direction}</b>  |  <i>{expiry}</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Entry   :  <b>₹{entry_premium:.1f}</b>\n"
            f"🎯 Exit    :  <b>₹{tp_price:.1f}</b>  <i>(+{tp_pts:.0f} pts  /  +{gain_pct}%)</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>{TRADE_LOTS}-Lot Profit : ₹{trade_profit:,}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Book full profit. Trade closed. Well done. 🎯\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def vip_moonbag_start(cfg: dict, strike: int, direction: str, expiry: str,
                          entry_premium: float, final_price: float, curr: float,
                          sl_price: float, locked_profit: int) -> str:
        de = "🟢" if direction == "CE" else "🔴"
        return (
            f"🏁 <b>FINAL TARGET HIT — MOONBAG MODE — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{strike} {direction} | Expiry {expiry}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Entry        : <b>₹{entry_premium:.1f}</b>\n"
            f"🎯 Final Target : <b>₹{final_price:.1f}</b>\n"
            f"📈 Now          : <b>₹{curr:.1f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 <b>SL moved to ₹{sl_price:.1f}</b> (full target profit secured)\n"
            f"💰 <b>{TRADE_LOTS}-Lot Locked Profit : ₹{locked_profit:,}</b>\n"
            f"🚀 <i>Signal stays OPEN. SL trails up automatically.</i>\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def free_jackpot_hit(cfg: dict, strike: int, direction: str, expiry: str,
                         entry_premium: float, tp_price: float, tp_pts: float,
                         trade_profit: int) -> str:
        de  = "🟢" if direction == "CE" else "🔴"
        opp = "CALL" if direction == "CE" else "PUT"
        gain_pct = round(((tp_price - entry_premium) / entry_premium) * 100)
        ai_hook  = _generate_ai_hook(cfg["label"], direction, 15.0, alert_type="jackpot")
        return (
            f"🎰 <b>JACKPOT — ₹{trade_profit:,} PROFIT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{cfg['label']}  {strike} {opp}</b>  |  <i>{expiry}</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 VIP entry  :  <b>₹{entry_premium:.1f}</b>\n"
            f"🎯 Exited at  :  <b>₹{tp_price:.1f}</b>  <i>(+{tp_pts:.0f} pts  /  +{gain_pct}%)</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>{ai_hook}</i>\n\n"
            f"VIPs were in from ₹{entry_premium:.0f} to ₹{tp_price:.0f}.\n"
            f"<i>Same market. Different information. 👇</i>"
            + _pay_block()
        )

    @staticmethod
    def vip_sniper(cfg: dict, analysis: dict, s: dict, spot: float,
                   vix: float, entry_premium: float = 0.0, is_jackpot: bool = False) -> str:
        de = "📈" if s["direction"] == "CE" else "📉"

        if entry_premium > 0:
            sl_price     = round(entry_premium - s["sl_pts"], 1)
            trade_profit = round(s["tp_pts"] * cfg["lot"] * TRADE_LOTS)

            # Universal 3-stage target ladder
            t1 = round(entry_premium + (s["tp_pts"] * 0.4), 1)
            t2 = round(entry_premium + (s["tp_pts"] * 0.7), 1)
            t3 = round(entry_premium + s["tp_pts"], 1)

            tgt_block = (f"🎯 <b>Target 1 : ₹{t1}</b>\n"
                         f"🎯 <b>Target 2 : ₹{t2}</b>\n"
                         f"🎯 <b>Final Target : ₹{t3} (+{s['tp_pts']} pts)</b>")

            prem_block = (
                f"💰 <b>Entry Premium : ₹{entry_premium:.1f}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛑 <b>Stop Loss : ₹{sl_price} (−{s['sl_pts']} pts)</b>\n"
                f"{tgt_block}\n"
                f"💰 <b>{TRADE_LOTS}-Lot Profit (Max) : ₹{trade_profit:,}</b>"
            )
        else:
            prem_block = (f"🛑 <b>Stop Loss : −{s['sl_pts']} pts from entry</b>\n"
                          f"🎯 <b>Target : +{s['tp_pts']} pts from entry</b>")

        header = (f"🎰 <b>JACKPOT TRADE — {cfg['label']}</b> 🎰"
                  if is_jackpot else f"{de} <b>SNIPER TRADE — {cfg['label']}</b>")

        # Lot advisory based on VIX
        if vix > 20:   lot_adv = f"\n⚠️ <b>VIX {vix:.1f} — Trade 1 lot today. High volatility.</b>"
        elif vix > 17: lot_adv = f"\n⚠️ <b>VIX {vix:.1f} — Consider half-size.</b>"
        else:          lot_adv = ""

        confirm = "\n✅ <i>Volume-surge filter passed ✓</i>" if entry_premium > 0 else ""

        # Display option entry right below strike if available
        entry_display = f"💳 <b>Option Entry : ₹{entry_premium:.1f}</b>\n" if entry_premium > 0 else ""

        return (
            f"{header}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Direction : {'CALL (CE)' if s['direction']=='CE' else 'PUT (PE)'}</b>\n"
            f"📦 <b>Strike : {s['strike']} | Expiry {s['expiry']}</b>\n"
            f"{entry_display}"
            f"💳 <b>Spot : {spot:.1f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{prem_block}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>ENTER AT MARKET PRICE</b>\n"
            f"🕒 <i>{_now()}</i>{lot_adv}{confirm}\n\n"
            f"<i>SL is mandatory. Protect capital first.</i>"
        )
                       
    @staticmethod
    def vip_live_sniper(cfg: dict, s: dict, spot: float, vix: float,
                        entry_premium: float, prob: float, index_key: str = "") -> str:
        """v10/v11 — Between-scan live detection message."""
        de = "📈" if s["direction"] == "CE" else "📉"
        direction_label = "CALL (CE)" if s["direction"] == "CE" else "PUT (PE)"

        if entry_premium > 0:
            sl_price    = round(entry_premium - s["sl_pts"], 1)
            tp_price    = round(entry_premium + s["tp_pts"], 1)
            trade_profit = round(s["tp_pts"] * cfg["lot"] * TRADE_LOTS)

            # Show all 3 targets universally
            t1 = round(entry_premium + (s["tp_pts"] * 0.4), 1)
            t2 = round(entry_premium + (s["tp_pts"] * 0.7), 1)

            tgt_block = (f"🎯 <b>Target 1 : ₹{t1}</b>\n"
                         f"🎯 <b>Target 2 : ₹{t2}</b>\n"
                         f"🎯 <b>Final Target : ₹{tp_price} (+{s['tp_pts']} pts)</b>")

            prem_block = (
                f"💰 <b>Entry Premium : ₹{entry_premium:.1f}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛑 <b>Stop Loss : ₹{sl_price} (−{s['sl_pts']} pts)</b>\n"
                f"{tgt_block}\n"
                f"💰 <b>{TRADE_LOTS}-Lot Profit if TGT : ₹{trade_profit:,}</b>"
            )
        else:
            prem_block = (
                f"🛑 <b>Stop Loss : −{s['sl_pts']} pts from entry</b>\n"
                f"🎯 <b>Target : +{s['tp_pts']} pts from entry</b>"
            )

        if vix > 20:   lot_adv = f"\n⚠️ <b>VIX {vix:.1f} — Trade 1 lot today.</b>"
        elif vix > 17: lot_adv = f"\n⚠️ <b>VIX {vix:.1f} — Consider half-size.</b>"
        else:          lot_adv = ""

        conviction = "EXTREME" if abs(prob) >= 0.70 else "HIGH"

        return (
            f"⚡ <b>LIVE DETECTION — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Algo caught this between scheduled scans.</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>Direction : {direction_label}</b>\n"
            f"📦 <b>Strike : {s['strike']} | Expiry {s['expiry']}</b>\n"
            f"💳 <b>Spot : {spot:.1f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{prem_block}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🧠 <b>Conviction : {conviction} (prob {prob:+.3f})</b>\n"
            f"✅ <i>Volume-surge filter passed ✓</i>\n"
            f"⚡ <b>ENTER AT MARKET PRICE</b>\n"
            f"🕒 <i>{_now()}</i>{lot_adv}\n\n"
            f"<i>SL is mandatory. Protect capital first.</i>"
        )

    @staticmethod
    def vip_swing(cfg: dict, analysis: dict, s: dict, spot: float,
                  entry_premium: float = 0.0) -> str:
        de   = "🟢" if s["direction"] == "CE" else "🔴"
        bull = s["direction"] == "CE"
        lot  = cfg["lot"]

        if entry_premium > 0:
            sl_price    = round(entry_premium - s["sl_pts"], 1)
            tp_price    = round(entry_premium + s["tp_pts"], 1)
            trade_profit = round(s["tp_pts"] * lot * TRADE_LOTS)
            price_block = (
                f"💰 <b>Entry Premium : ₹{entry_premium:.1f}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛑 <b>Stop Loss : ₹{sl_price} (−{s['sl_pts']} pts)</b>\n"
                f"🎯 <b>Target    : ₹{tp_price} (+{s['tp_pts']} pts)</b>\n"
                f"💰 <b>{TRADE_LOTS}-Lot Profit if TGT : ₹{trade_profit:,}</b>"
            )
        else:
            price_block = (
                f"🛑 <b>Stop Loss : −{s['sl_pts']} pts from your entry price</b>\n"
                f"🎯 <b>Target    : +{s['tp_pts']} pts from your entry price</b>"
            )

        return (
            f"🌙 <b>NEXT-DAY SWING — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{'BULLISH' if bull else 'BEARISH'}</b>\n\n"
            f"📦 <b>Strike : {s['strike']} | Expiry {s['expiry']}</b>\n"
            f"💳 <b>Spot : {spot:.1f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{price_block}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>Rationale : {s['rationale'][:90]}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ <b>EXIT BY : 09:25 AM TOMORROW — no exceptions</b>\n"
            f"📅 <i>Expiry dynamically selected for optimal theta protection</i>\n"
            f"⚠️  <i>This is an OVERNIGHT trade. Do NOT exit today.</i>\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def vip_iron_condor(cfg: dict, s: dict, spot: float, vix: float) -> str:
        return (
            f"🎯 <b>PREMIUM SELLING — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <b>Market is in CHOP — directional trades risky.</b>\n"
            f"<b>Today's strategy: Iron Condor (sell time decay)</b>\n\n"
            f"💳 <b>Spot : {spot:.1f} | ATM {s.get('atm', 'N/A')}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📤 <b>SELL {s['short_call']} CE</b>\n"
            f"📥 <b>BUY  {s['long_call']} CE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📤 <b>SELL {s['short_put']} PE</b>\n"
            f"📥 <b>BUY  {s['long_put']} PE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ <b>Profit zone : {s['profit_zone']} ({s['range_pts']:.0f} pts wide)</b>\n"
            f"📅 <b>Expiry : {s['expiry']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 <i>Exit: 50% profit early OR spot breaks short strikes</i>\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def vip_key_levels(cfg: dict, bk: dict, spot: float, prob: float) -> str:
        de = "🟢" if prob >= 0 else "🔴"
        return (
            f"📍 *KEY LEVELS — {cfg['label']}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} Spot : `{spot:.1f}`\n\n"
            f"📈 Resistance : `{bk['r1']}`  (period high `{bk['period_high']}`)\n"
            f"📉 Support    : `{bk['s1']}`  (period low  `{bk['period_low']}`)\n"
            f"🔲 Pivot      : `{bk['pivot']}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 Bull break above `{bk['bull_break']:.0f}` → CE trigger\n"
            f"🔻 Bear break below `{bk['bear_break']:.0f}` → PE trigger\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 _{_insight()}_\n"
            f"🕒 `{_now()}`"
        )

    @staticmethod
    def vip_no_setup(cfg: dict, signal_type: str, prob: float, vix: float = 15.0,
                     threshold: float = SNIPER_MIN_PROB) -> str:
        insight = _generate_ai_hook(cfg['label'], "NEUTRAL", vix, alert_type="insight")

        if signal_type == "CHOP":
            reason = "Range-bound — Iron Condor advisory sent above"
        elif signal_type == "HOLD":
            reason = f"Prob `{prob:+.4f}` below threshold `{threshold:.2f}` — waiting for conviction"
        elif signal_type == "WARMUP":
            reason = "Not enough candle data yet — still warming up"
        else:
            reason = f"Signal type `{signal_type}` — no directional trade"

        return (
            f"🔍 <b>{cfg['label']} — No Directional Setup</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>Reason :</b> <i>{reason}</i>\n\n"
            f"💡 <b>Market Insight :</b>\n"
            f"<i>{insight}</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def vip_scan_summary(label: str, results: list) -> str:
        lines = [f"🔍 *SCAN — {label}*  `{_now()}`\n━━━━━━━━━━━━━━━━━━━━━━━"]
        for r in results:
            idx  = r.get("index", "?")
            cfg  = INDICES.get(idx, {})
            prob = r.get("prob", 0)
            sig  = r.get("signal_type", "?")
            de   = "🟢" if prob > 0 else ("🔴" if prob < 0 else "⬜")
            spot = r.get("spot", 0)
            lines.append(
                f"{cfg.get('emoji','•')} *{cfg.get('label', idx)}*  "
                f"Spot `{spot:.0f}`  prob `{prob:+.3f}`  → `{sig}`  {de}"
            )
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(lines)

    @staticmethod
    def free_daily_scalp(cfg: dict, s: dict, spot: float, prob: float,
                         vix: float, entry_premium: float = 0.0) -> str:
        de  = "📈" if s["direction"] == "CE" else "📉"
        opp = "CALL" if s["direction"] == "CE" else "PUT"
        lot = cfg["lot"]
        if entry_premium > 0:
            tp_price    = round(entry_premium + s["tp_pts"], 1)
            trade_profit = round(s["tp_pts"] * lot * TRADE_LOTS)
            prem_line   = f"💰 <b>Entry : ₹{entry_premium:.1f}</b>\n"
            sl_line     = f"🛑 <b>SL : STRICT (Exclusive for VIP Members) 🔒</b>"
            tp_line     = (f"🎯 <b>TGT : ₹{tp_price}</b> <i>(+{s['tp_pts']:.0f} pts)</i>\n"
                           f"💰 <b>{TRADE_LOTS}-Lot Profit if TGT hit : ₹{trade_profit:,}</b>\n"
                           f"🔔 <b>Trail SL : VIPs get live trailing alerts 🔒</b>")
        else:
            prem_line = ""
            sl_line   = f"🛑 <b>SL : STRICT (Exclusive for VIP Members) 🔒</b>"
            tp_line   = f"🎯 <b>TGT : {s['tp_pts']} pts above your entry</b>"

        return (
            f"{de} <b>BLACK — TODAY'S FREE SIGNAL</b>\n"
            f"📅 <b>{_today()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>Index: {cfg['label']}</b>\n"
            f"📊 <b>Trade: BUY {opp}</b>\n"
            f"📦 <b>Strike : {s['strike']} | Expiry {s['expiry']}</b>\n"
            f"💳 <b>Spot: {spot:.1f}</b>\n"
            f"{prem_line}"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{sl_line}\n"
            f"{tp_line}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Confidence: {'HIGH' if abs(prob)>=0.40 else 'MEDIUM'} | VIX: {vix:.1f}</i>\n"
            f"<i>⚠️ Trading without an SL is dangerous. Join VIP for exact SL points. 👇</i>"
            + _pay_block()
        )

    @staticmethod
    def vip_trail_alert(cfg: dict, strike: int, direction: str, expiry: str,
                        entry_premium: float, current_premium: float,
                        new_sl: float, tp_price: float) -> str:
        de = "🟢" if direction == "CE" else "🔴"
        gain_now = round(current_premium - entry_premium, 1)
        return (
            f"🔔 <b>MOVE YOUR SL NOW — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{strike} {direction}</b>  |  <i>{expiry}</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Entry     :  <b>₹{entry_premium:.1f}</b>\n"
            f"📈 Now at    :  <b>₹{current_premium:.1f}</b>  <i>(+{gain_now:.1f} pts running)</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔄 <b>New SL : ₹{new_sl:.1f}</b>  —  trade is now risk-free ✅\n"
            f"🎯 <b>Target : ₹{tp_price:.1f}</b>  —  let it run\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Half way there. Lock in the floor. Let the rest ride. 🚀</i>\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def vip_target_hit(cfg: dict, strike: int, direction: str, expiry: str,
                       entry_premium: float, tp_price: float,
                       current_premium: float, trade_profit: int,
                       tgt_name: str = "Target", is_final: bool = True) -> str:
        de     = "🟢" if direction == "CE" else "🔴"
        action = "BOOK PROFIT & EXIT NOW 🎯" if is_final else "BOOK PARTIAL & TRAIL SL 🔒"
        header = (
            f"🏁 <b>FINAL TARGET HIT — {cfg['label']}</b>"
            if is_final
            else f"🎯 <b>{tgt_name.upper()} ACHIEVED — {cfg['label']}</b>"
        )
        return (
            f"{header}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{strike} {direction} | Expiry {expiry}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 <b>Entry premium : ₹{entry_premium:.1f}</b>\n"
            f"🎯 <b>{tgt_name} : ₹{tp_price:.1f}</b>\n"
            f"📈 <b>Current : ₹{current_premium:.1f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>{TRADE_LOTS}-Lot Profit so far: ₹{trade_profit:,}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📤 <b>{action}</b>\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def vip_reversal_exit(cfg: dict, strike: int, direction: str, expiry: str,
                          entry_premium: float, exit_premium: float, trade_pnl: int) -> str:
        de        = "🟢" if direction == "CE" else "🔴"
        pnl_emoji = "🔥" if trade_pnl >= 0 else "🛡️"
        pnl_text  = "Profit" if trade_pnl >= 0 else "Loss"
        return (
            f"⚠️ <b>REVERSAL EXIT — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{strike} {direction} | Expiry {expiry}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Market trend reversed. Closing position immediately.</i>\n"
            f"📥 <b>Entry : ₹{entry_premium:.1f}</b>\n"
            f"📤 <b>Exit  : ₹{exit_premium:.1f}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>{TRADE_LOTS}-Lot {pnl_text} : ₹{trade_pnl:,}</b> {pnl_emoji}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def free_target_hit(cfg: dict, strike: int, direction: str, expiry: str,
                        entry_premium: float, tp_price: float,
                        trade_profit: int, tgt_name: str = "Target") -> str:
        de       = "🟢" if direction == "CE" else "🔴"
        opp      = "CALL" if direction == "CE" else "PUT"

        # Show "Target 1 Hit" instead of just "Target Hit"
        return (
            f"🎯 <b>{tgt_name.upper()} ACHIEVED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{cfg['label']} {strike} {opp}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 VIP entry  :  <b>₹{entry_premium:.1f}</b>\n"
            f"🎯 Exit price :  <b>₹{tp_price:.1f}</b>\n"
            f"💰 <b>Profit so far : ₹{trade_profit:,}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>VIPs have already booked this target.</i>\n"
            f"<i>Want live alerts for every target hit? Join VIP 👇</i>"
            + _pay_block()
        )

    @staticmethod
    def vip_sl_hit(cfg: dict, strike: int, direction: str, expiry: str,
                   entry_premium: float, sl_price: float,
                   current_premium: float, trade_loss: int) -> str:
        de = "🟢" if direction == "CE" else "🔴"
        return (
            f"🛑 <b>STOP LOSS HIT — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} `{strike} {direction}`  Expiry `{expiry}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Entry premium : `₹{entry_premium:.1f}`\n"
            f"🛑 SL price      : `₹{sl_price:.1f}`\n"
            f"📉 Current       : `₹{current_premium:.1f}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💸 *{TRADE_LOTS}-Lot Loss   : ₹{trade_loss:,}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📤 *EXIT THE TRADE. SL IS FINAL.* 🛡️\n"
            f"_Risk managed. Capital protected. Next setup incoming._\n"
            f"🕒 `{_now()}`"
        )

    @staticmethod
    def free_sl_hit(cfg: dict, strike: int, direction: str, expiry: str,
                    entry_premium: float, sl_price: float) -> str:
        de        = "🔴" if direction == "CE" else "🟢"
        opp       = "CALL" if direction == "CE" else "PUT"
        loss_pts  = round(entry_premium - sl_price, 1)
        return (
            f"🛑 <b>SL HIT — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{de} <b>{strike} {opp}</b>  |  <i>{expiry}</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 Entry   :  <b>₹{entry_premium:.1f}</b>\n"
            f"🛑 SL hit  :  <b>₹{sl_price:.1f}</b>  <i>(−{loss_pts:.0f} pts)</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Exit the trade. SL is final. Capital protected. 🛡️\n\n"
            f"<i>Not every trade wins. That's why SL exists.\n"
            f"VIP members had the exact SL level from the start.</i>\n\n"
            f"Next setup is being scanned right now. 👇"
            + _pay_block()
        )

    @staticmethod
    def vip_zero_hero(cfg: dict, strike: int, direction: str, expiry: str,
                      entry_premium: float) -> str:
        de       = "📈" if direction == "CE" else "📉"
        tp_price = round(entry_premium * 2.5, 1)
        sl_price = max(round(entry_premium * 0.4, 1), 1)
        return (
            f"🚀 <b>ZERO-TO-HERO (0DTE) — {cfg['label']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>Extreme Conviction Setup</b>\n"
            f"{de} <b>{strike} {direction} | Expiry TODAY</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 <b>Entry Premium : ₹{entry_premium:.1f}</b>\n"
            f"🛑 <b>Stop Loss : ₹{sl_price}</b> (Strict!)\n"
            f"🎯 <b>Target : ₹{tp_price}+</b> (150%+ ROI)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>0DTE Risk: Use small capital.</i>\n"
            f"🕒 <i>{_now()}</i>"
        )

    @staticmethod
    def free_zero_hero_alert(cfg: dict, direction: str) -> str:
        de = "CALL (CE)" if direction == "CE" else "PUT (PE)"
        return (
            f"🚨 <b>ZERO-TO-HERO EXPIRY JACKPOT DETECTED!</b> 🚨\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Retailers are getting trapped in the chop. 🪤\n"
            f"BLACK just detected massive Operator volume on <b>{cfg['label']}</b> for a 0DTE {de} setup.\n\n"
            f"⚡ <b>Algorithm Probability: 92% (High Conviction)</b>\n"
            f"💸 <b>Aiming for 150%+ ROI before market close.</b>\n\n"
            f"🔒 <i>VIP members have already received the exact strike and entry price.</i>\n\n"
            f"Are you going to miss the biggest expiry move of the day? 👇\n"
            + _pay_block()
        )

    @staticmethod
    def free_zero_hero_result(cfg: dict, strike: int, direction: str,
                               entry_premium: float, exit_premium: float, trade_pnl: int,
                               vix: float = 15.0) -> str:
        roi      = ((exit_premium - entry_premium) / entry_premium) * 100
        ai_story = _generate_ai_hook(cfg["label"], direction, vix, alert_type="hero")
        return (
            f"💥 <b>ZERO-TO-HERO RESULT — YOU MISSED IT!</b> 💥\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>{cfg['label']} {strike} {direction}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 <b>VIP Entry : ₹{entry_premium:.1f}</b>\n"
            f"🚀 <b>Target Hit : ₹{exit_premium:.1f}</b>\n"
            f"🔥 <b>Massive Gain : +{roi:.0f}% ROI</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>{TRADE_LOTS}-Lot Profit : ₹{trade_pnl:,}</b>\n\n"
            f"🗣️ <i>{ai_story}</i>\n\n"
            f"Stop watching others make money. Upgrade now. 👇\n"
            + _pay_block()
        )


# ══════════════════════════════════════════════════════════════════════════════
#  ACTIVE SIGNAL
# ══════════════════════════════════════════════════════════════════════════════

class ActiveSignal:
    def __init__(self, signal_id: str, index_key: str, direction: str,
                 strike: int, expiry: str, entry_premium: float,
                 sl_pts: float, tp_pts: float, trail_pts: float,
                 lot_size: int, signal_type: str, is_free_signal: bool = False):
        self.signal_id      = signal_id
        self.index_key      = index_key
        self.direction      = direction
        self.strike         = strike
        self.expiry         = expiry
        self.entry_premium  = entry_premium
        self.sl_price       = round(entry_premium - sl_pts,   1)
        self.tp_price       = round(entry_premium + tp_pts,   1)
        self.trail_trigger  = round(entry_premium + trail_pts, 1)
        self.tp_pts         = tp_pts
        self.lot_size       = lot_size
        self.signal_type    = signal_type
        self.is_free_signal = is_free_signal
        self.trailed        = False
        self.active         = True
        self.created_at     = time.time()

        # 🔥 UNIVERSAL TARGET LADDER WITH T2.5 PROFIT LOCK
        t1_p   = round(entry_premium + (tp_pts * 0.4), 1)
        t2_p   = round(entry_premium + (tp_pts * 0.7), 1)
        t3_p   = self.tp_price
        t2_5_p = round(t2_p + (t3_p - t2_p) / 2, 1) # 50% Midpoint between T2 and Final

        self.targets = [
            {"name": "Target 1",        "price": t1_p,   "hit": False},
            {"name": "Target 2",        "price": t2_p,   "hit": False},
            {"name": "Target 2.5 Lock", "price": t2_5_p, "hit": False},
            {"name": "Final Target",    "price": t3_p,   "hit": False},
        ]

        # Moonbag fields are in-memory signal state. The persistence module must
        # serialize moonbag and peak as well for restart recovery.
        self.moonbag = False
        self.peak = float(entry_premium)
        self._last_sl_alert = self.sl_price
        self.moonbag_atr = None
        self.moonbag_swing_low = None
        self.moonbag_last_candle_ts = None
        self.moonbag_last_sl_trigger_premium = float(entry_premium)
        self._moonbag_last_refresh = 0.0
        self._moonbag_last_peak_persist = 0.0
        self._moonbag_warmup_logged = False

    def ratchet_sl(self, curr: float, atr: float | None = None,
                   swing_low: float | None = None) -> bool:
        """ATR/swing Moonbag ratchet. The existing SL is always the hard floor."""
        self.peak = max(float(self.peak), float(curr))
        try:
            atr_value = float(atr)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(atr_value) or atr_value <= 0:
            return False

        candidates = [float(self.sl_price), self.peak - 1.5 * atr_value]
        try:
            swing_value = float(swing_low)
            if math.isfinite(swing_value) and swing_value > 0:
                candidates.append(swing_value - 0.25 * atr_value)
        except (TypeError, ValueError, OverflowError):
            pass

        new_sl = round(max(candidates), 1)
        if new_sl > self.sl_price:
            self.sl_price = new_sl
            self.moonbag_atr = atr_value
            self.moonbag_swing_low = (
                float(swing_low) if swing_low is not None else None
            )
            return True
        self.moonbag_atr = atr_value
        self.moonbag_swing_low = (
            float(swing_low) if swing_low is not None else None
        )
        return False


def _is_market_hours_static() -> bool:
    now = _ist_now()
    return (now.weekday() < 5 and
            pd.Timestamp("09:15").time() <= now.time() <= pd.Timestamp("15:30").time())


# ══════════════════════════════════════════════════════════════════════════════
#  TARGET MONITOR
#  v11: atomic close guard on ALL exit paths (target, SL, trail)
# ══════════════════════════════════════════════════════════════════════════════

class TargetMonitor:


    def __init__(self, fetcher: SignalDataFetcher, start_thread: bool = True):
        self.fetcher = fetcher
        self._signals: dict = {}
        self._registering: set = set()
        self._lock = threading.Lock()
        self._thread = None
        self._persistence_failed = threading.Event()
        if start_thread:
            self.start()

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="target-monitor"
        )
        self._thread.start()
        _log.info("TargetMonitor started")

    def register(self, sig: ActiveSignal, record_ledger: bool = True, persist_state: bool = True, submit_live_entry: bool = True):
        # Claim the signal in-process before persistence or broker submission.
        # PostgreSQL's unique ENTRY intent remains the cross-process authority.
        with self._lock:
            if sig.signal_id in self._signals or sig.signal_id in self._registering:
                raise RuntimeError(f"Signal already registered/registering: {sig.signal_id}")
            self._registering.add(sig.signal_id)

        try:
            # STEP 1: Persist the signal and paper ledger before any live submission.
            if (
                record_ledger
                and persist_state
                and SIGNAL_STATE_PG_ENABLED
                and _paper_ledger_enabled()
                and not sig.is_free_signal
            ):
                from trading_live.signal_state import create_signal_and_paper_trade

                quantity = int(sig.lot_size) * TRADE_LOTS
                create_signal_and_paper_trade(
                    sig,
                    quantity=quantity,
                    metadata={
                        "source": "daily_signal_engine_v11",
                        "signal_type": sig.signal_type,
                        "lot_size_units": int(sig.lot_size),
                        "trade_lots": TRADE_LOTS,
                        "sl_price": float(sig.sl_price),
                        "tp_price": float(sig.tp_price),
                    },
                )
            else:
                if record_ledger:
                    _open_paper_ledger_trade(sig)
                if persist_state and SIGNAL_STATE_PG_ENABLED and not sig.is_free_signal:
                    from trading_live.signal_state import save_signal_state
                    save_signal_state(sig)

            # A live-entry rejection/unknown result must not prevent the strategy
            # monitor from tracking the signal and processing its eventual exit.
            if submit_live_entry and not sig.is_free_signal:
                from trading_live.engine_hooks import submit_engine_entry

                try:
                    live_result = submit_engine_entry(
                        sig,
                        self.fetcher,
                        TRADE_LOTS,
                    )

                    if live_result is None:
                        _log.error(
                            "LIVE ENTRY NOT CONFIRMED signal=%s reason=NO_RESULT",
                            sig.signal_id,
                        )
                    else:
                        live_status = str(
                            live_result.get("status", "UNKNOWN")
                        ).upper()

                        if live_status in {"SKIPPED", "REJECTED", "ENTRY_FAILED"}:
                            _log.warning(
                                "LIVE ENTRY NOT EXECUTED signal=%s status=%s reason=%s",
                                sig.signal_id,
                                live_status,
                                live_result.get("reason", live_result.get("error", "")),
                            )
                        else:
                            _log.info(
                                "LIVE ENTRY BRIDGE ACCEPTED signal=%s status=%s",
                                sig.signal_id,
                                live_status,
                            )

                except Exception as exc:
                    _log.error(
                        "LIVE ENTRY NOT CONFIRMED signal=%s reason=%s",
                        sig.signal_id,
                        exc,
                        exc_info=True,
                    )

            with self._lock:
                self._signals[sig.signal_id] = sig
            _log.info(
                f"Monitor registered: {sig.signal_id}  "
                f"entry=₹{sig.entry_premium}  SL=₹{sig.sl_price}  TGT=₹{sig.tp_price}"
            )
        finally:
            with self._lock:
                self._registering.discard(sig.signal_id)



    def _loop(self):
        while True:
            if self._persistence_failed.is_set() or _signal_persistence_failed.is_set():
                _log.critical("TargetMonitor halted: persistence failure requires manual recovery")
                return

            try:
                if _is_market_hours_static():
                    self._check_all()
            except Exception as e:
                _log.error(f"TargetMonitor loop error: {e}", exc_info=True)
                time.sleep(2)
            time.sleep(1)


    def _check_all(self):
        with self._lock:
            signals = list(self._signals.values())
        to_remove = []
        for sig in signals:
            if not sig.active:
                to_remove.append(sig.signal_id)
                continue
            try:
                self._check_one(sig)
            except Exception as e:
                if SIGNAL_STATE_PG_ENABLED:
                    self._persistence_failed.set()
                    _signal_persistence_failed.set()
                    _log.critical("TargetMonitor halted after signal %s error: %s", sig.signal_id, e, exc_info=True)
                    raise
                _log.debug(f"Monitor check {sig.signal_id}: {e}")
        if to_remove:
            with self._lock:
                for sid in to_remove:
                    self._signals.pop(sid, None)

    @staticmethod
    def _load_moonbag_atr_swing(sig: ActiveSignal):
        """Return ATR(14), latest confirmed 2-left/2-right swing low and newest bar ts."""
        from collectors.scripts import market_store

        try:
            rows = market_store.load_option_ohlcv_candles(
                sig.index_key, sig.expiry, sig.strike, sig.direction, limit=80
            )
        except Exception as exc:
            # Candle-feed/database unavailability must not stop the target monitor.
            # Existing stop protection remains in force until valid ATR bars return.
            _log.warning(
                "Moonbag candle data unavailable for %s; preserving SL ₹%.1f: %s",
                sig.signal_id, sig.sl_price, exc,
            )
            return None, None, None
        # Require 15 completed bars for 14 true-range observations.
        if len(rows) < 15:
            if not getattr(sig, "_moonbag_warmup_logged", False):
                _log.info(
                    "Moonbag ATR warmup for %s: %d/15 completed option candles; SL remains ₹%.1f",
                    sig.signal_id, len(rows), sig.sl_price,
                )
                sig._moonbag_warmup_logged = True
            return None, None, (rows[-1]["timestamp"].isoformat() if rows else None)
        sig._moonbag_warmup_logged = False

        true_ranges = []
        for i in range(1, len(rows)):
            bar, prev = rows[i], rows[i - 1]
            true_ranges.append(max(
                float(bar["high"]) - float(bar["low"]),
                abs(float(bar["high"]) - float(prev["close"])),
                abs(float(bar["low"]) - float(prev["close"])),
            ))
        atr = sum(true_ranges[-14:]) / 14.0
        if not math.isfinite(atr) or atr <= 0:
            return None, None, rows[-1]["timestamp"].isoformat()

        # A pivot is confirmed only after two later completed bars.
        swing_low = None
        lows = [float(row["low"]) for row in rows]
        for i in range(2, len(rows) - 2):
            if lows[i] < min(lows[i - 2:i]) and lows[i] < min(lows[i + 1:i + 3]):
                swing_low = lows[i]
        return atr, swing_low, rows[-1]["timestamp"].isoformat()

    def _check_one(self, sig: ActiveSignal):
        # v9/v11: Atomic close guard — bail if already closed
        with self._lock:
            if not sig.active:
                return

        curr = self.fetcher.get_option_premium(sig.index_key, sig.strike, sig.direction, sig.expiry, side="bid")
        try:
            curr = float(curr)
        except (TypeError, ValueError, OverflowError):
            return
        if not math.isfinite(curr) or curr <= 0:
            return
        if curr <= 0:
            return
        cfg    = INDICES[sig.index_key]
        lot    = sig.lot_size
        expiry = sig.expiry

        # Multi-target ladder. Final target starts moonbag mode instead of closing.
        target_hit_this_tick = False
        for tgt in sig.targets:
            if tgt["hit"] or curr < tgt["price"]:
                continue

            tgt["hit"] = True
            target_hit_this_tick = True
            is_final = (tgt is sig.targets[-1])

            if is_final:
                sig.moonbag = True
                sig.peak = max(sig.peak, curr)
                # Start measuring premium movement from the final-target hit.
                sig.moonbag_last_sl_trigger_premium = float(sig.peak)

                # Final Target: move SL to Target 2.5.
                # Never lower an already higher stop.
                t2_5_price = float(sig.targets[2]["price"])
                old_sl = float(sig.sl_price)
                sig.sl_price = max(old_sl, t2_5_price)

                sig._moonbag_last_refresh = 0.0

                if sig.sl_price > old_sl:
                    _log.info(
                        "MOONBAG START: SL upgraded from ₹%.1f to T2.5 ₹%.1f",
                        old_sl, sig.sl_price
                    )
                locked = round((sig.sl_price - sig.entry_premium) * lot * TRADE_LOTS)
                if not sig.is_free_signal:
                    _to_vip(Fmt.vip_moonbag_start(
                        cfg, sig.strike, sig.direction, expiry,
                        sig.entry_premium, tgt["price"], curr, sig.sl_price, locked))
                _log.info(
                    f"Final target hit → MOONBAG. signal={sig.signal_id} "
                    f"SL=₹{sig.sl_price:.1f} peak=₹{sig.peak:.1f}"
                )
                break

            if tgt["name"] == "Target 1":
                # Lock up to +20% from entry, but keep the stop below Target 1.
                # Lock 10% of the difference between entry and Target 1.
                t1 = tgt["price"]
                new_sl = round(
                    sig.entry_premium + 0.10 * (t1 - sig.entry_premium), 1
                )
                sig.trailed = True
                sig.sl_price = max(sig.sl_price, new_sl)
                if not sig.is_free_signal:
                    _to_vip(
                        f"🔒 <b>TRAIL SL — {cfg['label']}</b>\n"
                        f"Target 1 hit! SL moved to ₹{sig.sl_price:.1f}. Profit locked. 💸"
                    )
                _log.info(f"T1 hit. SL → ₹{sig.sl_price:.1f}")

            elif tgt["name"] == "Target 2":
                sig.sl_price = max(sig.sl_price, sig.targets[0]["price"])
                if not sig.is_free_signal:
                    _to_vip(
                        f"🔒 <b>TRAIL SL UPGRADED — {cfg['label']}</b>\n"
                        f"Target 2 hit! SL moved to Target 1 (₹{sig.sl_price:.1f}). 💸"
                    )
                _log.info(f"T2 hit. SL → T1 ₹{sig.sl_price:.1f}")

            elif tgt["name"] == "Target 2.5 Lock":
                sig.sl_price = max(sig.sl_price, sig.targets[1]["price"])
                if not sig.is_free_signal:
                    _to_vip(
                        f"🔥 <b>MAX PROFIT LOCK — {cfg['label']}</b>\n"
                        f"SL moved to Target 2 (₹{sig.sl_price:.1f}). 🚀"
                    )
                _log.info(f"T2.5 hit. SL → T2 ₹{sig.sl_price:.1f}")

        if target_hit_this_tick:
            if sig.active and SIGNAL_STATE_PG_ENABLED and not sig.is_free_signal:
                from trading_live.signal_state import save_signal_state
                save_signal_state(sig)
            if not sig.is_free_signal:
                from trading_live.engine_hooks import sync_engine_stop
                sync_engine_stop(sig)
            return

        # MOONBAG: peak updates on every valid quote; ATR/swing refresh only periodically.
                # MOONBAG: Track OPTION PREMIUM movement.
        # ATR/swing SL recalculation happens only after the configured
        # premium increase for the relevant index.
        if sig.moonbag:
            prior_peak = float(sig.peak)
            sig.peak = max(prior_peak, curr)

            now_mono = time.monotonic()

            # Persist new premium peak periodically.
            if (
                sig.peak > prior_peak
                and now_mono - float(
                    getattr(sig, "_moonbag_last_peak_persist", 0.0)
                ) >= 30.0
                and SIGNAL_STATE_PG_ENABLED
                and not sig.is_free_signal
            ):
                from trading_live.signal_state import save_signal_state
                save_signal_state(sig)
                sig._moonbag_last_peak_persist = now_mono

            # Get the premium movement threshold for this index.
            step = float(
                MOONBAG_PREMIUM_SL_STEP.get(sig.index_key, 0.0)
            )

            last_trigger = float(
                getattr(
                    sig,
                    "moonbag_last_sl_trigger_premium",
                    sig.peak,
                )
            )

            premium_movement = sig.peak - last_trigger

            # Only recalculate ATR/swing SL after the premium threshold.
            # The five-second guard prevents repeated calculations.
            if (
                step > 0
                and premium_movement >= step
                and now_mono - float(
                    getattr(sig, "_moonbag_last_refresh", 0.0)
                ) >= 5.0
            ):
                sig._moonbag_last_refresh = now_mono

                atr, swing_low, candle_ts = (
                    self._load_moonbag_atr_swing(sig)
                )

                try:
                    atr_value = float(atr)
                except (TypeError, ValueError, OverflowError):
                    atr_value = 0.0

                # Do not consume the premium threshold if ATR is unavailable.
                if math.isfinite(atr_value) and atr_value > 0:
                    old_sl = float(sig.sl_price)

                    sl_changed = sig.ratchet_sl(
                        sig.peak,
                        atr=atr_value,
                        swing_low=swing_low,
                    )

                    # One ratchet per threshold crossing.
                    # The next trigger is measured from this new peak.
                    sig.moonbag_last_sl_trigger_premium = float(
                        sig.peak
                    )

                    if candle_ts:
                        sig.moonbag_last_candle_ts = candle_ts

                    if sl_changed and not sig.is_free_signal:
                        from trading_live.engine_hooks import sync_engine_stop
                        sync_engine_stop(sig)

                    if (
                        SIGNAL_STATE_PG_ENABLED
                        and not sig.is_free_signal
                    ):
                        from trading_live.signal_state import save_signal_state
                        save_signal_state(sig)

                    if sl_changed:
                        _log.info(
                            "MOONBAG PREMIUM SL RATCHET | %s | "
                            "peak=₹%.1f | movement=₹%.1f | "
                            "step=₹%.1f | SL ₹%.1f -> ₹%.1f",
                            sig.signal_id,
                            sig.peak,
                            premium_movement,
                            step,
                            old_sl,
                            sig.sl_price,
                        )

            # Alert only after SL advances by at least 10% of the target distance.
            if (not sig.is_free_signal and
                    sig.sl_price - sig._last_sl_alert >= sig.tp_pts * 0.10):
                sig._last_sl_alert = sig.sl_price
                if SIGNAL_STATE_PG_ENABLED:
                    from trading_live.signal_state import save_signal_state
                    save_signal_state(sig)
                locked = round((sig.sl_price - sig.entry_premium) * lot * TRADE_LOTS)
                _to_vip(
                    f"🔄 <b>TRAIL SL — MOVE YOUR SL — {cfg['label']}</b>\\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\\n"
                    f"📈 Peak ₹{sig.peak:.1f} | Now ₹{curr:.1f}\\n"
                    f"🔒 <b>New SL : ₹{sig.sl_price:.1f}</b>\\n"
                    f"💰 <b>{TRADE_LOTS}-Lot Locked : ₹{locked:,}</b>\\n"
                    f"🕒 <i>{_now()}</i>"
                )

        # SL hit
        if curr <= sig.sl_price:
            exit_reason = (
                "TRAILED_SL_PROFIT"
                if sig.sl_price >= sig.entry_premium
                else "SL"
            )

            if not sig.is_free_signal:
                from trading_live.engine_hooks import submit_engine_exit
                if not submit_engine_exit(sig, exit_reason, curr):
                    _log.error(
                        "LIVE EXIT NOT CONFIRMED: %s. Keeping signal active.",
                        sig.signal_id
                    )
                    return
            with self._lock:
                if not sig.active:
                    return
                sig.active = False  # atomic close

            if sig.sl_price >= sig.entry_premium:
                # Trailed SL — secured profit
                trade_lots = _display_trade_lots(sig.index_key)
                trade_profit = round(
                    (curr - sig.entry_premium) * lot * trade_lots
                )
                if not sig.is_free_signal:
                    if trade_profit > 0:
                        header = f"🛡️ <b>TRAILED SL HIT (PROFIT SECURED) — {cfg['label']}</b>"
                        foot   = f"💰 <b>{TRADE_LOTS}-Lot Profit Locked : ₹{trade_profit:,}</b> ✅"
                        note   = f"<i>Market lost momentum. Gains safely locked and trade closed.</i> 🔒"
                    else:
                        header = f"🛡️ <b>TRAILED SL HIT (BREAKEVEN) — {cfg['label']}</b>"
                        foot   = f"⚖️ <b>{TRADE_LOTS}-Lot Profit Locked : ₹0 (Scratch Trade)</b> 🤝"
                        note   = f"<i>Market lost momentum. Trade closed safely with no loss.</i> 🔒"
                    # Trade exit updates are not broadcast to the free channel.
                    _to_vip(
                        f"{header}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🟢 <b>{sig.strike} {sig.direction} | Expiry {expiry}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📥 Entry : ₹{sig.entry_premium:.1f}\n"
                        f"🛑 Trailed SL Trigger : ₹{sig.sl_price:.1f} | Observed bid exit : ₹{curr:.1f}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{foot}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{note}"
                    )
                _update_signal_exit(sig.signal_id, curr,
                                    curr - sig.entry_premium, trade_profit, "TRAILED_SL_PROFIT")
                _log.info(f"Trailed SL (profit): {sig.signal_id}  ₹{trade_profit}")
            else:
                # Normal SL hit — loss
                trade_lots = _display_trade_lots(sig.index_key)
                trade_loss = round(
                    (sig.entry_premium - curr) * lot * trade_lots
                )
                if not sig.is_free_signal:
                    _to_vip(Fmt.vip_sl_hit(
                        cfg, sig.strike, sig.direction, expiry,
                        sig.entry_premium, sig.sl_price, curr, trade_loss))
                _update_signal_exit(sig.signal_id, curr,
                                    curr - sig.entry_premium, -trade_loss, "SL")
                _log.info(f"SL hit: {sig.signal_id}  loss₹{trade_loss}")


    def force_close_intraday(self):
        """15:25 — square off all non-swing positions."""
        with self._lock:
            signals = list(self._signals.values())
        for sig in signals:
            if sig.active and sig.signal_type != "SWING":
                curr = self.fetcher.get_option_premium(
                    sig.index_key, sig.strike, sig.direction, sig.expiry, side="bid")
                if curr <= 0:
                    _log.error("Force-close blocked: no validated bid for %s; signal remains open", sig.signal_id)
                    continue
                # force_close_intraday()
                if not sig.is_free_signal:
                    from trading_live.engine_hooks import submit_engine_exit
                    if not submit_engine_exit(sig, "3:25_EXIT", curr):
                        _log.error("LIVE EOD exit not confirmed: %s", sig.signal_id)
                        continue

                # v9/v11: atomic close guard
                with self._lock:
                    if not sig.active:
                        continue
                    sig.active = False

                trade_lots = _display_trade_lots(sig.index_key)
                trade_profit = round(
                    (curr - sig.entry_premium) * sig.lot_size * trade_lots
                )
                cfg = INDICES[sig.index_key]
                de  = "🟢" if sig.direction == "CE" else "🔴"
                _to_vip(
                    f"⏰ <b>INTRADAY FORCE CLOSE — {cfg['label']}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{de} <b>{sig.strike} {sig.direction} | Expiry {sig.expiry}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 <b>Entry : ₹{sig.entry_premium:.1f}</b>\n"
                    f"📈 <b>Exit  : ₹{curr:.1f}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>{TRADE_LOTS}-Lot P&L : ₹{trade_profit:,}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<i>Market closing. All intraday positions squared off.</i> ⏳\n"
                    f"🕒 <i>{_ist_now().strftime('%d %b  %H:%M')}</i>"
                )
                _update_signal_exit(sig.signal_id, curr, curr - sig.entry_premium,
                                    trade_profit, "3:25_EXIT")
                _log.info(f"Force closed {sig.signal_id} at {curr}")

    def force_close_swing(self):
        """09:25 — square off all overnight SWING positions."""
        with self._lock:
            signals = list(self._signals.values())
        for sig in signals:
            if sig.active and getattr(sig, "signal_type", "") == "SWING":
                curr = self.fetcher.get_option_premium(
                    sig.index_key, sig.strike, sig.direction, sig.expiry, side="bid")
                if curr <= 0:
                    _log.error("Scheduled close blocked: no validated bid for %s; signal remains open", sig.signal_id)
                    continue
                # force_close_swing()
                if not sig.is_free_signal:
                    from trading_live.engine_hooks import submit_engine_exit
                    if not submit_engine_exit(sig, "9:25_SWING_EXIT", curr):
                        _log.error("LIVE swing exit not confirmed: %s", sig.signal_id)
                        continue

                with self._lock:
                    if not sig.active:
                        continue
                    sig.active = False

                trade_profit = round((curr - sig.entry_premium) * sig.lot_size * TRADE_LOTS)
                cfg = INDICES[sig.index_key]
                de  = "🟢" if sig.direction == "CE" else "🔴"
                _to_vip(
                    f"⏰ <b>SWING FORCE CLOSE — {cfg['label']}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{de} <b>{sig.strike} {sig.direction} | Expiry {sig.expiry}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 <b>Entry : ₹{sig.entry_premium:.1f}</b>\n"
                    f"📈 <b>Exit  : ₹{curr:.1f}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>{TRADE_LOTS}-Lot P&L : ₹{trade_profit:,}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<i>09:25 AM reached. Swing position squared off.</i> ⏳\n"
                    f"🕒 <i>{_ist_now().strftime('%d %b  %H:%M')}</i>"
                )
                _update_signal_exit(sig.signal_id, curr, curr - sig.entry_premium,
                                    trade_profit, "9:25_SWING_EXIT")
                _log.info(f"Force closed SWING {sig.signal_id} at {curr}")

    def get_active_direction(self, index_key: str) -> Optional[str]:
        with self._lock:
            for sig in self._signals.values():
                if sig.active and sig.index_key == index_key and sig.signal_type != "SWING":
                    return sig.direction
        return None

    def close_active_reversal(self, index_key: str):
        with self._lock:
            signals = list(self._signals.values())
        for sig in signals:
            if sig.active and sig.index_key == index_key and sig.signal_type != "SWING":
                curr = self.fetcher.get_option_premium(
                    sig.index_key, sig.strike, sig.direction, sig.expiry, side="bid")
                if curr <= 0:
                    _log.error("Scheduled close blocked: no validated bid for %s; signal remains open", sig.signal_id)
                    continue
                # close_active_reversal()
                if not sig.is_free_signal:
                    from trading_live.engine_hooks import submit_engine_exit
                    if not submit_engine_exit(sig, "REVERSAL", curr):
                        _log.error("LIVE reversal exit not confirmed: %s", sig.signal_id)
                        continue

                with self._lock:
                    if not sig.active:
                        continue
                    sig.active = False

                trade_profit = round((curr - sig.entry_premium) * sig.lot_size * TRADE_LOTS)
                cfg = INDICES[sig.index_key]
                _to_vip(Fmt.vip_reversal_exit(
                    cfg, sig.strike, sig.direction, sig.expiry,
                    sig.entry_premium, curr, trade_profit))
                _update_signal_exit(sig.signal_id, curr, curr - sig.entry_premium,
                                    trade_profit, "REVERSAL")
                _log.info(f"Reversal closed {sig.signal_id} at {curr} | P&L: ₹{trade_profit}")


# ══════════════════════════════════════════════════════════════════════════════
#  DAILY SIGNAL ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class DailySignalEngine:

    def __init__(self):
        self.fetcher  = SignalDataFetcher()
        self.strategy = StrategyEngine()
        self.monitor  = TargetMonitor(self.fetcher, start_thread=False)

        # Start broker reconciliation before the strategy begins generating signals.
        # This is required even when new entries are disabled or LIVE_SHADOW is on.
        from trading_live.engine_hooks import (
            live_management_requested,
            live_execution_requested,
        )
        
        # Initialise the shared authenticated broker before any live hook can use it.
        # This covers both existing-position management and new-entry execution.
        if live_management_requested() or live_execution_requested():
            try:
                from trading_live.live_bridge import get_bridge
        
                bridge = get_bridge(auth_provider=self.fetcher)
        
                if getattr(bridge.broker, "_auth_provider", None) is not self.fetcher:
                    raise RuntimeError(
                        "LiveBridge is not bound to the SignalDataFetcher authentication provider"
                    )
        
                _log.info(
                    "LIVE bridge bootstrapped successfully "
                    "(shadow=%s, entry_enabled=%s, shared_auth=True)",
                    bridge.cfg.shadow,
                    bridge.entry_enabled,
                )
        
            except Exception:
                _log.critical(
                    "LIVE bridge startup/authentication initialization failed; "
                    "refusing to start the signal engine",
                    exc_info=True,
                )
                raise

        self._fired: set = set()
        self._date: str  = ""
        self._running    = False

        self._day_signal_count  = 0
        self._free_sent_today   = False
        self._swing_fired_today = set()
        self._ic_fired_today = set()
        self._best_signal: Optional[dict] = None
        self._last_results: list = []

        self._last_regime = "CHOP"
        self._last_spot   = 0.0
        self._last_vix    = 15.0

        self._last_sniper: dict  = {}
        # In DailySignalEngine.__init__():
        self._last_strong_breakout: dict = {}

        self._last_bk_check      = 0.0
        self._strong_breakout_pending = {}
        self._strong_breakout_last_candle = {}
        self._strong_breakout_fired = set()
        self._week_signal_count  = 0

        # v10/v11: live scan timestamps
        self._last_live_check     = 0.0
        self._last_scheduled_scan = 0.0
        self._last_prob = {}           # For 2-bar confirmation
        self._trade_open_time = {}     # For 15m minimum hold time
        self._last_reversal_time = {}  # For 30m reversal cooldown

        # v8: isolated persistent state
        # 🔥 UPGRADE: State is now safely loaded from SQLite
        self._load_state()
        self._restore_open_signals()
        self.monitor.start()

    def _load_state(self):
        if SIGNAL_STATE_PG_ENABLED:
            from trading_live.signal_state import load_engine_state
            st = load_engine_state()
            if st and st.get("date") == _ist_now().strftime("%Y-%m-%d"):
                self._date = st["date"]
                self._fired = set(st.get("fired", []))
                self._day_signal_count = st.get("day_signal_count", 0)
                self._free_sent_today = st.get("free_sent_today", False)
                self._swing_fired_today = set(st.get("swing_fired_today", []))
                self._ic_fired_today = set(st.get("ic_fired_today", []))
                self._best_signal = st.get("best_signal")
                self._last_sniper = st.get("last_sniper", {})
                self._last_strong_breakout = {}
                self._week_signal_count = st.get("week_signal_count", 0)
            return
        try:
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute("SELECT state_json FROM engine_state WHERE id = 1").fetchone()
                if row:
                    st = json.loads(row[0])
                    if st.get("date") == _ist_now().strftime("%Y-%m-%d"):
                        self._date              = st["date"]
                        self._fired             = set(st.get("fired", []))
                        self._day_signal_count  = st.get("day_signal_count", 0)
                        self._free_sent_today   = st.get("free_sent_today", False)
                        self._swing_fired_today = set(st.get("swing_fired_today", []))
                        self._ic_fired_today    = set(st.get("ic_fired_today", []))
                        self._best_signal       = st.get("best_signal", None)
                        self._last_sniper       = st.get("last_sniper", {})
                        self._week_signal_count = st.get("week_signal_count", 0)
                        _log.info(f"💾 Restored state from DB. Posts today: {list(self._fired)}")
        except Exception as e:
            _log.warning(f"Could not load state from DB: {e}")

    def _save_state(self):
        if not self._date: return
        if SIGNAL_STATE_PG_ENABLED:
            from trading_live.signal_state import save_engine_state
            save_engine_state({
                "date": self._date,
                "fired": list(self._fired),
                "day_signal_count": self._day_signal_count,
                "free_sent_today": self._free_sent_today,
                "swing_fired_today": list(self._swing_fired_today),
                "ic_fired_today": list(self._ic_fired_today),
                "best_signal": self._best_signal,
                "last_sniper": self._last_sniper,
                "week_signal_count": self._week_signal_count,
            })
            return
        try:
            st = {
                "date": self._date, "fired": list(self._fired),
                "day_signal_count": self._day_signal_count,
                "free_sent_today": self._free_sent_today,
                "swing_fired_today": list(self._swing_fired_today),
                "ic_fired_today": list(self._ic_fired_today),
                "best_signal": self._best_signal,
                "last_sniper": self._last_sniper,
                "week_signal_count": self._week_signal_count,
            }
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO engine_state (id, state_json) VALUES (1, ?)",
                    (json.dumps(st),)
                )
        except Exception as e:
            _log.debug(f"State DB save error: {e}")

    def _restore_open_signals(self):

        if SIGNAL_STATE_PG_ENABLED:
            # Do not silently fall back to local files if PostgreSQL recovery fails.
            from trading_live.signal_state import list_open_signal_states, close_signal_state

            records = list_open_signal_states()
            restored = 0
            today_str = _ist_now().strftime("%Y-%m-%d")

            for r in records:
                if not isinstance(r, dict):
                    raise ValueError("Invalid PostgreSQL open-signal payload")
                if r.get("active") is not True:
                    raise ValueError(f"Open PostgreSQL row has inactive payload: {r.get('signal_id')}")
                if r.get("is_free_signal", False):
                    close_signal_state(r["signal_id"], {
                        "outcome": "RECOVERY_SKIPPED_FREE",
                        "exit_time": _ist_now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    continue

                # Keep the existing same-day rule for intraday signals.
                # Swing signals may legitimately span trading days.
                created_at = float(r["created_at"])
                created_date = datetime.fromtimestamp(created_at, IST).strftime("%Y-%m-%d")
                if r.get("signal_type") != "SWING" and created_date != today_str:
                    raise RuntimeError(
                        f"Unreconciled stale intraday signal {r.get('signal_id')}; "
                        "manual quote/ledger reconciliation is required before engine startup"
                    )

                required = {
                    "signal_id", "index_key", "direction", "strike", "expiry",
                    "entry_premium", "sl_price", "tp_price", "trail_trigger",
                    "tp_pts", "lot_size", "signal_type", "is_free_signal",
                    "trailed", "active", "created_at", "targets",
                }
                missing = required - r.keys()
                if missing:
                    raise ValueError(
                        f"Saved signal {r.get('signal_id')} missing fields: {sorted(missing)}"
                    )

                expiry_date = pd.to_datetime(str(r["expiry"]), errors="coerce", dayfirst=True)
                if pd.isna(expiry_date):
                    raise ValueError(f"Unparseable expiry for signal {r['signal_id']}: {r['expiry']}")
                if expiry_date.date() < _ist_now().date():
                    raise RuntimeError(
                        f"Unreconciled expired signal {r.get('signal_id')}; "
                        "no exit price will be fabricated; manual reconciliation is required"
                    )

                for target in r["targets"]:
                    if not isinstance(target, dict) or not {"name", "price", "hit"}.issubset(target):
                        raise ValueError(f"Malformed target for signal {r['signal_id']}")
                    price = float(target["price"])
                    if not (price > 0 and price < float("inf")) or not isinstance(target["hit"], bool):
                        raise ValueError(f"Invalid target values for signal {r['signal_id']}")

                idx_key = r["index_key"]
                if idx_key not in INDICES:
                    raise ValueError(
                        f"Unsupported index in saved signal: {idx_key}"
                    )
                if r["direction"] not in {"CE", "PE"}:
                    raise ValueError(
                        f"Unsupported direction in saved signal: {r['direction']}"
                    )
                if not isinstance(r["targets"], list) or not r["targets"]:
                    raise ValueError(
                        f"Invalid target ladder for signal: {r['signal_id']}"
                    )

                sig = self._restore_signal_from_pg_payload(r)
                self.monitor.register(
                    sig,
                    record_ledger=False,
                    persist_state=False,
                    submit_live_entry=False,
                )
                # On restart, reconcile the persisted engine SL with the
                # broker-side stop immediately. A stop ratchet saved just
                # before a crash must not remain only in engine memory.
                try:
                    from trading_live.engine_hooks import live_management_requested, sync_engine_stop
                    if live_management_requested() and not sig.is_free_signal:
                        sync_engine_stop(sig)
                except Exception:
                    _log.critical(
                        "LIVE stop restoration sync failed for %s",
                        sig.signal_id,
                        exc_info=True,
                    )
                if r.get("signal_type") != "SWING":
                    prior = self._trade_open_time.get(idx_key)
                    self._trade_open_time[idx_key] = (
                        min(prior, created_at) if prior is not None else created_at
                    )
                restored += 1

            _log.info("PostgreSQL recovery completed: %d signals restored", restored)
            return

        if not SIGNALS_LOG.exists():
            return
        try:
            with open(SIGNALS_LOG, "r", encoding="utf-8") as f:
                records = json.load(f)
            restored = 0
            today_str = _ist_now().strftime("%Y-%m-%d")
            for r in records:
                # FIX: Only restore open trades if they were opened TODAY
                if r.get("signal_type") == "FREE_SAMPLE" or r.get("channel") == "FREE":
                    continue
                if r.get("status") == "open" and r.get("date") == today_str:
                    idx_key = r.get("index")
                    sig = ActiveSignal(
                        signal_id     = r["signal_id"],
                        index_key     = idx_key,
                        direction     = r["direction"],
                        strike        = int(r["strike"]),
                        expiry        = r["expiry"],
                        entry_premium = float(r["entry_price"]),
                        sl_pts        = float(r["sl_pts"]),
                        tp_pts        = float(r["tp_pts"]),
                        trail_pts     = SWING_NO_TRAIL if r.get("signal_type") == "SWING" else float(r["tp_pts"]) * 0.5,
                        lot_size      = int(r.get("lot", INDICES[idx_key]["lot"])),
                        signal_type   = r.get("signal_type"),
                        is_free_signal= (r.get("channel") == "FREE"),
                    )
                    self.monitor.register(sig, submit_live_entry=False)
                    try:
                        from trading_live.engine_hooks import live_management_requested, sync_engine_stop
                        if live_management_requested() and not sig.is_free_signal:
                            sync_engine_stop(sig)
                    except Exception:
                        _log.critical(
                            "LIVE stop restoration sync failed for %s",
                            sig.signal_id,
                            exc_info=True,
                        )
                    if r.get("signal_type") != "SWING":
                        raw_created = r.get("created_at", r.get("timestamp"))
                        try:
                            stamp = float(raw_created)
                        except (TypeError, ValueError, OverflowError):
                            parsed = pd.to_datetime(raw_created, errors="coerce")
                            if pd.notna(parsed):
                                if parsed.tzinfo is None:
                                    parsed = parsed.tz_localize(IST)
                                stamp = parsed.timestamp()
                            else:
                                stamp = _ist_now().timestamp()
                        prior = self._trade_open_time.get(idx_key)
                        self._trade_open_time[idx_key] = min(prior, stamp) if prior is not None else stamp
                    restored += 1
            if restored > 0:
                _log.info(f"🔄 Restored {restored} open positions from ledger.")
        except Exception as e:
            _log.error(f"Failed to restore open signals: {e}")


    def _restore_signal_from_pg_payload(self, r: dict) -> ActiveSignal:
        """Rebuild an existing signal from its PostgreSQL snapshot."""
        idx_key = r["index_key"]
        entry = float(r["entry_premium"])
        tp_pts = float(r["tp_pts"])

        sig = ActiveSignal(
            signal_id=str(r["signal_id"]),
            index_key=idx_key,
            direction=str(r["direction"]),
            strike=int(r["strike"]),
            expiry=str(r["expiry"]),
            entry_premium=entry,
            sl_pts=max(0.0, entry - float(r["sl_price"])),
            tp_pts=tp_pts,
            trail_pts=max(0.0, float(r["trail_trigger"]) - entry),
            lot_size=int(r["lot_size"]),
            signal_type=str(r["signal_type"]),
            is_free_signal=bool(r["is_free_signal"]),
        )

        # Restore mutable fields instead of resetting targets or stop-loss.
        sig.sl_price = float(r["sl_price"])
        sig.tp_price = float(r["tp_price"])
        sig.trail_trigger = float(r["trail_trigger"])
        sig.trailed = bool(r["trailed"])
        sig.active = bool(r["active"])
        sig.created_at = float(r["created_at"])
        sig.targets = [
            {
                "name": str(t["name"]),
                "price": float(t["price"]),
                "hit": bool(t["hit"]),
            }
            for t in r["targets"]
        ]
        # Backward-compatible recovery: older persistence versions may not have
        # explicit moonbag/peak keys, but the final target's hit flag is saved.
        final_target_hit = any(
            t.get("name") == "Final Target" and bool(t.get("hit"))
            for t in sig.targets
        )
        sig.moonbag = bool(r.get("moonbag", final_target_hit))
        sig.peak = float(r.get("peak", max(sig.tp_price, sig.entry_premium) if sig.moonbag else sig.entry_premium))
        sig.moonbag_last_sl_trigger_premium = float(
            r.get(
                "moonbag_last_sl_trigger_premium",
                sig.peak,
            )
        )
        sig._last_sl_alert = sig.sl_price
        sig.moonbag_atr = r.get("moonbag_atr")
        sig.moonbag_swing_low = r.get("moonbag_swing_low")
        sig.moonbag_last_candle_ts = r.get("moonbag_last_candle_ts")
        sig._moonbag_last_refresh = 0.0
        sig._moonbag_last_peak_persist = 0.0
        sig._moonbag_warmup_logged = False
        return sig


    def _reset_if_new_day(self):
        today = _ist_now().strftime("%Y-%m-%d")
        if today != self._date:
            self._fired              = set()
            self._date               = today
            self._day_signal_count   = 0
            self._free_sent_today    = False
            self._swing_fired_today  = set()
            self._ic_fired_today     = set()
            self._best_signal        = None
            self._last_results       = []
            self._last_sniper        = {}
            self._last_strong_breakout  = {}
            self._strong_breakout_pending.clear()
            self._strong_breakout_fired.clear()
            _log.info(f"🌅 New trading day: {today}")
            if _ist_now().weekday() == 0:
                self._week_signal_count = 0
            self._save_state()

    def _is_market_hours(self) -> bool:
        now = _ist_now()
        return (now.weekday() < 5 and
                pd.Timestamp("09:15").time() <= now.time() <= pd.Timestamp("15:30").time())

    def _sniper_cooldown_ok(self, index_key: str) -> bool:
        last = self._last_sniper.get(index_key, 0)
        return (time.time() - last) > (SNIPER_COOLDOWN_MIN * 60)

    def _strong_breakout_cooldown_ok(self, index_key: str) -> bool:
        last = self._last_strong_breakout.get(index_key, 0)
        return (time.time() - last) > (SNIPER_COOLDOWN_MIN * 60)

    def _register_strong_breakout_signal(
        self,
        idx_key: str,
        cfg: dict,
        snap: dict,
        candles: pd.DataFrame,
        spot: float,
        breakout: dict,
    ) -> bool:

        try:
            # No new breakout trade at or after 15:00 IST.
            now_hm = _ist_now().strftime("%H:%M")

            if now_hm >= INTRADAY_CUTOFF_HM:
                _log.info(
                    "[STRONG BK %s] Blocked: entry cutoff reached (%s)",
                    idx_key,
                    now_hm,
                )
                return False
            if not _is_valid_entry_window(now_hm):
                _log.info(
                    "[STRONG BK %s] Blocked: outside entry window (%s)",
                    idx_key,
                    now_hm,
                )
                return False
            
            direction = breakout.get("direction")
            expected_direction = "CE" if direction == "UP" else "PE"

            level = float(breakout["level"])

            cache = (
                f"strong_bk_{idx_key}_{direction}_{int(level)}"
            )

            if cache in self._strong_breakout_fired:
                return False

            # 1. Fresh market data is mandatory.
            if not _snapshot_is_fresh(snap, LIVE_SNAPSHOT_MAX_AGE_S):
                _log.warning(
                    "[STRONG BK %s] Blocked: stale snapshot",
                    idx_key,
                )
                return False

            if spot <= 0 or candles is None or candles.empty:
                return False

            # 2. Never open another position on an index already occupied.
            active_dir = self.monitor.get_active_direction(idx_key)

            if active_dir is not None:
                _log.info(
                    "[STRONG BK %s] Blocked: existing position %s",
                    idx_key,
                    active_dir,
                )
                return False

            # 3. Reuse existing cooldown.
            if not self._strong_breakout_cooldown_ok(idx_key):
                _log.info(
                    "[STRONG BK %s] Blocked: strong breakout cooldown",
                    idx_key,
                )
                return False

            # 4. Run existing technical analysis.
            analysis = TechnicalBrain.analyze(candles, snap)

            analysis["index"] = idx_key
            analysis["spot"] = spot

            prob = abs(float(analysis.get("prob", 0)))

            min_prob = _effective_prob_threshold(idx_key, is_live=False)

            if prob < min_prob:
                _log.info(
                    "[STRONG BK %s] Blocked: probability %.3f < %.3f",
                    idx_key,
                    prob,
                    min_prob,
                )
                return False

            # 5. Require existing SNIPER confirmation.
            if analysis.get("signal_type") != "SNIPER":
                _log.info(
                    "[STRONG BK %s] Blocked: technical signal is %s",
                    idx_key,
                    analysis.get("signal_type"),
                )
                return False

            if analysis.get("regime", "CHOP") == "CHOP":
                _log.info(
                    "[STRONG BK %s] Blocked: CHOP regime",
                    idx_key,
                )
                return False

            if analysis.get("direction") != expected_direction:
                _log.info(
                    "[STRONG BK %s] Blocked: breakout and technical directions disagree",
                    idx_key,
                )
                return False

            # 6. Reuse the existing SNIPER strategy and risk calculations.
            sniper = self.strategy.sniper(
                analysis,
                snap,
                idx_key,
            )

            if not sniper.get("active"):
                _log.info(
                    "[STRONG BK %s] Blocked: existing SNIPER filters rejected setup",
                    idx_key,
                )
                return False

            if sniper.get("direction") != expected_direction:
                _log.warning("[STRONG BK %s] Blocked: SNIPER direction mismatch | expected=%s actual=%s", idx_key, expected_direction, sniper.get("direction"))
                return False

            base_strike = int(sniper["strike"])

            # 7. Resolve the exact expiry.
            trade_expiry = _get_expiry(
                idx_key,
                trade_type="SNIPER",
                strike=base_strike,
                direction=expected_direction,
            )

            if not trade_expiry:
                _log.warning(
                    "[STRONG BK %s] Blocked: expiry unavailable",
                    idx_key,
                )
                return False

            # 8. Reuse optional ITM upgrade.
            itm_selection = select_optional_itm_strike(
                self.fetcher,
                idx_key,
                base_strike,
                expected_direction,
                trade_expiry,
            )

            selected_strike = int(itm_selection["strike"])

            # 9. Exact-contract premium validation.
            entry_prem = self.fetcher.get_option_premium(
                idx_key,
                selected_strike,
                expected_direction,
                trade_expiry,
                side="ask",
            )

            if not math.isfinite(float(entry_prem)) or entry_prem <= 0:
                _log.warning(
                    "[STRONG BK %s] Blocked: invalid exact-contract premium",
                    idx_key,
                )
                return False

            # 10. Final duplicate-position check before registration.
            if self.monitor.get_active_direction(idx_key) is not None:
                _log.warning("[STRONG BK %s] Blocked: active position already exists", idx_key)
                return False

            sig_id = (
                f"STRONG_BK_{idx_key}_"
                f"{selected_strike}{expected_direction}_"
                f"{int(time.time() * 1000)}"
            )

            # 11. Register through existing persistence and paper-ledger pipeline.
            sig = ActiveSignal(
                signal_id=sig_id,
                index_key=idx_key,
                direction=expected_direction,
                strike=selected_strike,
                expiry=trade_expiry,
                entry_premium=entry_prem,
                sl_pts=sniper["sl_pts"],
                tp_pts=sniper["tp_pts"],
                trail_pts=sniper.get(
                    "trail_pts",
                    round(sniper["tp_pts"] * 0.5 / 5) * 5,
                ),
                lot_size=cfg["lot"],
                signal_type="STRONG_BREAKOUT",
                is_free_signal=False,
            )

            self.monitor.register(sig)

            # 12. Update engine state only after successful registration.
            self._strong_breakout_fired.add(cache)
            self._last_strong_breakout[idx_key] = time.time()
            self._trade_open_time[idx_key] = _ist_now().timestamp()

            self._day_signal_count += 1
            self._week_signal_count += 1

            if (
                self._best_signal is None
                or prob > abs(self._best_signal.get("prob", 0))
            ):
                self._best_signal = {
                    "index": idx_key,
                    "direction": expected_direction,
                    "strike": selected_strike,
                    "sl_pts": sniper["sl_pts"],
                    "tp_pts": sniper["tp_pts"],
                    "entry_premium": entry_prem,
                    "prob": analysis["prob"],
                }

            # 13. Existing signal log.
            _append_signal_log({
                "signal_id": sig_id,
                "timestamp": _ist_now().strftime("%Y-%m-%d %H:%M:%S"),
                "date": _ist_now().strftime("%Y-%m-%d"),
                "index": idx_key,
                "label": cfg["label"],
                "direction": expected_direction,
                "strike": selected_strike,
                "expiry": trade_expiry,
                "entry_price": entry_prem,
                "sl_pts": sniper["sl_pts"],
                "tp_pts": sniper["tp_pts"],
                "lot": cfg["lot"],
                "prob": round(analysis["prob"], 4),
                "regime": analysis.get("regime", ""),
                "spot": spot,
                "signal_type": "STRONG_BREAKOUT",
                "channel": "VIP",
                "status": "open",
                "exit_price": None,
                "pnl_pts": None,
                "pnl_inr": None,
                "outcome": None,
                "exit_time": None,
                "trigger": "strong_breakout_retest",
                "breakout_level": level,
                "breakout_volume_ratio": breakout.get("volume_ratio"),
                "breakout_body_ratio": breakout.get("body_ratio"),
            })

            # 14. Notify only after successful registration.
            sl_price = max(0.0, float(entry_prem) - float(sniper["sl_pts"]))
            target_price = float(entry_prem) + float(sniper["tp_pts"])
            
            action = "BUY CALL" if expected_direction == "CE" else "BUY PUT"
            direction_emoji = "🟢" if expected_direction == "CE" else "🔴"
            
            _to_vip(
                f"🚀 <b>STRONG BREAKOUT SIGNAL</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{direction_emoji} <b>{cfg['label']} — {action}</b>\n"
                f"<b>{selected_strike} {expected_direction}</b>\n"
                f"Expiry: <code>{trade_expiry}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 <b>ENTRY</b>     ₹{entry_prem:.2f}\n"
                f"🛑 <b>STOP LOSS</b> ₹{sl_price:.2f}\n"
                f"🎯 <b>TARGET</b>    ₹{target_price:.2f}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Spot: <code>{spot:.2f}</code>\n"
                f"Breakout: <code>{level:.2f}</code>\n"
                f"Volume: <b>{breakout['volume_ratio']:.2f}x</b>\n"
                f"Candle Strength: <b>{breakout['body_ratio']:.2f}</b>\n"
                f"Probability: <b>{prob:.1%}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>SETUP: RETEST CONFIRMED</b>\n"
                f"⚠️ Signal registered — broker fill not confirmed.\n"
                f"<code>{sig_id}</code>\n"
                f"🕒 {_now()}"
            )

            _log.info(
                "[STRONG BREAKOUT] REGISTERED | %s %s %s | "
                "entry=%.2f | probability=%.3f | signal=%s",
                idx_key,
                selected_strike,
                expected_direction,
                entry_prem,
                prob,
                sig_id,
            )

            return True

        except Exception:
            _log.exception(
                "[STRONG BREAKOUT %s] Registration failed",
                idx_key,
            )
            return False
 

    # ─────────────────────────────────────────────────────────────────────────
    #  CORE SCAN
    # ─────────────────────────────────────────────────────────────────────────
    def run_scan(self, label: str = "Scan", stealth_mode: bool = False,
                 force: bool = False) -> list:
        now_hm = _ist_now().strftime("%H:%M")
        if now_hm >= INTRADAY_CUTOFF_HM and not force:
            _log.warning(f"⏰ run_scan at {now_hm} — BLOCKED (after {INTRADAY_CUTOFF_HM})")
            return self._last_results

        vix = self.fetcher.fetch_vix()
        self._last_vix            = vix
        self._last_scheduled_scan = time.time()
        results = []

        scan_stats = {
            "checked": 0,
            "no_data": 0,
            "stale_snapshot": 0,
            "signals_generated": 0,
            "strategy_inactive": 0,
            "exceptions": 0,
        }

        nifty_thresh   = _effective_prob_threshold("NIFTY", is_live=False)
        scalp_blocked  = now_hm >= NO_NEW_SCALP_HM
        sniper_blocked = now_hm >= NO_NEW_SNIPER_HM

        _log.info(
            f"🔍 Scan [{label}] {now_hm}  VIX={vix:.1f}  "
            f"nifty_thresh={nifty_thresh:.2f}  "
            f"scalp={'BLOCKED' if scalp_blocked else 'OK'}  "
            f"sniper={'BLOCKED' if sniper_blocked else 'OK'}"
        )

        import concurrent.futures

        # 🔥 CONCURRENT FETCH: Fetch all indices simultaneously (Milliseconds instead of seconds)
        fetched_snaps = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_idx = {executor.submit(self.fetcher.get_index_data, k): k for k in INDICES.keys()}
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    fetched_snaps[idx] = future.result()
                except Exception as e:
                    _log.error(f"Concurrent fetch failed for {idx}: {e}")

        for idx_key, cfg in INDICES.items():
            try:
                snap    = fetched_snaps.get(idx_key, {})
                spot    = snap.get("spot", 0)
                candles = snap.get("candles", pd.DataFrame())

                if spot == 0 or candles.empty:
                    scan_stats["no_data"] += 1
                    _log.warning(f"No data for {idx_key} — skipping")
                    continue

                if not _snapshot_is_fresh(snap, LIVE_SNAPSHOT_MAX_AGE_S):
                    scan_stats["stale_snapshot"] += 1
                    _log.warning(
                        "[SCHEDULED %s] Skipped — missing, invalid or stale snapshot age: %s",
                        idx_key,
                        snap.get("_age_s"),
                    )
                    continue
                scan_stats["checked"] += 1

                analysis          = TechnicalBrain.analyze(candles, snap)
                analysis["index"] = idx_key
                analysis["spot"]  = spot

                self._last_spot   = spot
                self._last_regime = analysis.get("regime", "CHOP")

                # 🔥 1. TIME-WINDOW FILTER (Eliminate Lunchtime Chop)
                # 🔥 1. TIME-WINDOW FILTER (Eliminate Lunchtime Chop)
                now_hm = _ist_now().strftime("%H:%M")
                is_valid_time = _is_valid_entry_window(now_hm)

                # 🔥 2. DYNAMIC INDEX + TIME-OF-DAY PROBABILITY FLOOR
                min_required_prob = _effective_prob_threshold(idx_key, is_live=False)
                signal_prob = abs(analysis.get("prob", 0))
                active_dir = self.monitor.get_active_direction(idx_key)

                # Allow the -0.05 reversal relaxation to reach the reversal logic below
                is_potential_reversal = (
                    active_dir is not None
                    and analysis.get("direction") is not None
                    and active_dir != analysis["direction"]
                )
                entry_floor = (min_required_prob - 0.05) if is_potential_reversal else min_required_prob

                if not is_valid_time and analysis["signal_type"] in ["SNIPER", "SCALP"]:
                    _log.info(f"⏳ Skipping {idx_key}: Outside high-momentum windows")
                    analysis["signal_type"] = "HOLD"
                elif signal_prob < entry_floor and analysis["signal_type"] in ["SNIPER", "SCALP"]:
                    _log.info(f"⚠️ Signal blocked for {idx_key}: Prob {signal_prob:.3f} < {entry_floor:.3f} threshold")
                    analysis["signal_type"] = "HOLD"

                sniper     = self.strategy.sniper(analysis, snap, idx_key)

                # 🚫 PERMANENT QUALITY FILTER: Scalp is disabled based on the 21-day audit data.
                scalp      = {"active": False}
                sw         = {"active": False}
                chop       = {"active": False}
                bk         = {"active": False}
                #sw         = self.strategy.swing(analysis, snap, idx_key)
                #chop       = self.strategy.chop_sell(analysis, snap, idx_key)
                #bk         = self.strategy.breakout_watch(candles, spot, idx_key)
                active_dir = self.monitor.get_active_direction(idx_key)

                # 🔧 PREPARE REVERSAL FILTERS
                prev_prob       = self._last_prob.get(idx_key, 0.0)
                open_time       = self._trade_open_time.get(idx_key, 0)
                last_rev_time   = self._last_reversal_time.get(idx_key, 0)
                now_ts          = time.time()
                rsi             = analysis.get("breakdown", {}).get("rsi", 50)

                self._last_prob[idx_key] = analysis["prob"]

                is_reversal  = False
                trigger_type = "fresh"

                # ── STRICT REVERSAL LOGIC ─────────────────────────────────
                if active_dir == analysis["direction"]:
                    # Same side already open — block everything
                    sniper["active"] = False
                    scalp["active"]  = False
                    analysis["signal_type"] = "HOLD"

                elif active_dir is not None and active_dir != analysis["direction"] and sniper["active"]:
                    time_since_open = now_ts - open_time
                    time_since_rev  = now_ts - last_rev_time

                    # 1. 2-Bar Confirmation: Prev prob moving through zero
                    two_bar_confirm = (analysis["direction"] == "CE" and prev_prob > 0.0) or \
                                      (analysis["direction"] == "PE" and prev_prob < 0.0)

                    # 2. Exhaustion Filter: Old trend must have hit RSI extreme
                    exhausted = (active_dir == "CE" and rsi < 45) or (active_dir == "PE" and rsi > 55)


                    if analysis.get("regime", "CHOP") == "CHOP":
                        _log.info(f"⚠️ Reversal blocked on {idx_key}: Market is CHOP")
                        sniper["active"] = scalp["active"] = False

                    # 🔥 REVERSAL FIX: Lower conviction bar by 0.05 to allow more flip entries
                    # 🔥 REVERSAL FIX: Lower conviction bar by 0.05 to allow more flip entries
                    elif abs(analysis["prob"]) < (min_required_prob - 0.05):
                        _log.info(f"⚠️ Reversal skipped on {idx_key}: Prob < {(min_required_prob - 0.05):.2f} threshold")
                        sniper["active"] = scalp["active"] = False

                    elif not two_bar_confirm:
                        _log.info(f"⚠️ Reversal skipped on {idx_key}: Failed 2-bar confirm")
                        sniper["active"] = scalp["active"] = False


                    # if analysis.get("regime", "CHOP") == "CHOP":
                    #     _log.info(f"⚠️ Reversal blocked on {idx_key}: Market is CHOP")
                    #     sniper["active"] = scalp["active"] = False
                    # elif abs(analysis["prob"]) < 0.65:
                    #     _log.info(f"⚠️ Reversal skipped on {idx_key}: Prob < 0.65 threshold")
                    #     sniper["active"] = scalp["active"] = False
                    # elif not two_bar_confirm:
                    #     _log.info(f"⚠️ Reversal skipped on {idx_key}: Failed 2-bar confirm")
                    #     sniper["active"] = scalp["active"] = False
                    elif time_since_open < 900:  # 15 mins
                        _log.info(f"⚠️ Reversal skipped on {idx_key}: Old trade open < 15 mins")
                        sniper["active"] = scalp["active"] = False
                    elif time_since_rev < 1800:  # 30 mins
                        _log.info(f"⚠️ Reversal skipped on {idx_key}: Reversal cooldown active")
                        sniper["active"] = scalp["active"] = False
                    elif not exhausted:
                        _log.info(f"⚠️ Reversal skipped on {idx_key}: Old trend not exhausted (RSI {rsi:.1f})")
                        sniper["active"] = scalp["active"] = False
                    else:
                        _log.info(f"🔄 High Conviction Reversal on {idx_key}! Closing {active_dir}.")
                        self.monitor.close_active_reversal(idx_key)
                        self._last_reversal_time[idx_key] = now_ts

                        is_reversal  = True
                        trigger_type = "reversal"

                        # Re-run with tighter SL and trail rules
                        sniper = self.strategy.sniper(analysis, snap, idx_key, is_reversal=True)
                        scalp["active"] = False # Do not scalp on reversals, only sniper
                if not sniper.get("active"):
                    scan_stats["strategy_inactive"] += 1
                                               
                result = {
                    "index": idx_key, "spot": spot,
                    "prob": analysis["prob"], "signal_type": analysis["signal_type"],
                    "regime": analysis.get("regime", "CHOP"),
                    "sniper": sniper, "scalp": scalp, "swing": sw, "chop": chop, "bk": bk,
                }
                results.append(result)

                # ── VIP: Sniper ───────────────────────────────────────────────
                # 🔥 REVERSAL FIX: Accept lowered threshold if it is a reversal flip
                # ── VIP: Sniper ───────────────────────────────────────────────
                # 🔥 REVERSAL FIX: Reversals bypass 90m fresh-entry cooldown (already gated by 15m hold & 30m rev cooldown)
                # ── VIP: Sniper ───────────────────────────────────────────────
                # 🔥 REVERSAL FIX: Reversals bypass 90m fresh-entry cooldown (already gated by 15m hold & 30m rev cooldown)
                if sniper["active"] and not sniper_blocked \
                    and (self._sniper_cooldown_ok(idx_key) or is_reversal) \
                    and (abs(analysis["prob"]) >= min_required_prob or (is_reversal and abs(analysis["prob"]) >= (min_required_prob - 0.05))):

                    # Preserve original 1-step ITM strike for Zero-to-Hero.
                    base_strike = int(sniper["strike"])

                    # Resolve expiry using the original baseline contract.
                    trade_expiry = _get_expiry(
                        idx_key,
                        trade_type="SNIPER",
                        strike=base_strike,
                        direction=sniper["direction"]
                    )

                    entry_prem = 0.0

                    if trade_expiry:
                        # Optional feature: may upgrade 1-step to 2-step ITM.
                        # It never rejects a trade or changes the expiry.
                        itm_selection = select_optional_itm_strike(
                            self.fetcher,
                            idx_key,
                            base_strike,
                            sniper["direction"],
                            trade_expiry
                        )

                        sniper["strike"] = itm_selection["strike"]
                        sniper["strike_selection"] = itm_selection

                        entry_prem = self.fetcher.get_option_premium(
                            idx_key,
                            sniper["strike"],
                            sniper["direction"],
                            trade_expiry,
                            side="ask"
                        )

                    current_time = _ist_now().strftime("%H:%M")

                    # Zero-to-Hero deliberately retains the original 1-step ITM.
                    hero_expiry = _get_expiry(
                        idx_key,
                        trade_type="ZERO_TO_HERO",
                        strike=base_strike,
                        direction=sniper["direction"],
                        same_day_only=True
                    )

                    hero_premium = 0.0

                    if hero_expiry:
                        hero_premium = self.fetcher.get_option_premium(
                            idx_key,
                            base_strike,
                            sniper["direction"],
                            hero_expiry,
                            side="ask"
                        )
                    
                    is_hero = (
                        bool(hero_expiry)
                        and abs(float(analysis.get("prob", 0.0))) >= 0.85
                        and "12:30" <= current_time <= "14:45"
                    )
                    if is_hero:
                        # Hero always uses its original baseline contract.
                        sniper["strike"] = base_strike
                        
                    is_jackpot = ((idx_key in ["NIFTY", "FINNIFTY"]   and sniper["tp_pts"] >= 80) or
                                  (idx_key in ["BANKNIFTY", "SENSEX"] and sniper["tp_pts"] >= 180))

                    sniper_type = "DISABLED"
                    is_free_sig = False

                    if is_hero:
                        # A ZERO-HERO trade must have a valid same-day contract and premium.
                        if (
                            not hero_expiry
                            or not math.isfinite(float(hero_premium))
                            or float(hero_premium) <= 0
                        ):
                            _log.warning(
                                "[ZERO_HERO %s] BLOCKED: invalid same-day contract | "
                                "strike=%s direction=%s expiry=%r premium=%r",
                                idx_key,
                                base_strike,
                                sniper["direction"],
                                hero_expiry,
                                hero_premium,
                            )
                            continue
                    
                        trade_expiry = hero_expiry
                        entry_prem = float(hero_premium)
                        _to_vip(Fmt.vip_zero_hero(cfg, sniper["strike"], sniper["direction"],
                                                   trade_expiry, entry_prem))
                        sniper["tp_pts"]    = round(entry_prem * 1.5, 1)
                        sniper["sl_pts"]    = max(round(entry_prem * 0.4, 1), 1)
                        sniper["trail_pts"] = round(sniper["tp_pts"] * 0.5, 1)
                        sniper_type = "ZERO_HERO"
                        is_free_sig = False
                    elif is_jackpot:
                            if entry_prem <= 0:
                                _log.warning(
                                    "[%s] Jackpot blocked: normal exact-contract premium unavailable.",
                                    idx_key
                                )
                                continue
    
                            _to_vip(Fmt.vip_sniper(
                                cfg, analysis, sniper, spot, vix,
                                entry_premium=entry_prem,
                                is_jackpot=True
                            ))
                            sniper_type = "JACKPOT"
                            is_free_sig = False
                    else:
                        # 🔥 USER COMMAND: Stop taking standard SNIPER trades
                        _log.info(f"🚫 Standard SNIPER skipped for {idx_key} (disabled by user).")
                        sniper_type = "DISABLED"

                    if sniper_type != "DISABLED":
                        sig_id = f"{idx_key}_{sniper['strike']}{sniper['direction']}_{int(time.time())}"
                        if entry_prem > 0:
                            self._last_sniper[idx_key]  = time.time()
                            self._day_signal_count      += 1
                            self._week_signal_count     += 1
                            self.monitor.register(ActiveSignal(
                                signal_id     = sig_id,
                                index_key     = idx_key,
                                direction     = sniper["direction"],
                                strike        = sniper["strike"],
                                expiry        = trade_expiry,
                                entry_premium = entry_prem,
                                sl_pts        = sniper["sl_pts"],
                                tp_pts        = sniper["tp_pts"],
                                trail_pts     = sniper.get("trail_pts", round(sniper["tp_pts"]*0.5, 1)),
                                lot_size      = cfg["lot"],
                                signal_type   = sniper_type,
                                is_free_signal= is_free_sig,
                            ))
                            scan_stats["signals_generated"] += 1
                        
                            if not is_reversal:
                                self._trade_open_time[idx_key] = _ist_now().timestamp()

                        if (self._best_signal is None or
                                abs(analysis["prob"]) > abs(self._best_signal.get("prob", 0))):
                            self._best_signal = {
                                "index": idx_key, "direction": sniper["direction"],
                                "strike": sniper["strike"], "sl_pts": sniper["sl_pts"],
                                "tp_pts": sniper["tp_pts"], "entry_premium": entry_prem,
                                "prob": analysis["prob"],
                            }

                        _append_signal_log({
                            "signal_id"  : sig_id,
                            "timestamp"  : _ist_now().strftime("%Y-%m-%d %H:%M:%S"),
                            "date"       : _ist_now().strftime("%Y-%m-%d"),
                            "index"      : idx_key,
                            "label"      : cfg["label"],
                            "direction"  : sniper["direction"],
                            "strike"     : sniper["strike"],
                            "expiry"     : trade_expiry,
                            "entry_price": entry_prem,
                            "sl_pts"     : sniper["sl_pts"],
                            "tp_pts"     : sniper["tp_pts"],
                            "lot"        : cfg["lot"],
                            "prob"       : round(analysis["prob"], 4),
                            "regime"     : analysis.get("regime", ""),
                            "spot"       : spot,
                            "signal_type": sniper_type,
                            "channel"    : "VIP",
                            "status"     : "open",
                            "exit_price" : None, "pnl_pts": None, "pnl_inr": None,
                            "outcome"    : None, "exit_time": None,
                            "trigger"    : trigger_type,
                        })
                        _log.info(f"📝 {sniper_type} logged: {sig_id}")

                else:
                    if not sniper["active"] and not stealth_mode and active_dir is None:
                        _to_vip(Fmt.vip_no_setup(
                            cfg, analysis["signal_type"], analysis["prob"],
                            vix=vix, threshold=min_required_prob
                        ))

                # ── VIP: Swing ────────────────────────────────────────────────
                # FIX: One swing trade per index per day (not one for the whole market)
                if sw["active"] and idx_key not in self._swing_fired_today:
                    swing_prem = self.fetcher.get_option_premium(
                        idx_key, sw["strike"], sw["direction"], sw["expiry"], side="ask")
                    if swing_prem <= 0:
                        _log.warning("Swing entry blocked: no fresh valid LTP for %s %s", idx_key, sw.get("strike"))
                        continue
                    _to_vip(Fmt.vip_swing(cfg, analysis, sw, spot, entry_premium=swing_prem))
                    self._day_signal_count  += 1
                    self._week_signal_count += 1
                    self._swing_fired_today.add(idx_key)

                    sw_sig_id = f"SWING_{idx_key}_{sw['strike']}{sw['direction']}_{int(time.time())}"
                    _append_signal_log({
                        "signal_id"  : sw_sig_id,
                        "timestamp"  : _ist_now().strftime("%Y-%m-%d %H:%M:%S"),
                        "date"       : _ist_now().strftime("%Y-%m-%d"),
                        "index"      : idx_key, "label": cfg["label"],
                        "direction"  : sw["direction"], "strike": sw["strike"],
                        "expiry"     : sw.get("expiry", ""),
                        "entry_price": swing_prem,
                        "sl_pts"     : sw["sl_pts"], "tp_pts": sw["tp_pts"],
                        "lot"        : cfg["lot"],
                        "prob"       : round(analysis["prob"], 4),
                        "regime"     : analysis.get("regime", ""), "spot": spot,
                        "signal_type": "SWING", "channel": "VIP", "status": "open",
                        "exit_price" : None, "pnl_pts": None, "pnl_inr": None,
                        "outcome"    : None, "exit_time": None,
                    })
                    if swing_prem > 0:
                        self.monitor.register(ActiveSignal(
                            signal_id     = sw_sig_id,
                            index_key     = idx_key, direction=sw["direction"],
                            strike        = sw["strike"], expiry=sw["expiry"],
                            entry_premium = swing_prem,
                            sl_pts        = sw["sl_pts"], tp_pts=sw["tp_pts"],
                            trail_pts     = SWING_NO_TRAIL,
                            lot_size      = cfg["lot"], signal_type="SWING",
                        ))

                # ── VIP: Iron Condor on chop ──────────────────────────────────
                # ── VIP: Iron Condor on chop — once per index per day ─────────
                
                # ── VIP: Iron Condor advisory — only when strategy is active ──
                if (
                    analysis["signal_type"] == "CHOP"
                    and idx_key not in self._ic_fired_today
                    and isinstance(chop, dict)
                    and chop.get("active") is True
                ):
                    required_ic_fields = (
                        "short_call",
                        "long_call",
                        "short_put",
                        "long_put",
                        "profit_zone",
                        "range_pts",
                        "expiry",
                    )
                
                    missing_ic_fields = [
                        field for field in required_ic_fields
                        if chop.get(field) is None
                    ]
                
                    try:
                        valid_ic_range = (
                            math.isfinite(float(chop["range_pts"]))
                            and float(chop["range_pts"]) > 0
                        )
                    except (TypeError, ValueError, OverflowError):
                        valid_ic_range = False
                
                    if missing_ic_fields or not valid_ic_range:
                        _log.warning(
                            "[IRON CONDOR BLOCKED] %s | missing_fields=%s "
                            "| valid_range=%s",
                            idx_key,
                            missing_ic_fields,
                            valid_ic_range,
                        )
                    else:
                        _to_vip(Fmt.vip_iron_condor(cfg, chop, spot, vix))
                        self._ic_fired_today.add(idx_key)


                # ── VIP: Key levels ───────────────────────────────────────────
                if bk["active"] and not stealth_mode:
                    _to_vip(Fmt.vip_key_levels(cfg, bk, spot, analysis["prob"]))

                # Free-channel sample trade entries are disabled. Scheduled engagement posts remain enabled.

            except Exception as e:
                scan_stats["exceptions"] += 1
                _log.error(f"Scan error ({idx_key}): {e}", exc_info=True)

        _log.info(
            "[SCAN SUMMARY] %s | checked=%s no_data=%s "
            "stale=%s strategy_inactive=%s generated=%s exceptions=%s",
            label,
            scan_stats["checked"],
            scan_stats["no_data"],
            scan_stats["stale_snapshot"],
            scan_stats["strategy_inactive"],
            scan_stats["signals_generated"],
            scan_stats["exceptions"],
        )

        if results and not stealth_mode:
            _to_vip(Fmt.vip_scan_summary(label, results))

        self._last_results = results
        return results

    # ─────────────────────────────────────────────────────────────────────────
    #  SCHEDULED ACTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _do_pre_market(self):
        vix   = self.fetcher.fetch_vix()
        self._last_vix = vix
        spots = {}
        for idx_key in INDICES:
            time.sleep(1)
            snap = self.fetcher.get_index_data(idx_key)
            spots[idx_key] = snap.get("spot", 0)
            if spots[idx_key] > 0:
                self._last_spot = spots[idx_key]

        lines = [
            f"🌅 <b>PRE-MARKET BIAS</b>  `{_now()}`",
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            f"😨 India VIX : `{vix:.1f}` — "
            f"{'⚠️ ELEVATED — expect wide moves' if vix > 18 else '✅ Normal — structured setups'}",
            f"━━━━━━━━━━━━━━━━━━━━━━━",
        ]
        for idx_key, cfg in INDICES.items():
            s = spots.get(idx_key, 0)
            lines.append(f"{cfg['emoji']} {cfg['label']} : `{s:.0f}`" if s > 0
                         else f"{cfg['emoji']} {cfg['label']} : `Pre-market settling...`")
        lines += [
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            f"🤖 Engine scanning. First trade alert when confident.",
            f"⚠️ Do NOT trade in the first 5 minutes (9:15–9:20).",
        ]
        _to_vip("\n".join(lines))
        _free_post_morning(spots=spots, vix=vix, regime=self._last_regime)

    def _do_mid_morning_free(self):
        _free_post_mid_morning(
            signals_fired=self._day_signal_count, vip_pnl_so_far=0.0,
            spot=self._last_spot)

    def _do_lunch_free(self):
        _free_post_lunch(
            trades_done=self._day_signal_count, morning_pnl=0.0,
            best_trade_pts=self._best_signal.get("tp_pts", 0) if self._best_signal else 0,
            still_in_trade=False)

    def _do_afternoon_free(self):
        _free_post_afternoon(
            spot=self._last_spot, regime=self._last_regime,
            day_signals=self._day_signal_count, day_pnl_so_far=0.0)

    def _do_pre_close_free(self):
        _free_post_pre_close(
            spot=self._last_spot, day_signals=self._day_signal_count,
            swing_fired=bool(self._swing_fired_today))

        if not self._free_sent_today:
            all_chop = all(r.get("signal_type") == "CHOP" for r in self._last_results) \
                       if self._last_results else False
            if all_chop:
                _free_post_chop(self._last_vix)
            else:
                _free_post_no_trade(
                    vix=self._last_vix,
                    reason=f"prob {self._last_results[0]['prob']:+.3f} below threshold"
                           if self._last_results else "")
            self._free_sent_today = True

    def _do_eod(self):
        vix = self.fetcher.fetch_vix()
        self._last_vix = vix

        day_vibe = ("✅ <b>Targets hit & Risk managed.</b>" if self._day_signal_count > 0
                    else "🛡️ <b>Zero forced trades. Capital fully protected today.</b>")

        lines = [
            f"🌙 <b>VIP END OF DAY SUMMARY</b>",
            f"📅 <i>{_today()}</i>",
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            f"{day_vibe}",
            f"📡 <b>Total Signals Fired : {self._day_signal_count}</b>",
            f"😨 <b>India VIX : {vix:.1f}</b>",
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            f"📊 <b>Closing Market Structure:</b>",
        ]
        for r in self._last_results:
            idx    = r.get("index", "?")
            cfg    = INDICES.get(idx, {})
            spot   = r.get("spot", 0)
            regime = r.get("regime", "CHOP").upper()
            if "BULL" in regime:   trend = "📈 Bullish control"
            elif "BEAR" in regime: trend = "📉 Bearish weakness"
            else:                  trend = "⚠️ Choppy / Rangebound"
            lines.append(f"{cfg.get('emoji','•')} <b>{cfg.get('label', idx)}</b> : ₹{spot:.0f}  ({trend})")
        lines += [
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            f"💡 <i>{_insight()}</i>",
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            f"🔋 <i>System shutting down. See you tomorrow at 9:02 AM.</i>",
        ]
        _to_vip("\n".join(lines))
        _free_post_eod(day_signals=self._day_signal_count, best_signal=self._best_signal,
                       vix=vix, swing_open=bool(self._swing_fired_today))
        if _ist_now().weekday() == 4:
            time.sleep(3)
            _free_post_friday_recap(week_signals=self._week_signal_count, vix=vix,
                                    best_signal=self._best_signal)

    # ─────────────────────────────────────────────────────────────────────────
    #  v10/v11: LIVE CONTINUOUS SNIPER CHECK (every 5 min between scans)
    #  v11 improvement: skips any index whose snapshot is > 60s old
    # ─────────────────────────────────────────────────────────────────────────
    def _live_sniper_check(self):
        # 🔥 REACTIVATED: Live scanner now uses index-specific probability floors
        now_hm = _ist_now().strftime("%H:%M")

        # 1. TIME-WINDOW FILTER (Matches scheduled scan)
        is_valid_time = _is_valid_entry_window(now_hm)
        if not is_valid_time:
            return

        if now_hm >= INTRADAY_CUTOFF_HM:
            return
        if (time.time() - self._last_live_check) < LIVE_SCAN_INTERVAL_SEC:
            return
        self._last_live_check = time.time()

        # Don't run within 2 minutes of a full scheduled scan
        if (time.time() - self._last_scheduled_scan) < 120:
            return

        vix            = self._last_vix
        sniper_blocked = now_hm >= NO_NEW_SNIPER_HM
        if sniper_blocked:
            return

        import concurrent.futures

        # 🔥 CONCURRENT FETCH: Live scanner monitors all 4 simultaneously
        fetched_snaps = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_idx = {executor.submit(self.fetcher.get_index_data, k): k for k in INDICES.keys()}
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    fetched_snaps[idx] = future.result()
                except Exception:
                    pass

        for idx_key, cfg in INDICES.items():
            try:
                snap    = fetched_snaps.get(idx_key, {})
                spot    = snap.get("spot", 0)
                candles = snap.get("candles", pd.DataFrame())

                if spot == 0 or candles.empty:
                    continue

                if not _snapshot_is_fresh(snap, LIVE_SNAPSHOT_MAX_AGE_S):
                    _log.warning(
                        "[LIVE %s] Skipped — missing, invalid or stale snapshot age: %s",
                        idx_key,
                        snap.get("_age_s"),
                    )
                    continue

                analysis = TechnicalBrain.analyze(candles, snap)
                prob     = analysis.get("prob", 0.0)

                # 🔥 INDEX-SPECIFIC PROBABILITY FLOOR
                min_required_prob = _effective_prob_threshold(idx_key, is_live=True)
                if abs(prob) < min_required_prob:
                    continue

                if analysis.get("signal_type") != "SNIPER":
                    continue

                sniper = self.strategy.sniper(analysis, snap, idx_key)
                if not sniper.get("active"):
                    continue

                if not self._sniper_cooldown_ok(idx_key):
                    _log.debug(f"[LIVE {idx_key}] Skipped — cooldown")
                    continue

                active_dir = self.monitor.get_active_direction(idx_key)
                if active_dir == analysis["direction"]:
                    continue
                if active_dir is not None and active_dir != analysis["direction"]:
                    _log.info(f"[LIVE {idx_key}] Reversal — closing {active_dir}, skipping new entry this tick")
                    self.monitor.close_active_reversal(idx_key)
                    continue

                # Preserve original 1-step ITM strike for Zero-to-Hero.
                base_strike = int(sniper["strike"])

                # Resolve expiry using the original baseline contract.
                trade_expiry = _get_expiry(
                    idx_key,
                    trade_type="SNIPER",
                    strike=base_strike,
                    direction=sniper["direction"]
                )

                entry_prem = 0.0

                if trade_expiry:
                    # Optional adaptive ITM upgrade. Failure means 1-step ITM.
                    itm_selection = select_optional_itm_strike(
                        self.fetcher,
                        idx_key,
                        base_strike,
                        sniper["direction"],
                        trade_expiry
                    )

                    sniper["strike"] = itm_selection["strike"]
                    sniper["strike_selection"] = itm_selection

                    entry_prem = self.fetcher.get_option_premium(
                        idx_key,
                        sniper["strike"],
                        sniper["direction"],
                        trade_expiry,
                        side="ask"
                    )

                current_time = _ist_now().strftime("%H:%M")

                # Zero-to-Hero retains the original 1-step ITM contract.
                hero_expiry = _get_expiry(
                    idx_key,
                    trade_type="ZERO_TO_HERO",
                    strike=base_strike,
                    direction=sniper["direction"],
                    same_day_only=True
                )

                hero_premium = 0.0

                if hero_expiry:
                    hero_premium = self.fetcher.get_option_premium(
                        idx_key,
                        base_strike,
                        sniper["direction"],
                        hero_expiry,
                        side="ask"
                    )
                
                is_hero = (
                    bool(hero_expiry)
                    and abs(analysis["prob"]) >= 0.85
                    and "12:30" <= current_time <= "14:45"
                )
                if is_hero:
                    # Hero always uses its original baseline contract.
                    sniper["strike"] = base_strike
                
                is_jackpot = (
                    (idx_key in ["NIFTY", "FINNIFTY"] and sniper["tp_pts"] >= 80)
                    or
                    (idx_key in ["BANKNIFTY", "SENSEX"] and sniper["tp_pts"] >= 180)
                )

                if is_hero:
                    # Never register or publish ZERO-HERO with missing expiry or invalid LTP.
                    if (
                        not hero_expiry
                        or not math.isfinite(float(hero_premium))
                        or float(hero_premium) <= 0
                    ):
                        _log.warning(
                            "[LIVE ZERO_HERO %s] BLOCKED: invalid same-day contract | "
                            "strike=%s direction=%s expiry=%r premium=%r",
                            idx_key,
                            base_strike,
                            sniper["direction"],
                            hero_expiry,
                            hero_premium,
                        )
                        continue
                
                    trade_expiry = hero_expiry
                    entry_prem = float(hero_premium)
                
                    _to_vip(
                        Fmt.vip_zero_hero(
                            cfg,
                            sniper["strike"],
                            sniper["direction"],
                            trade_expiry,
                            entry_prem
                        )
                    )
                
                    sniper["tp_pts"] = round(entry_prem * 1.5, 1)
                    sniper["sl_pts"] = max(round(entry_prem * 0.4, 1), 1)
                    sniper["trail_pts"] = round(sniper["tp_pts"] * 0.5, 1)
                
                    sniper_type = "ZERO_HERO"
                    is_free_sig = False

                elif is_jackpot:
                    if (
                        not trade_expiry
                        or not math.isfinite(float(entry_prem))
                        or float(entry_prem) <= 0
                    ):
                        _log.warning(
                            "[LIVE %s] Jackpot blocked: invalid contract | expiry=%r premium=%r",
                            idx_key,
                            trade_expiry,
                            entry_prem,
                        )
                        continue

                    _to_vip(Fmt.vip_sniper(
                        cfg, analysis, sniper, spot, vix,
                        entry_premium=entry_prem,
                        is_jackpot=True
                    ))
                    sniper_type = "JACKPOT"
                    is_free_sig = False

                else:
                    if entry_prem <= 0:
                        _log.warning(
                            "[LIVE %s] Sniper blocked: no fresh normal exact-contract premium.",
                            idx_key
                        )
                        continue

                    _to_vip(Fmt.vip_live_sniper(
                        cfg, sniper, spot, vix, entry_prem, prob, idx_key
                    ))
                    sniper_type = "LIVE_SNIPER"
                    is_free_sig = False
                    
                # Final validation for BOTH ZERO-HERO and normal LIVE signals.
                if (
                    not trade_expiry
                    or not math.isfinite(float(entry_prem))
                    or float(entry_prem) <= 0
                ):
                    _log.error(
                        "[LIVE ENTRY BLOCKED] Invalid contract data | "
                        "%s %s %s expiry=%r premium=%r",
                        idx_key,
                        sniper.get("strike"),
                        sniper.get("direction"),
                        trade_expiry,
                        entry_prem,
                    )
                    continue


                self._last_sniper[idx_key]  = time.time()
                self._day_signal_count     += 1
                self._week_signal_count    += 1

                sig_id = f"LIVE_{idx_key}_{sniper['strike']}{sniper['direction']}_{int(time.time())}"
                self.monitor.register(ActiveSignal(
                    signal_id     = sig_id,
                    index_key     = idx_key, direction=sniper["direction"],
                    strike        = sniper["strike"], expiry=trade_expiry,
                    entry_premium = entry_prem,
                    sl_pts        = sniper["sl_pts"], tp_pts=sniper["tp_pts"],
                    trail_pts     = sniper.get("trail_pts", round(sniper["tp_pts"]*0.5/5)*5),
                    lot_size      = cfg["lot"],
                    signal_type   = sniper_type,
                    is_free_signal= is_free_sig,
                ))
                
                self._trade_open_time[idx_key] = _ist_now().timestamp()

                _append_signal_log({
                    "signal_id"  : sig_id,
                    "timestamp"  : _ist_now().strftime("%Y-%m-%d %H:%M:%S"),
                    "date"       : _ist_now().strftime("%Y-%m-%d"),
                    "index"      : idx_key, "label": cfg["label"],
                    "direction"  : sniper["direction"], "strike": sniper["strike"],
                    "expiry"     : trade_expiry, "entry_price": entry_prem,
                    "sl_pts"     : sniper["sl_pts"], "tp_pts": sniper["tp_pts"],
                    "lot"        : cfg["lot"], "prob": round(prob, 4),
                    "regime"     : analysis.get("regime", ""), "spot": spot,
                    "signal_type": sniper_type, "channel": "VIP", "status": "open",
                    "exit_price" : None, "pnl_pts": None, "pnl_inr": None,
                    "outcome"    : None, "exit_time": None,
                })

                if (self._best_signal is None or abs(prob) > abs(self._best_signal.get("prob", 0))):
                    self._best_signal = {
                        "index": idx_key, "direction": sniper["direction"],
                        "strike": sniper["strike"], "sl_pts": sniper["sl_pts"],
                        "tp_pts": sniper["tp_pts"], "entry_premium": entry_prem, "prob": prob,
                    }

                _log.info(
                    f"⚡ LIVE SNIPER fired: {idx_key} {sniper['direction']} "
                    f"{sniper['strike']}  prob={prob:+.4f}  entry=₹{entry_prem}"
                )

            except Exception as e:
                _log.debug(f"Live sniper check ({idx_key}): {e}")

    # ─────────────────────────────────────────────────────────────────────────
    #  LIVE BREAKOUT CHECK (every 1 min, proximity alerts only)
    # ─────────────────────────────────────────────────────────────────────────
    def _live_breakout_check(self):
        now_hm = _ist_now().strftime("%H:%M")

        if now_hm >= INTRADAY_CUTOFF_HM:
            _log.info("[STRONG BK SCAN] SKIPPED | reason=intraday_cutoff | time=%s", now_hm)
            return

        if (time.time() - self._last_bk_check) < 60:
            _log.debug("[STRONG BK SCAN] SKIPPED | reason=cooldown")
            return

        self._last_bk_check = time.time()
        _log.info("[STRONG BK SCAN] STARTED | time=%s", _ist_now().strftime("%Y-%m-%d %H:%M:%S"))

        for idx_key, cfg in INDICES.items():
            try:
                time.sleep(1)

                snap = self.fetcher.get_index_data(idx_key)
                spot = float(snap.get("spot", 0) or 0)
                candles = snap.get("candles", pd.DataFrame())
                futures_volume_bars = (
                    int(candles["futures_volume"].notna().sum())
                    if isinstance(candles, pd.DataFrame)
                    and "futures_volume" in candles.columns
                    else 0
                )
                _log.info(
                    "[STRONG BK DATA] %s | spot=%.2f | spot_candles=%d | "
                    "futures_volume_bars=%d | pending=%s",
                    idx_key, spot, 0 if candles is None else len(candles),
                    futures_volume_bars,
                    bool(self._strong_breakout_pending.get(idx_key)),
                )

                if spot <= 0 or candles is None or candles.empty:
                    _log.warning(
                        "[STRONG BK DATA] %s | SKIPPED | spot=%.2f | candles=%d",
                        idx_key, spot, 0 if candles is None else len(candles),
                    )
                    continue

                if "futures_volume" not in candles.columns or futures_volume_bars < STRONG_BK_MIN_BARS + 1:
                    _log.info(
                        "[STRONG BK STATE] %s | NOT READY | "
                        "reason=insufficient_genuine_futures_volume | bars=%d required=%d",
                        idx_key, futures_volume_bars, STRONG_BK_MIN_BARS + 1,
                    )
                    continue

                breakout_candles = candles.copy()
                breakout_candles["volume"] = pd.to_numeric(
                    breakout_candles["futures_volume"], errors="coerce"
                )

                pending = self._strong_breakout_pending.setdefault(
                    idx_key, {}
                )

                bk = self.strategy.strong_breakout_retest(
                    candles=breakout_candles,
                    spot=spot,
                    index_key=idx_key,
                    pending=pending,
                    now_ts=time.time(),
                )
                _log.info("[STRONG BK RESULT] %s | stage=%s | direction=%s | level=%s | reason=%s | volume=%s | body=%s | ATR=%s", idx_key, bk.get("stage"), bk.get("direction"), bk.get("level"), bk.get("reason"), bk.get("volume_ratio"), bk.get("body_ratio"), bk.get("atr"))

                stage = bk.get("stage")
                direction = bk.get("direction")
                level = bk.get("level")

                if stage == "BREAKOUT_DETECTED":
                    _log.info(
                        "[STRONG BREAKOUT] %s %s detected | "
                        "level=%.2f volume=%.2fx body=%.2f",
                        idx_key,
                        direction,
                        level,
                        bk["volume_ratio"],
                        bk["body_ratio"],
                    )
                    _log.info("[STRONG BK STATE] %s | PENDING RETEST | direction=%s | level=%.2f", idx_key, direction, level)

                    continue

                if stage != "RETEST_CONFIRMED":
                    _log.info("[STRONG BK STATE] %s | NOT READY | stage=%s | reason=%s", idx_key, stage, bk.get("reason"))
                    continue
                registered = self._register_strong_breakout_signal(
                    idx_key=idx_key,
                    cfg=cfg,
                    snap=snap,
                    candles=candles,
                    spot=spot,
                    breakout=bk,
                )

                if not registered:
                    _log.info(
                        "[STRONG BREAKOUT] %s %s retest confirmed "
                        "but trade registration failed.",
                        idx_key,
                        direction,
                    )
                    continue

                cache = f"strong_bk_{idx_key}_{direction}"

                if cache in self._strong_breakout_fired:
                    continue

                self._strong_breakout_fired.add(cache)

                option_direction = "CE" if direction == "UP" else "PE"

                _to_vip(
                    f"🚀 *STRONG BREAKOUT — RETEST CONFIRMED*\\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\\n"
                    f"*{cfg['label']}*\\n"
                    f"Direction: *{option_direction}*\\n"
                    f"Spot: `{spot:.2f}`\\n"
                    f"Breakout Level: `{level:.2f}`\\n"
                    f"Volume: `{bk['volume_ratio']:.2f}x`\\n"
                    f"Body Strength: `{bk['body_ratio']:.2f}`\\n"
                    f"ATR: `{bk['atr']:.2f}`\\n"
                    f"Entry Type: *Retest Confirmation*\\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\\n"
                    f"⚠️ Paper signal candidate — option premium, "
                    f"spread, liquidity and risk checks still required.\\n"
                    f"🕒 `{_now()}`"
                )

                _log.info(
                    "[STRONG BREAKOUT] %s %s RETEST CONFIRMED | "
                    "level=%.2f spot=%.2f",
                    idx_key,
                    direction,
                    level,
                    spot,
                )

            except Exception as e:
                _log.exception(
                    "Strong breakout check failed (%s): %s",
                    idx_key,
                    e,
                )

    # ─────────────────────────────────────────────────────────────────────────
    #  MAIN LOOP
    # ─────────────────────────────────────────────────────────────────────────
    def start(self):
        self._running = True
        _log.info("🚀 Daily Signal Engine v11.0 started — Ultimate Hybrid")

        while self._running:
            if _signal_persistence_failed.is_set():
                _log.critical("DailySignalEngine halted: signal persistence failure requires manual recovery")
                self._running = False
                return
            try:
                self._reset_if_new_day()
                now  = _ist_now().strftime("%H:%M")
                wday = _ist_now().weekday()

                if wday >= 5:
                    time.sleep(3600)
                    continue

                if now >= SCANS["pre_market"] and "pre" not in self._fired:
                    self._do_pre_market()
                    self._fired.add("pre")

                elif now >= SCANS["swing_close"] and "swing_close" not in self._fired:
                    _log.info("⏰ 09:25 — Force closing overnight SWING positions.")
                    self.monitor.force_close_swing()
                    self._fired.add("swing_close")

                elif now >= SCANS["scan_936"] and "scan_936" not in self._fired:
                    self.run_scan("09:36 Opening Range Breakout")
                    self._fired.add("scan_936")

                elif now >= SCANS["scan_1006"] and "scan_1006" not in self._fired:
                    self.run_scan("10:06 Morning Trend Confirmation")
                    self._do_mid_morning_free()
                    self._fired.add("scan_1006")

                elif now >= SCANS["scan_1046"] and "scan_1046" not in self._fired:
                    self.run_scan("10:46 Trend Continuation / VWAP Bounce")
                    self._fired.add("scan_1046")

                elif now >= SCANS["scan_1336"] and "scan_1336" not in self._fired:
                    self.run_scan("13:36 Afternoon Activity")
                    self._fired.add("scan_1336")

                elif now >= SCANS["scan_1401"] and "scan_1401" not in self._fired:
                    self.run_scan("14:01 Afternoon Trend")
                    self._do_afternoon_free()
                    self._fired.add("scan_1401")

                elif now >= SCANS["scan_1431"] and "scan_1431" not in self._fired:
                    self.run_scan("14:31 Final Entry Opportunity")
                    self._fired.add("scan_1431")

                elif now >= SCANS["pre_close"] and "pre_close" not in self._fired:
                    self._do_pre_close_free()
                    self._fired.add("pre_close")

                elif now >= SCANS["force_close"] and "force_close" not in self._fired:
                    _log.info("⏰ 15:12 — Force closing all intraday positions.")
                    self.monitor.force_close_intraday()
                    self._fired.add("force_close")

                elif now >= SCANS["eod"] and "eod" not in self._fired:
                    self._do_eod()
                    self._fired.add("eod")

                elif self._is_market_hours() and "scan_936" in self._fired:
                    # Between scheduled scans: continuous live monitoring
                    self._live_breakout_check()
                    self._live_sniper_check()

                self._save_state()

            except KeyboardInterrupt:
                _log.info("🛑 Engine stopped.")
                self._running = False
                break
            except Exception as e:
                _log.error(f"Scheduler error: {e}", exc_info=True)

            time.sleep(60)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "═" * 64)
    print("  BLACK — Daily Signal Engine  v11.0  (Ultimate Hybrid)")
    print("═" * 64)
    print(f"  ✅ Isolated memory  : data/signal_engine_v11/")
    print(f"  ✅ Volume surge filter (1.2× avg, flat-volume bypass)")
    print(f"  ✅ Candle-direction guard intentionally disabled; metrics remain diagnostic")
    print(f"  ✅ PCR/GEX doubled weight (0.30/0.20)")
    print(f"  ✅ Atomic close guard on ALL exit paths")
    print(f"  ✅ Live continuous scan every 5 min (prob >= 0.55)")
    print(f"  ✅ Live stale snapshot guard (skip if > 60s old)")
    print(f"  ✅ Zero-to-Hero (0DTE), Jackpot multi-target, Swing")
    print(f"  ✅ Reversal auto-close, Iron Condor on chop")
    print(f"  ✅ All 6 free channel posts using real engine data")
    print(f"  ✅ Lot size advisory (VIX-based)")
    print("═" * 64)
    print(f"  SIGNAL_BOT_TOKEN : {'✅ SET' if SIGNAL_BOT_TOKEN else '❌ MISSING'}")
    print(f"  Telegram destinations    : {len(TELEGRAM_CHAT_IDS)}/1 configured")
    print("  Telegram mode            : trade alerts to configured channel only")
    print(f"  Signal Angel API : {'✅ SET' if SIGNAL_API_KEY   else '⚠️  using ANGEL_API_KEY fallback'}")
    print()
    print(f"  Snapshots:")
    for k, p in SNAPSHOTS.items():
        exists = p.exists()
        age = ""
        if exists:
            age_s = time.time() - p.stat().st_mtime
            age   = f"  ({age_s:.0f}s old)"
        print(f"    {k:12s}: {'✅' if exists else '❌'} {p}{age}")
    print("═" * 64)

    if "--test" in sys.argv:
        print("\n🧪 Test mode — offline checks only; no engine, broker auth, threads, or Telegram posts.")
        df = pd.DataFrame({
            "close" : np.cumsum(np.random.randn(60)) + 24000,
            "high"  : np.cumsum(np.random.randn(60)) + 24050,
            "low"   : np.cumsum(np.random.randn(60)) + 23950,
            "open"  : np.cumsum(np.random.randn(60)) + 24000,
            "volume": np.random.randint(500, 5000, 60).astype(float),
        })
        # Make last candle bullish so sniper can fire in test
        df.loc[df.index[-1], "close"] = df["open"].iloc[-1] + 5
        snap = {"spot": 24000, "vix": 15.0, "pcr": 1.3, "net_gex": 80, "_age_s": 10}
        res  = TechnicalBrain.analyze(df, snap)
        print(f"  TechnicalBrain → prob={res['prob']:+.4f}  sig={res['signal_type']}  "
              f"atr={res['atr']:.1f}  regime={res['regime']}")
        print(f"  vol_surge={res['vol_surge']}  bull_candle={res['bull_candle']}  bear_candle={res['bear_candle']}")

        test_cfg    = INDICES["NIFTY"]
        test_sniper = {"direction": "CE", "strike": 24100, "expiry": "27 MAY 25",
                       "sl_pts": 45, "tp_pts": 110, "trail_pts": 55}

        print("\n  [1] Scheduled SNIPER message:")
        print(Fmt.vip_sniper(test_cfg, res, test_sniper, 24050.0, 15.0, entry_premium=155.0))

        print("\n  [2] LIVE DETECTION message:")
        print(Fmt.vip_live_sniper(test_cfg, test_sniper, 24050.0, 15.0, 155.0, 0.62))

        print("\n  [3] JACKPOT message:")
        jp = dict(test_sniper); jp["tp_pts"] = 120
        print(Fmt.vip_sniper(test_cfg, res, jp, 24050.0, 15.0, entry_premium=155.0, is_jackpot=True))

        print("\n  [4] ZERO-TO-HERO message:")
        print(Fmt.vip_zero_hero(test_cfg, 24100, "CE", "27 MAY 25", 42.0))

        print("\n  [5] Trail alert message:")
        print(Fmt.vip_trail_alert(test_cfg, 24100, "CE", "27 MAY 25", 155.0, 210.0, 155.0, 265.0))

        print("\n  [6] Jackpot hit message (compact v10 format):")
        print(Fmt.vip_jackpot_hit(test_cfg, 24100, "CE", "27 MAY 25", 155.0, 275.0, 120.0, 31200))

        print("\n  ✅ All message formats OK.")
        print("✅ Test passed.")
        return

    if not SIGNAL_BOT_TOKEN:
        print("\n❌  SIGNAL_BOT_TOKEN missing. Add to .env and retry.")
        sys.exit(1)

    engine = DailySignalEngine()

    if "--scan-now" in sys.argv:
        print("\n🥷 Running immediate STEALTH scan (no Telegram posts)...")
        results = engine.run_scan("Manual Test", stealth_mode=True, force=True)
        for r in results:
            print(f"  {r['index']:12s}  prob={r['prob']:+.4f}  sig={r['signal_type']}  regime={r['regime']}")
        time.sleep(2)
        print("✅ Done.")
        return

    print("\n  Scheduler running. Ctrl+C to stop.\n")
    engine.start()


if __name__ == "__main__":
    main()
