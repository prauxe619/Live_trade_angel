"""
==========================================================================
  FinNifty InstitutionalCollector  v1             
                                                                       
  Higher-Order Greeks  |  IV Surface & Skew  |  GEX Wall Map     
  Volume Intelligence  |  Charm/Speed/Zomma  |  Brain v6 Ready   
                                                                       
  ★ FINNIFTY EDITION — Strike 50 | Lot 25 | FN-specific paths    
==========================================================================

KEY DIFFERENCES vs nifty/banknifty collector
────────────────────────────────────────────────────────────────────
 CONFIG["STRIKE_STEP"]   100 → 50      (FinNifty strikes are ₹50 apart)
 CONFIG["LOT_SIZE"]      15  → 25      (FinNifty lot = 25 shares)
 CONFIG["ATM_RANGE"]     15  → 12      (FN capture scope)
 CONFIG["SPOT_JUMP_PCT"] 3.0%→ 2.5%    (FN volatility profile)
 Instrument master filter  BANKNIFTY  → FINNIFTY
 Snapshot path      banknifty_live_snapshot.json → finnifty_live_snapshot.json
 Parquet paths      data/live/banknifty/options  → data/live/finnifty/options
                    data/live/banknifty/candles  → data/live/finnifty/candles
 Candle symbol tag  "BANKNIFTY"                  → "FINNIFTY"
────────────────────────────────────────────────────────────────────
"""

import os
import sys
import time
import math
import json
import random
import logging
import subprocess
import requests
import pytz
import pyotp
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date
from scipy.stats import norm
from SmartApi import SmartConnect
from revenue_agent.telegram_vip_alert import send_telegram_message
from collectors.scripts.shared_snapshots import publish_snapshot as publish_market_snapshot
from collectors.scripts import market_store
from collectors.scripts.ohlcv_stream import start_index_stream
from collectors.scripts.shared_snapshots import get_recent_candles as get_ohlcv_candles
from collectors.scripts.collector_common import (
    normalise_spot, parse_quote, implied_vol, compute_pcr, AlertThrottle, safe_number)
from collectors.scripts.instrument_master_store import get_instrument_master

# ══════════════════════════════════════════════════════════════════
#  SYSTEM CONFIGURATION  — tune here, touch nothing else
# ══════════════════════════════════════════════════════════════════
def env(key, default=None):
    return os.getenv(key, default)


CONFIG = {
    # Timing
    "TZ"              : pytz.timezone("Asia/Kolkata"),
    "TICK_INTERVAL"   : 60,        # seconds between full chain scans
    "MARKET_OPEN"     : "09:15",
    "MARKET_CLOSE"    : "15:32",

    # ── FinNifty-specific option chain scope ──────────────────────
    "STRIKE_STEP"     : 50,        # FinNifty strikes are every ₹50
    "ATM_RANGE"       : 12,        # ±12 strikes = 25 strikes per expiry
    "NUM_EXPIRIES"    : 3,         # weekly + next 2

    # Finance
    "RISK_FREE_RATE"  : 0.065,     # RBI repo rate proxy
    "LOT_SIZE"        : 25,        # fallback only; instrument master overrides when available

    # Resilience
    "MAX_RETRIES"     : 5,
    "RETRY_DELAY"     : 10,

    # Data Quality
    "MIN_OPTIONS_PER_TICK" : 20,
    "GAMMA_SPIKE_MULT"     : 5.0,
    "PCR_EXTREME_LOW"      : 0.30,
    "PCR_EXTREME_HIGH"     : 3.00,
    "SPOT_JUMP_PCT"        : 0.025,  # 2.5% — FN is less noisy than BN

    # IV Surface
    "IV_SKEW_WINGS"   : 3,         # ±3 strikes from ATM for skew calculation
    "IV_HISTORY_BARS" : 20,        # bars to track IV trend
}

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("finnifty_collector.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("_FinNifty_Collector_V1")


# ══════════════════════════════════════════════════════════════════
#  JSON ENCODER  — handles all numpy / datetime types
# ══════════════════════════════════════════════════════════════════
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def _local_snapshot_enabled() -> bool:
    """Local data/live/*.json snapshot files are OFF by default: EC2 storage is persistent but still requires independent backups and the
    snapshot is already published to PostgreSQL. Set WRITE_LOCAL_SNAPSHOT=true to also write the file."""
    return os.getenv("WRITE_LOCAL_SNAPSHOT", "false").strip().lower() in ("1", "true", "yes")


def is_market_open():
    """Checks if today is a weekend or an official NSE market holiday."""
    today = datetime.now(CONFIG["TZ"])

    if today.weekday() >= 5:
        print(f"🛑 Market Closed: Today is the weekend ({today.strftime('%A')}).")
        return False

    # 2026 NSE Holidays (update this list yearly)
    nse_holidays_2026 = [
        "2026-01-26", # Republic Day
        "2026-03-03", # Maha Shivaratri
        "2026-03-23", # Holi
        "2026-04-03", # Good Friday (Example based on your logs!)
        "2026-04-14", # Dr. Baba Saheb Ambedkar Jayanti
        "2026-05-01", # Maharashtra Day
        "2026-05-28", # Bakri Id
        "2026-06-26", # Muharram
        "2026-08-15", # Independence Day
        "2026-09-14", # Ganesh Chaturthi
        "2026-10-02", # Mahatma Gandhi Jayanti
        "2026-10-20", # Dussehra
        "2026-11-10", # Diwali-Balipratipada
        "2026-11-24", # Gurunanak Jayanti
        "2026-12-25"  # Christmas
    ]

    today_str = today.strftime("%Y-%m-%d")
    if today_str in nse_holidays_2026:
        print(f"🛑 Market Closed: Today is an official NSE Holiday ({today_str}).")
        return False

    return True


# ══════════════════════════════════════════════════════════════════
#  INSTRUMENT MASTER LOADER
# ══════════════════════════════════════════════════════════════════
# ======================================================================
#  INSTRUMENT MASTER LOADER
# ======================================================================
def load_instrument_master() -> pd.DataFrame:
    return get_instrument_master(publisher=False)


# ══════════════════════════════════════════════════════════════════
#  FINNIFTY INSTITUTIONAL COLLECTOR
# ══════════════════════════════════════════════════════════════════
class FinNiftyCollector:
    """
    Mirrors InstitutionalCollector exactly — every method is identical
    except for the instrument-specific wiring in __init__ and start().

    Writes to PostgreSQL (see collectors/scripts/market_store.py):
        option_ticks, index_candles   (per-minute option chain + spot candles)
        market_snapshots              (live snapshot, via shared_snapshots)
    """

    def __init__(self):
        self.api         = None
        self.jwt_token   = None
        self.feed_token  = None
        self.last_spot   = None
        self.last_vix    = None
        self.last_vix_timestamp = None
        self.base_dir    = Path("data/live/finnifty")   # ★ FN-specific sub-folder
        self.today       = datetime.now(CONFIG["TZ"]).strftime("%Y-%m-%d")

        # -- PostgreSQL persistence (replaces the local parquet files) --------
        self.index_key         = "FINNIFTY"
        self.pending_options   : list = []   # rows not yet confirmed in PostgreSQL (retried every tick)
        self.pending_candles   : list = []
        self.option_rows_saved = 0
        self.candle_rows_saved = 0
        self.candle_buffer     : list = []   # last candles kept in memory for the snapshot

        # ── Health & Diagnostics ──────────────────────────────────────────
        self.health_score      = 100
        self.api_failures      = 0
        self.sync_failures     = 0
        self.gamma_history     : list[float] = []
        self.spot_history      : list[float] = []
        self.iv_atm_history    : list[float] = []
        self.gex_history       : list[float] = []
        self.last_health_alert = None
        self.last_hour_report  = None
        self.ticks_collected   = 0
        self._reconnected_today = None
        self.tick_spot = None
        self.ws_spot = None
        self._throttle = AlertThrottle(cooldown=1800)

        # ── GEX Wall Map (persisted across ticks) ─────────────────────────
        self.gex_wall_map : dict[float, float] = {}

        # ── New: Delta OI tracking, Realized Vol ─────────────────────────
        self.prev_oi         : dict = {}   # (sym, exp, strike, type) → last tick OI
        self.prev_ltp        : dict = {}   # same key → last tick LTP
        self.prev_volume     : dict = {}
        self.prev_volume_monotonic: dict = {}   # same key → last tick volume
        self.returns_history : list[float] = []  # log-returns for realized vol
        self.last_spot = None
        self.tick_spot = None
        self.ws_spot = None
        self.last_ws_tick_at = None
        self.last_rest_spot_at = None

        self._prepare_environment()
        self._restore_from_db()

    # ── Setup ─────────────────────────────────────────────────────────────

    def _prepare_environment(self):
        Path("data/live").mkdir(parents=True, exist_ok=True)
        import atexit
        atexit.register(lambda: getattr(self, "ohlcv_stream", None) and self.ohlcv_stream.stop())
        # Last-chance flush if the process exits (market close, crash, or SIGTERM during a service restart or shutdown).
        import atexit, signal
        atexit.register(self._flush_before_exit)
        try:
            signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        except Exception:
            pass

    def _flush_before_exit(self):
        """Retry queued rows for up to ~1 minute so a short DB outage at shutdown doesn't lose data."""
        for _ in range(6):
            if not (self.pending_options or self.pending_candles):
                return
            self.sync_to_db()
            if self.pending_options or self.pending_candles:
                time.sleep(10)

    def _restore_from_db(self):
        """Resume today's session from PostgreSQL after a restart or redeploy."""
        try:
            market_store.init_schema()
            purged = market_store.purge_old()
            if purged:
                logger.info(f"[FN] Retention purge removed {purged} old option rows.")
            self.option_rows_saved = market_store.count_option_rows(self.index_key, self.today)
            self.candle_rows_saved = market_store.count_candles(self.index_key, self.today)
            self.candle_buffer     = market_store.load_recent_candles(self.index_key, self.today, limit=240)
            for state_row in market_store.load_latest_option_state(self.index_key, self.today):
                state_key = (state_row["symbol"], state_row["expiry"], state_row["strike"], state_row["type"])
                self.prev_oi[state_key] = state_row["oi"]
                self.prev_ltp[state_key] = state_row["ltp"]
                self.prev_volume[state_key] = state_row["volume"]
            logger.info(
                f"[FN] Restored from PostgreSQL: {self.option_rows_saved} option rows, "
                f"{self.candle_rows_saved} candles for {self.today}."
            )
        except Exception as e:
            logger.error(f"[FN] PostgreSQL unavailable at startup ({e}). Collecting anyway; rows are queued and retried each tick.")
            send_telegram_message(f"🚨 FinNifty collector cannot reach PostgreSQL at startup: {e}")

    def _option_rows(self) -> int:
        return self.option_rows_saved + len(self.pending_options)

    def _candle_rows(self) -> int:
        return self.candle_rows_saved + len(self.pending_candles)

    # ── Authentication ─────────────────────────────────────────────────────

    def connect(self) -> bool:
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                logger.info(f"Connecting to Angel One (attempt {attempt+1})...")
                try:
                    token_script = Path(__file__).resolve().parents[1] / "scripts" / "auto_refresh_tokens.py"
                    subprocess.run(
                        [sys.executable, str(token_script), "--account", "C"],
                        check=True, timeout=30, cwd=str(Path(__file__).resolve().parents[1])
                    )
                    logger.info("Tokens refreshed.")
                except Exception as e:
                    logger.warning(f"Token refresh failed: {e}. Attempting direct login.")

                self.api = SmartConnect(api_key=env("ANGEL_API_KEY_C"))
                totp     = pyotp.TOTP(env("ANGEL_TOTP_C")).now()
                session  = self.api.generateSession(
                    env("ANGEL_CLIENT_ID_C"),
                    env("ANGEL_PASSWORD_C"),
                    totp
                )
                if session.get("status"):
                    self.jwt_token = (session.get("data") or {}).get("jwtToken")
                    self.feed_token = self.api.getfeedToken()
                    logger.info("Angel One session secured.")
                    return True
                else:
                    logger.error(f"Session failed: {session.get('message')}")

                time.sleep(CONFIG["RETRY_DELAY"])

            except Exception as e:
                logger.error(f"Connection error attempt {attempt+1}: {e}")
                time.sleep(CONFIG["RETRY_DELAY"])

        send_telegram_message("🚨 FATAL: FinNifty Collector failed to connect after all retries.")
        return False

    # ── Safe Data Accessor ─────────────────────────────────────────────────

    @staticmethod
    def _safe_val(data, keys, default=0):
        if not isinstance(data, dict):
            return default
        if not isinstance(keys, list):
            keys = [keys]
        for k in keys:
            if k in data:
                return data[k]
        return default

    def _call(self, fn, *args, retries=2, **kw):
        last = None
        for i in range(retries + 1):
            try:
                res = fn(*args, **kw)
                if isinstance(res, dict):
                    return res
            except Exception as e:
                last = e
            time.sleep(1.0 * (i + 1) + random.random())
        raise RuntimeError(f"Angel call failed after retries: {last}")

    @staticmethod
    def _depth_price(data, side):
        keys = ("bidPrice", "bestBid", "bid") if side == "buy" else ("askPrice", "bestAsk", "ask")
        value = FinNiftyCollector._safe_val(data, list(keys), 0)
        try:
            value = float(value or 0)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
        depth = data.get("depth", {}) if isinstance(data, dict) else {}
        levels = depth.get(side, []) if isinstance(depth, dict) else []
        if levels and isinstance(levels[0], dict):
            try:
                return float(levels[0].get("price", 0) or 0)
            except (TypeError, ValueError):
                pass
        return 0.0

    @staticmethod
    def _depth_qty(data, side):
        keys = ("totBuyQuan", "totalBuyQty", "buyQty", "bestBidQty") if side == "buy" else ("totSellQuan", "totalSellQty", "sellQty", "bestAskQty")
        value = FinNiftyCollector._safe_val(data, list(keys), 0)
        try:
            value = float(value or 0)
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
        depth = data.get("depth", {}) if isinstance(data, dict) else {}
        levels = depth.get(side, []) if isinstance(depth, dict) else []
        try:
            return float(sum(float(level.get("quantity", 0) or 0) for level in levels if isinstance(level, dict)))
        except (TypeError, ValueError):
            return 0.0

    def estimate_implied_volatility(self, spot, strike, dte, option_price, otype):
        """Estimate annualized IV from option price using a Black-Scholes bisection solver."""
        try:
            spot, strike, price = float(spot), float(strike), float(option_price)
            if not all(math.isfinite(x) for x in (spot, strike, price)) or min(spot, strike, price) <= 0:
                return 0.0
            t = max(float(dte), 0.5) / 365.0
            r = CONFIG["RISK_FREE_RATE"]
            is_call = otype == "CE"
            intrinsic = max(spot - strike, 0.0) if is_call else max(strike - spot, 0.0)
            if price + 1e-8 < intrinsic:
                return 0.0

            def bs_price(vol):
                st = vol * math.sqrt(t)
                d1 = (math.log(spot / strike) + (r + 0.5 * vol * vol) * t) / st
                d2 = d1 - st
                if is_call:
                    return spot * norm.cdf(d1) - strike * math.exp(-r * t) * norm.cdf(d2)
                return strike * math.exp(-r * t) * norm.cdf(-d2) - spot * norm.cdf(-d1)

            lo, hi = 0.01, 5.0
            if price > bs_price(hi) + 1e-6:
                return 0.0
            for _ in range(80):
                mid = (lo + hi) / 2
                if bs_price(mid) < price:
                    lo = mid
                else:
                    hi = mid
            return (lo + hi) / 2
        except (ValueError, TypeError, OverflowError, ZeroDivisionError):
            return 0.0

    # ══════════════════════════════════════════════════════════════
    #  WORLD-CLASS GREEKS ENGINE 
    # ══════════════════════════════════════════════════════════════
    def get_greeks(self, spot: float, strike: float, dte: float,
                   iv: float, otype: str, oi: float, volume: float = 0
                   ) -> dict:
        """
        Full institutional Greek suite — 14 metrics per contract.

        Standard  : delta, gamma, vega, theta
        2nd Order : vanna, charm, vomma, speed
        3rd Order : zomma, color
        Flow      : gex, vanna_exposure, charm_exposure, iv_used

        NOTE: CONFIG["LOT_SIZE"] is dynamically loaded for FinNifty, 
        so GEX and exposure figures are already correctly scaled.
        """
        ZERO = {
            "delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0,
            "vanna": 0.0, "charm": 0.0, "vomma": 0.0, "speed": 0.0,
            "zomma": 0.0, "color": 0.0,
            "gex": 0.0, "vanna_exposure": 0.0, "charm_exposure": 0.0,
            "iv_used": float(iv),
        }

        try:
            if not all(math.isfinite(float(v)) for v in (spot, strike, dte, iv, oi)) or spot <= 0 or strike <= 0 or iv <= 0:
                return ZERO
        except (TypeError, ValueError):
            return ZERO

        t = max(dte, 0.5) / 365.0
        r = CONFIG["RISK_FREE_RATE"]

        try:
            sqt  = math.sqrt(t)
            log_moneyness = math.log(spot / strike)

            d1 = (log_moneyness + (r + 0.5 * iv ** 2) * t) / (iv * sqt)
            d2 = d1 - iv * sqt

            Nd1  = norm.cdf(d1)
            Nd1_ = norm.cdf(-d1)
            Nd2  = norm.cdf(d2)
            Nd2_ = norm.cdf(-d2)
            nd1  = norm.pdf(d1)

            # ── Standard Greeks ───────────────────────────────────────────
            gamma = nd1 / (spot * iv * sqt)
            vega  = spot * nd1 * sqt / 100.0

            if otype == "CE":
                delta = Nd1
                theta = (
                    -(spot * nd1 * iv) / (2 * sqt)
                    - r * strike * math.exp(-r * t) * Nd2
                ) / 365.0
                gex   = gamma * oi * (spot ** 2) * 0.01 * CONFIG["LOT_SIZE"]
            else:
                delta = Nd1 - 1.0
                theta = (
                    -(spot * nd1 * iv) / (2 * sqt)
                    + r * strike * math.exp(-r * t) * Nd2_
                ) / 365.0
                gex   = -gamma * oi * (spot ** 2) * 0.01 * CONFIG["LOT_SIZE"]

            # ── 2nd-Order Greeks ──────────────────────────────────────────
            vanna = (vega * 100 / spot) * (1.0 - d1 / (iv * sqt))

            charm = -nd1 * (
                (r / (iv * sqt)) - (d2 / (2 * t))
            ) / 365.0

            vomma = vega * 100 * (d1 * d2) / iv
            speed = -(gamma / spot) * (1.0 + d1 / (iv * sqt))

            # ── 3rd-Order Greeks ──────────────────────────────────────────
            zomma = gamma * (d1 * d2 - 1.0) / iv

            color = (
                gamma * (
                    (1.0 - d1 * d1) / (2.0 * t)
                    + d1 * (r + 0.5 * iv * iv) / (iv * sqt)
                )
            ) / 365.0

            # ── Exposure Metrics ──────────────────────────────────────────
            vanna_exposure = vanna * oi * CONFIG["LOT_SIZE"]
            charm_exposure = charm * oi * CONFIG["LOT_SIZE"]

            return {
                "delta"          : round(delta,  4),
                "gamma"          : round(gamma,  7),
                "vega"           : round(vega,   4),
                "theta"          : round(theta,  4),
                "vanna"          : round(vanna,  4),
                "charm"          : round(charm,  6),
                "vomma"          : round(vomma,  4),
                "speed"          : round(speed,  7),
                "zomma"          : round(zomma,  6),
                "color"          : round(color,  7),
                "gex"            : round(gex,    2),
                "vanna_exposure" : round(vanna_exposure, 2),
                "charm_exposure" : round(charm_exposure, 2),
                "iv_used"        : round(iv,     4),
            }

        except Exception as e:
            logger.warning(f"Greek calculation error: {e}")
            return ZERO

    # ══════════════════════════════════════════════════════════════
    #  IV SURFACE ENGINE
    # ══════════════════════════════════════════════════════════════
    def compute_iv_surface(self, batch: list, atm_strike: float) -> dict:
        surface = {
            "atm_iv"          : 0.0,
            "iv_skew_call"    : 0.0,
            "iv_skew_put"     : 0.0,
            "put_call_iv_gap" : 0.0,
            "iv_curvature"    : 0.0,
            "iv_term_spread"  : 0.0,
            "iv_trend"        : 0.0,
        }

        if not batch:
            return surface

        try:
            df = pd.DataFrame(batch)
            if "iv_valid" in df.columns:
                df = df[df["iv_valid"] == True]
            df = df[pd.to_numeric(df.get("iv_used", 0), errors="coerce") > 0] if not df.empty else df
            if df.empty:
                return surface
            skew_wing = CONFIG["IV_SKEW_WINGS"] * CONFIG["STRIKE_STEP"]

            atm_ce = df[(df['strike'] == atm_strike) & (df['type'] == 'CE')]
            atm_pe = df[(df['strike'] == atm_strike) & (df['type'] == 'PE')]

            atm_ce_iv = float(atm_ce['iv_used'].mean()) if not atm_ce.empty else 0.0
            atm_pe_iv = float(atm_pe['iv_used'].mean()) if not atm_pe.empty else 0.0
            valid_atm_ivs = [v for v in (atm_ce_iv, atm_pe_iv) if v > 0]
            surface['atm_iv'] = round(float(np.mean(valid_atm_ivs)), 4) if valid_atm_ivs else 0.0
            surface['put_call_iv_gap'] = round(atm_pe_iv - atm_ce_iv, 4) if atm_ce_iv > 0 and atm_pe_iv > 0 else 0.0

            otm_ce = df[(df['strike'] == atm_strike + skew_wing) & (df['type'] == 'CE')]
            otm_pe = df[(df['strike'] == atm_strike - skew_wing) & (df['type'] == 'PE')]

            otm_ce_iv = float(otm_ce["iv_used"].mean()) if not otm_ce.empty else None
            otm_pe_iv = float(otm_pe["iv_used"].mean()) if not otm_pe.empty else None

            surface["iv_skew_call"] = (
                round(otm_ce_iv - surface["atm_iv"], 4)
                if otm_ce_iv is not None and surface["atm_iv"] > 0
                else None
            )
            surface["iv_skew_put"] = (
                round(otm_pe_iv - surface["atm_iv"], 4)
                if otm_pe_iv is not None and surface["atm_iv"] > 0
                else None
            )
            surface["iv_curvature"] = (
                round(((otm_ce_iv + otm_pe_iv) / 2) - surface["atm_iv"], 4)
                if (otm_ce_iv is not None and otm_pe_iv is not None and surface["atm_iv"] > 0)
                else None
            )

            if 'expiry' in df.columns:
                expiries = sorted(df['expiry'].unique())
                if len(expiries) >= 2:
                    near_iv = df[df['expiry'] == expiries[0]]['iv_used'].mean()
                    far_iv  = df[df['expiry'] == expiries[1]]['iv_used'].mean()
                    surface['iv_term_spread'] = round(
                        float(near_iv) - float(far_iv), 4
                    )

            self.iv_atm_history.append(surface['atm_iv'])
            if len(self.iv_atm_history) > CONFIG["IV_HISTORY_BARS"]:
                self.iv_atm_history = self.iv_atm_history[-CONFIG["IV_HISTORY_BARS"]:]
            if len(self.iv_atm_history) >= 3:
                surface['iv_trend'] = round(
                    surface['atm_iv'] - np.mean(self.iv_atm_history[:-1]), 4
                )

        except Exception as e:
            logger.warning(f"IV surface computation error: {e}")

        return surface

    # ══════════════════════════════════════════════════════════════
    #  GEX WALL MAP ENGINE
    # ══════════════════════════════════════════════════════════════
    def update_gex_wall_map(self, batch: list) -> dict:
        result = {
            "gex_walls"      : {},
            "gex_flip_point" : 0.0,
            "pin_strike"     : 0.0,
            "net_gex_total"  : 0.0,
            "gex_profile"    : [],
        }

        if not batch:
            return result

        try:
            current_map = {}
            for row in batch:
                s = float(row.get('strike', 0) or 0)
                gex = float(row.get('gex', 0) or 0)
                if s > 0 and math.isfinite(gex):
                    current_map[s] = current_map.get(s, 0.0) + gex
            self.gex_wall_map = current_map

            strikes_sorted = sorted(self.gex_wall_map.keys())
            gex_vals       = [self.gex_wall_map[s] for s in strikes_sorted]

            if not gex_vals:
                return result

            net_gex_total = sum(gex_vals)
            result['net_gex_total'] = round(net_gex_total, 2)

            abs_gex  = [abs(g) for g in gex_vals]
            pin_idx  = abs_gex.index(max(abs_gex))
            result['pin_strike'] = strikes_sorted[pin_idx]

            flip_strike = 0.0
            if self.last_spot:
                for i in range(len(gex_vals) - 1):
                    if gex_vals[i] * gex_vals[i+1] < 0:
                        s1, s2 = strikes_sorted[i], strikes_sorted[i+1]
                        flip_strike = s1 if abs(s1 - self.last_spot) < abs(s2 - self.last_spot) else s2
            result['gex_flip_point'] = flip_strike

            sorted_by_abs = sorted(
                zip(strikes_sorted, gex_vals),
                key=lambda x: abs(x[1]),
                reverse=True
            )[:5]
            result['gex_walls'] = {str(s): round(g, 2) for s, g in sorted_by_abs}

            result['gex_profile'] = [
                {"strike": s, "gex": round(g, 2)}
                for s, g in zip(strikes_sorted, gex_vals)
            ]

        except Exception as e:
            logger.warning(f"GEX wall map error: {e}")

        return result

    # ══════════════════════════════════════════════════════════════
    #  AGGREGATE FLOW METRICS
    # ══════════════════════════════════════════════════════════════
    def compute_aggregate_metrics(self, batch: list) -> dict:
        if not batch:
            return {}

        ce = [r for r in batch if r.get('type') == 'CE']
        pe = [r for r in batch if r.get('type') == 'PE']

        def safe_values(rows, key):
            values = []
            for row in rows:
                try:
                    value = float(row.get(key, 0) or 0)
                except (TypeError, ValueError, OverflowError):
                    continue
                if math.isfinite(value):
                    values.append(value)
            return values

        def safe_sum(rows, key):
            return sum(safe_values(rows, key))

        def safe_mean(rows, key):
            values = safe_values(rows, key)
            return float(np.mean(values)) if values else 0.0

        # ── OI & Volume ──────────────────────────────────────────────────
        call_oi    = safe_sum(ce, 'oi')
        put_oi     = safe_sum(pe, 'oi')
        total_oi   = call_oi + put_oi
        call_vol   = safe_sum(ce, 'volume')
        put_vol    = safe_sum(pe, 'volume')
        total_vol  = call_vol + put_vol

        pcr_oi     = put_oi  / (call_oi  + 1e-9)
        pcr_vol    = put_vol / (call_vol + 1e-9)
        pcr_gamma  = safe_sum(pe, 'gamma') / (safe_sum(ce, 'gamma') + 1e-9)

        # ── Standard Flow ────────────────────────────────────────────────
        raw_net_gex  = safe_sum(batch, 'gex')
        scaled_gex   = float(np.sign(raw_net_gex) * np.log1p(abs(raw_net_gex)))
        call_gamma   = safe_sum(ce, 'gamma') * 100
        put_gamma    = safe_sum(pe, 'gamma') * 100
        avg_vanna    = safe_mean(batch, 'vanna')
        avg_theta    = safe_mean(batch, 'theta')

        # ── NEW: Average Microstructure Metrics ──
        avg_obi        = safe_mean(batch, 'obi')
        avg_tape_vel   = safe_mean(batch, 'tape_velocity')
        avg_vpin_proxy = safe_mean(batch, 'vpin_proxy')

        # ── Higher-Order Exposure Totals ─────────────────────────────────
        total_charm_exposure = safe_sum(batch, 'charm_exposure')
        call_charm           = safe_sum(ce, 'charm_exposure')
        put_charm            = safe_sum(pe, 'charm_exposure')
        total_vanna_exposure = safe_sum(batch, 'vanna_exposure')
        avg_vomma = safe_mean(batch, 'vomma')
        avg_zomma = safe_mean(batch, 'zomma')

        # ── OTM Pressure Signals ─────────────────────────────────────────
        atm = round(self.last_spot / CONFIG["STRIKE_STEP"]) * CONFIG["STRIKE_STEP"] if self.last_spot else 0
        otm_range = 3 * CONFIG["STRIKE_STEP"]

        deep_otm_pe = safe_sum(
            [r for r in pe if safe_number(r.get("strike"), 0) > 0 and safe_number(r.get("strike"), 0) <= atm - otm_range], 'oi'
        )
        deep_otm_ce = safe_sum(
            [r for r in ce if safe_number(r.get("strike"), 0) > 0 and safe_number(r.get("strike"), 0) >= atm + otm_range], 'oi'
        )
        otm_hedge_ratio = deep_otm_pe / (deep_otm_ce + 1e-9)

        # ── Flow Imbalance ────────────────────────────────────────────────
        flow_imbalance = (call_vol - put_vol) / (total_vol + 1e-9)

        # ── GEX History ──────────────────────────────────────────────────
        self.gex_history.append(raw_net_gex)
        if len(self.gex_history) > 20:
            self.gex_history = self.gex_history[-20:]

        return {
            "call_gamma"    : round(call_gamma, 4),
            "put_gamma"     : round(put_gamma,  4),
            "avg_vanna"     : round(avg_vanna,  4),
            "avg_theta"     : round(avg_theta,  4),
            "net_gex"       : round(scaled_gex, 4),
            "raw_net_gex"   : round(raw_net_gex, 2),

            "call_oi"       : round(call_oi,    0),
            "put_oi"        : round(put_oi,     0),
            "total_oi"      : round(total_oi,   0),
            "call_vol"      : round(call_vol,   0),
            "put_vol"       : round(put_vol,    0),

            "pcr_oi"        : round(pcr_oi,     4),
            "pcr_vol"       : round(pcr_vol,    4),
            "pcr_gamma"     : round(pcr_gamma,  4),

            "total_charm_exposure"  : round(total_charm_exposure, 2),
            "call_charm_exposure"   : round(call_charm,            2),
            "put_charm_exposure"    : round(put_charm,             2),
            "total_vanna_exposure"  : round(total_vanna_exposure,  2),
            "avg_vomma"             : round(avg_vomma,             4),
            "avg_zomma"             : round(avg_zomma,             4),

            "otm_hedge_ratio"  : round(otm_hedge_ratio, 4),
            "flow_imbalance"   : round(flow_imbalance,  4),

            # ── NEW: Export to Snapshot ──
            "avg_obi"          : round(avg_obi, 4),
            "avg_tape_vel"     : round(avg_tape_vel, 2),
            "avg_vpin_proxy"   : round(avg_vpin_proxy, 4),
        }
    
    # ══════════════════════════════════════════════════════════════
    #  MAX PAIN ENGINE
    # ══════════════════════════════════════════════════════════════
    def compute_max_pain(self, batch: list) -> float:
        if not batch:
            return 0.0
        try:
            df      = pd.DataFrame(batch)
            strikes = sorted(df['strike'].unique())
            pain    = {}
            for test_strike in strikes:
                ce_pain = ((df[df['type']=='CE']['strike'] - test_strike)
                        .clip(lower=0) * df[df['type']=='CE']['oi']).sum()
                pe_pain = ((test_strike - df[df['type']=='PE']['strike'])
                        .clip(lower=0) * df[df['type']=='PE']['oi']).sum()
                pain[test_strike] = ce_pain + pe_pain
            return float(min(pain, key=pain.get))
        except Exception as e:
            logger.warning(f"Max pain error: {e}")
            return 0.0

    # ══════════════════════════════════════════════════════════════
    #  REGIME CLASSIFIER  (FinNifty thresholds)
    # ══════════════════════════════════════════════════════════════
    def classify_regime(self, aggregates: dict, iv_surface: dict,
                        gex_walls: dict, futures_basis: float,
                        iv_rv_spread: float, vix_fresh: bool) -> str:
        if not vix_fresh or iv_rv_spread is None or futures_basis is None:
          return "DATA_INCOMPLETE"
        net_gex  = aggregates.get('raw_net_gex', 0)
        pcr      = aggregates.get('pcr_oi', 1.0)
        atm_iv   = iv_surface.get('atm_iv', 0.15)
        pin      = gex_walls.get('pin_strike', 0)
        spot     = self.last_spot or 1

        near_pin = (abs(spot - pin) / spot < 0.003) if pin else False
        iv_rich  = iv_rv_spread > 0.03
        gex_high = net_gex > 5_000_000  

        if gex_high and near_pin and iv_rich:
            return "PINNING"
        if atm_iv < 0.10 and not gex_high and len(self.spot_history) > 5:
            rng = max(self.spot_history[-5:]) - min(self.spot_history[-5:])
            if rng / spot < 0.004:
                return "SQUEEZE"
        if iv_rv_spread < -0.02:
            return "VOLATILE"
        if futures_basis > 30 and pcr < 0.8:   
            return "TRENDING_BULL"
        if futures_basis < -20 and pcr > 1.5:
            return "TRENDING_BEAR"
        return "NEUTRAL"

    # ══════════════════════════════════════════════════════════════
    #  LIVE SNAPSHOT BUILDER
    # ══════════════════════════════════════════════════════════════
    def save_live_snapshot(self, batch: list, aggregates: dict,
                           iv_surface: dict, gex_walls: dict,
                           futures_basis: float = 0.0,
                           futures_basis_pct: float = 0.0,
                           futures_basis_valid: bool = False,
                           realized_vol: float = 0.0,
                           iv_rv_spread: float = 0.0,
                           vix_fresh: bool = False,
                           max_pain: float = 0.0,
                           regime: str = "NEUTRAL"):
        """
        Writes data/live/finnifty_live_snapshot.json every tick.
        """
        try:
            # 🔥 NON-DESTRUCTIVE CANDLE INJECTION
            # Only closed, genuine index-spot WebSocket OHLC bars are exposed as candles.
            # One-per-minute REST/spot samples remain separate and are never relabelled as OHLC.
            try:
                formatted_candles = get_ohlcv_candles(
                    self.index_key,
                    limit=120,
                    max_age_seconds=180
                )
            except Exception as candle_exc:
                logger.warning("%s OHLCV read failed: %s", self.index_key, candle_exc)
                formatted_candles = []
            spot_samples = list(self.candle_buffer[-120:])
            latest_candle_age_s = (
                float(formatted_candles[-1].get("age_seconds", float("inf")))
                if formatted_candles else None
            )
            # Strict candle quality gate
            def validate_candle_quality(candles):
                if not candles or len(candles) < 6:
                    return False, "INSUFFICIENT_CANDLES"
            
                timestamps = []
            
                for candle in candles:
                    try:
                        if candle.get("is_complete") is not True:
                            return False, "INCOMPLETE_CANDLE"
            
                        if candle.get("quality_status") != "VALID":
                            return False, "INVALID_CANDLE_QUALITY"
            
                        tick_count = int(candle.get("tick_count") or 0)
            
                        if tick_count < 3:
                            return False, "LOW_TICK_COUNT"
            
                        o = float(candle["open"])
                        h = float(candle["high"])
                        l = float(candle["low"])
                        c = float(candle["close"])
            
                        if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                            return False, "INVALID_OHLC"
            
                        if h < max(o, c, l):
                            return False, "INVALID_HIGH"
            
                        if l > min(o, c, h):
                            return False, "INVALID_LOW"
            
                        timestamps.append(str(candle["timestamp"]))
            
                    except (TypeError, ValueError, KeyError, OverflowError):
                        return False, "MALFORMED_CANDLE"
            
                if len(timestamps) != len(set(timestamps)):
                    return False, "DUPLICATE_CANDLE_TIMESTAMP"
            
                return True, "VALID"
            
            
            candles_valid, candle_quality_status = validate_candle_quality(
                formatted_candles
            )
            
            candles_fresh = bool(
                candles_valid
                and latest_candle_age_s is not None
                and 0 <= latest_candle_age_s <= 180
            )

            snapshot = {
                "timestamp"    : datetime.now(CONFIG["TZ"]).strftime("%H:%M:%S"),
                "symbol"       : "FINNIFTY",           # ★ agent can distinguish
                "spot"         : float(self.last_spot) if self.last_spot else 0.0,
                "vix"          : self.last_vix,
                "vix_valid"    : self.last_vix is not None,
                "vix_fresh"    : vix_fresh,
                "ticks_today"  : self.ticks_collected,
                "health"       : self.health_score,
                "lot_size"     : CONFIG["LOT_SIZE"],
                "candle_quality": "angelone_websocket_ohlc" if candles_fresh else "unavailable",
                "latest_candle_age_s": latest_candle_age_s,
                "candles_fresh": candles_fresh,
                "iv_method"    : "black_scholes_bisection_from_option_price",
                "gex_assumption": "call_positive_put_negative_proxy_not_verified_dealer_positions",
                **aggregates,
                "market_pcr"   : aggregates.get("pcr_oi", 1.0),
                "pcr"          : aggregates.get("pcr_oi", 1.0),
                "iv_surface"   : iv_surface,
                "gex_walls"    : gex_walls,
                "data"         : batch,
                "candles"      : formatted_candles,
                "spot_samples" : spot_samples,
                "futures_basis": futures_basis,
                "futures_basis_pct": futures_basis_pct,
                "futures_basis_valid": futures_basis_valid,
                "realized_vol"      : realized_vol,
                "iv_rv_spread"      : iv_rv_spread,
                "max_pain"          : max_pain,
                "regime"            : regime,
                "candles_fresh": candles_fresh,
  
                "candle_quality": (
                    "angelone_websocket_ohlc"
                    if candles_fresh
                    else "unavailable"
                ),
                
                "candle_quality_status": candle_quality_status,
                
                "candle_count": len(formatted_candles),
                
                "latest_candle_age_s": latest_candle_age_s,
                
                "trading_data_eligible": candles_fresh,
                 }

            # Publish to shared PostgreSQL; preserve local JSON output on DB failure.
            _snapshot_published = True
            try:
                publish_market_snapshot("FINNIFTY", snapshot)
            except Exception as db_exc:
                _snapshot_published = False
                logger.error("PostgreSQL snapshot publish failed for FINNIFTY: %s", db_exc)

            if _local_snapshot_enabled() or not _snapshot_published:
                tmp_path  = Path("data/live/finnifty_live_snapshot.tmp")
                snap_path = Path("data/live/finnifty_live_snapshot.json")

                with open(tmp_path, "w") as f:
                    json.dump(snapshot, f, cls=DateTimeEncoder)

                for _ in range(5):
                    try:
                        tmp_path.replace(snap_path)
                        break
                    except PermissionError:
                        time.sleep(0.05)

        except Exception as e:
            logger.error(f"Critical error saving FinNifty snapshot: {e}")

    # ══════════════════════════════════════════════════════════════
    #  HEALTH & ANOMALY ENGINE
    # ══════════════════════════════════════════════════════════════
    def evaluate_health(self, batch: list, aggregates: dict, iv_surface: dict | None = None):
        score  = 100
        alerts = []

        score -= min(self.api_failures * 5, 25)

        if not self.last_spot or self.last_spot <= 0:
            score -= 15
            alerts.append("⚠️ [FN] Invalid spot price")

        if not batch:
            score -= 20
            alerts.append("⚠️ [FN] No option rows collected")
        elif len(batch) < CONFIG["MIN_OPTIONS_PER_TICK"]:
            score -= 10
            alerts.append(f"⚠️ [FN] Only {len(batch)} rows (expected ≥{CONFIG['MIN_OPTIONS_PER_TICK']})")

        if batch:
            avg_gamma = np.mean([abs(r.get('gamma', 0)) for r in batch])
            self.gamma_history.append(float(avg_gamma))
            if len(self.gamma_history) > 10:
                self.gamma_history = self.gamma_history[-10:]
                rolling_mean = np.mean(self.gamma_history[:-1])
                if rolling_mean > 0 and avg_gamma > rolling_mean * CONFIG["GAMMA_SPIKE_MULT"]:
                    score -= 10
                    alerts.append(f"🚨 [FN] Gamma explosion: {avg_gamma:.6f} vs mean {rolling_mean:.6f}")

        if batch:
            quote_eligible = sum(bool(r.get("quote_entry_eligible")) for r in batch)
            iv_eligible = sum(bool(r.get("iv_valid")) for r in batch)
            quote_pct = quote_eligible / max(1, len(batch))
            iv_pct = iv_eligible / max(1, len(batch))
            if quote_pct < 0.50:
                score -= 15
                alerts.append(f"[{self.index_key}] Low eligible quote coverage: {quote_pct:.1%}")
            elif quote_pct < 0.75:
                score -= 7
                alerts.append(f"[{self.index_key}] Reduced eligible quote coverage: {quote_pct:.1%}")
            if iv_pct < 0.40:
                score -= 10

        pcr = aggregates.get("pcr_oi", 1.0)
        if pcr > CONFIG["PCR_EXTREME_HIGH"]:
            score -= 10
            alerts.append(f"[FN] Extreme PCR HIGH: {pcr:.2f}")
        elif pcr < CONFIG["PCR_EXTREME_LOW"]:
            score -= 10
            alerts.append(f"[FN] Extreme PCR LOW: {pcr:.2f}")

        if len(self.spot_history) >= 2:
            prev = self.spot_history[-2]
            curr = self.spot_history[-1]
            if prev > 0:
                jump = abs(curr - prev) / prev
                if jump > CONFIG["SPOT_JUMP_PCT"]:
                    alerts.append(f"🚨 [FN] Spot jump {jump*100:.2f}%: {prev:.1f} → {curr:.1f}")

        iv_surface = iv_surface if isinstance(iv_surface, dict) else {}
        score -= min(self.sync_failures * 5, 10)

        self.health_score = max(score, 0)
        logger.info(
            f"[FN] HEALTH: {self.health_score}/100 | "
            f"Rows: {len(batch)} | GEX: {aggregates.get('raw_net_gex', 0):.0f}"
        )

        for alert in alerts:
            logger.warning(alert)
            send_telegram_message(alert)

        if self.health_score < 60:
            msg = f"🚨 [FINNIFTY] CRITICAL HEALTH: {self.health_score}/100 — continuing collection; data quality is degraded."
            logger.error(msg)
            send_telegram_message(msg)

    # ── Disk Persistence ──────────────────────────────────────────────────

    def sync_to_db(self):
        """Write rows not yet stored to PostgreSQL. Idempotent; on failure rows stay queued for the next tick."""
        failed = False
        try:
            if self.pending_options:
                queued = len(self.pending_options)
                inserted = market_store.insert_option_rows(self.index_key, self.pending_options)
                self.option_rows_saved += int(inserted or 0)
                logger.info("[%s] Options persisted: attempted=%d inserted=%d duplicates_or_skipped=%d",
                            self.index_key, queued, int(inserted or 0),
                            max(0, queued - int(inserted or 0)))
                self.pending_options = []
        except Exception as e:
            failed = True
            logger.error(f"[FN] DB SAVE ERROR (options, {len(self.pending_options)} queued): {e}")
        try:
            if self.pending_candles:
                queued = len(self.pending_candles)
                inserted = market_store.insert_candles(self.index_key, self.pending_candles)
                self.candle_rows_saved += int(inserted or 0)
                logger.info("[%s] Feature rows persisted: attempted=%d inserted=%d duplicates_or_skipped=%d",
                            self.index_key, queued, int(inserted or 0),
                            max(0, queued - int(inserted or 0)))
                self.pending_candles = []
        except Exception as e:
            failed = True
            logger.error(f"[FN] DB SAVE ERROR (candles, {len(self.pending_candles)} queued): {e}")

        if failed:
            self.sync_failures += 1
            max_pending = 30000                      # roughly 2.5 hours of ticks
            if len(self.pending_options) > max_pending:
                logger.critical(
                    "[%s] PostgreSQL backlog exceeded %d rows; retaining all queued rows to avoid silent data loss.",
                    self.index_key, max_pending
                )
                if self.sync_failures == 3:
                    send_telegram_message(
                        f"🚨 {self.index_key}: PostgreSQL backlog exceeded {max_pending} rows; queue retained."
                    )
            if self.sync_failures == 3:
                send_telegram_message(f"🚨 FinNifty: PostgreSQL writes failing ({self.sync_failures} ticks in a row). Rows are queued.")
        else:
            self.sync_failures = 0
            logger.info(
                f"[FN] DB SYNC {datetime.now(CONFIG['TZ']).strftime('%H:%M:%S')} "
                f"| Options today: {self._option_rows()} rows"
            )

    # ══════════════════════════════════════════════════════════════
    #  MAIN COLLECTION LOOP
    # ══════════════════════════════════════════════════════════════
    def start(self):
        if not self.connect():
            return

        master = load_instrument_master()

        # ── FinNifty spot token ────────────────────────────────────────
        fn_spot_row = master.query("name=='FINNIFTY' and exch_seg=='NSE'")
        if fn_spot_row.empty:
            # Fallback if Angel One uses a different name in NSE
            fn_spot_row = master.query("name=='NIFTY FIN SERVICE' and exch_seg=='NSE'")
        if fn_spot_row.empty:
            logger.error("[FN] FINNIFTY spot token not found in instrument master.")
            return
        finnifty_token = str(fn_spot_row.iloc[0]['token'])
        self.ohlcv_stream = start_index_stream(self, finnifty_token, 1, env("ANGEL_API_KEY_C"), env("ANGEL_CLIENT_ID_C"))


        # ── India VIX token (same as Nifty collector) ──────────────────
        vix_rows = master.query("name=='INDIA VIX' and exch_seg=='NSE'")
        vix_token = str(vix_rows.iloc[0]['token']) if not vix_rows.empty else None
        if not vix_token:
            logger.warning("[FN] INDIA VIX token missing; using last valid/default VIX.")

        fut_rows = master.query("name=='FINNIFTY' and exch_seg=='NFO' and instrumenttype=='FUTIDX'").copy()
        fn_fut_expiry = None
        if fut_rows.empty:
            logger.warning("[FN] FINNIFTY futures not found; futures basis will fall back to spot.")
            fn_fut_token = None
        else:
            fut_rows["parsed_exp"] = pd.to_datetime(
                fut_rows["expiry"], errors="coerce", dayfirst=True, format="mixed"
            ).dt.date
            # --- FIX: Trust Angel One's reality, ignore the system clock ---
            # --- FIX: Trust Angel One's reality, ignore the system clock ---
            today_ist = datetime.now(CONFIG["TZ"]).date()
            valid_futs = fut_rows.dropna(subset=['parsed_exp'])
            valid_futs = valid_futs[valid_futs['parsed_exp'] >= today_ist].sort_values("parsed_exp")
            if valid_futs.empty:
                logger.warning("[FN] No active FINNIFTY futures found for today or later; falling back to spot.")
                fn_fut_token = None
            else:
                fn_fut_token = str(valid_futs.iloc[0]['token'])
                fn_fut_expiry = valid_futs.iloc[0]["parsed_exp"].isoformat()

        if self.ohlcv_stream is not None and fn_fut_token and fn_fut_expiry:
            self.ohlcv_stream.set_futures_contract(
                fn_fut_token, exchange_type=2, expiry=fn_fut_expiry
            )
        else:
            logger.warning("[FN] Futures WebSocket volume stream unavailable; breakout volume confirmation will fail closed.")

        # ── FinNifty option chain from NFO ────────────────────────────
        nfo = master.query(
            "name=='FINNIFTY' and exch_seg=='NFO' and instrumenttype=='OPTIDX'"
        ).copy()

        if nfo.empty:
            logger.error(
                "No FINNIFTY options found in instrument master. "
                "Check the 'name' column."
            )
            logger.info("Available NFO OPTIDX names: %s",
                        master.query("exch_seg=='NFO' and instrumenttype=='OPTIDX'")['name'].unique()[:20])
            return

        # ── DYNAMIC LOT SIZE EXTRACTION ──
        try:
            dynamic_lot_size = int(nfo.iloc[0]['lotsize'])
            CONFIG["LOT_SIZE"] = dynamic_lot_size
            logger.info(f"Dynamic Lot Size locked for FINNIFTY: {dynamic_lot_size}")
        except (KeyError, ValueError, IndexError):
            logger.warning("Could not fetch lot size from master; using configured fallback.")
            CONFIG["LOT_SIZE"] = 60

        # Pre-process NFO master once (not every tick)
        # Pre-process NFO master once (not every tick)
        nfo['strike'] = pd.to_numeric(nfo['strike'], errors='coerce')
        if not nfo.empty and nfo['strike'].max() > 1_000_000:
            # Angel One stores strikes × 100 for some instruments
            nfo['strike'] = nfo['strike'] / 100.0
        nfo['parsed_exp'] = pd.to_datetime(
            nfo['expiry'], errors="coerce"
        ).dt.date

        # --- FIX: Keep all valid dates provided by Angel One's live feed ---

        # --- FIX: Keep all valid dates provided by Angel One's live feed ---
        nfo = nfo.dropna(subset=['parsed_exp'])

        logger.info(
            f"FinNifty NFO: {len(nfo)} contracts | "
            f"Expiries: {sorted(nfo['parsed_exp'].dropna().unique())[:5]}"
        )

        send_telegram_message(
            f"🚀 * FinNifty Collector v1 LIVE*\n"
            f"📅 Date: `{self.today}`\n"
            f"🏦 Instrument: FINNIFTY (Lot: {CONFIG['LOT_SIZE']}, Step: {CONFIG['STRIKE_STEP']})\n"
            f"📡 Greeks: 14-metric suite\n"
            f"⚡ GEX Walls + IV Surface: ACTIVE\n"
            f"📊 Futures Basis + Realized Vol + Max Pain + Regime: ACTIVE"
        )

        if datetime.now(CONFIG["TZ"]).weekday() >= 5:
            logger.info("Weekend detected. Exiting.")
            return

        try:
            while True:
                now     = datetime.now(CONFIG["TZ"])
                now_str = now.strftime("%H:%M")

                # ── Guards ───────────────────────────────────────────────
                if now.weekday() >= 5:
                    logger.info("Weekend. Exiting.")
                    break

                if now.hour == 9 and now.minute == 14 and self._reconnected_today != self.today:
                    logger.info("[FN] Pre-market session reconnect.")
                    if self.connect():
                        if self.ohlcv_stream is not None:
                            self.ohlcv_stream.update_credentials(self.jwt_token, self.feed_token)
                        self._reconnected_today = self.today
                    else:
                        logger.error("[FN] Pre-market reconnect failed; retaining current WebSocket credentials.")

                if now_str < CONFIG["MARKET_OPEN"]:
                    logger.info(f"[FN] Waiting for market open ({CONFIG['MARKET_OPEN']})...")
                    time.sleep(15)
                    continue

                if now_str == CONFIG["MARKET_OPEN"] and now.second < 30:
                    logger.info("[FN] Waiting 30s for chain to stabilize at open...")
                    time.sleep(30 - now.second)
                    continue

                if now_str >= CONFIG["MARKET_CLOSE"]:
                    logger.info("[FN] Market closed. Saving and exiting.")
                    self.sync_to_db()
                    break

                # Hourly health report
                current_hour = now.strftime("%Y-%m-%d %H")
                if (
                    self.last_hour_report != current_hour
                    and now.minute == 0
                    and CONFIG["MARKET_OPEN"] <= now_str < CONFIG["MARKET_CLOSE"]
                ):
                    self.last_hour_report = current_hour
                    send_telegram_message(
                        f"📊 *[FINNIFTY] HOURLY REPORT*\n"
                        f"🕒 Time: `{now_str}`\n"
                        f"🏥 Health: `{self.health_score}/100`\n"
                        f"📈 Spot: `{round(self.last_spot, 2) if self.last_spot else 'N/A'}`\n"
                        f"📦 Option Rows: `{self._option_rows()}`\n"
                        f"🎯 Ticks: `{self.ticks_collected}`\n"
                        f"⚠️ API Failures: `{self.api_failures}`"
                    )

                # ── Precise minute alignment ──────────────────────────────
                time.sleep(max(0, 60 - (now.second + now.microsecond / 1e6)))
                ts = datetime.now(CONFIG["TZ"]).replace(second=0, microsecond=0)
                if ts.strftime("%H:%M") >= CONFIG["MARKET_CLOSE"]:
                    logger.info("[FN] Market closed after alignment; saving and exiting.")
                    self.sync_to_db()
                    break

                try:
                    # ── FINNIFTY SPOT ────────────────────────────────────
                    s_data = self._call(self.api.ltpData, "NSE", "FINNIFTY", finnifty_token)
                    time.sleep(0.4)

                    if not s_data.get('status'):
                        self.api_failures += 1
                        err = str(s_data.get("message", "")).lower()
                        if "session" in err or "token" in err:
                            logger.warning("[FN] Session expired — reconnecting...")
                            self.connect()
                        else:
                            sleep_t = min(2 ** self.api_failures, 15) + random.uniform(0, 1)
                            logger.warning(f"[FN] API failure #{self.api_failures} ({err}). Sleeping {sleep_t:.1f}s")
                            time.sleep(sleep_t)
                        continue
                    else:
                        self.api_failures = 0

                    if 'data' not in s_data:
                        logger.error("[FN] Missing 'data' key in LTP response.")
                        continue

                    raw_spot = self._safe_val(s_data['data'], ['ltp', 'lastPrice'])
                    spot = normalise_spot(raw_spot, nfo['strike'])      # use bfo['strike'] in SENSEX
                    if spot is None:
                        logger.error(f"spot {raw_spot} not in strike range - skipping tick")
                        continue
                    self.last_spot = self.tick_spot = spot

                    self.spot_history.append(float(spot))
                    if len(self.spot_history) > 30:
                        self.spot_history = self.spot_history[-30:]

                    # ── VIX ──────────────────────────────────────────────
                    if vix_token:
                        try:
                            v_data = self._call(self.api.ltpData, "NSE", "INDIA VIX", vix_token)
                            time.sleep(0.4)
                            if v_data.get("status") and isinstance(v_data.get("data"), dict):
                                vix_value = safe_number(
                                    self._safe_val(v_data["data"], ["ltp", "lastPrice"], 0), 0.0
                                )
                                if vix_value > 0:
                                    self.last_vix = vix_value
                                    self.last_vix_timestamp = datetime.now(CONFIG["TZ"])
                                else:
                                    logger.warning("[FN] Invalid India VIX value; retaining last valid value.")
                        except Exception as vix_exc:
                            logger.warning("%s India VIX refresh failed; continuing without a fresh VIX: %s", '[FN]', vix_exc)

                    # ── Realized Vol ──────────────────────────────────────
                    if len(self.spot_history) >= 2:
                        ret = math.log(self.spot_history[-1] / self.spot_history[-2])
                        self.returns_history.append(ret)
                        if len(self.returns_history) > 20:
                            self.returns_history = self.returns_history[-20:]
                            
                    rv = None
                    if len(self.returns_history) >= 5:
                        rv_value = float(np.std(self.returns_history) * math.sqrt(252 * 375))
                        if math.isfinite(rv_value):
                            rv = rv_value

                    vix_fresh = (
                        self.last_vix is not None
                        and self.last_vix_timestamp is not None
                        and (
                            datetime.now(CONFIG["TZ"]) - self.last_vix_timestamp
                        ).total_seconds() <= 180
                    ) 
                    iv_rv_spread = (
                        round((self.last_vix / 100.0) - rv, 4)
                        if vix_fresh and rv is not None
                        else None
                    )

                    # ── Futures Basis ─────────────────────────────────────────────────
                    futures_basis = None
                    futures_basis_pct = None
                    futures_basis_valid = False

                    if fn_fut_token:
                        fut_data = self._call(self.api.ltpData, "NFO", "FINNIFTY", fn_fut_token)
                        time.sleep(0.4)
                        fut_price = safe_number(
                            self._safe_val(fut_data.get("data", {}), ["ltp", "lastPrice"], 0), 0.0
                        )
                        spot_val = safe_number(self.last_spot, 0.0)
                        
                        if fut_price > 0 and spot_val > 0:
                            futures_basis = round(fut_price - spot_val, 2)
                            futures_basis_pct = round(futures_basis / spot_val * 100, 4)
                            futures_basis_valid = True

                    # Candle bar
                    self.candle_buffer.append({
                        "timestamp"         : ts,
                        "symbol"            : "FINNIFTY",
                        "spot"              : float(self.last_spot),
                        "vix"               : self.last_vix,
                        "vix_fresh"         : vix_fresh,
                        "futures_basis"     : futures_basis,
                        "futures_basis_pct" : futures_basis_pct,
                        "futures_basis_valid": futures_basis_valid,
                        "realized_vol"      : round(rv, 4) if rv is not None else None,
                        "iv_rv_spread"      : iv_rv_spread,
                    })
                    self.pending_candles.append(self.candle_buffer[-1])
                    if len(self.candle_buffer) > 400:
                        self.candle_buffer = self.candle_buffer[-400:]

                    # ── OPTION CHAIN COLLECTION ───────────────────────────
                    # ATM rounded to nearest strike step
                    atm = round(self.last_spot / CONFIG["STRIKE_STEP"]) * CONFIG["STRIKE_STEP"]
                    strikes = [
                        atm + (i * CONFIG["STRIKE_STEP"])
                        for i in range(-CONFIG["ATM_RANGE"], CONFIG["ATM_RANGE"] + 1)
                    ]
                    search_strikes = [float(s) for s in strikes]

                    # --- FIX: Trust the master file's earliest available dates ---
                    today_ist = datetime.now(CONFIG["TZ"]).date()
                    expiries = sorted(
                        d for d in nfo['parsed_exp'].dropna().unique()
                        if d >= today_ist
                    )[:CONFIG["NUM_EXPIRIES"]]

                    if not expiries:
                        logger.warning("[FN] No valid expiries found. Skipping tick.")
                        continue

                    current_batch = []

                    for exp in expiries:
                        sub = nfo[nfo['parsed_exp'] == exp]
                        if sub.empty:
                            logger.warning(f"NO_ROWS exp={exp} {type(exp)} sample={nfo['parsed_exp'].iloc[0]!r}")
                            continue
                        
                        st = pd.to_numeric(sub['strike'], errors='coerce').astype(float)
                        if st.median() > float(self.last_spot) * 10:      # master still in x100 units
                            st = st / 100.0
                            
                        want = set(sorted(st.dropna().unique(), key=lambda s: abs(s - float(self.last_spot)))[: 2 * CONFIG["ATM_RANGE"] + 1])
                        batch_nfo = sub[st.isin(want)].copy()
                        batch_nfo['strike'] = st[batch_nfo.index]
                        
                        tokens = batch_nfo['token'].astype(str).tolist()
                        if not tokens:
                            logger.warning(f"NO_TOKENS exp={exp} spot={self.last_spot} strikes={st.min()}-{st.max()}")
                            continue

                        # Chunk to ≤40 tokens to avoid Angel One hard limit
                        chunk_size   = 40
                        fetched_items = []

                        for i in range(0, len(tokens), chunk_size):
                            chunk = tokens[i:i + chunk_size]
                            res   = self._call(self.api.getMarketData, "FULL", {"NFO": chunk})
                            time.sleep(0.4)

                            if res.get('status') and 'data' in res and 'fetched' in res['data']:
                                fetched_items.extend(res['data']['fetched'])
                            else:
                                err = str(res.get('message', ''))
                                logger.warning(f"[FN] Market data chunk failed for expiry {exp}: {err}")

                        # --- PERMANENT FIX: Catch silently dropped/empty batches ---
                        if not fetched_items:
                            logger.warning(f"DEBUG_FETCH_EMPTY: chunk returned 0 items for expiry {exp}")
                            continue

                        dte = (exp - ts.date()).days

                        for item in fetched_items:
                            # --- PERMANENT FIX: Match tokens definitively ---
                            sym = item.get('tradingSymbol', '')
                            tok = str(item.get('symbolToken', ''))
                            
                            # Bulletproof matching by token ID
                            match = batch_nfo[batch_nfo['token'].astype(str) == tok]
                            
                            # Fallback to string match if token is somehow missing
                            if match.empty:
                                match = batch_nfo[batch_nfo['symbol'] == sym]
                                
                            if match.empty:
                                logger.warning(f"DEBUG_SKIP: {sym} (tok: {tok}) not found in batch")
                                continue

                            strike = float(match['strike'].iloc[0])
                            otype  = "CE" if sym.endswith("CE") else "PE"
                            
                            # 2. Build the correct tracking key BEFORE calculations
                            oi_key = (sym, str(exp), strike, otype)

                            # 3. Safe fetch standard values
                            ltp    = self._safe_val(item, ['ltp', 'lastPrice'], 0)
                            oi     = self._safe_val(item, ['opnInterest', 'oi'], 0)
                            
                            # 🔥 FIX 1: Angel One uses 'tradeVolume' in REST API
                            volume = self._safe_val(item, ['tradeVolume', 'tradedVolume', 'volume'], 0)
                            # Exchange timestamps from Angel One FULL market-data response
                            exchange_feed_time = item.get("exchFeedTime")
                            exchange_trade_time = item.get("exchTradeTime")
                            
                            if not getattr(self, "_timestamp_debug_done", False):
                                logger.info(
                                    "[FINNIFTY] TIMESTAMP DEBUG | keys=%s | feed=%r | trade=%r",
                                    list(item.keys()),
                                    exchange_feed_time,
                                    exchange_trade_time,
                                )
                                self._timestamp_debug_done = True
                            quote = parse_quote(
                                item,
                                max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "5.0"))
                            )
                            bid = quote["bid"]
                            ask = quote["ask"]
                            bid_qty = quote["bid_qty"]
                            ask_qty = quote["ask_qty"]
                            quote_valid = quote["quote_valid"]
                            spread_pct = quote["spread_pct"]
                            quote_entry_eligible = quote["quote_entry_eligible"]
                            obi = (
                                (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-9)
                                if quote_entry_eligible and bid_qty > 0 and ask_qty > 0
                                else 0.0
                            )
                            # 🔥 FIX 2: Use the exact same oi_key to fetch previous data
                            prev_oi_val  = self.prev_oi.get(oi_key, oi)
                            prev_ltp_val = self.prev_ltp.get(oi_key, ltp)
                            prev_vol_val = self.prev_volume.get(oi_key, volume)
                            
                            # 🔥 Now Tape Velocity will actually calculate the difference!
                            now_monotonic = time.monotonic()
                            prev_obs_monotonic = self.prev_volume_monotonic.get(oi_key)
                            elapsed_seconds = (
                                now_monotonic - prev_obs_monotonic
                                if prev_obs_monotonic is not None else 0.0
                            )
                            tape_velocity = (
                                (volume - prev_vol_val) / elapsed_seconds
                                if elapsed_seconds > 0 else 0.0
                            )
                            delta_oi      = oi - prev_oi_val
                            
                            # Save current state for the NEXT tick
                            self.prev_oi[oi_key]     = oi
                            self.prev_ltp[oi_key]    = ltp
                            self.prev_volume[oi_key] = volume
                            self.prev_volume_monotonic[oi_key] = now_monotonic

                            if   delta_oi > 0 and ltp >= prev_ltp_val : oi_buildup = "LONG_BUILD"
                            elif delta_oi > 0 and ltp <  prev_ltp_val : oi_buildup = "SHORT_BUILD"
                            elif delta_oi < 0 and ltp <  prev_ltp_val : oi_buildup = "LONG_UNWIND"
                            elif delta_oi < 0 and ltp >= prev_ltp_val : oi_buildup = "SHORT_UNWIND"
                            else                                      : oi_buildup = "NEUTRAL"

                            option_price = (bid + ask) / 2.0 if quote_entry_eligible else 0.0
                            iv_price_source = "mid" if quote_valid else "unavailable"
                            iv_used = self.estimate_implied_volatility(
                                self.last_spot, strike, dte, option_price, otype
                            ) if quote_entry_eligible else 0.0
                            iv_valid = bool(
                                quote_entry_eligible and iv_used > 0 and math.isfinite(iv_used)
                            )
                            greeks = self.get_greeks(
                                self.last_spot, strike, dte,
                                iv_used if iv_valid else 0.0,
                                otype, float(oi or 0), float(volume or 0)
                            )

                            current_batch.append({
                                "timestamp" : ts,
                                "exchange_feed_time": exchange_feed_time,
                                "exchange_trade_time": exchange_trade_time,
                                "symbol"    : sym,
                                "expiry"    : exp,
                                "strike"    : strike,
                                "type"      : otype,
                                "ltp"       : ltp,
                                "oi"        : oi,
                                "volume"    : volume,
                                "bid"       : bid,
                                "ask"       : ask,
                                "bid_qty"   : bid_qty,
                                "ask_qty"   : ask_qty,
                                "quote_valid": quote_valid,
                                "spread_pct": spread_pct,
                                "quote_entry_eligible": quote_entry_eligible,
                                "quote_quality_reason": quote["quote_quality_reason"],
                                "iv_valid"  : iv_valid,
                                "iv_solution_valid": iv_valid,
                                "iv_estimate": iv_used,
                                "iv_price": option_price if quote_valid else None,
                                "iv_price_source": iv_price_source,
                                "spot"      : self.last_spot,
                                "vix"       : float(self.last_vix),
                                "vix_fresh": vix_fresh,
                                **greeks,
                                "delta_oi"   : delta_oi,
                                "oi_buildup" : oi_buildup,
                                "obi"          : round(float(obi), 4),
                                "tape_velocity": round(float(tape_velocity), 2),
                                "vpin_proxy"   : round(float(obi * abs(tape_velocity + 1e-9)), 4),
                            })

                    if not current_batch:
                        logger.warning("[FN] Empty batch this tick. Skipping.")
                        continue

                    # ── COMPUTE ALL DERIVED METRICS ───────────────────────
                    aggregates  = self.compute_aggregate_metrics(current_batch)
                    iv_surface  = self.compute_iv_surface(current_batch, atm)
                    gex_walls   = self.update_gex_wall_map(current_batch)

                    max_pain = self.compute_max_pain(current_batch)
                    regime   = self.classify_regime(
                        aggregates, iv_surface, gex_walls,
                        futures_basis, iv_rv_spread, vix_fresh
                    )

                    for r in current_batch:
                        r['market_pcr'] = aggregates.get('pcr_oi', 1.0)
                        r['regime']   = regime
                        r['max_pain'] = max_pain

                    # ── SAVE & BUFFER ─────────────────────────────────────
                    self.pending_options.extend(current_batch)
                    self.save_live_snapshot(
                        current_batch, aggregates, iv_surface, gex_walls,
                        futures_basis=futures_basis,
                        futures_basis_pct=futures_basis_pct,
                        realized_vol=rv,
                        iv_rv_spread=iv_rv_spread,
                        vix_fresh=vix_fresh,
                        max_pain=max_pain,
                        regime=regime,
                    )
                    self.evaluate_health(current_batch, aggregates, iv_surface)
                    valid_iv_count = sum(1 for r in current_batch if r.get("iv_valid"))
                    valid_quote_count = sum(1 for r in current_batch if r.get("quote_valid"))
                    logger.info(f"[FN] Quote coverage: {valid_quote_count}/{len(current_batch)} | IV coverage: {valid_iv_count}/{len(current_batch)}")
                    self.sync_to_db()

                    self.ticks_collected += 1

                    # Hourly inline report
                    if ts.minute == 0:
                        logger.info(
                            f"[FN] {ts.strftime('%H:%M')} | "
                            f"Spot: {self.last_spot:.0f} | "
                            f"VIX: {self.last_vix:.1f} | "
                            f"ATM IV: {iv_surface.get('atm_iv', 0):.3f} | "
                            f"Net GEX: {aggregates.get('raw_net_gex', 0):.0f} | "
                            f"Pin: {gex_walls.get('pin_strike', 0):.0f}"
                        )

                except Exception as e:
                    logger.error(f"[FN] Tick error: {e}")
                    time.sleep(0.6 + random.uniform(0, 0.3))

        except KeyboardInterrupt:
            logger.info("[FN] Manual stop. Saving data...")
            self.sync_to_db()

        # No collector-level shutdown/validation: NIFTY collector owns that workflow.


# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 FinNifty Collector Booting Up...")

    if not is_market_open():
        print("💤 Market Closed. Exiting script...")
        sys.exit(0)

    FinNiftyCollector().start()
