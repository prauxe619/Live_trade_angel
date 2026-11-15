"""
==========================================================================
  InstitutionalCollector  v6  (World-Class)

  Higher-Order Greeks  |  IV Surface & Skew  |  GEX Wall Map
  Volume Intelligence  |  Charm/Speed/Zomma  |  Brain v6 Ready
==========================================================================
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
from dotenv import load_dotenv
from collectors.scripts.collector_common import (
    normalise_spot, parse_quote, implied_vol, compute_pcr, AlertThrottle, safe_number)
from collectors.scripts.instrument_master_store import get_instrument_master
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

def env(key, default=None):
    return os.getenv(key, default)


def safe_number(value, default=0.0):
    """Convert broker values to finite floats; tolerate strings and nulls."""
    try:
        number = float(value or 0)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default

# Ensure repo root is importable when launched as a Railway script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collectors.scripts.shared_snapshots import (
    init_snapshot_table,
    publish_snapshot as publish_market_snapshot,
)
from collectors.scripts import market_store
from collectors.scripts.ohlcv_stream import start_index_stream
from collectors.scripts.shared_snapshots import get_recent_candles as get_ohlcv_candles

# Ensure repo root is importable when launched as a Railway script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Make console output safe for emojis on Windows terminals
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ======================================================================
#  SYSTEM CONFIGURATION  - tune here, touch nothing else
# ======================================================================
CONFIG = {
    # Timing
    "TZ"              : IST,
    "TICK_INTERVAL"   : 60,       # seconds between full chain scans
    "MARKET_OPEN"     : "09:15",
    "MARKET_CLOSE"    : "15:32",

    # Option Chain Scope
    "STRIKE_STEP"     : 50,
    "ATM_RANGE"       : 12,       # +-12 strikes = 25 strikes per expiry
    "NUM_EXPIRIES"    : 3,        # weekly + next 2

    # Finance
    "RISK_FREE_RATE"  : 0.065,    # RBI repo rate proxy
    "LOT_SIZE"        : 65,       # overwritten from instrument master at start

    # Resilience
    "MAX_RETRIES"     : 5,
    "RETRY_DELAY"     : 10,
    # OS shutdown is intentionally unsupported; Railway manages service lifecycle.

    # Data Quality
    "MIN_OPTIONS_PER_TICK" : 20,   # Alert if fewer rows collected
    "GAMMA_SPIKE_MULT"     : 5.0,  # Gamma x this = explosion alert
    "PCR_EXTREME_LOW"      : 0.30,
    "PCR_EXTREME_HIGH"     : 3.00,
    "SPOT_JUMP_PCT"        : 0.025, # 2.5% single-tick jump alert
    "MAX_SPREAD_PCT"       : 5.0,   # screening threshold; validate on historical data

    # IV Surface
    "IV_SKEW_WINGS"   : 3,        # +-3 strikes from ATM for skew calculation
    "IV_HISTORY_BARS" : 20,       # bars to track IV trend
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("collector.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("_Collector_V6")


# ======================================================================
#  JSON ENCODER  - handles all numpy / datetime types
# ======================================================================
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


def run_closed_market_shutdown_sequence():
    """Run optional data validation on a closed day; never shut down the host.

    Railway owns the worker lifecycle. This function must not invoke OS power
    commands, regardless of environment variables or platform.
    """
    logger.info("Market is closed. Running validation; host shutdown is disabled.")
    send_telegram_message(
        "🛑 *NIFTY MARKET CLOSED*\n"
        "Weekend/NSE holiday detected. Running data validation; no host shutdown will be scheduled."
    )
    try:
        subprocess.run(
            [sys.executable, "-m", "collectors.validate_daily_data"],
            check=False,
            timeout=120,
        )
    except Exception as e:
        logger.error("Validation error on closed-market day: %s", e)
        send_telegram_message(f"⚠️ Validation error on closed-market day: {e}")


def _local_snapshot_enabled() -> bool:
    """Local data/live/*.json snapshot files are OFF by default: Railway's disk is ephemeral and the
    snapshot is already published to PostgreSQL. Set WRITE_LOCAL_SNAPSHOT=true to also write the file."""
    return os.getenv("WRITE_LOCAL_SNAPSHOT", "false").strip().lower() in ("1", "true", "yes")


def is_market_open():
    """Checks if today is a weekend or an official NSE market holiday (IST)."""
    today = datetime.now(CONFIG["TZ"])

    # 1. Weekends (5 = Saturday, 6 = Sunday)
    if today.weekday() >= 5:
        print(f"Market Closed: Today is the weekend ({today.strftime('%A')}).")
        return False

    # 2. 2026 NSE Holidays (update this list yearly)
    nse_holidays_2026 = [
        "2026-01-26",  # Republic Day
        "2026-03-03",  # Maha Shivaratri
        "2026-03-23",  # Holi
        "2026-04-03",  # Good Friday
        "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
        "2026-05-01",  # Maharashtra Day
        "2026-05-28",  # Bakri Id
        "2026-06-26",  # Muharram
        "2026-08-15",  # Independence Day
        "2026-09-14",  # Ganesh Chaturthi
        "2026-10-02",  # Mahatma Gandhi Jayanti
        "2026-10-20",  # Dussehra
        "2026-11-10",  # Diwali-Balipratipada
        "2026-11-24",  # Gurunanak Jayanti
        "2026-12-25",  # Christmas
    ]

    today_str = today.strftime("%Y-%m-%d")
    if today_str in nse_holidays_2026:
        print(f"Market Closed: Today is an official NSE Holiday ({today_str}).")
        return False

    return True


# ======================================================================
#  INSTRUMENT MASTER LOADER
# ======================================================================
# ======================================================================
#  INSTRUMENT MASTER LOADER
# ======================================================================
def load_instrument_master():
    return get_instrument_master(publisher=False)


# ======================================================================
#  INSTITUTIONAL COLLECTOR - main class
# ======================================================================
class InstitutionalCollector:

    def __init__(self):
        self.api         = None
        self.jwt_token   = None
        self.feed_token  = None
        self.last_spot   = None
        self.last_vix    = None
        self.last_vix_timestamp = None
        self.base_dir    = Path("data/live")
        self.today       = datetime.now(CONFIG["TZ"]).strftime("%Y-%m-%d")

        # -- PostgreSQL persistence (replaces the local parquet files) --------
        self.index_key         = "NIFTY"
        self.pending_options   : list = []   # rows not yet confirmed in PostgreSQL (retried every tick)
        self.pending_candles   : list = []
        self.option_rows_saved = 0
        self.candle_rows_saved = 0
        self.candle_buffer     : list = []   # last candles kept in memory for the snapshot

        # -- Health & Diagnostics ----------------------------------------
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

        # -- Session control ---------------------------------------------
        self._reconnected_today = None   # date string of last 9:14 reconnect
        self.manual_stop        = False  # True if stopped with Ctrl+C

        # -- GEX Wall Map (persisted across ticks) ------------------------
        self.gex_wall_map : dict[float, float] = {}

        # -- Delta OI tracking, Realized Vol ------------------------------
        self.prev_oi         : dict = {}
        self.prev_ltp        : dict = {}
        self.prev_volume     : dict = {}
        self.prev_volume_monotonic: dict = {}
        self.returns_history : list[float] = []

        self._prepare_environment()
        self._restore_from_db()
        self.tick_spot = None
        self.ws_spot = None
        self._throttle = AlertThrottle(cooldown=1800)
        self.last_spot = None
        self.tick_spot = None
        self.ws_spot = None
        self.last_ws_tick_at = None
        self.last_rest_spot_at = None

    # -- Setup ------------------------------------------------------------

    def _prepare_environment(self):
        Path("data/live").mkdir(parents=True, exist_ok=True)
        import atexit
        atexit.register(lambda: getattr(self, "ohlcv_stream", None) and self.ohlcv_stream.stop())
        # Last-chance flush if the process exits (market close, crash, or Railway SIGTERM on redeploy).
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
                logger.info(f"Retention purge removed {purged} old option rows.")
            self.option_rows_saved = market_store.count_option_rows(self.index_key, self.today)
            self.candle_rows_saved = market_store.count_candles(self.index_key, self.today)
            self.candle_buffer     = market_store.load_recent_candles(self.index_key, self.today, limit=240)
            for state_row in market_store.load_latest_option_state(self.index_key, self.today):
                state_key = (state_row["symbol"], state_row["expiry"], state_row["strike"], state_row["type"])
                self.prev_oi[state_key] = state_row["oi"]
                self.prev_ltp[state_key] = state_row["ltp"]
                self.prev_volume[state_key] = state_row["volume"]
            logger.info(
                f"Restored from PostgreSQL: {self.option_rows_saved} option rows, "
                f"{self.candle_rows_saved} candles for {self.today}."
            )
        except Exception as e:
            logger.error(f"PostgreSQL unavailable at startup ({e}). Collecting anyway; rows are queued and retried each tick.")
            send_telegram_message(f"🚨 Nifty collector cannot reach PostgreSQL at startup: {e}")

    def _option_rows(self) -> int:
        return self.option_rows_saved + len(self.pending_options)

    def _candle_rows(self) -> int:
        return self.candle_rows_saved + len(self.pending_candles)

    # -- Authentication -----------------------------------------------------

    
    def connect(self) -> bool:
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                logger.info(f"Connecting to Angel One (attempt {attempt + 1})...")

                self.api = SmartConnect(api_key=env("ANGEL_API_KEY"))
                totp = pyotp.TOTP(env("ANGEL_TOTP")).now()
                session = self.api.generateSession(
                    env("ANGEL_CLIENT_ID"),
                    env("ANGEL_PASSWORD"),
                    totp
                )

                if session.get("status"):
                    self.jwt_token = (session.get("data") or {}).get("jwtToken")
                    self.feed_token = self.api.getfeedToken()
                    logger.info("Angel One session secured.")
                    return True

                logger.error(f"Session failed: {session.get('message')}")
                time.sleep(CONFIG["RETRY_DELAY"])

            except Exception as e:
                logger.error(f"Connection error attempt {attempt + 1}: {e}")
                time.sleep(CONFIG["RETRY_DELAY"])

        send_telegram_message(
            "🚨 FATAL: Collector failed to connect after all retries."
        )
        return False


    # -- Safe Data Accessor ---------------------------------------------------

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

    # ==================================================================
    #  CONTRACT-SPECIFIC IMPLIED VOLATILITY
    # ==================================================================
    @staticmethod
    def _bs_option_price(spot: float, strike: float, t: float,
                         rate: float, volatility: float, otype: str) -> float:
        """European Black-Scholes price used only for IV inversion."""
        if spot <= 0 or strike <= 0 or t <= 0 or volatility <= 0:
            return 0.0
        sqrt_t = math.sqrt(t)
        d1 = (math.log(spot / strike) +
              (rate + 0.5 * volatility * volatility) * t) / (volatility * sqrt_t)
        d2 = d1 - volatility * sqrt_t
        if otype == "CE":
            return spot * norm.cdf(d1) - strike * math.exp(-rate * t) * norm.cdf(d2)
        return strike * math.exp(-rate * t) * norm.cdf(-d2) - spot * norm.cdf(-d1)

    def estimate_implied_volatility(self, spot: float, strike: float, dte: float,
                                    option_price: float, otype: str) -> float:
        """Solve annualized IV from a valid option market price; return 0 if unsolvable."""
        try:
            spot, strike, option_price = float(spot), float(strike), float(option_price)
            if (not all(math.isfinite(x) for x in (spot, strike, option_price))
                    or spot <= 0 or strike <= 0 or option_price <= 0
                    or otype not in ("CE", "PE")):
                return 0.0
            t = max(float(dte), 0.5) / 365.0
            rate = float(CONFIG["RISK_FREE_RATE"])
            intrinsic = max(spot - strike, 0.0) if otype == "CE" else max(strike - spot, 0.0)
            # Reject prices materially below intrinsic value or beyond a broad sanity bound.
            if option_price + max(0.5, spot * 0.00002) < intrinsic:
                return 0.0
            low, high = 0.01, 5.0
            low_price = self._bs_option_price(spot, strike, t, rate, low, otype)
            high_price = self._bs_option_price(spot, strike, t, rate, high, otype)
            if option_price < low_price - 0.5 or option_price > high_price + 0.5:
                return 0.0
            for _ in range(80):
                mid = (low + high) / 2.0
                model_price = self._bs_option_price(spot, strike, t, rate, mid, otype)
                if model_price < option_price:
                    low = mid
                else:
                    high = mid
            iv = (low + high) / 2.0
            return float(iv) if 0.01 <= iv <= 5.0 and math.isfinite(iv) else 0.0
        except (TypeError, ValueError, OverflowError):
            return 0.0

    # ==================================================================
    #  WORLD-CLASS GREEKS ENGINE
    # ==================================================================
    def get_greeks(self, spot: float, strike: float, dte: float,
                   iv: float, otype: str, oi: float, volume: float = 0
                   ) -> dict:
        """
        Standard  : delta, gamma, vega, theta
        2nd Order : vanna, charm, vomma, speed
        3rd Order : zomma, color
        Flow      : gex, vanna_exposure, charm_exposure
        """
        ZERO = {
            "delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0,
            "vanna": 0.0, "charm": 0.0, "vomma": 0.0, "speed": 0.0,
            "zomma": 0.0, "color": 0.0,
            "gex": 0.0, "vanna_exposure": 0.0, "charm_exposure": 0.0,
            "iv_used": float(iv),
        }

        if spot <= 0 or strike <= 0 or iv <= 0:
            return ZERO

        # Minimum 0.5 day to avoid division explosion at expiry
        t = max(dte, 0.5) / 365.0
        r = CONFIG["RISK_FREE_RATE"]

        try:
            sqt  = math.sqrt(t)
            log_moneyness = math.log(spot / strike)

            d1 = (log_moneyness + (r + 0.5 * iv ** 2) * t) / (iv * sqt)
            d2 = d1 - iv * sqt

            Nd1  = norm.cdf(d1)
            Nd2  = norm.cdf(d2)
            Nd2_ = norm.cdf(-d2)
            nd1  = norm.pdf(d1)

            # -- Standard Greeks -----------------------------------------
            gamma = nd1 / (spot * iv * sqt)
            vega  = spot * nd1 * sqt / 100.0   # per 1% IV move

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

            # -- 2nd-Order Greeks ----------------------------------------
            # Vanna: dDelta/dIV
            vanna = (vega * 100 / spot) * (1.0 - d1 / (iv * sqt))

            # Charm: dDelta/dT (per day)
            charm = -nd1 * (
                (r / (iv * sqt)) - (d2 / (2 * t))
            ) / 365.0

            # Vomma: dVega/dIV
            vomma = vega * 100 * (d1 * d2) / iv

            # Speed: dGamma/dSpot
            speed = -(gamma / spot) * (1.0 + d1 / (iv * sqt))

            # -- 3rd-Order Greeks ----------------------------------------
            # Zomma: dGamma/dIV
            zomma = gamma * (d1 * d2 - 1.0) / iv

            # Color: dGamma/dT per day
            color = (
                gamma * (
                    (1.0 - d1 * d1) / (2.0 * t)
                    + d1 * (r + 0.5 * iv * iv) / (iv * sqt)
                )
            ) / 365.0

            # -- Exposure Metrics ----------------------------------------
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

    # ==================================================================
    #  IV SURFACE ENGINE
    # ==================================================================
    def compute_iv_surface(self, batch: list, atm_strike: float) -> dict:
        """
        ATM IV, call/put skew, put-call IV gap, curvature,
        term structure (near vs next expiry) and IV velocity.
        """
        surface = {
            "atm_iv"          : 0.0,
            "iv_skew_call"    : 0.0,   # OTM CE IV - ATM IV
            "iv_skew_put"     : 0.0,   # OTM PE IV - ATM IV
            "put_call_iv_gap" : 0.0,   # ATM PE IV - ATM CE IV
            "iv_curvature"    : 0.0,   # avg wing IV - ATM IV
            "iv_term_spread"  : 0.0,   # near expiry IV - far expiry IV
            "iv_trend"        : 0.0,   # vs last N bars
        }

        if not batch:
            return surface

        try:
            df = pd.DataFrame(batch)
            skew_wing = CONFIG["IV_SKEW_WINGS"] * CONFIG["STRIKE_STEP"]
            if "iv_valid" in df.columns:
                df = df[df["iv_valid"] == True]
            else:
                df = df[pd.to_numeric(df["iv_used"], errors="coerce") > 0]
            if df.empty:
                return surface

            atm_ce = df[(df['strike'] == atm_strike) & (df['type'] == 'CE')]
            atm_pe = df[(df['strike'] == atm_strike) & (df['type'] == 'PE')]

            atm_ce_iv = float(atm_ce['iv_used'].mean()) if not atm_ce.empty else 0.0
            atm_pe_iv = float(atm_pe['iv_used'].mean()) if not atm_pe.empty else 0.0
            available_atm_ivs = [v for v in (atm_ce_iv, atm_pe_iv) if v > 0]
            surface['atm_iv'] = round(float(np.mean(available_atm_ivs)), 4) if available_atm_ivs else 0.0
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

    # ==================================================================
    #  GEX WALL MAP ENGINE
    # ==================================================================
    def update_gex_wall_map(self, batch: list) -> dict:
        """
        Builds and updates a GEX wall map: strike -> cumulative GEX.

        Returns:
            gex_walls, gex_flip_point, pin_strike, net_gex_total, gex_profile
        """
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
            # Rebuild from this tick's chain. Re-adding every snapshot would
            # inflate GEX throughout the session and distort the wall/pin map.
            current_map = {}
            for row in batch:
                try:
                    s = float(row.get('strike', 0) or 0)
                    gex = float(row.get('gex', 0) or 0)
                except (TypeError, ValueError):
                    continue
                if s > 0 and math.isfinite(s) and math.isfinite(gex):
                    current_map[s] = current_map.get(s, 0.0) + gex

            self.gex_wall_map = current_map
            strikes_sorted = sorted(current_map.keys())
            gex_vals       = [current_map[s] for s in strikes_sorted]

            if not gex_vals:
                return result

            net_gex_total = sum(gex_vals)
            result['net_gex_total'] = round(net_gex_total, 2)

            # Pin strike (highest absolute GEX)
            abs_gex    = [abs(g) for g in gex_vals]
            pin_idx    = abs_gex.index(max(abs_gex))
            result['pin_strike'] = strikes_sorted[pin_idx]

            # GEX flip point (sign change closest to spot)
            flip_strike = 0.0
            if self.last_spot:
                for i in range(len(gex_vals) - 1):
                    if gex_vals[i] * gex_vals[i+1] < 0:
                        s1, s2 = strikes_sorted[i], strikes_sorted[i+1]
                        flip_strike = s1 if abs(s1 - self.last_spot) < abs(s2 - self.last_spot) else s2
            result['gex_flip_point'] = flip_strike

            # Top 5 GEX walls by absolute size
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

    # ==================================================================
    #  AGGREGATE FLOW METRICS  (what agent_brain v6 needs)
    # ==================================================================
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

        # -- OI & Volume ---------------------------------------------------
        call_oi    = safe_sum(ce, 'oi')
        put_oi     = safe_sum(pe, 'oi')
        total_oi   = call_oi + put_oi
        call_vol   = safe_sum(ce, 'volume')
        put_vol    = safe_sum(pe, 'volume')
        total_vol  = call_vol + put_vol

        pcr_oi     = put_oi  / (call_oi  + 1e-9)
        pcr_vol    = put_vol / (call_vol + 1e-9)
        pcr_gamma  = safe_sum(pe, 'gamma') / (safe_sum(ce, 'gamma') + 1e-9)

        # -- Standard Flow ---------------------------------------------------
        raw_net_gex  = safe_sum(batch, 'gex')
        scaled_gex   = float(np.sign(raw_net_gex) * np.log1p(abs(raw_net_gex)))
        call_gamma   = safe_sum(ce, 'gamma') * 100
        put_gamma    = safe_sum(pe, 'gamma') * 100
        greek_rows = [r for r in batch if r.get('iv_valid') is True]
        avg_vanna    = safe_mean(greek_rows, 'vanna')
        avg_theta    = safe_mean(greek_rows, 'theta')

        # -- Microstructure ----------------------------------------------------
        avg_obi        = safe_mean(batch, 'obi')
        avg_tape_vel   = safe_mean(batch, 'tape_velocity')
        avg_vpin_proxy = safe_mean(batch, 'vpin_proxy')

        # -- Higher-Order Exposure Totals -------------------------------------
        total_charm_exposure = safe_sum(batch, 'charm_exposure')
        call_charm           = safe_sum(ce, 'charm_exposure')
        put_charm            = safe_sum(pe, 'charm_exposure')
        total_vanna_exposure = safe_sum(batch, 'vanna_exposure')
        avg_vomma = safe_mean(greek_rows, 'vomma')
        avg_zomma = safe_mean(greek_rows, 'zomma')

        # -- OTM Pressure Signals ---------------------------------------------
        atm = round(self.last_spot / CONFIG["STRIKE_STEP"]) * CONFIG["STRIKE_STEP"] if self.last_spot else 0
        otm_range = 3 * CONFIG["STRIKE_STEP"]

        deep_otm_pe = safe_sum(
            [r for r in pe if safe_number(r.get("strike"), 0) > 0 and safe_number(r.get("strike"), 0) <= atm - otm_range], 'oi'
        )
        deep_otm_ce = safe_sum(
            [r for r in ce if safe_number(r.get("strike"), 0) > 0 and safe_number(r.get("strike"), 0) >= atm + otm_range], 'oi'
        )
        otm_hedge_ratio = deep_otm_pe / (deep_otm_ce + 1e-9)  # >1 = bearish hedge

        # -- Flow Imbalance -----------------------------------------------------
        flow_imbalance = (call_vol - put_vol) / (total_vol + 1e-9)  # +1 = CE dominance

        # -- GEX History --------------------------------------------------------
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

            "avg_obi"          : round(avg_obi, 4),
            "avg_tape_vel"     : round(avg_tape_vel, 2),
            "avg_vpin_proxy"   : round(avg_vpin_proxy, 4),
        }

    # ==================================================================
    #  MAX PAIN ENGINE
    # ==================================================================
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

    # ==================================================================
    #  REGIME CLASSIFIER
    # ==================================================================
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

    # ==================================================================
    #  LIVE SNAPSHOT BUILDER  (what angel_paper_trader reads)
    # ==================================================================
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
        Writes data/live/live_snapshot.json every tick (atomic write).
        """
        try:
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
                "spot"         : float(self.last_spot) if self.last_spot else 0.0,
                "vix"          : self.last_vix,
                "vix_valid"    : self.last_vix is not None,
                "vix_fresh"    : vix_fresh,
                "ticks_today"  : self.ticks_collected,
                "health"       : self.health_score,
                "lot_size"     : CONFIG["LOT_SIZE"],

                **aggregates,

                "market_pcr"   : aggregates.get("pcr_oi", 1.0),
                "pcr"          : aggregates.get("pcr_oi", 1.0),

                "iv_surface"   : iv_surface,
                "gex_walls"    : gex_walls,

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

                "data"         : batch,
                "candles"      : formatted_candles,
                "spot_samples" : spot_samples,
                "candle_quality": "angelone_websocket_ohlc" if candles_fresh else "unavailable",
                "latest_candle_age_s": latest_candle_age_s,
                "candles_fresh": candles_fresh,
                "iv_method"     : "black_scholes_bisection_from_option_price",
                "gex_method"    : "current_snapshot_strike_aggregation",
            }

            _snapshot_published = True
            try:
                publish_market_snapshot("NIFTY", snapshot)
            except Exception as db_exc:
                _snapshot_published = False
                logger.error("PostgreSQL snapshot publish failed for NIFTY: %s", db_exc)

            if _local_snapshot_enabled() or not _snapshot_published:
                tmp_path  = Path("data/live/live_snapshot.tmp")
                snap_path = Path("data/live/live_snapshot.json")

                # Atomic write: tmp -> rename (prevents partial reads by trader)
                with open(tmp_path, "w") as f:
                    json.dump(snapshot, f, cls=DateTimeEncoder)

                # Micro-retry loop to handle Windows file locking collisions
                for _ in range(5):
                    try:
                        tmp_path.replace(snap_path)
                        break
                    except PermissionError:
                        time.sleep(0.05)

        except Exception as e:
            logger.error(f"Critical error saving snapshot: {e}")

    # ==================================================================
    #  HEALTH & ANOMALY ENGINE
    # ==================================================================
    def evaluate_health(self, batch: list, aggregates: dict, iv_surface: dict):
        score = 100
        alerts = []

        # -- API failures ----------------------------------------------------
        score -= min(self.api_failures * 5, 25)

        # -- Spot validity ---------------------------------------------------
        if not self.last_spot or self.last_spot <= 0:
            score -= 15
            alerts.append("⚠️ Invalid spot price")

        # -- Chain completeness ----------------------------------------------
        if not batch:
            score -= 20
            alerts.append("⚠️ No option rows collected")
        elif len(batch) < CONFIG["MIN_OPTIONS_PER_TICK"]:
            score -= 10
            alerts.append(f"⚠️ Only {len(batch)} rows (expected >={CONFIG['MIN_OPTIONS_PER_TICK']})")

        # -- Gamma anomaly ---------------------------------------------------
        if batch:
            avg_gamma = np.mean([abs(r.get('gamma', 0)) for r in batch])
            self.gamma_history.append(float(avg_gamma))
            if len(self.gamma_history) > 10:
                self.gamma_history = self.gamma_history[-10:]
            if len(self.gamma_history) >= 5:
                rolling_mean = np.mean(self.gamma_history[:-1])
                if rolling_mean > 0 and avg_gamma > rolling_mean * CONFIG["GAMMA_SPIKE_MULT"]:
                    score -= 10
                    alerts.append(f"🚨 Gamma explosion: {avg_gamma:.6f} vs mean {rolling_mean:.6f}")

        # -- PCR extremes ----------------------------------------------------
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
            alerts.append(f"⚠️ Extreme PCR HIGH: {pcr:.2f}")
        elif pcr < CONFIG["PCR_EXTREME_LOW"]:
            score -= 10
            alerts.append(f"⚠️ Extreme PCR LOW: {pcr:.2f}")

        # -- Spot jump detection -----------------------------------------------
        if len(self.spot_history) >= 2:
            prev = self.spot_history[-2]
            curr = self.spot_history[-1]
            if prev > 0:
                jump = abs(curr - prev) / prev
                if jump > CONFIG["SPOT_JUMP_PCT"]:
                    alerts.append(f"🚨 Spot jump {jump*100:.2f}%: {prev:.1f} -> {curr:.1f}")

        # -- Disk failures -----------------------------------------------------
        score -= min(self.sync_failures * 5, 10)

        self.health_score = max(score, 0)
        logger.info(f"HEALTH: {self.health_score}/100 | Rows: {len(batch)} | GEX: {aggregates.get('raw_net_gex', 0):.0f}")

        for alert in alerts:
            logger.warning(alert)
            send_telegram_message(alert)

        # Critical threshold pause
        if self.health_score < 60:
            msg = f"🚨 CRITICAL HEALTH: {self.health_score}/100 - continuing collection; data quality is degraded."
            logger.error(msg)
            send_telegram_message(msg)

    # -- Disk Persistence ------------------------------------------------------

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
            logger.error(f"DB SAVE ERROR (options, {len(self.pending_options)} queued): {e}")
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
            logger.error(f"DB SAVE ERROR (candles, {len(self.pending_candles)} queued): {e}")

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
                send_telegram_message(f"🚨 Nifty: PostgreSQL writes failing ({self.sync_failures} ticks in a row). Rows are queued.")
        else:
            self.sync_failures = 0
            logger.info(
                f"DB SYNC {datetime.now(CONFIG['TZ']).strftime('%H:%M:%S')} "
                f"| Options today: {self._option_rows()} rows"
            )

    # ==================================================================
    #  SHUTDOWN & VALIDATION
    # ==================================================================
    def _system_shutdown(self):
        """Compatibility stub: host shutdown is intentionally disabled.

        Railway controls service lifecycle; this collector must never power off
        the host running it.
        """
        logger.warning("OS shutdown is disabled; Railway manages worker lifecycle.")

    def perform_shutdown_sequence(self):
        logger.info("Running daily validation...")
        send_telegram_message("🔍 Collector finished. Running validation...")

        try:
            subprocess.run(
                [sys.executable, "-m", "collectors.validate_daily_data"],
                check=False, timeout=120
            )
        except Exception as e:
            logger.error(f"Validation error: {e}")

        # Never power off the host. Railway (or the local OS) manages process
        # lifecycle; the collector only saves data and exits its collection loop.
        if self.manual_stop:
            shutdown_line = "Manual stop - host shutdown disabled."
        else:
            shutdown_line = "Host shutdown disabled; service lifecycle is externally managed."

        send_telegram_message(
            f"✅ *DATA COLLECTION COMPLETE*\n"
            f"📅 Date: `{self.today}`\n"
            f"📊 Option Rows: `{self._option_rows()}`\n"
            f"🕯 Candle Rows: `{self._candle_rows()}`\n"
            f"🏥 Final Health: `{self.health_score}/100`\n"
            f"🎯 Ticks Collected: `{self.ticks_collected}`\n"
            f"{shutdown_line}"
        )

        logger.info("Host shutdown is disabled; no OS power command will be issued.")

    # ==================================================================
    #  MAIN COLLECTION LOOP
    # ==================================================================
    def start(self):
        if not self.connect():
            return

        master = load_instrument_master()
        nifty_rows = master.query("name=='NIFTY' and exch_seg=='NSE'")
        vix_rows = master.query("name=='INDIA VIX' and exch_seg=='NSE'")
        fut_rows = master.query(
            "name=='NIFTY' and exch_seg=='NFO' and instrumenttype=='FUTIDX'"
        ).copy()
        nfo = master.query(
            "name=='NIFTY' and exch_seg=='NFO' and instrumenttype=='OPTIDX'"
        ).copy()

        if nifty_rows.empty or vix_rows.empty or fut_rows.empty or nfo.empty:
            raise RuntimeError(
                "Instrument master is missing NIFTY spot, INDIA VIX, NIFTY futures, "
                "or NIFTY options contracts. Refresh OpenAPIScripMaster.json."
            )
        nifty_token = str(nifty_rows.iloc[0]["token"])
        self.ohlcv_stream = start_index_stream(self, nifty_token, 1, env("ANGEL_API_KEY"), env("ANGEL_CLIENT_ID"))

        vix_token = str(vix_rows.iloc[0]["token"])
        today_date = datetime.now(CONFIG["TZ"]).date()
        fut_rows["parsed_exp"] = pd.to_datetime(
            fut_rows["expiry"], errors="coerce"
        ).dt.date
        # Select the nearest non-expired futures contract only.
        fut_rows = (
            fut_rows.dropna(subset=['parsed_exp'])
            .loc[lambda df: df['parsed_exp'] >= today_date]
            .sort_values('parsed_exp')
        )
        if fut_rows.empty:
            raise RuntimeError('No active NIFTY futures contract found in instrument master.')
        nifty_fut_token = str(fut_rows.iloc[0]['token'])

        # -- Dynamic lot size --
        try:
            dynamic_lot_size = int(nfo.iloc[0]['lotsize'])
            CONFIG["LOT_SIZE"] = dynamic_lot_size
            logger.info(f"Dynamic Lot Size locked for NIFTY: {dynamic_lot_size}")
        except (KeyError, ValueError, IndexError):
            logger.warning("Could not fetch lot size from master! Falling back to 65.")
            CONFIG["LOT_SIZE"] = 65

        # Pre-process NFO master once (not every tick)
        # Pre-process NFO master once (not every tick)
        nfo['strike'] = pd.to_numeric(nfo['strike'], errors='coerce')
        if not nfo.empty and nfo['strike'].max() > 100_000:
            nfo['strike'] = nfo['strike'] / 100.0
        nfo['parsed_exp'] = pd.to_datetime(
            nfo['expiry'], errors="coerce"
        ).dt.date
        
        # Keep all valid dates provided by Angel One's live feed
        
        # Keep all valid dates provided by Angel One's live feed
        nfo = nfo.dropna(subset=['parsed_exp'])

        logger.info(
            f"Nifty NFO: {len(nfo)} contracts | "
            f"Expiries: {sorted(nfo['parsed_exp'].dropna().unique())[:5]}"
        )

        send_telegram_message(
            f"🚀 *Nifty Collector v4.1 LIVE*\n"
            f"📅 Date: `{self.today}`\n"
            f"🏦 Instrument: NIFTY (Lot: {CONFIG['LOT_SIZE']}, Step: {CONFIG['STRIKE_STEP']})\n"
            f"📡 Greeks: 14-metric suite\n"
            f"⚡ GEX Walls + IV Surface: ACTIVE\n"
            f"📊 Futures Basis + Realized Vol + Max Pain + Regime: ACTIVE\n"
            f"⏻ Host shutdown: DISABLED (Railway-safe)"
        )

        if datetime.now(CONFIG["TZ"]).weekday() >= 5:
            logger.info("Weekend detected. Exiting.")
            return

        try:
            while True:
                now = datetime.now(CONFIG["TZ"])
                now_str = now.strftime("%H:%M")

                # -- Guards -------------------------------------------------
                if now.weekday() >= 5:
                    logger.info("Weekend. Exiting.")
                    break

                # Daily session reconnect at 9:14 (runs once, BEFORE the
                # market-open guard so it is actually reachable)
                if (
                    now.hour == 9 and now.minute == 14
                    and self._reconnected_today != self.today
                ):
                    logger.info("Pre-market session reconnect.")
                    self.connect()
                    self._reconnected_today = self.today

                if now_str < CONFIG["MARKET_OPEN"]:
                    logger.info(f"Waiting for market open ({CONFIG['MARKET_OPEN']})...")
                    time.sleep(30)
                    continue

                # Opening stabilization (first 30 seconds)
                if now_str == CONFIG["MARKET_OPEN"] and now.second < 30:
                    logger.info("Waiting 30s for chain to stabilize at open...")
                    time.sleep(30 - now.second)
                    continue

                if now_str >= CONFIG["MARKET_CLOSE"]:
                    logger.info("Market closed. Saving and exiting.")
                    self.sync_to_db()
                    break

                # Hourly health report (first loop pass inside the first
                # 3 minutes of each hour - robust to slow ticks)
                current_hour = now.strftime("%Y-%m-%d %H")
                if (
                    self.last_hour_report != current_hour
                    and now.minute <= 2
                    and CONFIG["MARKET_OPEN"] <= now_str < CONFIG["MARKET_CLOSE"]
                ):
                    self.last_hour_report = current_hour
                    send_telegram_message(
                        f"📊 *HOURLY REPORT*\n"
                        f"🕒 Time: `{now_str}`\n"
                        f"🏥 Health: `{self.health_score}/100`\n"
                        f"📈 Spot: `{round(self.last_spot, 2) if self.last_spot else 'N/A'}`\n"
                        f"📦 Option Rows: `{self._option_rows()}`\n"
                        f"🎯 Ticks: `{self.ticks_collected}`\n"
                        f"⚠️ API Failures: `{self.api_failures}`"
                    )

                # -- Precise minute alignment ---------------------------------
                time.sleep(max(0, 60 - (now.second + now.microsecond / 1e6)))
                ts = datetime.now(CONFIG["TZ"]).replace(second=0, microsecond=0)

                # Re-check close AFTER the alignment sleep so no extra tick
                # is collected at/after MARKET_CLOSE.
                if ts.strftime("%H:%M") >= CONFIG["MARKET_CLOSE"]:
                    continue   # top of loop will sync and break

                try:
                    # -- SPOT ------------------------------------------------
                    s_data = self._call(self.api.ltpData, "NSE", "NIFTY", nifty_token)
                    time.sleep(0.4)  # rate-limit spacer

                    if not s_data.get('status'):
                        self.api_failures += 1
                        err = str(s_data.get("message", "")).lower()
                        if "session" in err or "token" in err:
                            logger.warning("Session expired - reconnecting...")
                            self.connect()
                        else:
                            sleep_t = min(2 ** self.api_failures, 15) + random.uniform(0, 1)
                            logger.warning(f"API failure #{self.api_failures} ({err}). Sleeping {sleep_t:.1f}s")
                            time.sleep(sleep_t)
                        continue
                    else:
                        self.api_failures = 0

                    if 'data' not in s_data:
                        logger.error("Missing 'data' key in LTP response.")
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

                    # -- VIX: refresh before calculating volatility features --
                    if vix_token:
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
                                logger.warning("[NIFTY] Invalid India VIX value; retaining last valid value.")

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

                    # -- Futures Basis ---------------------------------------------
                    futures_basis = None
                    futures_basis_pct = None
                    futures_basis_valid = False

                    if nifty_fut_token:
                        fut_data = self._call(self.api.ltpData, "NFO", "NIFTY", nifty_fut_token)
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
                        "symbol"            : "NIFTY",
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

                    # -- OPTION CHAIN COLLECTION ---------------------------------
                    atm = round(self.last_spot / CONFIG["STRIKE_STEP"]) * CONFIG["STRIKE_STEP"]
                    strikes = [
                        atm + (i * CONFIG["STRIKE_STEP"])
                        for i in range(-CONFIG["ATM_RANGE"], CONFIG["ATM_RANGE"] + 1)
                    ]
                    search_strikes = [float(s) for s in strikes]

                    # Trust the master file's earliest available dates
                    expiries = sorted(
                        d for d in nfo['parsed_exp'].dropna().unique()
                        if d >= ts.date()
                    )[:CONFIG["NUM_EXPIRIES"]]

                    if not expiries:
                        logger.warning("No valid expiries found. Skipping tick.")
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

                        # Angel One fails on >50 tokens per call; chunk by 40.
                        chunk_size = 40
                        fetched_items = []

                        for i in range(0, len(tokens), chunk_size):
                            chunk = tokens[i:i + chunk_size]
                            res = self._call(self.api.getMarketData, "FULL", {"NFO": chunk})

                            # One-time schema debug (first tick only, avoids log spam)
                            if (
                                self.ticks_collected == 0 and i == 0
                                and res.get("status") and isinstance(res.get("data"), dict)
                            ):
                                fetched = res["data"].get("fetched", [])
                                if fetched:
                                    sample = fetched[0]
                                    logger.info("QUOTE DEBUG: response keys=%s", list(res.keys()))
                                    logger.info("QUOTE DEBUG: data keys=%s", list(res["data"].keys()))
                                    logger.info("QUOTE DEBUG: item keys=%s", list(sample.keys()))
                                    logger.info("QUOTE DEBUG: direct bid/ask fields=%s", {k: sample.get(k) for k in ["bidPrice", "bestBid", "askPrice", "bestAsk"] if k in sample})
                                    depth = sample.get("depth")
                                    if isinstance(depth, dict):
                                        logger.info("QUOTE DEBUG: depth buy sample=%s", depth.get("buy", [])[:1])
                                        logger.info("QUOTE DEBUG: depth sell sample=%s", depth.get("sell", [])[:1])
                                    else:
                                        logger.info("QUOTE DEBUG: depth type=%s", type(depth).__name__)
                            time.sleep(0.4)  # rate-limit spacer

                            if res.get('status') and 'data' in res and 'fetched' in res['data']:
                                fetched_items.extend(res['data']['fetched'])
                            else:
                                err = str(res.get('message', ''))
                                logger.warning(f"Market data chunk failed for expiry {exp}: {err}")

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

                            oi_key = (sym, str(exp), strike, otype)

                            ltp = safe_number(self._safe_val(item, ['ltp', 'lastPrice']))
                            oi = safe_number(self._safe_val(item, ['opnInterest', 'oi']))
                            volume = safe_number(
                                self._safe_val(item, ['tradeVolume', 'tradedVolume', 'volume'], 0)
                            )
                            bid = self._safe_val(item, ['bidPrice', 'bestBid'], 0) or 0
                            ask = self._safe_val(item, ['askPrice', 'bestAsk'], 0) or 0
                            # Normalize direct fields before testing truthiness (e.g. "0" is truthy).
                            try:
                                bid = float(bid)
                                if not math.isfinite(bid) or bid <= 0:
                                    bid = 0.0
                            except (TypeError, ValueError, OverflowError):
                                bid = 0.0
                            try:
                                ask = float(ask)
                                if not math.isfinite(ask) or ask <= 0:
                                    ask = 0.0
                            except (TypeError, ValueError, OverflowError):
                                ask = 0.0

                            depth = item.get('depth', {})
                            buy_levels = depth.get('buy', []) if isinstance(depth, dict) else []
                            sell_levels = depth.get('sell', []) if isinstance(depth, dict) else []

                            def get_price(level):
                                if not isinstance(level, dict):
                                    return 0.0
                                for key in ('price', 'buyPrice', 'sellPrice', 'bestPrice', 'rate'):
                                    try:
                                        value = float(level.get(key, 0) or 0)
                                        if math.isfinite(value) and value > 0:
                                            return value
                                    except (TypeError, ValueError, OverflowError):
                                        continue
                                return 0.0

                            def get_qty(level):
                                for key in ('quantity', 'qty', 'totalQuantity', 'totalQty', 'volume'):
                                    try:
                                        value = float(level.get(key, 0) or 0)
                                        if value > 0:
                                            return value
                                    except (TypeError, ValueError):
                                        pass
                                return 0

                            # Preserve the exchange timestamps for later freshness audits.
                            exchange_feed_time = item.get('exchFeedTime')
                            exchange_trade_time = item.get('exchTradeTime')

                            # Search all available depth levels instead of assuming level 1 is valid.
                            if not bid:
                                for level in buy_levels:
                                    candidate = get_price(level)
                                    if candidate > 0:
                                        bid = candidate
                                        break

                            if not ask:
                                for level in sell_levels:
                                    candidate = get_price(level)
                                    if candidate > 0:
                                        ask = candidate
                                        break

                            # Shared parser is the single source of truth for all four indices.
                            quote = parse_quote(
                                item,
                                max_spread_pct=CONFIG["MAX_SPREAD_PCT"]
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

                            # LTP fallback is diagnostic only; it must not qualify as a
                            # quote-backed IV or feed Greeks/GEX/IV-surface calculations.
                            if quote_entry_eligible:
                                option_price = (bid + ask) / 2.0
                                iv_price_source = "mid"
                            else:
                                try:
                                    option_price = float(ltp or 0)
                                    if not math.isfinite(option_price) or option_price <= 0:
                                        option_price = 0.0
                                except (TypeError, ValueError, OverflowError):
                                    option_price = 0.0
                                iv_price_source = "ltp_fallback"

                            iv_used = self.estimate_implied_volatility(
                                self.last_spot, strike, dte, option_price, otype
                            )
                            iv_solution_valid = bool(
                                iv_used > 0 and math.isfinite(iv_used)
                            )
                            iv_valid = bool(
                                quote_entry_eligible
                                and iv_price_source == "mid"
                                and iv_solution_valid
                            )

                            prev_oi_val  = self.prev_oi.get(oi_key, oi)
                            prev_ltp_val = self.prev_ltp.get(oi_key, ltp)
                            prev_vol_val = self.prev_volume.get(oi_key, volume)

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

                            greeks = self.get_greeks(
                                self.last_spot, strike, dte,
                                iv_used if iv_valid else 0.0,
                                otype, oi, volume
                            )

                            current_batch.append({
                                "timestamp" : ts,
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
                                "quote_valid" : quote_valid,
                                "spread_pct"  : spread_pct,
                                "quote_entry_eligible": quote_entry_eligible,
                                "exchange_feed_time": exchange_feed_time,
                                "exchange_trade_time": exchange_trade_time,
                                "iv_valid"    : iv_valid,
                                "iv_solution_valid": iv_solution_valid,
                                "iv_estimate" : iv_used,
                                "iv_price"    : option_price,
                                "iv_price_source": iv_price_source,
                                "spot"      : self.last_spot,
                                "vix": self.last_vix,
                                "vix_valid": self.last_vix is not None,
                                "vix_fresh": vix_fresh,
                                **greeks,
                                "delta_oi"   : delta_oi,
                                "oi_buildup" : oi_buildup,
                                "obi"          : round(float(obi), 4),
                                "tape_velocity": round(float(tape_velocity), 2),
                                "vpin_proxy"   : round(float(obi * abs(tape_velocity + 1e-9)), 4),
                            })

                    if not current_batch:
                        logger.warning("Empty batch this tick. Skipping.")
                        continue

                    valid_quotes = sum(1 for r in current_batch if r.get("quote_valid", False))
                    valid_ivs = sum(1 for r in current_batch if r.get("iv_valid", False))
                    bid_qty_count = sum(1 for r in current_batch if float(r.get("bid_qty", 0) or 0) > 0)
                    ask_qty_count = sum(1 for r in current_batch if float(r.get("ask_qty", 0) or 0) > 0)
                    wide_spreads = sum(
                        1 for r in current_batch
                        if r.get("spread_pct") is not None
                        and r["spread_pct"] > CONFIG["MAX_SPREAD_PCT"]
                    )
                    entry_eligible = sum(
                        1 for r in current_batch
                        if r.get("quote_entry_eligible", False)
                    )

                    logger.info(
                        "QUOTE COVERAGE: rows=%d valid_quotes=%d (%.2f%%) bid_qty=%d ask_qty=%d",
                        len(current_batch),
                        valid_quotes,
                        100 * valid_quotes / max(len(current_batch), 1),
                        bid_qty_count,
                        ask_qty_count,
                    )

                    logger.info(
                        "QUOTE QUALITY: wide_spreads=%d (>%.1f%%) | entry_eligible=%d/%d",
                        wide_spreads,
                        CONFIG["MAX_SPREAD_PCT"],
                        entry_eligible,
                        len(current_batch),
                    )

                    logger.info(
                        "IV COVERAGE: valid_ivs=%d/%d (%.2f%%)",
                        valid_ivs, len(current_batch),
                        100 * valid_ivs / max(len(current_batch), 1)
                    )
                    if valid_quotes == 0:
                        logger.warning("NO VALID BID/ASK QUOTES - investigate API depth fields.")
                    if valid_ivs == 0:
                        logger.warning("NO VALID CONTRACT IVS - Greeks and GEX will be zero for this tick.")

                    # -- COMPUTE ALL DERIVED METRICS -----------------------------
                    aggregates  = self.compute_aggregate_metrics(current_batch)
                    iv_surface  = self.compute_iv_surface(current_batch, atm)
                    gex_walls   = self.update_gex_wall_map(current_batch)

                    max_pain = self.compute_max_pain(current_batch)
                    regime   = self.classify_regime(
                        aggregates, iv_surface, gex_walls,
                        futures_basis, iv_rv_spread, vix_fresh
                    )

                    # Inject aggregates into every row for agent compatibility
                    for r in current_batch:
                        r['market_pcr'] = aggregates.get('pcr_oi', 1.0)
                        r['regime']   = regime
                        r['max_pain'] = max_pain

                    # -- SAVE & BUFFER ---------------------------------------------
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
                    self.sync_to_db()

                    self.ticks_collected += 1

                    # Hourly inline log (at :00 minutes)
                    if ts.minute == 0:
                        logger.info(
                            f"{ts.strftime('%H:%M')} | "
                            f"Spot: {self.last_spot:.0f} | "
                            f"VIX: {self.last_vix:.1f} | "
                            f"ATM IV: {iv_surface.get('atm_iv', 0):.3f} | "
                            f"Net GEX: {aggregates.get('raw_net_gex', 0):.0f} | "
                            f"Pin: {gex_walls.get('pin_strike', 0):.0f}"
                        )

                except Exception as e:
                    logger.error(f"Tick error: {e}")
                    time.sleep(0.6 + random.uniform(0, 0.3))

        except KeyboardInterrupt:
            logger.info("Manual stop. Saving data...")
            self.manual_stop = True
            self.sync_to_db()

        self.perform_shutdown_sequence()


if __name__ == "__main__":
    print("🚀 Data Collector Booting Up...", flush=True)

    if not is_market_open():
        print("Market closed. Running validation; host shutdown is disabled.", flush=True)
        run_closed_market_shutdown_sequence()
        sys.exit(0)

    try:
        init_snapshot_table()
        print("Initializing InstitutionalCollector...", flush=True)
        collector = InstitutionalCollector()
        print("Starting collector and connecting to Angel One...", flush=True)
        collector.start()
    except KeyboardInterrupt:
        print("Collector stopped by user.", flush=True)
    except Exception:
        logger.exception("FATAL: Collector startup failed.")
        raise
