"""
Railway session supervisor for the four Angel One market-data collectors.

Run from the project root:
    python -u supervisor.py

This starts collectors once per trading date, restarts unexpected failures with
backoff during the session, avoids weekends/known holidays, and never shuts down
the host. It does not enable or submit trades.
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, date, time as clock_time
from pathlib import Path
from zoneinfo import ZoneInfo
import json
from http.cookiejar import CookieJar
from urllib.request import HTTPCookieProcessor, Request, build_opener

from collectors.banknifty_collector_v1 import env

ROOT = Path(__file__).resolve().parent
TZ = ZoneInfo(os.getenv("MARKET_TIMEZONE", "Asia/Kolkata"))
POLL_SECONDS = max(5, int(os.getenv("SUPERVISOR_POLL_SECONDS", "15")))
START_TIME = clock_time(9, 0)
RESTART_CUTOFF = clock_time(15, 20)
STOP_TIME = clock_time(15, 35)
RESTART_DELAY_SECONDS = max(10, int(os.getenv("SUPERVISOR_RESTART_DELAY_SECONDS", "30")))
MAX_RESTARTS_PER_COLLECTOR = max(0, int(os.getenv("SUPERVISOR_MAX_RESTARTS", "5")))
DRY_RUN = os.getenv("SUPERVISOR_DRY_RUN", "").strip().lower() in {"1", "true", "yes"}

# Stagger startup delay between each collector to prevent Angel One API rate limits
STAGGER_SECONDS = max(2, int(os.getenv("SUPERVISOR_STAGGER_SECONDS", "8")))

NSE_CACHE_DIR = ROOT / "data" / "cache"
# Keep synchronized with the holiday calendar used by the collectors.
# Verify/update this list from the official NSE/BSE calendar each year.
NSE_HOLIDAY_API = "https://www.nseindia.com/api/holiday-master?type=trading"
NSE_HOLIDAY_PAGE = "https://www.nseindia.com/resources/exchange-communication-holidays"
NSE_HOLIDAY_SEGMENT = os.getenv("NSE_HOLIDAY_SEGMENT", "FO").upper()

NSE_STALE_CACHE_MAX_AGE_SECONDS = 180 * 24 * 60 * 60
NSE_ALLOW_STALE_CACHE = (
    os.getenv("NSE_ALLOW_STALE_CACHE", "false").strip().lower()
    in {"1", "true", "yes"}
)

NSE_REFRESH_SECONDS = 6 * 60 * 60
NSE_MAX_CACHE_AGE_SECONDS = 24 * 60 * 60
NSE_RETRY_SECONDS = 5 * 60

_NSE_HOLIDAY_CACHE: dict[int, set[date]] = {}
_NSE_HOLIDAY_SUCCESS_AT: dict[int, float] = {}
_NSE_HOLIDAY_LAST_ATTEMPT: dict[int, float] = {}


COLLECTORS = {
    "NIFTY": "collectors.nifty50_collector_v4",
    "BANKNIFTY": "collectors.banknifty_collector_v1",
    "FINNIFTY": "collectors.finnifty_collector_v1",
    "SENSEX": "collectors.sensex_collector_v1",
    "SIGNAL_ENGINE": "daily_signal_engine_v11",
}


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [supervisor] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("railway_supervisor")



def fetch_nse_holidays(year: int) -> set[date]:
    import requests

    url = "https://www.nseindia.com/api/holiday-master?type=trading"
    page = "https://www.nseindia.com/resources/exchange-communication-holidays"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": page,
    }

    last_error = None

    for user_agent in (
        headers["User-Agent"],
        "PostmanRuntime/7.26.5",
    ):
        try:
            with requests.Session() as session:
                session.headers.update(headers)
                session.headers["User-Agent"] = user_agent
                session.get("https://www.nseindia.com/", timeout=10).raise_for_status()
                session.get(page, timeout=10).raise_for_status()
                response = session.get(url, timeout=15)
                response.raise_for_status()
                payload = response.json()

            rows = payload.get(NSE_HOLIDAY_SEGMENT)
            if not isinstance(rows, list):
                raise ValueError(
                    f"NSE response has no {NSE_HOLIDAY_SEGMENT!r} list"
                )

            holidays = set()
            for row in rows:
                raw_date = row.get("tradingDate")
                if raw_date:
                    parsed = datetime.strptime(
                        raw_date, "%d-%b-%Y"
                    ).date()
                    if parsed.year == year:
                        holidays.add(parsed)

            if not holidays:
                raise ValueError(f"NSE returned no holiday dates for {year}")

            return holidays

        except Exception as exc:
            last_error = exc
            log.warning("NSE calendar request attempt failed: %s", exc)

    raise RuntimeError(
        f"Could not fetch a verified NSE holiday calendar: {last_error}"
    )




def get_nse_holidays(year: int) -> set[date] | None:
    """Fetch NSE holidays, persisting verified data for temporary outages."""
    now = time.time()
    cached = _NSE_HOLIDAY_CACHE.get(year)
    last_success = _NSE_HOLIDAY_SUCCESS_AT.get(year, 0.0)
    last_attempt = _NSE_HOLIDAY_LAST_ATTEMPT.get(year, 0.0)

    if cached and now - last_success < NSE_REFRESH_SECONDS:
        return cached

    if now - last_attempt < NSE_RETRY_SECONDS:
        if cached and now - last_success < NSE_MAX_CACHE_AGE_SECONDS:
            return cached
        return _load_nse_holiday_file(year)

    _NSE_HOLIDAY_LAST_ATTEMPT[year] = now

    try:
        holidays = fetch_nse_holidays(year)
        if not holidays:
            raise ValueError("NSE returned an empty holiday calendar")

        NSE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = NSE_CACHE_DIR / f"nse_holidays_{NSE_HOLIDAY_SEGMENT}_{year}.json"
        temp_file = cache_file.with_suffix(".tmp")

        temp_file.write_text(json.dumps({
            "year": year,
            "segment": NSE_HOLIDAY_SEGMENT,
            "fetched_at": time.time(),
            "dates": sorted(d.isoformat() for d in holidays),
        }), encoding="utf-8")
        temp_file.replace(cache_file)

        _NSE_HOLIDAY_CACHE[year] = holidays
        _NSE_HOLIDAY_SUCCESS_AT[year] = time.time()
        log.info("Verified and cached %s NSE %s holidays for %s",
                 len(holidays), NSE_HOLIDAY_SEGMENT, year)
        return holidays

    except Exception as exc:
        log.warning("NSE calendar refresh failed: %s", exc)

        if cached and now - last_success < NSE_MAX_CACHE_AGE_SECONDS:
            log.warning("Using recently verified in-memory calendar")
            return cached

        return _load_nse_holiday_file(year)


def _load_nse_holiday_file(year: int) -> set[date] | None:
    """Load a previously verified calendar only while its cache is fresh."""
    cache_file = NSE_CACHE_DIR / f"nse_holidays_{NSE_HOLIDAY_SEGMENT}_{year}.json"

    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        age = time.time() - float(data["fetched_at"])

        if data.get("year") != year:
            raise ValueError("Cached calendar year mismatch")
        if data.get("segment") != NSE_HOLIDAY_SEGMENT:
            raise ValueError("Cached calendar segment mismatch")
        if age < 0:
            raise ValueError("Cached calendar timestamp is invalid")
        
        if age > NSE_MAX_CACHE_AGE_SECONDS:
            if not (
                NSE_ALLOW_STALE_CACHE
                and age <= NSE_STALE_CACHE_MAX_AGE_SECONDS
            ):
                raise ValueError("Cached calendar is expired")
        
            log.warning(
                "Using stale NSE calendar because network refresh failed; age=%.1f days",
                age / 86400,
            )

        holidays = {date.fromisoformat(d) for d in data["dates"]}
        if not holidays or any(d.year != year for d in holidays):
            raise ValueError("Cached calendar dates are invalid")

        _NSE_HOLIDAY_CACHE[year] = holidays
        _NSE_HOLIDAY_SUCCESS_AT[year] = time.time()
        log.warning("Using previously verified NSE calendar cache; age=%.1f hours",
                    age / 3600)
        return holidays

    except Exception as exc:
        log.error("No usable NSE holiday cache: %s", exc)
        return None



def market_day(today: datetime) -> bool:
    if today.weekday() >= 5:
        return False

    holidays = get_nse_holidays(today.year)
    if holidays is None:
        return False

    return today.date() not in holidays


def within_start_window(now: datetime) -> bool:
    return START_TIME <= now.time().replace(tzinfo=None) < STOP_TIME


def start_collector(name: str, module: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    # Safety defaults: do not enable order placement via supervisor.
    # Hard safety overrides for this market-data supervisor.
    if name == "SIGNAL_ENGINE":
        env["TRADING_MODE"] = env.get("TRADING_MODE", "paper").strip().lower()
        env["TRADING_ENABLED"] = env.get("TRADING_ENABLED", "false").strip().lower()
        env["ALLOW_ORDER_SUBMISSION"] = env.get("ALLOW_ORDER_SUBMISSION", "false").strip().lower()
        env["LIVE_SHADOW"] = env.get("LIVE_SHADOW", "true").strip().lower()
        env["LIVE_TRADING_ACK"] = env.get("LIVE_TRADING_ACK", "")
        # Fail closed if any explicit live-entry confirmation is missing.
        if (env["TRADING_MODE"] == "live" and
                (env["TRADING_ENABLED"] != "true"
                 or env["ALLOW_ORDER_SUBMISSION"] != "true"
                 or env["LIVE_TRADING_ACK"] != "I_UNDERSTAND_REAL_ORDERS")):
            env["ALLOW_ORDER_SUBMISSION"] = "false"
            env["LIVE_SHADOW"] = "true"
    else:
        env["TRADING_MODE"] = "paper"
        env["TRADING_ENABLED"] = "false"
        env["ALLOW_ORDER_SUBMISSION"] = "false"
    env["DISABLE_SYSTEM_SHUTDOWN"] = "true"
    env["PAPER_LEDGER_ENABLED"] = "true" if name == "SIGNAL_ENGINE" else "false"
    log.info("Starting %s using python -u -m %s", name, module)
    return subprocess.Popen(
        [sys.executable, "-u", "-m", module],
        cwd=str(ROOT),
        env=env,
        stdin=subprocess.DEVNULL,
    )


def stop_process(name: str, proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    log.info("Stopping %s (PID %s)", name, proc.pid)
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
      log.warning("%s did not stop gracefully; killing process", name)
      proc.kill()
      proc.wait(timeout=5)


def main() -> int:
    log.info("Railway collector supervisor started; timezone=%s", TZ)
    log.info(
        "Signal engine trading config: mode=%s enabled=%s allow_submit=%s shadow=%s ack_present=%s; "
        "collectors are forced to paper mode. No host shutdown will be issued.",
        os.getenv("TRADING_MODE", "paper"),
        os.getenv("TRADING_ENABLED", "false"),
        os.getenv("ALLOW_ORDER_SUBMISSION", "false"),
        os.getenv("LIVE_SHADOW", "true"),
        os.getenv("LIVE_TRADING_ACK", "") == "I_UNDERSTAND_REAL_ORDERS",
    )
    if not (ROOT / "collectors").is_dir():
        log.error("Expected a 'collectors/' directory next to supervisor.py. "
                  "Place this file in the project root.")
        return 2
    missing_modules = [
        module for module in COLLECTORS.values()
        if not (ROOT / (module.replace(".", "/") + ".py")).is_file()
    ]
    if missing_modules:
        log.error("Missing collector module files: %s", ", ".join(missing_modules))
        return 2

    if DRY_RUN:
        now = datetime.now(TZ)
        if market_day(now) and within_start_window(now):
            log.info("DRY RUN: would start: %s", ", ".join(COLLECTORS))
        else:
            log.info("DRY RUN: no launch now; market_day=%s time=%s",
                     market_day(now), now.strftime("%H:%M:%S"))
        return 0

    active_date: date | None = None
    processes: dict[str, subprocess.Popen] = {}
    restart_counts = {name: 0 for name in COLLECTORS}
    next_restart_at = {name: 0.0 for name in COLLECTORS}
    session_started = False

    try:
        while True:
            now = datetime.now(TZ)
            today = now.date()
            is_day = market_day(now)
            current_time = now.time().replace(tzinfo=None)

            # Reset session tracking when the local date changes.
            if active_date != today:
                for name, proc in list(processes.items()):
                    stop_process(name, proc)
                processes.clear()
                restart_counts = {name: 0 for name in COLLECTORS}
                next_restart_at = {name: 0.0 for name in COLLECTORS}
                active_date = today
                session_started = False
                log.info("Date changed to %s; market_day=%s", today.isoformat(), is_day)

            if not is_day:
                time.sleep(POLL_SECONDS)
                continue

            # Launch once for the day, at/after 09:00 IST, with staggering to protect API limits.
            if not session_started and START_TIME <= current_time < STOP_TIME:
                log.info("Trading day startup window reached; launching collectors with %ss stagger.", STAGGER_SECONDS)
                for i, (name, module) in enumerate(COLLECTORS.items()):
                    if i > 0:
                        log.info("Waiting %ss before launching next collector...", STAGGER_SECONDS)
                        time.sleep(STAGGER_SECONDS)
                    processes[name] = start_collector(name, module)
                session_started = True

            if session_started:
                # Restart a collector only for an unexpected early exit, with a
                # bounded retry count and never after the restart cutoff.
                for name, module in COLLECTORS.items():
                    proc = processes.get(name)
                    if proc is None:
                        continue
                    code = proc.poll()
                    if code is None:
                        continue
                    processes.pop(name, None)
                    if current_time < RESTART_CUTOFF and restart_counts[name] < MAX_RESTARTS_PER_COLLECTOR:
                        restart_counts[name] += 1
                        next_restart_at[name] = time.monotonic() + RESTART_DELAY_SECONDS
                        log.error("%s exited with code %s; restart %s/%s scheduled in %ss",
                                  name, code, restart_counts[name],
                                  MAX_RESTARTS_PER_COLLECTOR, RESTART_DELAY_SECONDS)
                    else:
                        log.warning("%s exited with code %s; no further restart today.",
                                    name, code)

                for name, module in COLLECTORS.items():
                    if name in processes or next_restart_at[name] == 0:
                        continue
                    if time.monotonic() >= next_restart_at[name]:
                        if current_time < RESTART_CUTOFF and restart_counts[name] <= MAX_RESTARTS_PER_COLLECTOR:
                            processes[name] = start_collector(name, module)
                        next_restart_at[name] = 0.0

                if current_time >= STOP_TIME:
                    if processes:
                        log.info("Session stop time reached; stopping any remaining collectors.")
                        for name, proc in list(processes.items()):
                            stop_process(name, proc)
                        processes.clear()
                    session_started = True
                    time.sleep(POLL_SECONDS)
                    continue

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        log.info("Supervisor interrupted; stopping child collectors.")
    except Exception:
        log.exception("Supervisor fatal error; stopping child collectors.")
        raise
    finally:
        for name, proc in list(processes.items()):
            stop_process(name, proc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
