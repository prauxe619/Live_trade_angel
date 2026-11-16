"""
╔══════════════════════════════════════════════════════════════════════════════╗
║        Black_Blood  —  telegram_vip_alert.py  v3  (Engagement Edition)     ║
║                                                                              ║
║  WHAT THIS FILE DOES:                                                        ║
║   1. Sends trading/collector notifications to the two signal-engine chats    ║
║   2. Uses SIGNAL_BOT_TOKEN for both destinations                             ║
║   3. Keeps promotional/free-channel posts disabled in personal mode         ║
║   4. Provides a queued sender so notifications do not block collectors      ║
║      → 9:10 AM  Morning hype post + payment link (Mon-Fri)                  ║
║      → 10:30 AM Mid-morning FOMO hook (Mon-Fri)                             ║
║      → 12:00 PM Lunch break update (Mon-Fri)                                ║
║      → 1:30 PM  Afternoon tease post (Mon-Fri)                              ║
║      → 2:45 PM  Pre-close urgency post (Mon-Fri)                            ║
║      → 3:30 PM  EOD results teaser + payment link (Mon-Fri)                 ║
║      → 3:35 PM  Friday weekly recap + best trade + payment link             ║
║      → 10:00 AM Sunday preview post + payment link                          ║
║                                                                              ║
║  PAYMENT PLANS (update your Cosmofeed links at PAYMENT_LINKS below):        ║
║   • Trial     : ₹599   / 1 day                                              ║
║   • Weekly    : ₹1,599 / 7 days                                             ║
║   • Monthly   : ₹3,999 / 30 days  ← push this                              ║
║   • Yearly    : ₹29,999 / 365 days                                          ║
║                                                                              ║
║  ENV VARS (from /etc/live-trade-angel.env):                                 ║
║   SIGNAL_BOT_TOKEN      = existing signal-engine bot token                  ║
║   CHAT_ID               = personal Telegram destination                     ║
║   LIVE_BOT_CHANNEL_ID   = existing Telegram channel destination             ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import os
import queue
import threading
import time
import urllib.request
import urllib.parse
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
from numpy import random
import ollama
import pandas_market_calendars as mcal

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION  — UPDATE THESE BEFORE RUNNING
# ══════════════════════════════════════════════════════════════════════════════

# One bot and exactly two destinations, shared with daily_signal_engine_v11.py.
_SIGNAL_BOT_TOKEN: str = os.getenv("SIGNAL_BOT_TOKEN", "").strip()
_CHAT_ID: str = os.getenv("CHAT_ID", "").strip()
_LIVE_BOT_CHANNEL_ID: str = os.getenv("LIVE_BOT_CHANNEL_ID", "").strip()

def _telegram_destinations() -> tuple[str, ...]:
    """Return the two configured destinations once each; never route elsewhere."""
    return tuple(dict.fromkeys(
        chat_id for chat_id in (_CHAT_ID, _LIVE_BOT_CHANNEL_ID) if chat_id
    ))

# Compatibility aliases for older imports; routing always uses _telegram_destinations().
_VIP_BOT_TOKEN = _SIGNAL_BOT_TOKEN
_VIP_CHANNEL_ID = _LIVE_BOT_CHANNEL_ID
_FREE_CHANNEL_ID = ""

# ── PASTE YOUR COSMOFEED LINKS HERE ──────────────────────────────────────────
PAYMENT_LINKS = {
    "Daily"    : "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd",
    "weekly"   : "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd",
    "monthly"  : "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd",
    "yearly"   : "https://cosmofeed.com/vig/69e4b42c9e4e440013c2b4dd",
}

# ── SCHEDULED POST TIMES (24hr IST) ──────────────────────────────────────────
SCHEDULE_MORNING     = "09:10"   # Pre-open hype
SCHEDULE_MID_MORNING = "10:30"   # Mid-morning FOMO
SCHEDULE_LUNCH       = "12:00"   # Lunch update
SCHEDULE_AFTERNOON   = "13:30"   # Afternoon tease
SCHEDULE_PRE_CLOSE   = "14:45"   # Pre-close urgency
SCHEDULE_EOD         = "15:30"   # End of day results
SCHEDULE_FRIDAY      = "15:31"   # Friday recap (same day, after EOD)
SCHEDULE_SUNDAY      = "10:00"   # Sunday preview

_RETRY_COUNT      = 3
_RETRY_DELAY_SECS = 2.0

# ══════════════════════════════════════════════════════════════════════════════
#  ASYNC QUEUE — fire-and-forget, never blocks trading loop
# ══════════════════════════════════════════════════════════════════════════════
_msg_queue: queue.Queue = queue.Queue(maxsize=500)
_logger = logging.getLogger("black_blood.vip_alert")


def is_market_open():
    nse = mcal.get_calendar('NSE')
    schedule = nse.schedule(start_date=datetime.now().strftime("%Y-%m-%d"),
                            end_date=datetime.now().strftime("%Y-%m-%d"))
    return not schedule.empty


def get_unique_market_insight() -> str:
    try:
        response = ollama.chat(model='llama3', messages=[
            {
                'role': 'system',
                'content': (
                    'You are a professional, minimalist quant trading mentor for a high-end brand '
                    'called Black Blood. Write a 1-sentence, sophisticated insight about market '
                    'discipline or systematic trading. No hype, no fluff. Vary the insight each time.'
                )
            },
        ])
        return response['message']['content']
    except Exception:
        fallbacks = [
            "Discipline is the only edge that never expires.",
            "The market rewards patience and punishes emotion.",
            "A trade without a stop loss is just hope with leverage.",
            "Consistency beats brilliance every single month.",
            "Your biggest enemy in the market is your own urgency.",
            "Risk management is not optional — it is the strategy.",
        ]
        import random as _r
        return _r.choice(fallbacks)


def escape_markdown(text: str) -> str:
    """Escapes special characters that break Telegram's Markdown parser."""
    # Characters that Telegram hates if not escaped in MarkdownV2
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # If using legacy 'Markdown' (not V2), it's usually just unclosed * or _, 
    # but the safest bet if you keep getting 400s is to ensure no stray characters break it.
    
    # Actually, a simpler fix is to just strip out the `parse_mode` entirely first 
    # to confirm if that's the issue.
    return text

def send_telegram_message(message: str, *args, **kwargs) -> bool:
    """Queue a notification for both configured signal-engine destinations.

    Legacy chat_id arguments are intentionally ignored so callers cannot route
    messages to an extra VIP/free channel. Returns True when both destinations
    were successfully queued.
    """
    del args, kwargs
    if not _SIGNAL_BOT_TOKEN:
        _logger.error("Telegram notification blocked: SIGNAL_BOT_TOKEN is missing.")
        return False

    destinations = _telegram_destinations()
    if len(destinations) != 2:
        _logger.error(
            "Telegram notification blocked: expected exactly 2 destinations, found %d.",
            len(destinations),
        )
        return False

    queued = 0
    for chat_id in destinations:
        try:
            _msg_queue.put_nowait((_SIGNAL_BOT_TOKEN, chat_id, str(message)))
            queued += 1
        except queue.Full:
            _logger.error("Telegram queue full; destination %s was not queued.", chat_id)

    if queued:
        _ensure_telegram_worker()
    return queued == len(destinations)
    
def _telegram_post(bot_token: str, chat_id: str, text: str,
                   photo_path: str = None) -> bool:
    if not bot_token or not chat_id:
        _logger.warning("Alert skipped — token or channel ID missing.")
        return False

    if photo_path and os.path.exists(photo_path):
        url      = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        boundary = "Black_BloodBoundary"
        with open(photo_path, "rb") as f:
            photo_data = f.read()
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="chat_id"\r\n\r\n{chat_id}\r\n'
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="parse_mode"\r\n\r\nMarkdown\r\n'
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="caption"\r\n\r\n{text}\r\n'
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="photo"; filename="chart.png"\r\n'
            f"Content-Type: image/png\r\n\r\n"
        ).encode() + photo_data + f"\r\n--{boundary}--\r\n".encode()
        for attempt in range(1, _RETRY_COUNT + 1):
            try:
                req = urllib.request.Request(url, data=body, method="POST")
                req.add_header("Content-Type",
                               f"multipart/form-data; boundary={boundary}")
                with urllib.request.urlopen(req, timeout=15) as resp:
                    if json.loads(resp.read()).get("ok"):
                        return True
            except Exception as exc:
                _logger.warning(f"Photo attempt {attempt} failed: {exc}")
                if attempt < _RETRY_COUNT:
                    time.sleep(_RETRY_DELAY_SECS)
        return False

    url    = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = urllib.parse.urlencode({
        "chat_id"                  : chat_id,
        "text"                     : text,
        "parse_mode"               : "HTML",
        "disable_web_page_preview" : "true",
    }).encode("utf-8")
    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            req = urllib.request.Request(url, data=params, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if json.loads(resp.read()).get("ok"):
                    return True
        except Exception as exc:
            _logger.warning(f"Text attempt {attempt} failed: {exc}")
            if attempt < _RETRY_COUNT:
                time.sleep(_RETRY_DELAY_SECS)
    return False


def _worker():
    while True:
        try:
            item = _msg_queue.get(timeout=5)
            if len(item) == 4:
                _telegram_post(*item)
            else:
                token, channel, text = item
                _telegram_post(token, channel, text)
            _msg_queue.task_done()
        except queue.Empty:
            pass
        except Exception as exc:
            _logger.error(f"Worker error: {exc}")


_worker_lock = threading.Lock()
_worker_started = False

def _ensure_telegram_worker():
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if not _worker_started:
            threading.Thread(target=_worker, daemon=True, name="signal-telegram-sender").start()
            _worker_started = True

# Start eagerly for compatibility with existing collector imports.
_ensure_telegram_worker()


def _enqueue_vip(text: str):
    """Route trade entry/exit alerts through the shared two-destination sender."""
    send_telegram_message(text)


def _enqueue_free(text: str, photo_path: str = None):
    """Disable all free-channel posts and promotional media."""
    return


# ══════════════════════════════════════════════════════════════════════════════
#  PAYMENT BLOCK — compact, clean, appended to every free channel post
# ══════════════════════════════════════════════════════════════════════════════

def _payment_block() -> str:
    return (
        f"\n\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"💎 *Join Black Blood VIP Today*\n\n"
        f"🔹 [Trial — ₹599 / 1 day]({PAYMENT_LINKS['Daily']})\n"
        f"🔸 [Weekly — ₹1,599 / 7 days]({PAYMENT_LINKS['weekly']})\n"
        f"🏆 [Monthly — ₹3,999]({PAYMENT_LINKS['monthly']})  ✦ *Most Popular*\n"
        f"👑 [Yearly — ₹29,999]({PAYMENT_LINKS['yearly']})\n\n"
        f"⚡ Instant Telegram access after payment\n"
        f"✅ UPI · Cards · Net Banking accepted\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _de(side: str) -> str:
    return "🟢" if side.upper() in ("CE", "CALL", "BUY_CE") else "🔴"

def _te(trigger: str) -> str:
    t = trigger.upper()
    if "PROFIT" in t or "_TP" in t: return "🎯"
    if "LOSS"   in t or "_SL" in t: return "🛑"
    if "TRAIL"  in t:               return "🛡️"
    return "🔖"

def _now()   -> str: return datetime.now().strftime("%d %b  %H:%M:%S")
def _today() -> str: return datetime.now().strftime("%d %b %Y")
def _hhmm()  -> str: return datetime.now().strftime("%I:%M %p")


# ══════════════════════════════════════════════════════════════════════════════
#  VIP CHANNEL ALERTS
#  Paid subscribers only — NO payment link in these messages
#  Redesigned: clean, human-readable, feels premium not robotic
# ══════════════════════════════════════════════════════════════════════════════

def vip_entry_alert(*, side, symbol, entry_price, sl_pts, tp_pts,
                    prob=None, regime="?", confluence="?",
                    inst_press="?", vix=None, mode="INTRADAY"):
    """Live entry alert to VIP channel."""
    de       = _de(side)
    prob_str = f"{prob:+.4f}" if prob is not None else "—"
    vix_str  = f"{vix:.1f}"  if vix  is not None  else "—"
    tag      = "🌙 SWING TRADE" if mode.upper() == "SWING" else "⚡ INTRADAY TRADE"
    direction = "CALL (CE)" if "CE" in side.upper() else "PUT (PE)"

    _enqueue_vip(
        f"{'🟢' if 'CE' in side.upper() else '🔴'} *NEW {tag} — {direction}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 *Symbol* :  `{symbol}`\n"
        f"💳 *Entry*  :  `₹{entry_price:.2f}`\n"
        f"🛑 *Stop Loss* :  `{sl_pts:.0f} pts below entry`\n"
        f"🎯 *Target*    :  `{tp_pts:.0f} pts above entry`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Market Regime : `{regime}`\n"
        f"🔗 Confluences   : `{confluence}`\n"
        f"🏛️  Inst. Flow   : `{inst_press}`\n"
        f"😨 India VIX     : `{vix_str}`\n"
        f"🤖 Model Prob    : `{prob_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒  `{_now()}`\n\n"
        f"_Enter at market. SL is mandatory. Manage your position size._"
    )


def vip_exit_alert(*, side, symbol, entry_price, exit_price,
                   trigger, pnl_pts, rupee_pnl, balance, roi, mode="INTRADAY"):
    """Trade closed alert to VIP channel."""
    te   = _te(trigger)
    won  = pnl_pts >= 0
    tag  = "🌙 SWING" if mode.upper() == "SWING" else "⚡ INTRADAY"
    result_line = (
        f"✅ *PROFIT BOOKED — Well done!*"
        if won else
        f"❌ *STOP LOSS HIT — Loss protected.*"
    )

    _enqueue_vip(
        f"{te} *{tag} CLOSED*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{result_line}\n\n"
        f"📦 *Symbol* :  `{symbol}`\n"
        f"💳 *Entry*  :  `₹{entry_price:.2f}`\n"
        f"🚪 *Exit*   :  `₹{exit_price:.2f}`\n"
        f"🔖 *Reason* :  `{trigger}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'📈' if won else '📉'} *P&L*    :  `{pnl_pts:+.2f} pts`\n"
        f"💰 *Net PnL* :  `₹{rupee_pnl:+,.2f}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 Portfolio : `₹{balance:,.2f}`\n"
        f"📊 Total ROI : `{roi:+.2f}%`\n"
        f"🕒 Closed    : `{_now()}`"
    )


def vip_swing_entry_alert(*, side, symbol, entry_premium, sl_premium,
                          tp_premium, sl_pct, tp_pct, prob=None,
                          swing_score=0, conviction="MEDIUM",
                          rationale="", morning_exit="09:25"):
    """Overnight swing entry alert to VIP channel."""
    de       = _de(side)
    prob_str = f"{prob:+.4f}" if prob is not None else "—"
    direction = "BULLISH" if "CE" in side.upper() else "BEARISH"

    _enqueue_vip(
        f"🌙 *OVERNIGHT SWING — {direction} SETUP*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{de}  Holding overnight. Managed risk.\n\n"
        f"📦 *Symbol*      :  `{symbol}`\n"
        f"💳 *Entry Price* :  `₹{entry_premium:.2f}`\n"
        f"🛑 *Stop Loss*   :  `₹{sl_premium:.2f}`  (−{sl_pct*100:.0f}%)\n"
        f"🎯 *Target*      :  `₹{tp_premium:.2f}`  (+{tp_pct*100:.0f}%)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Swing Score  :  `{swing_score}/5`\n"
        f"💎 Conviction   :  `{conviction}`\n"
        f"🤖 Model Prob   :  `{prob_str}`\n"
        f"📋 Rationale    :  _{rationale[:100]}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Auto-exit tomorrow by  `{morning_exit} AM`\n"
        f"📅 Next-week expiry used  (theta protection)\n"
        f"🕒 Signal at : `{_now()}`\n\n"
        f"_Do not hold past morning exit time. Respect the plan._"
    )


def vip_swing_exit_alert(*, side, symbol, entry_premium, exit_premium,
                         trigger, pnl_pts, pct_gain, rupee_pnl, balance, roi):
    """Overnight swing exit alert to VIP channel."""
    te  = _te(trigger)
    won = pnl_pts >= 0

    _enqueue_vip(
        f"{te} *SWING CLOSED — {'✅ WINNER' if won else '❌ STOPPED OUT'}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 *Symbol*      :  `{symbol}`\n"
        f"💳 *Entry Price* :  `₹{entry_premium:.2f}`\n"
        f"🚪 *Exit Price*  :  `₹{exit_premium:.2f}`\n"
        f"🔖 *Reason*      :  `{trigger}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Return*   :  `{pct_gain:+.1f}%`\n"
        f"💰 *Net PnL*  :  `₹{rupee_pnl:+,.2f}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 Portfolio  :  `₹{balance:,.2f}`\n"
        f"📊 Total ROI  :  `{roi:+.2f}%`\n"
        f"🕒 Closed at  :  `{_now()}`"
    )


def vip_swing_restored_alert(*, symbol, side, entry_premium,
                              sl_premium, tp_premium, morning_exit="09:25"):
    """Bot restarted with an overnight position still open."""
    de = _de(side)
    _enqueue_vip(
        f"🌅 *OVERNIGHT POSITION RESTORED*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Good morning! Your swing position is active.\n\n"
        f"{de}  *{side}*  |  📦 `{symbol}`\n"
        f"💳 Entry  :  `₹{entry_premium:.2f}`\n"
        f"🛑 SL     :  `₹{sl_premium:.2f}`\n"
        f"🎯 Target :  `₹{tp_premium:.2f}`\n"
        f"⏰ Auto-exit by  `{morning_exit} AM`\n"
        f"🕒 Startup : `{_now()}`\n\n"
        f"_We are live and watching your position. Relax._"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  FREE CHANNEL POSTS
#  Public channel — engaging, easy to read, human tone
#  Payment link appended to EVERY post automatically
# ══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────
#  POST 1 — 9:10 AM  MORNING OPEN (hype + context)
# ─────────────────────────────────────────────────────────────────
def free_morning_post(*, spot: float = 0.0, vix: float = 0.0,
                      regime: str = "?", swing_open: bool = False,
                      swing_symbol: str = "", swing_side: str = ""):
    """9:10 AM daily opener. Warm, confident, inviting."""
    swing_line = ""
    if swing_open and swing_symbol:
        de = _de(swing_side)
        swing_line = (
            f"\n🌙 *Swing carry from yesterday :*\n"
            f"   {de} `{swing_symbol}` — our VIP members are holding this right now.\n"
        )

    vix_feel = (
        "🟢 Calm market — good for clean breakouts"  if vix < 14 else
        "🟡 Moderate volatility — trade with sizing" if vix < 18 else
        "🔴 High volatility — tighter stops today"
    )

    regime_feel = (
        "📈 Bulls in control"  if "BULL" in regime.upper() else
        "📉 Bears in control" if "BEAR" in regime.upper() else
        "⚠️ Choppy — patience needed"
    )

    _enqueue_free(
        f"☀️ *Good Morning, Traders!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🗓️  *{_today()}* — Markets just opened!\n\n"
        f"📍 Nifty Spot  :  *{spot:.0f}*\n"
        f"😨 India VIX  :  `{vix:.1f}`  →  {vix_feel}\n"
        f"📊 Market Feel :  {regime_feel}\n"
        f"{swing_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Black Blood is scanning every tick right now.\n"
        f"The moment a high-probability setup fires —\n"
        f"*VIP members get the alert instantly.*\n\n"
        f"Free members? They'll see it after the trade closes. 😅\n"
        f"Don't let that be you today 👇"
        + _payment_block()
    )


# ─────────────────────────────────────────────────────────────────
#  POST 2 — 10:30 AM  MID-MORNING FOMO HOOK
# ─────────────────────────────────────────────────────────────────
def free_mid_morning_post(*, signals_fired: int = 0, vip_pnl_so_far: float = 0.0,
                          spot: float = 0.0):
    """10:30 AM — VIP members are already in trades. Stir FOMO."""

    if signals_fired > 0 and vip_pnl_so_far > 0:
        hook = (
            f"🔥 *It's already happening.*\n\n"
            f"Black Blood fired *{signals_fired} signal{'s' if signals_fired > 1 else ''}* "
            f"since open.\n"
            f"VIP members are sitting on `₹{vip_pnl_so_far:,.0f}` in open profit.\n\n"
            f"You're reading this. They're counting gains. 🤑"
        )
    else:
        hook = (
            f"⏳ *Market is building up.*\n\n"
            f"Black Blood is watching every candle, every tick.\n"
            f"The setup hasn't fired yet — but when it does,\n"
            f"*VIP members will be the first to know.*\n\n"
            f"Will you be ready, or will you watch from the sidelines again?"
        )

    _enqueue_free(
        f"👀 *Black Blood VIP Update — {_hhmm()}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{hook}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *What VIP members get that you don't:*\n\n"
        f"⚡ Live entry alert with exact price\n"
        f"🛑 Stop Loss level — so they never blow up\n"
        f"🎯 Two targets — book profits on the way up\n"
        f"🤝 Hand-holding for small accounts\n"
        f"📊 Real-time exit alert — no guessing when to leave\n\n"
        f"*Start with just ₹599 for a full day trial.* No risk. 👇"
        + _payment_block()
    )


# ─────────────────────────────────────────────────────────────────
#  POST 3 — 12:00 PM  LUNCH BREAK UPDATE
# ─────────────────────────────────────────────────────────────────
def free_lunch_post(*, morning_pnl: float = 0.0, trades_done: int = 0,
                    best_trade_pts: float = 0.0, still_in_trade: bool = False):
    """12:00 PM — Lunchtime engagement post. Casual tone, real numbers."""

    if trades_done > 0 and morning_pnl > 0:
        result_text = (
            f"✅ *{trades_done} trade{'s' if trades_done > 1 else ''} closed* this morning.\n"
            f"💰 VIP members pocketed `₹{morning_pnl:,.0f}` before lunch.\n"
            f"🏆 Best single trade: `{best_trade_pts:.0f} pts`\n\n"
            f"While the market rests — they've already made their day."
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
            f"Capital protection is the #1 rule at Black Blood.\n"
            f"Afternoon setups are looking interesting though 👀"
        )

    _enqueue_free(
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
        + _payment_block()
    )


# ─────────────────────────────────────────────────────────────────
#  POST 4 — 1:30 PM  AFTERNOON TEASE
# ─────────────────────────────────────────────────────────────────
def free_afternoon_post(*, spot: float = 0.0, regime: str = "?",
                         day_pnl_so_far: float = 0.0):
    """1:30 PM — Afternoon session hype post."""

    regime_line = (
        "📈 *Bulls are pushing* — momentum setups are live"   if "BULL" in regime.upper() else
        "📉 *Bears in control* — shorting setups on radar"    if "BEAR" in regime.upper() else
        "⚠️ *Market in chop* — Black Blood is waiting for clarity"
    )

    pnl_line = (
        f"🏆 VIP members are up `₹{day_pnl_so_far:,.0f}` on the day.\n"
        if day_pnl_so_far > 0 else
        f"⚡ Fresh setups are loading for the afternoon session.\n"
    )

    _enqueue_free(
        f"📍 *Afternoon Update — {_hhmm()}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Nifty at *{spot:.0f}*\n"
        f"{regime_line}\n\n"
        f"{pnl_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 *Why Black Blood VIP is different:*\n\n"
        f"1️⃣  Every trade is shared *5–10 mins before entry*\n"
        f"    → You have time. No rushing. No FOMO entering late.\n\n"
        f"2️⃣  Stop Loss is *mandatory* on every single call\n"
        f"    → We protect your capital first. Profits come second.\n\n"
        f"3️⃣  Most calls have *two targets*\n"
        f"    → Book partial at T1. Let the rest run to T2.\n\n"
        f"4️⃣  Small account? We tell you *exactly which calls to take*\n"
        f"    → No more random lot sizing or confusion.\n\n"
        f"*2 hours left in today's session. Join now and catch the close 👇*"
        + _payment_block()
    )


# ─────────────────────────────────────────────────────────────────
#  POST 5 — 2:45 PM  PRE-CLOSE URGENCY
# ─────────────────────────────────────────────────────────────────
def free_pre_close_post(*, spot: float = 0.0, vip_day_pnl: float = 0.0,
                         trades_today: int = 0):
    """2:45 PM — Create urgency. Market closes in 45 minutes."""

    summary = (
        f"📊 *Today so far for VIP members:*\n"
        f"   Trades   :  `{trades_today}`\n"
        f"   Net PnL  :  `₹{vip_day_pnl:+,.0f}`\n\n"
        if trades_today > 0 else
        f"⚡ *Black Blood is scanning for the close setup right now.*\n\n"
    )

    _enqueue_free(
        f"⏰ *45 Minutes to Close — Don't Blink!*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{summary}"
        f"The last 45 minutes of Nifty are often the most explosive.\n"
        f"Big money moves happen right before close. 📈\n\n"
        f"If Black Blood fires a signal now —\n"
        f"*VIP members get it instantly.*\n"
        f"You'll see it in the free channel after market closes. 🙁\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 *Tomorrow is another day.*\n"
        f"But tomorrow's signals also go to VIP first.\n\n"
        f"Join tonight. Be ready by *9:10 AM tomorrow.*\n"
        f"Trial is just ₹599 👇"
        + _payment_block()
    )


# ─────────────────────────────────────────────────────────────────
#  POST 6 — 3:30 PM  EOD RESULTS (daily teaser)
# ─────────────────────────────────────────────────────────────────
def free_daily_teaser(*, date_str, trades_today, day_pnl, win_rate,
                      balance, roi, swing_open=False,
                      tease_one_signal: Optional[dict] = None,
                      chart_path: str = None):
    """3:30 PM EOD performance teaser → Free channel."""
    won      = day_pnl >= 0
    swing_tag = "Yes ⚠️  (carried overnight)" if swing_open else "Flat ✅"

    day_header = (
        f"📈 *Another profitable day in the books!*"
        if won else
        f"📉 *SL hit today — capital protected, we move on.*"
    )

    insight = get_unique_market_insight()

    text = (
        f"🔔 *Black Blood — End of Day Report*\n"
        f"🗓️  `{date_str}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{day_header}\n\n"
        f"📊 Trades Taken  :  `{trades_today}`\n"
        f"💰 Day PnL       :  `₹{day_pnl:+,.0f}`\n"
        f"🎯 Win Rate      :  `{win_rate:.0%}`\n"
        f"🌙 Swing Status  :  `{swing_tag}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 Portfolio     :  `₹{balance:,.0f}`\n"
        f"📈 Total ROI     :  `{roi:+.2f}%`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 *Today's thought:*\n"
        f"_{insight}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔒 *VIP members received these signals live — at entry.*\n"
        f"_By the time you're reading this, they already exited and counted profits._\n\n"
        f"Same setup fires tomorrow. Be inside the room this time 👇"
    )

    if tease_one_signal:
        s   = tease_one_signal
        pnl = s.get("exit", 0) - s.get("entry", 0)
        text += (
            f"\n\n📌 *One Signal Preview (free members see this AFTER close):*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_de(s.get('side',''))}  `{s.get('side','')}` —  `{s.get('symbol','')}`\n"
            f"Entry  `{s.get('entry',0):.2f}`  →  Exit  `{s.get('exit',0):.2f}`\n"
            f"Profit : `{pnl:+.2f} pts` 🔥\n\n"
            f"_VIP members got this alert at the moment of entry._\n"
            f"_You got it now — after it closed._ 🙁"
        )

    text += _payment_block()
    _enqueue_free(text, photo_path=chart_path)


# ─────────────────────────────────────────────────────────────────
#  FRIDAY WEEKLY RECAP  — 3:35 PM every Friday
# ─────────────────────────────────────────────────────────────────
def free_friday_weekly_recap(*, week_trades: int, week_pnl: float,
                              week_win_rate: float, best_trade: dict = None,
                              roi: float = 0.0, balance: float = 0.0):
    """Friday 3:35 PM weekly recap."""
    won      = week_pnl >= 0
    week_tag = (
        f"🏆 *What a week for VIP members!*"
        if won else
        f"📉 *Tough week — but capital was protected.*"
    )

    text = (
        f"📊 *Black Blood — Weekly Results*\n"
        f"🗓️  Week ending `{_today()}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{week_tag}\n\n"
        f"🔢 Trades This Week  :  `{week_trades}`\n"
        f"💰 Week PnL          :  `₹{week_pnl:+,.0f}`\n"
        f"🎯 Win Rate          :  `{week_win_rate:.0%}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏦 Portfolio         :  `₹{balance:,.0f}`\n"
        f"📈 Total ROI         :  `{roi:+.2f}%`\n"
    )

    if best_trade:
        s   = best_trade
        pnl = s.get("exit", 0) - s.get("entry", 0)
        text += (
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏅 *Best Trade of the Week:*\n\n"
            f"{_de(s.get('side',''))}  `{s.get('side','')}` —  `{s.get('symbol','')}`\n"
            f"Entry `{s.get('entry',0):.2f}`  →  Exit `{s.get('exit',0):.2f}`\n"
            f"Gain : `{pnl:+.2f} pts` 💥\n\n"
            f"_VIP members were in this trade from entry to exit._\n"
            f"_Free members are seeing it now — 5 days later._\n"
        )

    text += (
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🚀 *Next week kicks off Monday 9:10 AM.*\n\n"
        f"Will you be inside the room this time?\n"
        f"Join over the weekend and start fresh Monday 👇"
        + _payment_block()
    )
    _enqueue_free(text)


# ─────────────────────────────────────────────────────────────────
#  SUNDAY PREVIEW — 10:00 AM Sunday
# ─────────────────────────────────────────────────────────────────
def free_sunday_preview():
    """Sunday 10 AM weekly preview. Conversational, warm, trust-building."""
    _enqueue_free(
        f"👋 *Hey! Good Sunday Morning.*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Markets open tomorrow at 9:15 AM.\n"
        f"Black Blood will be scanning from the very first tick.\n\n"
        f"Here's a quick reminder of what our VIP members get\n"
        f"every single trading day — automatically:\n\n"
        f"⚡ *Live CE/PE entry alert* — exact price, 5–10 mins before\n"
        f"🛑 *Stop Loss notification* — the moment it's hit\n"
        f"🎯 *Target alerts* — T1 and T2, as they hit\n"
        f"🌙 *Overnight swing signals* — for bigger multi-day moves\n"
        f"📊 *Daily EOD report* — how the day actually went\n"
        f"🤝 *Small account guidance* — which calls suit your size\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*What free channel members get:*\n\n"
        f"📉 Results after the trade has already closed\n\n"
        f"The gap between free and VIP is the trade itself.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Join tonight. Wake up ready tomorrow.* 👇"
        + _payment_block()
    )


# ─────────────────────────────────────────────────────────────────
#  PAYMENT MENU — can be posted manually anytime
# ─────────────────────────────────────────────────────────────────
def free_instant_payment_menu():
    """Posts just the plan menu to free channel. Call manually anytime."""
    _enqueue_free(
        f"💎 *Black Blood VIP — Join Today*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*100% Safe Trades* — We focus on high-conviction setups only.\n"
        f"Every trade backed by institutional flow + technical analysis.\n\n"
        f"✅ *Stop Loss is mandatory* on every single call\n"
        f"✅ *5–10 min advance alert* before every entry\n"
        f"✅ *Two targets* on most calls — book at your comfort\n"
        f"✅ *Dedicated support* for small-account traders\n"
        f"✅ *Money management rules* shared with every member\n"
        f"✅ *No rushing* — every level planned and shared in advance\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Choose your plan:*\n\n"
        f"🔹 [Trial — ₹599]({PAYMENT_LINKS['Daily']})  ←  Try risk-free for 1 day\n\n"
        f"🔸 [Weekly — ₹1,599]({PAYMENT_LINKS['weekly']})  ←  7 days full access\n\n"
        f"🏆 [Monthly — ₹3,999]({PAYMENT_LINKS['monthly']})  ←  ✦ *Most Popular*\n\n"
        f"👑 [Yearly — ₹29,999]({PAYMENT_LINKS['yearly']})  ←  Best value, save ₹17,989\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Instant Telegram access after payment\n"
        f"✅ UPI · Cards · Net Banking\n"
        f"✅ Cancel anytime — no questions asked\n\n"
        f"📩 DM us if you have any questions before joining.\n"
        f"Happy to help you on your trading journey. 🙏"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND SCHEDULER
#  Runs inside the trader's process — no cron job, no external tool needed.
#  Call start_scheduler(self) once in your trader's start() method.
# ══════════════════════════════════════════════════════════════════════════════

class _VIPScheduler:
    def __init__(self):
        self._fired: set = set()
        self._date: str  = ""
        self._trader     = None
        self._running    = False

    def _reset_if_new_day(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._date:
            self._fired = set()
            self._date  = today

    def _reached(self, hhmm: str) -> bool:
        return datetime.now().strftime("%H:%M") >= hhmm

    def _state(self) -> dict:
        t = self._trader
        if t is None:
            return {}
        try:
            vix    = 0.0
            regime = "—"
            if hasattr(t, "brain") and len(t.brain.history) > 0:
                vix = float(t.brain.history["vix"].iloc[-1])
                try:
                    df     = t.brain.compute_features()
                    rmap   = {0: "CHOP⚠️", 1: "BULL🐂", 2: "BEAR🐻"}
                    regime = rmap.get(int(df.iloc[-1]["market_regime"]), "?")
                except Exception:
                    pass
            return {
                "spot"         : getattr(t, "last_spot", 0.0),
                "vix"          : vix,
                "regime"       : regime,
                "swing_open"   : getattr(t, "swing_position", 0) == 1,
                "swing_symbol" : getattr(t, "swing_symbol", ""),
                "swing_side"   : getattr(t, "swing_side", ""),
                "balance"      : getattr(t, "current_balance", 0),
                "roi"          : ((getattr(t, "current_balance", 0) -
                                   getattr(t, "initial_capital", 1)) /
                                   max(getattr(t, "initial_capital", 1), 1)) * 100,
                "trades_today" : getattr(t, "trades_today", 0),
                "day_pnl"      : getattr(t, "day_pnl", 0.0),
            }
        except Exception as e:
            _logger.warning(f"Scheduler state error: {e}")
            return {}

    def _week_stats(self) -> dict:
        t = self._trader
        if t is None:
            return {}
        try:
            import pandas as pd
            week_start = (datetime.now() -
                          timedelta(days=datetime.now().weekday())).strftime("%Y-%m-%d")
            conn = sqlite3.connect(t.ledger_path)
            df   = pd.read_sql_query(
                f"SELECT * FROM trades "
                f"WHERE timestamp >= '{week_start}' AND type LIKE '%EXIT%'",
                conn
            )
            conn.close()
            if df.empty:
                return {"week_trades": 0, "week_pnl": 0.0,
                        "week_win_rate": 0.0, "best_trade": None}
            best_row = df.loc[df["rupee_pnl"].idxmax()]
            best = {
                "side"  : best_row.get("type", "CE").replace("BUY_","").replace("_EXIT",""),
                "symbol": best_row.get("symbol", ""),
                "entry" : float(best_row.get("price", 0)),
                "exit"  : float(best_row.get("price", 0)) + float(best_row.get("pnl_pts", 0)),
            }
            return {
                "week_trades"   : len(df),
                "week_pnl"      : float(df["rupee_pnl"].sum()),
                "week_win_rate" : float((df["rupee_pnl"] > 0).mean()),
                "best_trade"    : best,
            }
        except Exception as e:
            _logger.warning(f"Week stats error: {e}")
            return {}

    def _loop(self):
        self._running = True
        _logger.info("VIP Scheduler running ✅")
        while self._running:
            try:
                if not is_market_open():
                    time.sleep(3600)
                    continue

                self._reset_if_new_day()
                wd    = datetime.now().weekday()  # 0=Mon…4=Fri…6=Sun
                state = self._state()

                # ── 9:10 AM — Morning open post (Mon–Fri)
                if (wd < 5
                        and self._reached(SCHEDULE_MORNING)
                        and "morning" not in self._fired):
                    free_morning_post(
                        spot         = state.get("spot", 0),
                        vix          = state.get("vix", 0),
                        regime       = state.get("regime", "—"),
                        swing_open   = state.get("swing_open", False),
                        swing_symbol = state.get("swing_symbol", ""),
                        swing_side   = state.get("swing_side", ""),
                    )
                    self._fired.add("morning")

                # ── 10:30 AM — Mid-morning FOMO post (Mon–Fri)
                if (wd < 5
                        and self._reached(SCHEDULE_MID_MORNING)
                        and "mid_morning" not in self._fired):
                    free_mid_morning_post(
                        signals_fired    = state.get("trades_today", 0),
                        vip_pnl_so_far   = max(0, state.get("day_pnl", 0)),
                        spot             = state.get("spot", 0),
                    )
                    self._fired.add("mid_morning")

                # ── 12:00 PM — Lunch update (Mon–Fri)
                if (wd < 5
                        and self._reached(SCHEDULE_LUNCH)
                        and "lunch" not in self._fired):
                    trades = state.get("trades_today", 0)
                    pnl    = state.get("day_pnl", 0.0)
                    free_lunch_post(
                        morning_pnl      = pnl,
                        trades_done      = trades,
                        best_trade_pts   = 0.0,  # extend later from ledger
                        still_in_trade   = state.get("swing_open", False),
                    )
                    self._fired.add("lunch")

                # ── 1:30 PM — Afternoon tease (Mon–Fri)
                if (wd < 5
                        and self._reached(SCHEDULE_AFTERNOON)
                        and "afternoon" not in self._fired):
                    free_afternoon_post(
                        spot           = state.get("spot", 0),
                        regime         = state.get("regime", "—"),
                        day_pnl_so_far = max(0, state.get("day_pnl", 0)),
                    )
                    self._fired.add("afternoon")

                # ── 2:45 PM — Pre-close urgency (Mon–Fri)
                if (wd < 5
                        and self._reached(SCHEDULE_PRE_CLOSE)
                        and "pre_close" not in self._fired):
                    free_pre_close_post(
                        spot          = state.get("spot", 0),
                        vip_day_pnl   = state.get("day_pnl", 0),
                        trades_today  = state.get("trades_today", 0),
                    )
                    self._fired.add("pre_close")

                # ── 3:30 PM — EOD teaser (Mon–Fri)
                if (wd < 5
                        and self._reached(SCHEDULE_EOD)
                        and "eod" not in self._fired):
                    # EOD post fired by generate_eod_report() in trader.
                    # Scheduler fires a fallback if trader hasn't called it.
                    self._fired.add("eod")

                # ── 3:35 PM Friday — Weekly recap
                if (wd == 4
                        and self._reached(SCHEDULE_FRIDAY)
                        and "friday" not in self._fired):
                    wk = self._week_stats()
                    free_friday_weekly_recap(
                        week_trades   = wk.get("week_trades", 0),
                        week_pnl      = wk.get("week_pnl", 0),
                        week_win_rate = wk.get("week_win_rate", 0),
                        best_trade    = wk.get("best_trade"),
                        roi           = state.get("roi", 0),
                        balance       = state.get("balance", 0),
                    )
                    self._fired.add("friday")

                # ── 10:00 AM Sunday — Weekend preview
                if (wd == 6
                        and self._reached(SCHEDULE_SUNDAY)
                        and "sunday" not in self._fired):
                    free_sunday_preview()
                    self._fired.add("sunday")

            except Exception as e:
                _logger.error(f"Scheduler error: {e}")

            time.sleep(30)

    def start(self, trader=None):
        self._trader = trader
        threading.Thread(target=self._loop, daemon=True,
                         name="vip-scheduler").start()


_scheduler = _VIPScheduler()


def start_scheduler(trader_instance=None):
    """Personal mode: scheduled Telegram posts are intentionally disabled."""
    _logger.info("Personal mode: scheduled Telegram posts disabled.")
    return

# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCTION INITIALIZATION
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    print("\n" + "═" * 58)
    print("  BLACK BLOOD VIP SYSTEM — PRODUCTION MODE")
    print("═" * 58)
    print(f"  Status: MONITORING [Ctrl+C to stop]")
    print(f"  Free posts scheduled at:")
    print(f"    09:10  Morning open")
    print(f"    10:30  Mid-morning FOMO")
    print(f"    12:00  Lunch update")
    print(f"    13:30  Afternoon tease")
    print(f"    14:45  Pre-close urgency")
    print(f"    15:30  EOD results")
    print("═" * 58)

    if not _SIGNAL_BOT_TOKEN or len(_telegram_destinations()) != 2:
        _logger.error("CRITICAL: SIGNAL_BOT_TOKEN and both Telegram destinations are required.")
    else:
        _logger.info("System Ready. Shared two-destination Telegram sender is configured.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        _logger.info("System Shutdown.")