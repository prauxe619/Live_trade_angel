"""Canonical two-destination Telegram notifier for trading scripts.

Only SIGNAL_BOT_TOKEN, CHAT_ID and LIVE_BOT_CHANNEL_ID are used.
Legacy token/chat variables are deliberately ignored to prevent accidental
routing through retired bots or additional channels.
"""
import logging
import os

import requests
from dotenv import load_dotenv

logger = logging.getLogger("telegram")
load_dotenv()

BOT_TOKEN = os.getenv("SIGNAL_BOT_TOKEN", "").strip()
TELEGRAM_DESTINATIONS = tuple(dict.fromkeys(
    chat for chat in (
        os.getenv("CHAT_ID", "").strip(),
        os.getenv("LIVE_BOT_CHANNEL_ID", "").strip(),
    ) if chat
))
# Compatibility name for older imports; delivery still fans out to both IDs.
CHAT_ID = TELEGRAM_DESTINATIONS[0] if TELEGRAM_DESTINATIONS else ""


def send_telegram(message: str):
    """Send the same alert to both canonical destinations; fail closed on bad config."""
    if not BOT_TOKEN or len(TELEGRAM_DESTINATIONS) != 2:
        logger.error("Telegram routing disabled: SIGNAL_BOT_TOKEN and two distinct destinations are required")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    all_ok = True
    for chat_id in TELEGRAM_DESTINATIONS:
        payload = {"chat_id": chat_id, "text": str(message)[:3900], "parse_mode": "HTML"}
        try:
            response = requests.post(url, json=payload, timeout=6)
            if response.status_code != 200:
                all_ok = False
                logger.error("Telegram delivery failed for one destination (HTTP %s): %s",
                             response.status_code, response.text[:500])
        except Exception:
            all_ok = False
            logger.exception("Telegram connection failed for one destination")
    return all_ok


# Backwards-compatible aliases used by existing callers.
send_alert = send_telegram
send_telegram_message = send_telegram


if __name__ == "__main__":
    print(f"Bot token: {'SET' if BOT_TOKEN else 'MISSING'}")
    print(f"Distinct Telegram destinations configured: {len(TELEGRAM_DESTINATIONS)} (expected 2)")
    send_telegram("🚀 <b>Live Trader System Connected!</b>\\nTesting unified two-destination connection...")
