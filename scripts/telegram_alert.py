import requests
import os
import logging
from dotenv import load_dotenv

# Setup Logger
logger = logging.getLogger("telegram")

load_dotenv()

# ---------------------------------------------
# 🔑 ENVIRONMENT CONFIGURATION
# Automatically checks your custom live variables first, 
# with fallback support for standard names.
# ---------------------------------------------
BOT_TOKEN = os.getenv("LIVE_BOT_TOKEN", os.getenv("SIGNAL_BOT_TOKEN", os.getenv("BOT_TOKEN", "")))
CHAT_ID = os.getenv("LIVE_BOT_CHANNEL_ID", os.getenv("TRADING_TELEGRAM_CHAT_ID", os.getenv("CHAT_ID", "")))
# ---------------------------------------------

def send_telegram(message: str):
    """
    Sends a message to the configured Telegram chat.
    This is the standard function name used by Autopilot.
    """
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning(" Telegram keys missing (BOT_TOKEN or CHAT_ID). Skipping alert.")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"  # Supports bold/italic
    }
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            # Uncomment if you want logs for every message
            # logger.info(" Telegram Alert Sent!")
            pass
        else:
            logger.error(f" Failed to send Telegram: {response.text}")
            print(f" Failed to send Telegram: {response.text}")
    except Exception as e:
        logger.error(f" Connection Error: {e}")
        print(f" Connection Error: {e}")

# --- ALIASES (CRITICAL) ---
# These ensure that if a script calls 'send_alert' or 'send_telegram_message', 
# it gets routed to the main function above.
send_alert = send_telegram
send_telegram_message = send_telegram

# Test it immediately when running this file
if __name__ == "__main__":
    print(f"Bot Token: {'SET (Length: ' + str(len(BOT_TOKEN)) + ')' if BOT_TOKEN else 'MISSING'}")
    print(f"Chat ID: {CHAT_ID if CHAT_ID else 'MISSING'}")
    send_telegram("🚀 <b>Live Trader System Connected!</b>\nTesting unified channel connection...")
