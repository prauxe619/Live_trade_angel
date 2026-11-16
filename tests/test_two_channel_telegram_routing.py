from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class TwoChannelTelegramRoutingTests(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_signal_engine_uses_signal_bot_and_exact_two_env_destinations(self):
        source = self.read("daily_signal_engine_v11.py")
        self.assertIn('SIGNAL_BOT_TOKEN = os.getenv("SIGNAL_BOT_TOKEN", "").strip()', source)
        self.assertIn('_CHAT_ID = os.getenv("CHAT_ID", "").strip()', source)
        self.assertIn('_LIVE_BOT_CHANNEL_ID = os.getenv("LIVE_BOT_CHANNEL_ID", "").strip()', source)
        self.assertIn('chat_id for chat_id in (_CHAT_ID, _LIVE_BOT_CHANNEL_ID) if chat_id', source)
        self.assertIn('for chat_id in TELEGRAM_CHAT_IDS:', source)

    def test_engine_does_not_use_single_destination_for_delivery(self):
        source = self.read("daily_signal_engine_v11.py")
        self.assertIn('TRADING_TELEGRAM_CHAT_ID = _LIVE_BOT_CHANNEL_ID', source)
        self.assertIn('if not SIGNAL_BOT_TOKEN or len(TELEGRAM_CHAT_IDS) != 2:', source)
        self.assertIn('_tq.put_nowait((SIGNAL_BOT_TOKEN, chat_id, text))', source)

    def test_collector_sender_uses_same_bot_and_two_destinations(self):
        source = self.read("revenue_agent/telegram_vip_alert.py")
        self.assertIn('_SIGNAL_BOT_TOKEN: str = os.getenv("SIGNAL_BOT_TOKEN", "").strip()', source)
        self.assertIn('_CHAT_ID: str = os.getenv("CHAT_ID", "").strip()', source)
        self.assertIn('_LIVE_BOT_CHANNEL_ID: str = os.getenv("LIVE_BOT_CHANNEL_ID", "").strip()', source)
        self.assertIn('for chat_id in (_CHAT_ID, _LIVE_BOT_CHANNEL_ID)', source)
        self.assertIn('for chat_id in destinations:', source)
        self.assertIn('del args, kwargs', source)

    def test_collector_vip_trade_alerts_use_shared_sender(self):
        source = self.read("revenue_agent/telegram_vip_alert.py")
        self.assertIn('def _enqueue_vip(text: str):', source)
        self.assertIn('send_telegram_message(text)', source)
        # Promotional/free-channel routing remains disabled.
        self.assertIn('def _enqueue_free(text: str, photo_path: str = None):', source)
        self.assertIn('def start_scheduler(trader_instance=None):', source)


    def test_live_bridge_uses_only_signal_bot_and_two_canonical_destinations(self):
        source = self.read("trading_live/live_bridge.py")
        self.assertIn('os.getenv("SIGNAL_BOT_TOKEN", "").strip()', source)
        self.assertIn('os.getenv("CHAT_ID", "").strip()', source)
        self.assertIn('os.getenv("LIVE_BOT_CHANNEL_ID", "").strip()', source)
        self.assertIn('if not token or len(destinations) != 2:', source)
        self.assertIn('for chat_id in destinations:', source)
        self.assertNotIn('os.getenv("TRADING_TELEGRAM_CHAT_ID"', source)
        self.assertNotIn('os.getenv("VIP_CHANNEL_ID"', source)

    def test_standalone_telegram_script_uses_only_canonical_credentials(self):
        source = self.read("scripts/telegram_alert.py")
        self.assertIn('os.getenv("SIGNAL_BOT_TOKEN", "").strip()', source)
        self.assertIn('os.getenv("CHAT_ID", "").strip()', source)
        self.assertIn('os.getenv("LIVE_BOT_CHANNEL_ID", "").strip()', source)
        self.assertIn('for chat_id in TELEGRAM_DESTINATIONS:', source)
        self.assertNotIn('os.getenv("LIVE_BOT_TOKEN"', source)
        self.assertNotIn('os.getenv("BOT_TOKEN"', source)
        self.assertNotIn('os.getenv("TRADING_TELEGRAM_CHAT_ID"', source)

    def test_all_telegram_python_files_parse(self):
        for relative in ("daily_signal_engine_v11.py", "revenue_agent/telegram_vip_alert.py",
                         "trading_live/live_bridge.py", "scripts/telegram_alert.py"):
            ast.parse(self.read(relative))


if __name__ == "__main__":
    unittest.main(verbosity=2)
