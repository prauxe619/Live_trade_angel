
from __future__ import annotations

import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SingleChannelTelegramRoutingTests(unittest.TestCase):
    def read(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def assert_channel_only(self, source: str, relative: str):
        self.assertIn(
            'os.getenv("LIVE_BOT_CHANNEL_ID"',
            source,
            f"{relative} must read LIVE_BOT_CHANNEL_ID",
        )
        self.assertNotIn(
            'os.getenv("CHAT_ID"',
            source,
            f"{relative} must not fall back to private CHAT_ID",
        )

    def test_signal_engine_uses_only_the_live_channel(self):
        source = self.read("daily_signal_engine_v11.py")

        self.assertIn(
            'SIGNAL_BOT_TOKEN = os.getenv("SIGNAL_BOT_TOKEN", "").strip()',
            source,
        )
        self.assert_channel_only(source, "daily_signal_engine_v11.py")
        self.assertIn("for chat_id in TELEGRAM_CHAT_IDS:", source)
        self.assertIn(
            "len(TELEGRAM_CHAT_IDS) != 1",
            source,
            "Signal Engine must fail closed unless exactly one destination exists",
        )

    def test_vip_alert_sender_uses_only_the_live_channel(self):
        source = self.read("revenue_agent/telegram_vip_alert.py")

        self.assertIn(
            '_SIGNAL_BOT_TOKEN: str = os.getenv("SIGNAL_BOT_TOKEN", "").strip()',
            source,
        )
        self.assert_channel_only(
            source, "revenue_agent/telegram_vip_alert.py"
        )
        self.assertIn("def _telegram_destinations(", source)
        self.assertIn("def _enqueue_vip(text: str):", source)
        self.assertIn("send_telegram_message(text)", source)

        # VIP/trade alerts use the shared sender.
        # Promotional/free-channel routing remains disabled.
        self.assertIn(
            "def _enqueue_free(text: str, photo_path: str = None):",
            source,
        )
        self.assertIn("def start_scheduler(trader_instance=None):", source)

    def test_live_bridge_uses_only_one_channel_destination(self):
        source = self.read("trading_live/live_bridge.py")

        self.assertIn(
            'os.getenv("SIGNAL_BOT_TOKEN", "").strip()',
            source,
        )
        self.assert_channel_only(source, "trading_live/live_bridge.py")
        self.assertIn("for chat_id in destinations:", source)
        self.assertIn(
            "len(destinations) != 1",
            source,
            "Live Bridge must fail closed unless exactly one destination exists",
        )

        self.assertNotIn('os.getenv("VIP_CHANNEL_ID"', source)

    def test_standalone_telegram_script_uses_only_one_channel(self):
        source = self.read("scripts/telegram_alert.py")

        self.assertIn(
            'os.getenv("SIGNAL_BOT_TOKEN", "").strip()',
            source,
        )
        self.assert_channel_only(source, "scripts/telegram_alert.py")
        self.assertIn("TELEGRAM_DESTINATIONS", source)
        self.assertIn(
            "for chat_id in TELEGRAM_DESTINATIONS:",
            source,
        )

        self.assertNotIn('os.getenv("LIVE_BOT_TOKEN"', source)
        self.assertNotIn('os.getenv("BOT_TOKEN"', source)

    def test_no_sender_reads_private_chat_id_as_a_destination(self):
        files = (
            "daily_signal_engine_v11.py",
            "revenue_agent/telegram_vip_alert.py",
            "trading_live/live_bridge.py",
            "scripts/telegram_alert.py",
        )

        for relative in files:
            with self.subTest(file=relative):
                source = self.read(relative)
                self.assertNotIn(
                    'os.getenv("CHAT_ID"',
                    source,
                    f"{relative} still reads the old private chat destination",
                )

    def test_all_telegram_python_files_parse(self):
        files = (
            "daily_signal_engine_v11.py",
            "revenue_agent/telegram_vip_alert.py",
            "trading_live/live_bridge.py",
            "scripts/telegram_alert.py",
        )

        for relative in files:
            with self.subTest(file=relative):
                ast.parse(self.read(relative))


if __name__ == "__main__":
    unittest.main(verbosity=2)
