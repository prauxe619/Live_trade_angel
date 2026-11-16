from __future__ import annotations

import ast
import importlib.util
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]


class Ec2PipelineRegressionTests(unittest.TestCase):
    def read(self, relative):
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_option_premium_is_fetcher_method(self):
        tree = ast.parse(self.read("daily_signal_engine_v11.py"))
        fetcher = next(
            node for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "SignalDataFetcher"
        )
        self.assertIn(
            "get_option_premium",
            {node.name for node in fetcher.body if isinstance(node, ast.FunctionDef)},
        )
        self.assertNotIn(
            "get_option_premium",
            {node.name for node in tree.body if isinstance(node, ast.FunctionDef)},
        )

    def test_iron_condor_daily_state_is_initialized_and_persisted(self):
        source = self.read("daily_signal_engine_v11.py")
        self.assertIn("self._ic_fired_today = set()", source)
        self.assertIn('"ic_fired_today": list(self._ic_fired_today)', source)
        self.assertIn('self._ic_fired_today = set(st.get("ic_fired_today", []))', source)

    def test_scheduler_scan_1400_key_exists(self):
        source = self.read("daily_signal_engine_v11.py")
        self.assertIn('"scan_1400"', source)
        self.assertIn('SCANS["scan_1400"]', source)

    def test_all_four_collectors_configure_futures_stream(self):
        expected = {
            "collectors/nifty50_collector_v4.py": "exchange_type=2",
            "collectors/banknifty_collector_v1.py": "exchange_type=2",
            "collectors/finnifty_collector_v1.py": "exchange_type=2",
            "collectors/sensex_collector_v1.py": "exchange_type=4",
        }
        for path, exchange_type in expected.items():
            source = self.read(path)
            self.assertIn("set_futures_contract", source, path)
            self.assertIn(exchange_type, source, path)
            self.assertIn("update_credentials(self.jwt_token, self.feed_token)", source, path)
        fin = self.read("collectors/finnifty_collector_v1.py")
        self.assertIn('"--account", "C"', fin)

    def test_engine_uses_separate_futures_volume(self):
        source = self.read("daily_signal_engine_v11.py")
        self.assertIn("get_recent_futures_candles", source)
        self.assertIn('breakout_candles["volume"] = pd.to_numeric(', source)
        self.assertIn('breakout_candles["futures_volume"]', source)

    def test_shared_futures_reader_is_available(self):
        source = self.read("collectors/scripts/shared_snapshots.py")
        self.assertIn("def get_recent_futures_candles(", source)
        self.assertIn("market_futures_ohlcv_candles", source)

    def test_spot_aggregator_never_invents_volume_and_futures_volume_accumulates(self):
        module_path = ROOT / "collectors/scripts/ohlcv_stream.py"
        spec = importlib.util.spec_from_file_location("ec2_ohlcv_stream_test", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        ist = ZoneInfo("Asia/Kolkata")
        tick_time = (datetime.now(ist) - timedelta(seconds=20)).replace(second=10, microsecond=0)

        spot = module.MinuteOHLCVAggregator("NIFTY")
        spot.add_tick(24000, tick_time)
        self.assertIsNone(spot.current["volume"])

        futures = module.MinuteOHLCVAggregator("NIFTY:FUTURES")
        futures.add_tick(24010, tick_time, volume_delta=0)
        futures.add_tick(24011, tick_time + timedelta(seconds=1), volume_delta=12)
        futures.add_tick(24012, tick_time + timedelta(seconds=2), volume_delta=8)
        self.assertEqual(futures.current["volume"], 20.0)
        self.assertEqual(futures.current["tick_count"], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
