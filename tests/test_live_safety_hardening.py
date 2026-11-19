import os
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from trading_live.angel_one_broker import AngelOneBroker, norm_status
from trading_live.live_bridge import LiveBridge
from trading_live.live_state import MemoryStore


class FakeBroker:
    def __init__(self, orders=None, net_qty=0):
        self._orders = list(orders or [])
        self._net_qty = net_qty
        self.orders_calls = 0
        self.exit_calls = 0

    def orders(self):
        self.orders_calls += 1
        return list(self._orders)

    def net_qty(self, contract):
        return self._net_qty

    def get_order(self, order_id):
        return next((o for o in self._orders if o.get("order_id") == str(order_id)), None)


class LiveSafetyHardeningTests(unittest.TestCase):
    def setUp(self):
        self.old_env = dict(os.environ)
        os.environ["TRADING_MODE"] = "live"
        os.environ["LIVE_TRADING_ACK"] = "I_UNDERSTAND_REAL_ORDERS"
        os.environ["TRADING_ENABLED"] = "false"
        os.environ["ALLOW_ORDER_SUBMISSION"] = "false"
        os.environ["LIVE_KILL_SWITCH"] = "true"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.old_env)

    def test_partial_order_status_is_active_until_cancelled(self):
        self.assertEqual(norm_status("partially executed"), "open")
        self.assertEqual(norm_status("partially filled"), "open")
        self.assertEqual(norm_status("pending"), "open")

    def test_modify_order_reuses_original_order_tag(self):
        captured = {}

        class FakeSmart:
            def modifyOrder(self, params):
                captured.update(params)
                return {"status": True, "data": {"orderid": "OID-1"}}

        broker = AngelOneBroker()
        broker.smart = FakeSmart()
        broker._login_at = time.time()
        contract = {
            "tradingsymbol": "NIFTY23NOV2625000CE",
            "symboltoken": "12345",
            "exchange": "NFO",
        }
        self.assertTrue(broker.modify_order(
            "OID-1", contract, variety="STOPLOSS", order_type="STOPLOSS_LIMIT",
            product="INTRADAY", quantity=65, price=95.0, trigger_price=98.0,
            tag="BK123456789012",
        ))
        self.assertEqual(captured.get("ordertag"), "BK123456789012")

    def test_no_live_position_does_not_claim_a_paper_signal(self):
        store = MemoryStore()
        bridge = LiveBridge(broker=FakeBroker(), store=store, sleep=lambda _: None)
        self.assertFalse(bridge.has_live_position("PAPER-SIGNAL"))
        store.put({"signal_id": "S1", "status": "ENTRY_FAILED"})
        self.assertFalse(bridge.has_live_position("S1"))
        store.put({"signal_id": "S2", "status": "UNKNOWN_ENTRY"})
        self.assertTrue(bridge.has_live_position("S2"))

    def test_unprotected_position_flattens_only_after_repeated_empty_sell_books(self):
        contract = {
            "exchange": "NFO", "symboltoken": "12345",
            "tradingsymbol": "NIFTY23NOV2625000CE",
        }
        broker = FakeBroker(orders=[], net_qty=65)
        store = MemoryStore()
        bridge = LiveBridge(broker=broker, store=store, sleep=lambda _: None)
        pos = {
            "signal_id": "S-STOP-FAIL", "tag": "BK123456789012",
            "contract": contract, "qty": 65, "status": "STOP_UNKNOWN",
            "product": "INTRADAY", "entry_avg": 100.0,
        }
        calls = []

        def fake_exit(p, reason, hint_price=0.0):
            calls.append((reason, broker._net_qty))
            return True

        bridge._exit_locked = fake_exit
        result = bridge._emergency_exit_if_unprotected(pos)
        self.assertEqual(result, True)
        self.assertEqual(broker.orders_calls, 3)
        self.assertEqual(calls, [("STOP_PROTECTION_FAILED", 65)])

    def test_unprotected_position_does_not_sell_if_an_active_sell_exists(self):
        contract = {
            "exchange": "NFO", "symboltoken": "12345",
            "tradingsymbol": "NIFTY23NOV2625000CE",
        }
        active_sell = {
            "order_id": "SELL-1", "status": "open", "side": "SELL",
            "exchange": "NFO", "symboltoken": "12345",
            "symbol": "NIFTY23NOV2625000CE", "quantity": 65,
            "filled": 0, "variety": "NORMAL", "tag": "OTHER",
        }
        broker = FakeBroker(orders=[active_sell], net_qty=65)
        bridge = LiveBridge(broker=broker, store=MemoryStore(), sleep=lambda _: None)
        pos = {"signal_id": "S2", "tag": "BK123456789012", "contract": contract,
               "qty": 65, "status": "STOP_UNKNOWN"}
        called = []
        bridge._exit_locked = lambda *args, **kwargs: called.append(True) or True
        result = bridge._emergency_exit_if_unprotected(pos)
        self.assertEqual(result["status"], "STOP_UNKNOWN")
        self.assertFalse(called)


    def test_reconcile_stop_requires_remaining_quantity_to_match_broker_position(self):
        contract = {
            "exchange": "NFO", "symboltoken": "12345",
            "tradingsymbol": "NIFTY23NOV2625000CE",
        }
        stop = {
            "order_id": "STOP-1", "status": "open", "side": "SELL",
            "exchange": "NFO", "symboltoken": "12345",
            "symbol": "NIFTY23NOV2625000CE", "quantity": 50,
            "filled": 0, "variety": "STOPLOSS", "tag": "BK123456789012",
            "order_type": "STOPLOSS_LIMIT", "trigger": 95.0, "price": 92.15,
        }
        broker = FakeBroker(orders=[stop], net_qty=65)
        bridge = LiveBridge(broker=broker, store=MemoryStore(), sleep=lambda _: None)
        pos = {"signal_id": "S4", "tag": "BK123456789012", "contract": contract,
               "qty": 65, "status": "STOP_UNKNOWN", "sl_order_id": "STOP-1",
               "sl_variety": "STOPLOSS", "product": "INTRADAY"}
        calls = []
        bridge._exit_locked = lambda p, reason, hint_price=0.0: calls.append(reason) or True
        result = bridge._reconcile_stop(pos, {"STOP-1": stop})
        self.assertTrue(result)
        self.assertEqual(calls, ["STOP_INTEGRITY_FAILURE"])

    def test_valid_stop_requires_exact_remaining_quantity_and_is_adopted(self):
        contract = {
            "exchange": "NFO", "symboltoken": "12345",
            "tradingsymbol": "NIFTY23NOV2625000CE",
        }
        stop = {
            "order_id": "STOP-2", "status": "open", "side": "SELL",
            "exchange": "NFO", "symboltoken": "12345",
            "symbol": "NIFTY23NOV2625000CE", "quantity": 65,
            "filled": 0, "variety": "STOPLOSS", "tag": "BK123456789012",
            "order_type": "STOPLOSS_LIMIT", "trigger": 95.0, "price": 92.15,
        }
        broker = FakeBroker(orders=[stop], net_qty=65)
        bridge = LiveBridge(broker=broker, store=MemoryStore(), sleep=lambda _: None)
        pos = {"signal_id": "S5", "tag": "BK123456789012", "contract": contract,
               "qty": 65, "status": "STOP_UNKNOWN", "product": "INTRADAY"}
        result = bridge._reconcile_stop(pos, {"STOP-2": stop})
        self.assertEqual(result["status"], "OPEN")
        self.assertEqual(result["sl_order_id"], "STOP-2")
        self.assertEqual(result["sl_trigger"], 95.0)

    def test_submitting_entry_can_be_reconciled_by_exit_path(self):
        store = MemoryStore()
        store.put({"signal_id": "S6", "status": "SUBMITTING"})
        bridge = LiveBridge(broker=FakeBroker(), store=store, sleep=lambda _: None)
        calls = []
        bridge._exit_locked = lambda pos, reason, hint_price=0.0: calls.append(pos["status"]) or True
        self.assertTrue(bridge.exit_position("S6", "TEST_EXIT"))
        self.assertEqual(calls, ["SUBMITTING"])

    def test_missing_exit_order_lookup_fails_closed(self):
        broker = FakeBroker(orders=[], net_qty=65)
        bridge = LiveBridge(broker=broker, store=MemoryStore(), sleep=lambda _: None)
        pos = {"signal_id": "S3", "status": "EXIT_PENDING", "exit_order_id": "EXIT-UNKNOWN"}
        self.assertTrue(bridge._has_active_exit(pos))
        self.assertEqual(pos["status"], "EXIT_PENDING")
        self.assertIn("refusing a duplicate SELL", pos["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

class LiveRiskCapTests(unittest.TestCase):
    def setUp(self):
        self.old_env = dict(os.environ)
        os.environ["TRADING_MODE"] = "live"
        os.environ["TRADING_ENABLED"] = "true"
        os.environ["LIVE_MAX_OPEN_POSITIONS"] = "1"
        os.environ["LIVE_MAX_DAILY_LOSS_INR"] = "1000"
        os.environ["LIVE_MAX_TRADE_RISK_INR"] = "700"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.old_env)

    def test_missing_live_risk_limits_fail_closed(self):
        from trading_live.risk_manager import RiskRejected, validate_order
        os.environ.pop("LIVE_MAX_OPEN_POSITIONS", None)
        with self.assertRaises(RiskRejected):
            validate_order(
                quantity=65, lot_size=65, open_positions=0,
                daily_realized_pnl=0, snapshot_age_s=1,
                signal_id="S-RISK", existing_signal_ids=set(),
                now=datetime.fromisoformat("2026-11-19T10:00:00+05:30"),
            )

    def test_open_position_limit_is_enforced(self):
        from trading_live.risk_manager import RiskRejected, validate_order
        with self.assertRaises(RiskRejected):
            validate_order(
                quantity=65, lot_size=65, open_positions=1,
                daily_realized_pnl=0, snapshot_age_s=1,
                signal_id="S-RISK", existing_signal_ids=set(),
                now=datetime.fromisoformat("2026-11-19T10:00:00+05:30"),
            )

    def test_daily_loss_limit_is_enforced(self):
        from trading_live.risk_manager import RiskRejected, validate_order
        with self.assertRaises(RiskRejected):
            validate_order(
                quantity=65, lot_size=65, open_positions=0,
                daily_realized_pnl=-1000, snapshot_age_s=1,
                signal_id="S-RISK", existing_signal_ids=set(),
                now=datetime.fromisoformat("2026-11-19T10:00:00+05:30"),
            )


    def test_open_position_risk_is_reserved_against_daily_loss_cap(self):
        from trading_live.risk_manager import RiskRejected, validate_live_extras
        # Existing open risk ₹700 + new planned risk ₹400 exceeds ₹1,000 daily cap.
        with self.assertRaises(RiskRejected):
            validate_live_extras(
                entry_price=100, stop_price=96, quantity=100,
                available_cash=100000, daily_realized_pnl=0,
                open_risk_rupees=700,
            )

    def test_per_trade_risk_cap_is_enforced(self):
        from trading_live.risk_manager import RiskRejected, validate_live_extras
        with self.assertRaises(RiskRejected):
            validate_live_extras(
                entry_price=100, stop_price=90, quantity=100,
                available_cash=100000,
            )
