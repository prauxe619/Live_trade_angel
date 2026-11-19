
import unittest

from trading_live.position_reconciler import (
    reconcile_existing,
    adopt_new_manual_positions,
    contract_key,
)


CONTRACT = {
    "exchange": "BFO",
    "symboltoken": "12345",
    "tradingsymbol": "SENSEX26111982000CE",
}

OTHER_CONTRACT = {
    "exchange": "BFO",
    "symboltoken": "99999",
    "tradingsymbol": "SENSEX26111981000CE",
}


def broker_position(qty=20, avg=125.0, contract=None, lot=20):
    return {
        "contract": dict(contract or CONTRACT),
        "net_qty": qty,
        "average_price": avg,
        "lotsize": lot,
    }


def stop_order(
    qty=20,
    trigger=110.0,
    order_id="SL-1",
    contract=None,
    status="trigger_pending",
    filled=0,
):
    return {
        "order_id": order_id,
        "contract": dict(contract or CONTRACT),
        "side": "SELL",
        "status": status,
        "order_type": "STOPLOSS_LIMIT",
        "quantity": qty,
        "filled": filled,
        "trigger_price": trigger,
        "variety": "STOPLOSS",
        "is_protective": True,
    }


def engine_position(qty=20, avg=125.0, sl=110.0):
    return {
        "signal_id": "ENGINE-1",
        "contract": dict(CONTRACT),
        "qty": qty,
        "entry_avg": avg,
        "sl_trigger": sl,
        "status": "OPEN",
    }


class ExistingPositionTests(unittest.TestCase):

    def run_plan(self, ep=None, positions=None, orders=None, complete=True):
        return reconcile_existing(
            ep or engine_position(),
            positions if positions is not None else [broker_position()],
            orders if orders is not None else [stop_order()],
            snapshot_complete=complete,
        )

    def test_unchanged_position(self):
        result = self.run_plan()
        self.assertEqual(result.decision, "UNCHANGED")
        self.assertEqual(result.broker_qty, 20)

    def test_manual_addition_combines_quantity(self):
        result = self.run_plan(
            positions=[broker_position(qty=40, avg=130.0)],
            orders=[stop_order(qty=40)],
        )
        self.assertEqual(result.decision, "UPDATE_COMBINED_POSITION")
        self.assertEqual(result.broker_qty, 40)
        self.assertEqual(result.broker_avg_price, 130.0)

    def test_manual_reduction_combines_quantity(self):
        result = self.run_plan(
            positions=[broker_position(qty=20, avg=125.0)],
            orders=[stop_order(qty=20)],
            ep=engine_position(qty=40, avg=130.0),
        )
        self.assertEqual(result.decision, "UPDATE_COMBINED_POSITION")
        self.assertEqual(result.broker_qty, 20)

    def test_quantity_change_with_old_stop_requests_resize(self):
        result = self.run_plan(
            positions=[broker_position(qty=40, avg=130.0)],
            orders=[stop_order(qty=20)],
        )
        self.assertEqual(result.decision, "RESIZE_PROTECTIVE_STOP")
        self.assertIn("CANCEL_OLD_STOP", result.actions)
        self.assertIn("CONFIRM_OLD_STOP_TERMINAL", result.actions)
        self.assertIn("RECHECK_BROKER_QTY_AND_ORDERS", result.actions)

    def test_manual_full_close_with_active_stop_cancels_stop_first(self):
        result = self.run_plan(
            positions=[broker_position(qty=0, avg=0)],
            orders=[stop_order(qty=20)],
        )
        self.assertEqual(result.decision, "CANCEL_PROTECTIVE_ORDERS")
        self.assertIn("CANCEL_EACH_PROTECTIVE_SELL_AND_CONFIRM", result.actions)

    def test_manual_full_close_only_after_stop_is_gone(self):
        result = self.run_plan(
            positions=[broker_position(qty=0, avg=0)],
            orders=[stop_order(status="cancelled")],
        )
        self.assertEqual(result.decision, "CLOSED_MANUAL")

    def test_missing_stop_uses_existing_engine_stop_price(self):
        result = self.run_plan(orders=[])
        self.assertEqual(result.decision, "CREATE_PROTECTIVE_STOP")
        self.assertIn("CREATE_STOP_FOR_BROKER_QTY", result.actions)

    def test_missing_stop_without_engine_stop_freezes(self):
        ep = engine_position(sl=0)
        result = self.run_plan(ep=ep, orders=[])
        self.assertEqual(result.decision, "FREEZE")

    def test_multiple_protective_stops_freeze(self):
        result = self.run_plan(
            orders=[stop_order(order_id="SL-1"), stop_order(order_id="SL-2")]
        )
        self.assertEqual(result.decision, "FREEZE")

    def test_stop_partial_fill_remaining_quantity_is_used(self):
        result = self.run_plan(
            positions=[broker_position(qty=10, avg=125.0, lot=10)],
            orders=[stop_order(qty=20, filled=10)],
            ep=engine_position(qty=20),
        )
        self.assertEqual(result.decision, "UPDATE_COMBINED_POSITION")
        self.assertEqual(result.broker_qty, 10)

    def test_partial_stop_fill_with_remaining_mismatch_requests_resize(self):
        result = self.run_plan(
            positions=[broker_position(qty=20, avg=125.0)],
            orders=[stop_order(qty=40, filled=10)],
        )
        self.assertEqual(result.decision, "RESIZE_PROTECTIVE_STOP")

    def test_net_short_freezes(self):
        result = self.run_plan(
            positions=[broker_position(qty=-20)],
            orders=[],
        )
        self.assertEqual(result.decision, "FREEZE")

    def test_incomplete_snapshot_freezes(self):
        result = self.run_plan(complete=False)
        self.assertEqual(result.decision, "FREEZE")

    def test_missing_position_row_does_not_mean_flat(self):
        result = self.run_plan(positions=[], orders=[])
        self.assertEqual(result.decision, "FREEZE")

    def test_duplicate_position_rows_freeze(self):
        result = self.run_plan(
            positions=[broker_position(), broker_position()]
        )
        self.assertEqual(result.decision, "FREEZE")

    def test_invalid_lot_size_freezes(self):
        result = self.run_plan(
            positions=[broker_position(qty=25, lot=20)],
            orders=[stop_order(qty=25)],
        )
        self.assertEqual(result.decision, "FREEZE")

    def test_invalid_average_price_freezes(self):
        result = self.run_plan(
            positions=[broker_position(qty=20, avg=0)],
            orders=[stop_order(qty=20)],
        )
        self.assertEqual(result.decision, "FREEZE")

    def test_invalid_stop_trigger_freezes(self):
        result = self.run_plan(
            orders=[stop_order(trigger=0)]
        )
        self.assertEqual(result.decision, "FREEZE")

    def test_missing_stop_order_id_freezes(self):
        order = stop_order()
        order["order_id"] = ""
        result = self.run_plan(orders=[order])
        self.assertEqual(result.decision, "FREEZE")

    def test_unresolved_non_stop_order_freezes(self):
        pending_buy = {
            "order_id": "BUY-PENDING",
            "contract": dict(CONTRACT),
            "side": "BUY",
            "status": "open",
            "order_type": "LIMIT",
            "quantity": 20,
            "filled": 0,
        }
        result = self.run_plan(orders=[stop_order(), pending_buy])
        self.assertEqual(result.decision, "FREEZE")

    def test_other_contract_stop_is_not_matched(self):
        result = self.run_plan(
            orders=[stop_order(contract=OTHER_CONTRACT)]
        )
        self.assertEqual(result.decision, "CREATE_PROTECTIVE_STOP")

    def test_wrong_contract_position_does_not_match(self):
        result = self.run_plan(
            positions=[broker_position(contract=OTHER_CONTRACT)],
            orders=[],
        )
        self.assertEqual(result.decision, "FREEZE")


class ManualAdoptionTests(unittest.TestCase):

    def run_adoption(self, positions=None, orders=None, complete=True):
        return adopt_new_manual_positions(
            positions if positions is not None else [broker_position()],
            orders if orders is not None else [stop_order()],
            engine_contract_keys=set(),
            snapshot_complete=complete,
        )

    def test_valid_manual_position_is_adopted(self):
        result = self.run_adoption()[0]
        self.assertEqual(result.decision, "ADOPT_MANUAL_POSITION")
        self.assertTrue(result.manual_signal_id.startswith("MANUAL-"))
        self.assertEqual(result.broker_qty, 20)
        self.assertEqual(result.stop_order_id, "SL-1")

    def test_manual_id_is_deterministic(self):
        first = self.run_adoption()[0].manual_signal_id
        second = self.run_adoption()[0].manual_signal_id
        self.assertEqual(first, second)

    def test_manual_position_without_stop_is_not_adopted(self):
        result = self.run_adoption(orders=[])[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_manual_position_with_wrong_stop_quantity_is_not_adopted(self):
        result = self.run_adoption(orders=[stop_order(qty=40)])[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_manual_position_with_multiple_stops_is_not_adopted(self):
        result = self.run_adoption(
            orders=[stop_order(), stop_order(order_id="SL-2")]
        )[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_manual_position_with_invalid_stop_price_is_not_adopted(self):
        result = self.run_adoption(orders=[stop_order(trigger=0)])[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_manual_net_short_is_not_adopted(self):
        result = self.run_adoption(
            positions=[broker_position(qty=-20)],
            orders=[],
        )[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_manual_invalid_lot_quantity_is_not_adopted(self):
        result = self.run_adoption(
            positions=[broker_position(qty=25, lot=20)],
            orders=[stop_order(qty=25)],
        )[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_existing_engine_contract_is_not_adopted_twice(self):
        key = contract_key(CONTRACT)
        result = adopt_new_manual_positions(
            [broker_position()],
            [stop_order()],
            engine_contract_keys={key},
            snapshot_complete=True,
        )
        self.assertEqual(result, [])

    def test_incomplete_account_snapshot_blocks_adoption(self):
        result = self.run_adoption(complete=False)[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_unresolved_manual_buy_blocks_adoption(self):
        pending = {
            "order_id": "BUY-1",
            "contract": dict(CONTRACT),
            "side": "BUY",
            "status": "open",
            "order_type": "LIMIT",
            "quantity": 20,
            "filled": 0,
        }
        result = self.run_adoption(
            orders=[stop_order(), pending]
        )[0]
        self.assertEqual(result.decision, "FREEZE")

    def test_duplicate_broker_position_rows_freeze(self):
        result = self.run_adoption(
            positions=[broker_position(), broker_position()]
        )
        self.assertEqual(result[0].decision, "FREEZE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
