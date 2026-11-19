
from __future__ import annotations

import pytest

from trading_live.live_state import MemoryStore
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
    "tradingsymbol": "SENSEX26111982000PE",
}


def position(qty=20, avg=100.0, contract=None, lotsize=20):
    return {
        "contract": dict(contract or CONTRACT),
        "net_qty": qty,
        "average_price": avg,
        "lotsize": lotsize,
    }


def stop(qty=20, trigger=80.0, order_id="STOP-1", filled=0,
         contract=None, status="trigger_pending"):
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


def engine_position(qty=20, avg=100.0, contract=None):
    return {
        "signal_id": "ENGINE-001",
        "contract": dict(contract or CONTRACT),
        "qty": qty,
        "entry_avg": avg,
        "sl_trigger": 80.0,
        "status": "OPEN",
    }


class TestManualPositionReconciliationIntegration:
    """Planner + in-memory state integration. No broker/API calls."""

    def test_manual_addition_plans_combined_quantity_update(self):
        result = reconcile_existing(
            engine_position(qty=20, avg=100),
            [position(qty=40, avg=110)],
            [stop(qty=40)],
            snapshot_complete=True,
        )

        assert result.decision == "UPDATE_COMBINED_POSITION"
        assert result.broker_qty == 40
        assert result.broker_avg_price == 110

    def test_manual_reduction_plans_stop_resize(self):
        result = reconcile_existing(
            engine_position(qty=40),
            [position(qty=20)],
            [stop(qty=40)],
            snapshot_complete=True,
        )

        assert result.decision == "RESIZE_PROTECTIVE_STOP"
        assert result.broker_qty == 20
        assert result.stop_order_id == "STOP-1"

    def test_unchanged_position_is_unchanged(self):
        result = reconcile_existing(
            engine_position(qty=20, avg=100),
            [position(qty=20, avg=100)],
            [stop(qty=20)],
            snapshot_complete=True,
        )

        assert result.decision == "UNCHANGED"

    def test_incomplete_snapshot_freezes(self):
        result = reconcile_existing(
            engine_position(),
            [position()],
            [stop()],
            snapshot_complete=False,
        )

        assert result.decision == "FREEZE"
        assert "ALERT_AND_BLOCK_CONTRACT" in result.actions

    def test_missing_position_row_does_not_assume_flat(self):
        result = reconcile_existing(
            engine_position(),
            [],
            [],
            snapshot_complete=True,
        )

        assert result.decision == "FREEZE"

    def test_net_short_position_freezes(self):
        result = reconcile_existing(
            engine_position(),
            [position(qty=-20)],
            [],
            snapshot_complete=True,
        )

        assert result.decision == "FREEZE"

    def test_duplicate_position_rows_freeze(self):
        result = reconcile_existing(
            engine_position(),
            [position(), position()],
            [stop()],
            snapshot_complete=True,
        )

        assert result.decision == "FREEZE"

    def test_multiple_protective_stops_freeze(self):
        result = reconcile_existing(
            engine_position(),
            [position()],
            [stop(order_id="STOP-1"), stop(order_id="STOP-2")],
            snapshot_complete=True,
        )

        assert result.decision == "FREEZE"

    
    def test_partial_stop_fill_uses_remaining_quantity(self):
        result = reconcile_existing(
            engine_position(qty=20, avg=100),
            [position(qty=20, avg=100)],
            [stop(qty=40, filled=20)],
            snapshot_complete=True,
        )

        assert result.decision == "UNCHANGED"
        assert result.broker_qty == 20
        assert result.stop_order_id == "STOP-1"


    def test_flat_position_with_active_stop_requests_cancellation(self):
        result = reconcile_existing(
            engine_position(),
            [position(qty=0)],
            [stop(qty=20)],
            snapshot_complete=True,
        )

        assert result.decision == "CANCEL_PROTECTIVE_ORDERS"

    def test_flat_position_without_active_stop_closes_manually(self):
        result = reconcile_existing(
            engine_position(),
            [position(qty=0)],
            [],
            snapshot_complete=True,
        )

        assert result.decision == "CLOSED_MANUAL"

    def test_new_manual_position_without_stop_is_not_adopted(self):
        results = adopt_new_manual_positions(
            [position()],
            [],
            engine_contract_keys=set(),
            snapshot_complete=True,
        )

        assert len(results) == 1
        assert results[0].decision == "FREEZE"

    def test_new_manual_position_with_matching_stop_is_adopted(self):
        results = adopt_new_manual_positions(
            [position()],
            [stop()],
            engine_contract_keys=set(),
            snapshot_complete=True,
        )

        assert len(results) == 1
        result = results[0]
        assert result.decision == "ADOPT_MANUAL_POSITION"
        assert result.broker_qty == 20
        assert result.stop_order_id == "STOP-1"
        assert result.manual_signal_id.startswith("MANUAL-")

    def test_manual_net_short_is_not_adopted(self):
        results = adopt_new_manual_positions(
            [position(qty=-20)],
            [],
            engine_contract_keys=set(),
            snapshot_complete=True,
        )

        assert results[0].decision == "FREEZE"

    def test_manual_position_with_wrong_stop_quantity_is_not_adopted(self):
        results = adopt_new_manual_positions(
            [position(qty=40)],
            [stop(qty=20)],
            engine_contract_keys=set(),
            snapshot_complete=True,
        )

        assert results[0].decision == "FREEZE"

    def test_duplicate_manual_position_rows_freeze_before_adoption(self):
        results = adopt_new_manual_positions(
            [position(), position()],
            [stop()],
            engine_contract_keys=set(),
            snapshot_complete=True,
        )

        assert len(results) == 1
        assert results[0].decision == "FREEZE"
        assert "Duplicate" in results[0].reason

    def test_existing_engine_contract_is_not_adopted_twice(self):
        key = contract_key(CONTRACT)

        results = adopt_new_manual_positions(
            [position()],
            [stop()],
            engine_contract_keys={key},
            snapshot_complete=True,
        )

        assert results == []

    def test_incomplete_account_snapshot_blocks_all_adoption(self):
        results = adopt_new_manual_positions(
            [position()],
            [stop()],
            engine_contract_keys=set(),
            snapshot_complete=False,
        )

        assert len(results) == 1
        assert results[0].decision == "FREEZE"

    def test_memory_store_persists_combined_position(self):
        store = MemoryStore()

        pos = engine_position(qty=20, avg=100)
        store.put(pos)

        updated = store.get("ENGINE-001")
        updated["qty"] = 40
        updated["entry_avg"] = 110
        store.put(updated)

        saved = store.get("ENGINE-001")
        assert saved["qty"] == 40
        assert saved["entry_avg"] == 110

    def test_memory_store_returns_independent_copies(self):
        store = MemoryStore()
        store.put(engine_position())

        first = store.get("ENGINE-001")
        first["qty"] = 999

        assert store.get("ENGINE-001")["qty"] == 20

    def test_memory_store_lists_saved_position(self):
        store = MemoryStore()
        store.put(engine_position())

        rows = store.list_all()

        assert len(rows) == 1
        assert rows[0]["signal_id"] == "ENGINE-001"
