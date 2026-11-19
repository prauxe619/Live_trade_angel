
import pytest

from trading_live.angel_one_broker import BrokerSafetyError
from trading_live.broker_reconciliation import (
    AngelOneReadOnlySnapshotAdapter,
    InstrumentMasterIndex,
    ReadOnlyReconciliationCoordinator,
)


CONTRACT = {
    "exchange": "BFO",
    "symboltoken": "12345",
    "tradingsymbol": "SENSEX26NOV82000CE",
    "lotsize": 20,
}

MASTER = [{
    "exch_seg": "BFO",
    "token": "12345",
    "symbol": "SENSEX26NOV82000CE",
    "lotsize": "20",
}]


def raw_position(qty=20, avg=100):
    return {
        "exchange": "BFO",
        "symboltoken": "12345",
        "tradingsymbol": "SENSEX26NOV82000CE",
        "netqty": str(qty),
        "avgnetprice": str(avg),
    }


def raw_stop(qty=20, filled=0):
    return {
        "order_id": "STOP-1",
        "exchange": "BFO",
        "symboltoken": "12345",
        "symbol": "SENSEX26NOV82000CE",
        "side": "SELL",
        "status": "trigger_pending",
        "order_type": "STOPLOSS_LIMIT",
        "quantity": qty,
        "filled": filled,
        "trigger": 90,
        "variety": "STOPLOSS",
    }


class FakeBroker:
    def __init__(self, positions=None, orders=None, fail_positions=False,
                 fail_orders=False):
        self.positions = positions or []
        self.order_rows = orders or []
        self.fail_positions = fail_positions
        self.fail_orders = fail_orders
        self.write_calls = []

    def position_book(self):
        if self.fail_positions:
            raise BrokerSafetyError("position endpoint unavailable")
        return self.positions

    def orders(self):
        if self.fail_orders:
            raise BrokerSafetyError("order endpoint unavailable")
        return self.order_rows

    def place_order(self, *args, **kwargs):
        self.write_calls.append(("place_order", args, kwargs))
        raise AssertionError("Read-only reconciliation attempted an order")

    def modify_order(self, *args, **kwargs):
        self.write_calls.append(("modify_order", args, kwargs))
        raise AssertionError("Read-only reconciliation attempted an order")

    def cancel_order(self, *args, **kwargs):
        self.write_calls.append(("cancel_order", args, kwargs))
        raise AssertionError("Read-only reconciliation attempted an order")


class FakeStore:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.write_calls = []

    def list_all(self):
        return [dict(row) for row in self.rows]

    def put(self, row):
        self.write_calls.append(("put", row))
        raise AssertionError("Read-only coordinator attempted persistence")


def engine_position(qty=20, avg=100, status="OPEN"):
    return {
        "signal_id": "ENGINE-1",
        "status": status,
        "contract": dict(CONTRACT),
        "qty": qty,
        "entry_avg": avg,
        "sl_trigger": 90,
        "sl_planned": 90,
    }


def make_adapter(broker, master_rows=MASTER):
    index = InstrumentMasterIndex(master_rows)
    return AngelOneReadOnlySnapshotAdapter(broker, index)


def test_valid_position_snapshot_is_normalized():
    broker = FakeBroker([raw_position()], [raw_stop()])
    snapshot = make_adapter(broker).read_snapshot()

    assert snapshot.complete is True
    assert snapshot.positions[0]["contract"] == CONTRACT
    assert snapshot.positions[0]["net_qty"] == 20
    assert snapshot.positions[0]["average_price"] == 100
    assert snapshot.positions[0]["lotsize"] == 20


def test_position_endpoint_failure_invalidates_whole_snapshot():
    broker = FakeBroker(fail_positions=True)
    snapshot = make_adapter(broker).read_snapshot()

    assert snapshot.complete is False
    assert snapshot.positions == ()
    assert snapshot.orders == ()
    assert "position endpoint unavailable" in snapshot.error


def test_order_endpoint_failure_invalidates_whole_snapshot():
    broker = FakeBroker([raw_position()], fail_orders=True)
    snapshot = make_adapter(broker).read_snapshot()

    assert snapshot.complete is False
    assert snapshot.positions == ()
    assert snapshot.orders == ()


def test_missing_position_token_fails_closed():
    row = raw_position()
    row.pop("symboltoken")

    snapshot = make_adapter(FakeBroker([row], [raw_stop()])).read_snapshot()

    assert snapshot.complete is False
    assert "symboltoken" in snapshot.error


def test_unknown_instrument_identity_fails_closed():
    row = raw_position()
    row["symboltoken"] = "WRONG"

    snapshot = make_adapter(FakeBroker([row], [raw_stop()])).read_snapshot()

    assert snapshot.complete is False
    assert "missing or ambiguous" in snapshot.error


def test_duplicate_master_identity_is_ambiguous():
    duplicate_master = MASTER + [dict(MASTER[0])]
    index = InstrumentMasterIndex(duplicate_master)

    with pytest.raises(BrokerSafetyError):
        index.resolve("BFO", "12345", "SENSEX26NOV82000CE")


def test_missing_average_price_fails_closed():
    row = raw_position()
    row.pop("avgnetprice")

    snapshot = make_adapter(FakeBroker([row], [raw_stop()])).read_snapshot()

    assert snapshot.complete is False
    assert "avgnetprice" in snapshot.error


def test_non_integer_net_quantity_fails_closed():
    row = raw_position()
    row["netqty"] = "20.5"

    snapshot = make_adapter(FakeBroker([row], [raw_stop()])).read_snapshot()

    assert snapshot.complete is False


def test_zero_quantity_does_not_require_average_price():
    row = raw_position(qty=0, avg=0)
    row.pop("avgnetprice")

    snapshot = make_adapter(FakeBroker([row], [])).read_snapshot()

    assert snapshot.complete is True
    assert snapshot.positions[0]["net_qty"] == 0
    assert snapshot.positions[0]["average_price"] == 0


def test_stop_order_is_normalized_as_protective():
    snapshot = make_adapter(
        FakeBroker([raw_position()], [raw_stop()])
    ).read_snapshot()

    stop = snapshot.orders[0]

    assert stop["is_protective"] is True
    assert stop["side"] == "SELL"
    assert stop["trigger_price"] == 90
    assert stop["quantity"] == 20
    assert stop["filled"] == 0



def test_unknown_order_status_is_preserved_as_unknown():
    order = raw_stop()
    order["status"] = "some-new-broker-status"

    snapshot = make_adapter(
        FakeBroker([raw_position()], [order])
    ).read_snapshot()

    assert snapshot.complete is True
    assert snapshot.orders[0]["status"] == "unknown"



def test_invalid_order_fill_fails_closed():
    order = raw_stop(qty=20, filled=21)

    snapshot = make_adapter(
        FakeBroker([raw_position()], [order])
    ).read_snapshot()

    assert snapshot.complete is False


def test_coordinator_returns_unchanged_for_matching_position():
    broker = FakeBroker([raw_position()], [raw_stop()])
    store = FakeStore([engine_position()])
    coordinator = ReadOnlyReconciliationCoordinator(
        adapter=make_adapter(broker),
        store=store,
    )

    results = coordinator.plan()

    assert len(results) == 1
    assert results[0].decision == "UNCHANGED"


def test_coordinator_updates_combined_quantity_from_broker():
    broker = FakeBroker([raw_position(qty=40, avg=105)], [raw_stop(qty=40)])
    store = FakeStore([engine_position(qty=20, avg=100)])
    coordinator = ReadOnlyReconciliationCoordinator(
        adapter=make_adapter(broker),
        store=store,
    )

    results = coordinator.plan()

    assert results[0].decision == "UPDATE_COMBINED_POSITION"
    assert results[0].broker_qty == 40
    assert results[0].broker_avg_price == 105


def test_coordinator_freezes_when_snapshot_is_incomplete():
    broker = FakeBroker(fail_orders=True)
    store = FakeStore([engine_position()])
    coordinator = ReadOnlyReconciliationCoordinator(
        adapter=make_adapter(broker),
        store=store,
    )

    results = coordinator.plan()

    assert len(results) == 1
    assert results[0].decision == "FREEZE"
    assert results[0].contract_key == "ACCOUNT"


def test_non_open_engine_state_uses_dedicated_recovery_path():
    broker = FakeBroker([raw_position()], [raw_stop()])
    store = FakeStore([engine_position(status="UNKNOWN_ENTRY")])
    coordinator = ReadOnlyReconciliationCoordinator(
        adapter=make_adapter(broker),
        store=store,
    )

    results = coordinator.plan()

    assert results[0].decision == "FREEZE"
    assert "dedicated recovery path" in results[0].reason


def test_duplicate_engine_records_freeze_contract():
    broker = FakeBroker([raw_position()], [raw_stop()])
    store = FakeStore([
        engine_position(),
        dict(engine_position(), signal_id="ENGINE-2"),
    ])
    coordinator = ReadOnlyReconciliationCoordinator(
        adapter=make_adapter(broker),
        store=store,
    )

    results = coordinator.plan()

    assert len(results) == 1
    assert results[0].decision == "FREEZE"
    assert "Multiple active engine records" in results[0].reason


def test_read_only_layer_never_calls_broker_or_store_writes():
    broker = FakeBroker([raw_position()], [raw_stop()])
    store = FakeStore([engine_position()])
    coordinator = ReadOnlyReconciliationCoordinator(
        adapter=make_adapter(broker),
        store=store,
    )

    coordinator.plan()

    assert broker.write_calls == []
    assert store.write_calls == []
