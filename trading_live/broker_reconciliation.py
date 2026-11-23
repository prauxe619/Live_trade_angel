
"""Read-only Angel One snapshot adapter and reconciliation planner coordinator.

No order submission, cancellation, modification, or persistence occurs here.
All uncertain broker data fails closed.
"""

from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Callable

from .angel_one_broker import BrokerSafetyError
from .position_reconciler import (
    ReconcileResult,
    adopt_new_manual_positions,
    contract_key,
    reconcile_existing,
)


PROTECTIVE_TYPES = {
    "STOPLOSS_LIMIT",
    "STOPLOSS_MARKET",
    "STOP_LOSS_LIMIT",
    "STOP_LOSS_MARKET",
    "SL",
    "SL-M",
}


def _required_text(row: dict, *keys: str) -> str:
    for key in keys:
        value = str(row.get(key) or "").strip()
        if value:
            return value
    raise BrokerSafetyError(f"Missing required broker field: {'/'.join(keys)}")


def _strict_int(value, field: str) -> int:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise BrokerSafetyError(f"Invalid integer field: {field}")

    if not number.is_integer():
        raise BrokerSafetyError(f"Non-integer broker field: {field}")

    return int(number)


def _strict_float(value, field: str, *, allow_zero=False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise BrokerSafetyError(f"Invalid numeric field: {field}")

    if number != number or number in (float("inf"), float("-inf")):
        raise BrokerSafetyError(f"Non-finite broker field: {field}")

    if number < 0 or (not allow_zero and number == 0):
        raise BrokerSafetyError(f"Invalid value for broker field: {field}")

    return number


@dataclass(frozen=True)
class BrokerSnapshot:
    positions: tuple[dict, ...]
    orders: tuple[dict, ...]
    complete: bool
    error: str | None = None
    captured_at: float = 0.0


class InstrumentMasterIndex:
    """Resolve exact broker instrument identity and lot size from scrip master."""

    def __init__(self, rows: list[dict]):
        if not isinstance(rows, list):
            raise ValueError("Instrument master must be a list")

        self._by_identity = {}

        for row in rows:
            if not isinstance(row, dict):
                continue

            token = str(row.get("token") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            exchange = str(row.get("exch_seg") or "").strip().upper()

            if not token or not symbol or exchange not in {"NFO", "BFO"}:
                continue

            try:
                lot_size = _strict_int(row.get("lotsize"), "master.lotsize")
            except BrokerSafetyError:
                continue

            if lot_size <= 0:
                continue

            identity = (exchange, token, symbol)

            # Duplicate identities are ambiguous and must not be resolved.
            if identity in self._by_identity:
                self._by_identity[identity] = None
            else:
                self._by_identity[identity] = {
                    "exchange": exchange,
                    "symboltoken": token,
                    "tradingsymbol": symbol,
                    "lotsize": lot_size,
                }

    def resolve(self, exchange: str, token: str, symbol: str) -> dict:
        identity = (
            str(exchange or "").strip().upper(),
            str(token or "").strip(),
            str(symbol or "").strip(),
        )

        contract = self._by_identity.get(identity)

        if contract is None:
            raise BrokerSafetyError(
                f"Instrument identity missing or ambiguous in master: {identity}"
            )

        return dict(contract)


class AngelOneReadOnlySnapshotAdapter:
    """Convert broker API rows into the planner's strict normalized schema."""

    def __init__(
        self,
        broker,
        instrument_master: InstrumentMasterIndex,
        *,
        average_price_field: str = "avgnetprice",
    ):
        self.broker = broker
        self.master = instrument_master
        self.average_price_field = average_price_field

    def _normalise_position(self, row: dict) -> dict:
        if not isinstance(row, dict):
            raise BrokerSafetyError("Malformed position row")

        exchange = _required_text(row, "exchange").upper()
        token = _required_text(row, "symboltoken")
        symbol = _required_text(row, "tradingsymbol", "symbol")

        contract = self.master.resolve(exchange, token, symbol)

        qty = _strict_int(row.get("netqty"), "position.netqty")

        if qty == 0:
            average = 0.0
        else:
            average = _strict_float(
                row.get(self.average_price_field),
                f"position.{self.average_price_field}",
            )

        return {
            "contract": contract,
            "net_qty": qty,
            "average_price": average,
            "lotsize": contract["lotsize"],
        }

    @staticmethod
    def _normalise_order(row: dict) -> dict:
        if not isinstance(row, dict):
            raise BrokerSafetyError("Malformed order row")

        exchange = _required_text(row, "exchange").upper()
        token = _required_text(row, "symboltoken")
        symbol = _required_text(row, "symbol", "tradingsymbol")

        side = _required_text(row, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise BrokerSafetyError("Unknown broker order side")

        order_type = str(row.get("order_type") or "").strip().upper()
        variety = str(row.get("variety") or "").strip().upper()
        raw_status = str(row.get("status") or "").strip().lower()

        known_statuses = {
            "open",
            "trigger_pending",
            "pending",
            "modify_pending",
            "unknown",
            "complete",
            "cancelled",
            "rejected",
        }

        status = raw_status if raw_status in known_statuses else "unknown"

        order_id = _required_text(row, "order_id")
        quantity = _strict_int(row.get("quantity"), "order.quantity")
        filled = _strict_int(row.get("filled", 0), "order.filled")
        trigger = _strict_float(
            row.get("trigger", 0),
            "order.trigger",
            allow_zero=True,
        )

        if quantity <= 0 or filled < 0 or filled > quantity:
            raise BrokerSafetyError("Invalid broker order quantity/fill")

        is_protective = (
            side == "SELL"
            and (
                variety == "STOPLOSS"
                or order_type in PROTECTIVE_TYPES
            )
        )

        return {
            "order_id": order_id,
            "contract": {
                "exchange": exchange,
                "symboltoken": token,
                "tradingsymbol": symbol,
            },
            "side": side,
            "status": status,
            "order_type": order_type,
            "quantity": quantity,
            "filled": filled,
            "trigger_price": trigger,
            "variety": variety,
            "is_protective": is_protective,
        }

    def read_snapshot(self) -> BrokerSnapshot:
        """
        Read broker positions and orders.
    
        Either read failing invalidates the complete snapshot.
        """
        captured_at = time.time()
    
        try:
            raw_positions = self.broker.position_book()
    
            # Force a fresh broker order book for account reconciliation.
            raw_orders = self.broker.orders(force_refresh=True)
    
            if not isinstance(raw_positions, list):
                raise BrokerSafetyError(
                    "Position book is not a list"
                )
    
            if not isinstance(raw_orders, list):
                raise BrokerSafetyError(
                    "Order book is not a list"
                )
    
            positions = tuple(
                self._normalise_position(row)
                for row in raw_positions
                if str(row.get("exchange") or "").upper() in {"NFO", "BFO"}
            )
    
            orders = tuple(
                self._normalise_order(row)
                for row in raw_orders
                if str(row.get("exchange") or "").upper() in {"NFO", "BFO"}
            )
    
            return BrokerSnapshot(
                positions=positions,
                orders=orders,
                complete=True,
                captured_at=captured_at,
            )
    
        except Exception as exc:
            return BrokerSnapshot(
                positions=(),
                orders=(),
                complete=False,
                error=f"{type(exc).__name__}: {exc}",
                captured_at=captured_at,
            )



class ReadOnlyReconciliationCoordinator:
    """Generate plans only. Never mutates broker, store, or engine state."""

    def __init__(self, *, adapter, store):
        self.adapter = adapter
        self.store = store

    def plan(self) -> list[ReconcileResult]:
        snapshot = self.adapter.read_snapshot()

        # Fail closed when either broker data source is unavailable.
        if not snapshot.complete:
            return [
                ReconcileResult(
                    decision="FREEZE",
                    contract_key="ACCOUNT",
                    reason=f"Broker snapshot unavailable: {snapshot.error}",
                    actions=("ALERT_AND_BLOCK_NEW_ACTIVITY",),
                )
            ]

        # Reject snapshots that are stale or have invalid timestamps.
        snapshot_age = time.time() - snapshot.captured_at

        if snapshot_age < 0 or snapshot_age > 5:
            return [
                ReconcileResult(
                    decision="FREEZE",
                    contract_key="ACCOUNT",
                    reason=f"Broker snapshot is stale: {snapshot_age:.2f}s",
                    actions=("ALERT_AND_BLOCK_NEW_ACTIVITY",),
                )
            ]

        engine_positions = [
            row for row in self.store.list_all()
            if row.get("status") in {
                "SUBMITTING",
                "UNKNOWN_ENTRY",
                "OPEN",
                "STOP_UNKNOWN",
                "EXITING",
                "EXIT_PENDING",
                "EXIT_BLOCKED",
            }
        ]

        engine_by_key = {}
        invalid_engine_rows = []

        for position in engine_positions:
            try:
                key = contract_key(position.get("contract") or {})
            except (TypeError, ValueError):
                invalid_engine_rows.append(
                    ReconcileResult(
                        decision="FREEZE",
                        contract_key="INVALID-ENGINE-CONTRACT",
                        reason="Engine position has invalid contract identity",
                        actions=("ALERT_AND_BLOCK_CONTRACT",),
                    )
                )
                continue

            engine_by_key.setdefault(key, []).append(position)

        results = list(invalid_engine_rows)

        # Duplicate engine records for the same contract are ambiguous.
        duplicate_keys = {
            key for key, rows in engine_by_key.items()
            if len(rows) != 1
        }

        for key in duplicate_keys:
            results.append(
                ReconcileResult(
                    decision="FREEZE",
                    contract_key=key,
                    reason="Multiple active engine records share one contract",
                    actions=("ALERT_AND_BLOCK_CONTRACT",),
                )
            )

        for key, rows in engine_by_key.items():
            if key in duplicate_keys:
                continue

            position = rows[0]

            if position.get("status") != "OPEN":
                results.append(
                    ReconcileResult(
                        decision="FREEZE",
                        contract_key=key,
                        reason=(
                            f"Engine lifecycle state {position.get('status')} "
                            "requires its dedicated recovery path"
                        ),
                        actions=("ALERT_AND_BLOCK_CONTRACT",),
                    )
                )
                continue

            results.append(
                reconcile_existing(
                    position,
                    list(snapshot.positions),
                    list(snapshot.orders),
                    snapshot_complete=True,
                )
            )

        results.extend(
            adopt_new_manual_positions(
                list(snapshot.positions),
                list(snapshot.orders),
                engine_contract_keys=set(engine_by_key),
                snapshot_complete=True,
            )
        )

        return results


    def __init__(self, *, adapter, store):
        self.adapter = adapter
        self.store = store

    def plan(self) -> list[ReconcileResult]:
        snapshot = self.adapter.read_snapshot()

        if not snapshot.complete:
            return [
                ReconcileResult(
                    decision="FREEZE",
                    contract_key="ACCOUNT",
                    reason=f"Broker snapshot unavailable: {snapshot.error}",
                    actions=("ALERT_AND_BLOCK_NEW_ACTIVITY",),
                )
            ]

        engine_positions = [
            row for row in self.store.list_all()
            if row.get("status") in {
                "SUBMITTING",
                "UNKNOWN_ENTRY",
                "OPEN",
                "STOP_UNKNOWN",
                "EXITING",
                "EXIT_PENDING",
                "EXIT_BLOCKED",
            }
        ]

        engine_by_key = {}
        invalid_engine_rows = []

        for position in engine_positions:
            try:
                key = contract_key(position.get("contract") or {})
            except (TypeError, ValueError):
                invalid_engine_rows.append(
                    ReconcileResult(
                        decision="FREEZE",
                        contract_key="INVALID-ENGINE-CONTRACT",
                        reason="Engine position has invalid contract identity",
                        actions=("ALERT_AND_BLOCK_CONTRACT",),
                    )
                )
                continue

            engine_by_key.setdefault(key, []).append(position)

        results = list(invalid_engine_rows)

        # More than one engine record for the same exact contract is ambiguous.
        duplicate_keys = {
            key for key, rows in engine_by_key.items() if len(rows) != 1
        }

        for key in duplicate_keys:
            results.append(
                ReconcileResult(
                    decision="FREEZE",
                    contract_key=key,
                    reason="Multiple active engine records share one contract",
                    actions=("ALERT_AND_BLOCK_CONTRACT",),
                )
            )

        for key, rows in engine_by_key.items():
            if key in duplicate_keys:
                continue

            position = rows[0]

            # Do not silently reconcile uncertain entry/exit lifecycle states
            # as ordinary OPEN positions.
            if position.get("status") != "OPEN":
                results.append(
                    ReconcileResult(
                        decision="FREEZE",
                        contract_key=key,
                        reason=(
                            f"Engine lifecycle state {position.get('status')} "
                            "requires its dedicated recovery path"
                        ),
                        actions=("ALERT_AND_BLOCK_CONTRACT",),
                    )
                )
                continue

            results.append(
                reconcile_existing(
                    position,
                    list(snapshot.positions),
                    list(snapshot.orders),
                    snapshot_complete=True,
                )
            )

        results.extend(
            adopt_new_manual_positions(
                list(snapshot.positions),
                list(snapshot.orders),
                engine_contract_keys=set(engine_by_key),
                snapshot_complete=True,
            )
        )

        return results
