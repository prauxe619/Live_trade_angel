
"""Broker-first manual position reconciliation.

This module is intentionally side-effect free:
- no Angel One SDK calls
- no order placement/cancellation
- no database writes
- no environment changes

The broker adapter must supply the normalized schemas documented below.
All ambiguous or incomplete states fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from hashlib import sha256
from math import isfinite
from typing import Any


ACTIVE_ORDER_STATUSES = {
    "open", "trigger_pending", "pending", "modify_pending", "unknown",
}
TERMINAL_ORDER_STATUSES = {"complete", "cancelled", "rejected"}

PROTECTIVE_ORDER_TYPES = {
    "STOPLOSS_LIMIT", "STOPLOSS_MARKET", "STOP_LOSS_LIMIT",
    "STOP_LOSS_MARKET", "SL", "SL-M",
}


@dataclass(frozen=True)
class ReconcileResult:
    decision: str
    contract_key: str
    reason: str
    actions: tuple[str, ...] = ()
    broker_qty: int | None = None
    broker_avg_price: float | None = None
    stop_order_id: str | None = None
    stop_trigger_price: float | None = None
    manual_signal_id: str | None = None

    def to_dict(self) -> dict:
        result = asdict(self)
        result["actions"] = list(self.actions)
        return result


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if isfinite(result) else None

def normalise_contract_identity(contract: dict) -> tuple[str, str, str]:
    if not isinstance(contract, dict):
        raise ValueError("Contract must be a dictionary")

    exchange = str(contract.get("exchange") or "").strip().upper()
    token = str(contract.get("symboltoken") or "").strip()
    symbol = str(
        contract.get("tradingsymbol")
        or contract.get("symbol")
        or ""
    ).strip().upper()

    if exchange not in {"NFO", "BFO"}:
        raise ValueError("Unsupported derivative exchange")

    if not token or not symbol:
        raise ValueError("Missing contract token or trading symbol")

    return exchange, token, symbol


def strict_signed_quantity(value, field="quantity") -> int:
    if isinstance(value, bool):
        raise ValueError(f"Invalid {field}")

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(f"Invalid {field}")

    if not number.is_integer():
        raise ValueError(f"Non-integer {field}")

    return int(number)
    

def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def contract_key(contract: dict) -> str:
    exchange, token, symbol = normalise_contract_identity(contract)
    return f"{exchange}:{token}:{symbol}"

    values = []
    for field in required:
        value = str(contract.get(field) or "").strip().upper()
        if not value:
            raise ValueError(f"contract missing {field}")
        values.append(value)
    return "|".join(values)


def _same_contract(left: dict, right: dict) -> bool:
    try:
        return contract_key(left) == contract_key(right)
    except (TypeError, ValueError):
        return False


def _freeze(key: str, reason: str, qty=None) -> ReconcileResult:
    return ReconcileResult(
        decision="FREEZE",
        contract_key=key,
        reason=reason,
        actions=("ALERT_AND_BLOCK_CONTRACT",),
        broker_qty=qty,
    )


def _active_orders(orders: list, contract: dict) -> list[dict]:
    found = []
    for order in orders:
        if not _same_contract(order.get("contract", {}), contract):
            continue
        status = str(order.get("status") or "").lower()
        if status in ACTIVE_ORDER_STATUSES:
            found.append(order)
    return found


def _protective_sells(orders: list, contract: dict) -> list[dict]:
    found = []
    for order in _active_orders(orders, contract):
        side = str(order.get("side") or "").upper()
        order_type = str(order.get("order_type") or "").upper()
        if side == "SELL" and (
            order.get("is_protective") is True
            or order_type in PROTECTIVE_ORDER_TYPES
        ):
            found.append(order)
    return found


def _remaining_qty(order: dict) -> int | None:
    quantity = _integer(order.get("quantity"))
    filled = _integer(order.get("filled", 0))
    if quantity is None or filled is None or quantity <= 0:
        return None
    remaining = quantity - filled
    return remaining if remaining >= 0 else None



def _valid_open_position(
    position: dict,
    lot_size: int,
) -> tuple[int, float] | None:
    """
    Validate a broker/engine long-option position.

    Returns:
        (quantity, average_price) when valid.
        None when the position is invalid or unsafe to reconcile.

    Safety:
    - Only positive net quantities are accepted.
    - Short positions are never converted using abs().
    - Quantity must be an exact multiple of the contract lot size.
    - Average price must be finite and positive.
    """

    if not isinstance(position, dict):
        return None

    if isinstance(lot_size, bool) or not isinstance(lot_size, int):
        return None

    if lot_size <= 0:
        return None

    qty = _integer(position.get("net_qty"))
    avg = _number(position.get("average_price"))

    # Reject missing, zero, negative or invalid quantities.
    if qty is None or qty <= 0:
        return None

    # Reject quantities that do not match the contract lot size.
    if qty % lot_size != 0:
        return None

    # Reject missing, zero, negative or invalid average prices.
    if avg is None or not math.isfinite(avg) or avg <= 0:
        return None

    return int(qty), float(avg)

def _manual_id(key: str) -> str:
    digest = sha256(key.encode("utf-8")).hexdigest()[:20].upper()
    return f"MANUAL-{digest}"


def reconcile_existing(
    engine_position: dict,
    broker_positions: list[dict],
    broker_orders: list[dict],
    *,
    snapshot_complete: bool,
) -> ReconcileResult:
    """Plan reconciliation for an existing engine-managed position.

    Normalized broker position:
      {
        "contract": {
          "exchange": "BFO",
          "symboltoken": "123",
          "tradingsymbol": "SENSEX..."
        },
        "net_qty": 130,
        "average_price": 125.50,
        "lotsize": 20
      }

    Normalized order:
      {
        "order_id": "12345",
        "contract": {...same exact contract...},
        "side": "SELL",
        "status": "trigger_pending",
        "order_type": "STOPLOSS_LIMIT",
        "quantity": 130,
        "filled": 0,
        "trigger_price": 110.0,
        "variety": "STOPLOSS",
        "is_protective": True
      }

    Existing engine_position must contain:
      signal_id, contract, qty, sl_trigger (or sl_planned)
    """
    contract = engine_position.get("contract") or {}
    try:
        key = contract_key(contract)
    except ValueError as exc:
        return _freeze("INVALID-CONTRACT", str(exc))

    if snapshot_complete is not True:
        return _freeze(key, "Broker position/order snapshot is incomplete")

    matches = [
        row for row in broker_positions
        if _same_contract(row.get("contract", {}), contract)
    ]
    if len(matches) != 1:
        return _freeze(
            key,
            f"Expected exactly one broker position row; found {len(matches)}",
        )

    broker_pos = matches[0]
    qty = _integer(broker_pos.get("net_qty"))
    lot = _integer(broker_pos.get("lotsize"))

    if qty is None:
        return _freeze(key, "Broker net quantity is missing or invalid")
    if qty < 0:
        return _freeze(key, f"Broker reports net short quantity {qty}", qty)
    if lot is None or lot <= 0:
        return _freeze(key, "Broker lot size is missing or invalid", qty)

    active = _active_orders(broker_orders, contract)
    protective = _protective_sells(broker_orders, contract)

    # Any unresolved non-protective order on the contract can change the
    # position after this snapshot. Do not resize protection against a moving
    # target until the order has a terminal broker status.
    unresolved_non_stop = [
        order for order in active
        if order not in protective
    ]
    if unresolved_non_stop:
        return _freeze(
            key,
            "Unresolved active order(s) exist for this contract",
            qty,
        )

    if qty == 0:
        if protective:
            return ReconcileResult(
                decision="CANCEL_PROTECTIVE_ORDERS",
                contract_key=key,
                reason="Broker quantity is zero but protective SELL order(s) remain",
                actions=("CANCEL_EACH_PROTECTIVE_SELL_AND_CONFIRM",),
                broker_qty=0,
            )
        return ReconcileResult(
            decision="CLOSED_MANUAL",
            contract_key=key,
            reason="Broker confirms flat and no active protective SELL remains",
            actions=("MARK_ENGINE_POSITION_CLOSED_MANUAL", "RECORD_EVENT"),
            broker_qty=0,
        )

    valid = _valid_open_position(broker_pos, lot)
    if valid is None:
        return _freeze(
            key,
            "Open broker quantity/lot-size/average-price validation failed",
            qty,
        )
    qty, avg = valid

    if len(protective) > 1:
        return _freeze(
            key,
            f"Multiple active protective SELL orders found ({len(protective)})",
            qty,
        )

    if not protective:
        stop_price = _number(
            engine_position.get("sl_trigger")
            or engine_position.get("sl_planned")
        )
        if stop_price is None or stop_price <= 0:
            return _freeze(
                key,
                "No active protective stop and no valid engine stop price",
                qty,
            )
        return ReconcileResult(
            decision="CREATE_PROTECTIVE_STOP",
            contract_key=key,
            reason="Open broker position has no active protective stop",
            actions=("VERIFY_NO_PENDING_SELL", "CREATE_STOP_FOR_BROKER_QTY",
                     "VERIFY_STOP_ACCEPTED_AND_QUANTITY"),
            broker_qty=qty,
            broker_avg_price=avg,
            stop_trigger_price=stop_price,
        )

    stop = protective[0]
    stop_id = str(stop.get("order_id") or "").strip()
    remaining = _remaining_qty(stop)
    trigger = _number(stop.get("trigger_price"))

    if not stop_id:
        return _freeze(key, "Protective stop has no broker order ID", qty)
    if remaining is None:
        return _freeze(key, "Protective stop quantity/fill is invalid", qty)
    if trigger is None or trigger <= 0:
        return _freeze(key, "Protective stop trigger is missing or invalid", qty)

    if remaining != qty:
        return ReconcileResult(
            decision="RESIZE_PROTECTIVE_STOP",
            contract_key=key,
            reason=f"Broker quantity {qty} differs from stop remaining quantity {remaining}",
            actions=("CANCEL_OLD_STOP", "CONFIRM_OLD_STOP_TERMINAL",
                     "RECHECK_BROKER_QTY_AND_ORDERS",
                     "PLACE_ONE_REPLACEMENT_STOP",
                     "VERIFY_REPLACEMENT_STOP"),
            broker_qty=qty,
            broker_avg_price=avg,
            stop_order_id=stop_id,
            stop_trigger_price=trigger,
        )

    old_qty = _integer(engine_position.get("qty"))
    old_avg = _number(engine_position.get("entry_avg"))

    if old_qty != qty or old_avg != avg:
        return ReconcileResult(
            decision="UPDATE_COMBINED_POSITION",
            contract_key=key,
            reason="Broker quantity/weighted-average entry differs from engine state",
            actions=("UPDATE_ENGINE_QTY_AND_BROKER_AVERAGE",
                     "RECORD_MANUAL_QUANTITY_CHANGE"),
            broker_qty=qty,
            broker_avg_price=avg,
            stop_order_id=stop_id,
            stop_trigger_price=trigger,
        )

    return ReconcileResult(
        decision="UNCHANGED",
        contract_key=key,
        reason="Engine quantity, broker quantity and protective stop agree",
        broker_qty=qty,
        broker_avg_price=avg,
        stop_order_id=stop_id,
        stop_trigger_price=trigger,
    )



def adopt_new_manual_positions(
    broker_positions: list[dict],
    broker_orders: list[dict],
    *,
    engine_contract_keys: set[str],
    snapshot_complete: bool,
) -> list[ReconcileResult]:
    """Plan adoption of new manual long positions.

    Duplicate contract rows are detected BEFORE any adoption decision.
    Every duplicate contract is frozen; none can be adopted.
    """
    if snapshot_complete is not True:
        return [
            _freeze(
                "ACCOUNT",
                "Broker snapshot incomplete; adoption disabled",
            )
        ]

    # First pass: validate identity and count every contract row.
    keyed_rows: dict[str, list[dict]] = {}
    invalid_rows: list[ReconcileResult] = []

    for position in broker_positions:
        contract = position.get("contract") or {}

        try:
            key = contract_key(contract)
        except (TypeError, ValueError):
            invalid_rows.append(
                _freeze("INVALID-CONTRACT", "Invalid broker contract row")
            )
            continue

        keyed_rows.setdefault(key, []).append(position)

    results = list(invalid_rows)

    # Second pass: make decisions only after duplicate detection.
    for key, rows in keyed_rows.items():

        if len(rows) != 1:
            results.append(
                _freeze(
                    key,
                    f"Duplicate broker position rows ({len(rows)}); adoption blocked",
                )
            )
            continue

        position = rows[0]

        if key in engine_contract_keys:
            continue

        qty = _integer(position.get("net_qty"))

        if qty is None:
            results.append(_freeze(key, "Invalid broker net quantity"))
            continue

        if qty == 0:
            continue

        if qty < 0:
            results.append(
                _freeze(key, "New manual net-short position is not adoptable", qty)
            )
            continue

        lot = _integer(position.get("lotsize"))
        valid = _valid_open_position(position, lot or 0)

        if valid is None:
            results.append(
                _freeze(
                    key,
                    "Manual position lot/average validation failed",
                    qty,
                )
            )
            continue

        qty, avg = valid
        contract = position["contract"]

        active = _active_orders(broker_orders, contract)
        protective = _protective_sells(broker_orders, contract)
        other_active = [o for o in active if o not in protective]

        if other_active:
            results.append(
                _freeze(
                    key,
                    "Unresolved active non-stop order; adoption blocked",
                    qty,
                )
            )
            continue

        if len(protective) != 1:
            results.append(
                _freeze(
                    key,
                    "Adoption requires exactly one active protective SELL; "
                    f"found {len(protective)}",
                    qty,
                )
            )
            continue

        stop = protective[0]
        stop_id = str(stop.get("order_id") or "").strip()
        remaining = _remaining_qty(stop)
        trigger = _number(stop.get("trigger_price"))

        if (
            not stop_id
            or remaining != qty
            or trigger is None
            or trigger <= 0
        ):
            results.append(
                _freeze(
                    key,
                    "Manual protective stop does not validate against "
                    "broker quantity",
                    qty,
                )
            )
            continue

        results.append(
            ReconcileResult(
                decision="ADOPT_MANUAL_POSITION",
                contract_key=key,
                reason=(
                    "New manual long position has valid lot size, "
                    "average and matching protective stop"
                ),
                actions=(
                    "CREATE_MANUAL_ENGINE_RECORD",
                    "PERSIST_BROKER_QTY_AND_AVERAGE",
                    "REGISTER_EXISTING_STOP_WITHOUT_DUPLICATING",
                    "RECONCILE_AGAIN_BEFORE_MANAGEMENT",
                ),
                broker_qty=qty,
                broker_avg_price=avg,
                stop_order_id=stop_id,
                stop_trigger_price=trigger,
                manual_signal_id=_manual_id(key),
            )
        )

    return results

