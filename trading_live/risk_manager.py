
"""Execution-integrity validation for the Angel One live trading bridge.

Strategy-level risk management, position sizing, exposure limits, and loss
management are owned by the signal engine.

This module validates order integrity and broker execution prerequisites.
It does not impose strategy-level trade-count, quantity, premium, or loss caps.
"""

from __future__ import annotations

import os
import math
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class RiskRejected(RuntimeError):
    pass


def _int_env(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError) as exc:
        raise RiskRejected(f"Invalid integer environment variable: {key}") from exc


def _finite_number(name: str, value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RiskRejected(f"Invalid numeric input: {name}") from exc

    if not math.isfinite(number):
        raise RiskRejected(f"Non-finite numeric input: {name}")

    return number


def validate_order(
    *,
    quantity: int,
    lot_size: int,
    open_positions: int,
    daily_realized_pnl: float,
    snapshot_age_s: float,
    signal_id: str,
    existing_signal_ids: set[str],
    now: datetime | None = None,
) -> dict:
    """Validate order integrity without imposing strategy-level risk limits.

    open_positions and daily_realized_pnl remain in the function signature
    for compatibility with existing live_bridge.py callers.

    Position count and daily P&L limits are deliberately not enforced here.
    The signal engine owns those decisions.
    """

    trading_mode = os.getenv("TRADING_MODE", "paper").strip().lower()

    if trading_mode not in ("paper", "live"):
        raise RiskRejected("Invalid TRADING_MODE")

    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise RiskRejected("Quantity must be an integer")

    if not isinstance(lot_size, int) or isinstance(lot_size, bool):
        raise RiskRejected("Lot size must be an integer")

    if quantity <= 0 or lot_size <= 0:
        raise RiskRejected("Quantity and lot size must be positive")

    if quantity % lot_size != 0:
        raise RiskRejected(
            "Quantity must be a multiple of the verified current exchange lot size"
        )

    snapshot_age_s = _finite_number("snapshot_age_s", snapshot_age_s)

    max_snapshot_age = _int_env("SNAPSHOT_MAX_AGE_SECONDS", 60)

    if max_snapshot_age < 0:
        raise RiskRejected("SNAPSHOT_MAX_AGE_SECONDS cannot be negative")

    if snapshot_age_s < 0 or snapshot_age_s > max_snapshot_age:
        raise RiskRejected("Market snapshot is stale")

    if not signal_id:
        raise RiskRejected("Missing signal_id")

    if signal_id in existing_signal_ids:
        raise RiskRejected("Duplicate signal_id")

    now = now or datetime.now(IST)

    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    else:
        now = now.astimezone(IST)

    market_open = now.replace(
        hour=9,
        minute=20,
        second=0,
        microsecond=0,
    )

    market_close = now.replace(
        hour=15,
        minute=0,
        second=0,
        microsecond=0,
    )

    if now.weekday() >= 5 or not (market_open <= now <= market_close):
        raise RiskRejected("Outside configured India market entry window")

    if (
        trading_mode == "live"
        and os.getenv("TRADING_ENABLED", "false").strip().lower() != "true"
    ):
        raise RiskRejected("Live mode selected but TRADING_ENABLED is false")

    return {
        "approved": True,
        "quantity": quantity,
        "lot_size": lot_size,
        "checked_at": now.isoformat(),
    }


def validate_live_extras(
    *,
    entry_price: float,
    stop_price: float,
    quantity: int,
    available_cash: float,
) -> dict:
    """Validate price, quantity and available funds.

    No maximum per-trade loss or premium restriction is imposed.
    Position sizing and loss management belong to the signal engine.
    """

    entry_price = _finite_number("entry_price", entry_price)
    stop_price = _finite_number("stop_price", stop_price)
    available_cash = _finite_number("available_cash", available_cash)

    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise RiskRejected("Quantity must be an integer")

    if quantity <= 0:
        raise RiskRejected("Quantity must be positive")

    if entry_price <= 0:
        raise RiskRejected("Entry price must be positive")

    if stop_price <= 0 or stop_price >= entry_price:
        raise RiskRejected(
            "Invalid stop price; long-option stop must be positive and below entry"
        )

    if available_cash < 0:
        raise RiskRejected("Available cash cannot be negative")

    planned_risk = (entry_price - stop_price) * quantity
    premium_outlay = entry_price * quantity

    if not math.isfinite(planned_risk) or not math.isfinite(premium_outlay):
        raise RiskRejected("Non-finite trade calculation")

    # Funds check is retained as an execution-integrity check.
    # No maximum premium or maximum trade-loss cap is applied.
    required_cash = premium_outlay * 1.05

    if not math.isfinite(required_cash):
        raise RiskRejected("Non-finite required funds calculation")

    if required_cash > available_cash:
        raise RiskRejected(
            f"Insufficient funds: need approximately Rs{required_cash:,.2f}, "
            f"available Rs{available_cash:,.2f}"
        )

    return {
        "risk_rupees": round(planned_risk, 2),
        "outlay_rupees": round(premium_outlay, 2),
        "required_cash": round(required_cash, 2),
    }

