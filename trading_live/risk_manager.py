"""Pre-trade fail-closed risk checks. These are guardrails, not a profitability guarantee."""
from __future__ import annotations
import os
import math
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class RiskRejected(RuntimeError):
    pass


def _int_env(key, default):
    try:
        return int(os.getenv(key, str(default)))
    except ValueError as e:
        raise RiskRejected(f"Invalid integer env {key}") from e


def _float_env(key, default):
    try:
        value = float(os.getenv(key, str(default)))
    except (TypeError, ValueError) as e:
        raise RiskRejected(f"Invalid number env {key}") from e
    if not math.isfinite(value):
        raise RiskRejected(f"Non-finite number env {key}")
    return value


def _finite_number(name, value):
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as e:
        raise RiskRejected(f"Invalid numeric input {name}") from e
    if not math.isfinite(number):
        raise RiskRejected(f"Non-finite numeric input {name}")
    return number


def validate_order(*, quantity: int, lot_size: int, open_positions: int, daily_realized_pnl: float,
                   snapshot_age_s: float, signal_id: str, existing_signal_ids: set[str],
                   now: datetime | None = None) -> dict:
    if os.getenv("TRADING_MODE", "paper").lower() not in ("paper", "live"):
        raise RiskRejected("Invalid TRADING_MODE")
    daily_realized_pnl = _finite_number("daily_realized_pnl", daily_realized_pnl)
    snapshot_age_s = _finite_number("snapshot_age_s", snapshot_age_s)
    if not isinstance(quantity, int) or isinstance(quantity, bool) or not isinstance(lot_size, int) or isinstance(lot_size, bool):
        raise RiskRejected("Quantity and lot_size must be integers")
    if not isinstance(open_positions, int) or isinstance(open_positions, bool) or open_positions < 0:
        raise RiskRejected("open_positions must be a non-negative integer")
    if quantity <= 0 or lot_size <= 0 or quantity % lot_size:
        raise RiskRejected("Quantity must be positive and a multiple of verified current lot size")
    max_lots = _int_env("MAX_LOTS_PER_TRADE", 1)
    if max_lots <= 0:
        raise RiskRejected("MAX_LOTS_PER_TRADE must be positive")
    if quantity > max_lots * lot_size:
        raise RiskRejected("Quantity exceeds MAX_LOTS_PER_TRADE")
    max_open_positions = _int_env("MAX_OPEN_POSITIONS", 1)
    if max_open_positions <= 0:
        raise RiskRejected("MAX_OPEN_POSITIONS must be positive")
    if open_positions >= max_open_positions:
        raise RiskRejected("MAX_OPEN_POSITIONS reached")
    max_loss = abs(_float_env("MAX_DAILY_LOSS_RUPEES", 1000))
    if max_loss <= 0:
        raise RiskRejected("MAX_DAILY_LOSS_RUPEES must be positive")
    if daily_realized_pnl <= -max_loss:
        raise RiskRejected("Daily loss limit reached")
    max_snapshot_age = _int_env("SNAPSHOT_MAX_AGE_SECONDS", 60)
    if max_snapshot_age < 0:
        raise RiskRejected("SNAPSHOT_MAX_AGE_SECONDS cannot be negative")
    if snapshot_age_s < 0 or snapshot_age_s > max_snapshot_age:
        raise RiskRejected("Market snapshot is stale")
    if not signal_id or signal_id in existing_signal_ids:
        raise RiskRejected("Missing/duplicate signal_id")
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    else:
        now = now.astimezone(IST)
    if now.weekday() >= 5 or not (now.replace(hour=9, minute=20, second=0, microsecond=0) <= now
                                  <= now.replace(hour=15, minute=0, second=0, microsecond=0)):
        raise RiskRejected("Outside configured India market entry window")
    if os.getenv("TRADING_MODE", "paper").lower() == "live" and os.getenv("TRADING_ENABLED", "false").lower() != "true":
        raise RiskRejected("Live mode selected but TRADING_ENABLED is false")
    return {"approved": True, "quantity": quantity, "max_lots": max_lots, "checked_at": now.isoformat()}


def validate_live_extras(*, entry_price: float, stop_price: float, quantity: int,
                         available_cash: float) -> dict:
    """Money-based checks that need the actual premium, stop and funds."""
    entry_price = _finite_number("entry_price", entry_price)
    stop_price = _finite_number("stop_price", stop_price)
    available_cash = _finite_number("available_cash", available_cash)
    if not isinstance(quantity, int) or isinstance(quantity, bool):
        raise RiskRejected("quantity must be an integer")
    if entry_price <= 0 or quantity <= 0 or stop_price <= 0 or stop_price >= entry_price or available_cash < 0:
        raise RiskRejected("Invalid entry price, stop price, quantity or available cash; long-option stop must be below entry")
    risk = (entry_price - stop_price) * quantity
    if not math.isfinite(risk):
        raise RiskRejected("Planned trade risk is non-finite")
    max_trade_loss = abs(_float_env("MAX_LOSS_PER_TRADE_RUPEES", 3000))
    if max_trade_loss <= 0:
        raise RiskRejected("MAX_LOSS_PER_TRADE_RUPEES must be positive")
    if risk > max_trade_loss:
        raise RiskRejected(f"Planned loss Rs{risk:,.0f} exceeds MAX_LOSS_PER_TRADE_RUPEES Rs{max_trade_loss:,.0f}")
    cost = entry_price * quantity
    if not math.isfinite(cost) or not math.isfinite(cost * 1.05):
        raise RiskRejected("Premium outlay is non-finite")
    max_cost = abs(_float_env("MAX_PREMIUM_PER_TRADE_RUPEES", 150000))
    if max_cost <= 0:
        raise RiskRejected("MAX_PREMIUM_PER_TRADE_RUPEES must be positive")
    if cost > max_cost:
        raise RiskRejected(f"Premium outlay Rs{cost:,.0f} exceeds MAX_PREMIUM_PER_TRADE_RUPEES")
    if cost * 1.05 > available_cash:
        raise RiskRejected(f"Insufficient funds: need ~Rs{cost*1.05:,.0f}, available Rs{available_cash:,.0f}")
    return {"risk_rupees": round(risk, 2), "outlay_rupees": round(cost, 2)}
