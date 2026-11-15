
"""Connect Signal Engine v11 decisions to the live execution bridge."""

import logging
import math
import os

log = logging.getLogger("black.engine_hooks")


def live_management_requested() -> bool:
    """True when the process is authorised to reconcile/manage existing live positions.

    This deliberately does not depend on TRADING_ENABLED: that switch disables new
    entries, not protective-stop synchronisation or exits for positions already held.
    """
    return (
        os.getenv("TRADING_MODE", "paper").strip().lower() == "live"
        and os.getenv("LIVE_TRADING_ACK", "") == "I_UNDERSTAND_REAL_ORDERS"
    )


def live_execution_requested() -> bool:
    """Permission to consider a new live entry; the bridge enforces all order gates."""
    return (
        os.getenv("TRADING_MODE", "paper").strip().lower() == "live"
        and os.getenv("TRADING_ENABLED", "false").strip().lower() == "true"
    )


def _bridge():
    from trading_live.live_bridge import get_bridge
    return get_bridge()


def submit_engine_entry(sig, fetcher, trade_lots=1):
    """Submit a new VIP signal using the engine's actual contract and quote age."""

    if sig.is_free_signal or not live_execution_requested():
        return None

    allowed = {x.strip().upper() for x in os.getenv("LIVE_ALLOWED_TYPES", "JACKPOT,ZERO_HERO,LIVE_SNIPER").split(",") if x.strip()}
    strategy = str(getattr(sig, "signal_type", "")).upper()
    if strategy not in allowed:
        log.info("LIVE ENTRY SKIP signal=%s: strategy %s is not allowlisted", sig.signal_id, strategy)
        return None

    quote = fetcher.get_option_quote(
        sig.index_key,
        int(sig.strike),
        sig.direction,
        sig.expiry,
    )

    if not quote or quote.get("entry_eligible") is not True:
        reason = (quote or {}).get("reason", "quote unavailable")
        raise RuntimeError(
            f"LIVE ENTRY BLOCKED: invalid or stale option quote: {reason}"
        )

    snapshot_age = quote.get("snapshot_age_s")

    if snapshot_age is None:
        raise RuntimeError("LIVE ENTRY BLOCKED: option quote age is missing")

    snapshot_age = float(snapshot_age)

    if not math.isfinite(snapshot_age) or snapshot_age < -2:
        raise RuntimeError(
            f"LIVE ENTRY BLOCKED: invalid quote age {snapshot_age}"
        )

    payload = {
        "signal_id": str(sig.signal_id),
        "index": str(sig.index_key),
        "direction": str(sig.direction),
        "strike": int(sig.strike),
        "expiry": str(sig.expiry),
        "entry_premium": float(sig.entry_premium),
        "sl_price": float(sig.sl_price),
        "signal_type": str(sig.signal_type),
        "lot_size": int(sig.lot_size),
        "quantity": int(sig.lot_size) * int(trade_lots),
        "trade_lots": int(trade_lots),
        "snapshot_age_s": snapshot_age,
    }

    log.info(
        "LIVE ENTRY REQUEST signal=%s index=%s type=%s age=%.2fs",
        sig.signal_id,
        sig.index_key,
        sig.signal_type,
        snapshot_age,
    )

    result = _bridge().open_position(payload)

    if result is None:
        raise RuntimeError(
            f"LIVE ENTRY was not accepted by bridge: {sig.signal_id}"
        )
    if str(result.get("status", "")).upper() in {"REJECTED", "ENTRY_FAILED"}:
        raise RuntimeError(
            f"LIVE ENTRY rejected by broker guard: {result.get('error') or result.get('status')}"
        )

    log.info(
        "LIVE ENTRY RESULT signal=%s status=%s",
        sig.signal_id,
        result.get("status"),
    )

    return result


def sync_engine_stop(sig):
    """Mirror the exact stop calculated by Signal Engine v11."""

    if sig.is_free_signal or not live_management_requested():
        return True

    stop_price = float(sig.sl_price)

    if not math.isfinite(stop_price) or stop_price <= 0:
        raise ValueError(f"Invalid engine stop price: {stop_price}")

    result = _bridge().sync_stop(
        str(sig.signal_id),
        stop_price,
    )

    log.info(
        "LIVE STOP SYNC signal=%s engine_sl=%.2f result=%s",
        sig.signal_id,
        stop_price,
        result,
    )

    return bool(result)


def submit_engine_exit(sig, reason, hint_price=0.0):
    """Request an exit and return only the bridge's confirmed result."""

    if sig.is_free_signal or not live_management_requested():
        return True

    price = float(hint_price or 0.0)

    if not math.isfinite(price) or price < 0:
        raise ValueError(f"Invalid exit hint price: {price}")

    result = _bridge().exit_position(
        str(sig.signal_id),
        str(reason),
        price,
    )

    log.info(
        "LIVE EXIT signal=%s reason=%s confirmed=%s",
        sig.signal_id,
        reason,
        result,
    )

    return bool(result)
