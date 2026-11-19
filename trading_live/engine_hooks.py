
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
    """Submit a live entry with explicit, auditable skip/block reasons."""

    sid = str(getattr(sig, "signal_id", "UNKNOWN"))
    strategy = str(getattr(sig, "signal_type", "")).strip().upper()

    def skip(reason):
        log.warning(
            "LIVE ENTRY SKIPPED signal=%s index=%s strategy=%s reason=%s",
            sid,
            getattr(sig, "index_key", "UNKNOWN"),
            strategy,
            reason,
        )
        return {"status": "SKIPPED", "reason": reason, "signal_id": sid}

    if getattr(sig, "is_free_signal", False):
        return skip("FREE_SIGNAL")

    if not live_execution_requested():
        return skip(
            "LIVE_EXECUTION_DISABLED: check TRADING_MODE and TRADING_ENABLED"
        )

    allowed = {
        x.strip().upper()
        for x in os.getenv(
            "LIVE_ALLOWED_TYPES",
            "JACKPOT,ZERO_HERO,LIVE_SNIPER"
        ).split(",")
        if x.strip()
    }

    if strategy not in allowed:
        return skip(f"STRATEGY_NOT_ALLOWLISTED:{strategy}")

    try:
        quote = fetcher.get_option_quote(
            sig.index_key,
            int(sig.strike),
            sig.direction,
            sig.expiry,
        )
    except Exception as exc:
        raise RuntimeError(
            f"LIVE ENTRY BLOCKED: quote_fetch_error:{type(exc).__name__}:{exc}"
        ) from exc

    if not isinstance(quote, dict):
        raise RuntimeError("LIVE ENTRY BLOCKED: quote_missing_or_invalid")

    if quote.get("entry_eligible") is not True:
        raise RuntimeError(
            "LIVE ENTRY BLOCKED: quote_not_eligible:"
            f"{quote.get('reason', 'unknown_reason')}"
        )

    snapshot_age = quote.get("snapshot_age_s")

    if snapshot_age is None:
        raise RuntimeError("LIVE ENTRY BLOCKED: quote_age_missing")

    try:
        snapshot_age = float(snapshot_age)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError("LIVE ENTRY BLOCKED: quote_age_invalid") from exc

    if not math.isfinite(snapshot_age) or snapshot_age < -2:
        raise RuntimeError(
            f"LIVE ENTRY BLOCKED: invalid_quote_age:{snapshot_age}"
        )

    payload = {
        "signal_id": sid,
        "index": str(sig.index_key),
        "direction": str(sig.direction),
        "strike": int(sig.strike),
        "expiry": str(sig.expiry),
        "entry_premium": float(sig.entry_premium),
        "sl_price": float(sig.sl_price),
        "signal_type": strategy,
        "lot_size": int(sig.lot_size),
        "quantity": int(sig.lot_size) * int(trade_lots),
        "trade_lots": int(trade_lots),
        "snapshot_age_s": snapshot_age,
    }

    log.info(
        "LIVE ENTRY PREFLIGHT PASSED signal=%s index=%s "
        "strategy=%s quantity=%s quote_age=%.2fs",
        sid,
        sig.index_key,
        strategy,
        payload["quantity"],
        snapshot_age,
    )

    result = _bridge().open_position(payload)

    if result is None:
        raise RuntimeError(
            f"LIVE ENTRY NOT ACCEPTED BY BRIDGE: {sid}"
        )

    status = str(result.get("status", "")).upper()

    if status in {
        "REJECTED",
        "ENTRY_FAILED",
        "DUPLICATE_BLOCKED",
    }:
        raise RuntimeError(
            f"LIVE ENTRY REJECTED signal={sid}: "
            f"{result.get('error') or status}"
        )

    log.info(
        "LIVE ENTRY BRIDGE RESULT signal=%s status=%s",
        sid,
        status or "UNKNOWN",
    )

    return result



def sync_engine_stop(sig):
    """Mirror the exact stop calculated by Signal Engine v11."""

    if sig.is_free_signal or not live_management_requested():
        return True

    stop_price = float(sig.sl_price)

    if not math.isfinite(stop_price) or stop_price <= 0:
        raise ValueError(f"Invalid engine stop price: {stop_price}")

    bridge = _bridge()
    if not bridge.has_live_position(str(sig.signal_id)):
        log.info("LIVE STOP SYNC skipped for non-live signal=%s", sig.signal_id)
        return True

    result = bridge.sync_stop(
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

    bridge = _bridge()
    if not bridge.has_live_position(str(sig.signal_id)):
        # The signal may be paper-only, disallowed, shadowed, or rejected
        # before any broker fill. Its engine lifecycle can close normally;
        # never manufacture a SELL for a position that was not opened live.
        log.info("LIVE EXIT skipped for non-live signal=%s reason=%s", sig.signal_id, reason)
        return True

    result = bridge.exit_position(
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
