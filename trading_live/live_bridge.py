"""Live execution bridge between the v11 signal engine and Angel One.

Design rules
  * Engine stays the strategy brain. This module only mirrors its decisions at the broker.
  * Every entry gets a broker-side STOPLOSS_LIMIT sell. If that cannot be confirmed -> emergency exit.
  * Stops only ever move UP. Modify the existing order; never stack new ones.
  * Exit order of operations: confirm SL state -> cancel SL -> confirm cancelled -> SELL -> confirm fill
    -> confirm broker net position is flat. If the SL already filled, never sell again.
  * Nothing here retries placeOrder blindly. Unknown outcomes are reconciled via the order book (ordertag).
  * Default is SHADOW mode (LIVE_SHADOW=true): everything runs and is logged, no order is sent.
"""
from __future__ import annotations
import os, json, math, time, hashlib, logging, threading, urllib.request, urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

from .angel_one_broker import (AngelOneBroker, BrokerSafetyError, OrderOutcomeUnknown,
                               tick, kill_switch_on, engine_expiry_to_master)
from .live_state import MemoryStore, PgStore, list_active, today_ist

log = logging.getLogger("black.live")
IST = ZoneInfo("Asia/Kolkata")
TERMINAL = {"complete", "rejected", "cancelled"}


def _f(key, default):
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return float(default)


class LiveConfig:
    def __init__(self):
        self.shadow = os.getenv("LIVE_SHADOW", "true").lower() != "false"
        self.allowed_types = {t.strip().upper() for t in
                              os.getenv("LIVE_ALLOWED_TYPES", "JACKPOT,ZERO_HERO,LIVE_SNIPER").split(",") if t.strip()}
        self.trade_lots = max(1, int(_f("TRADE_LOTS", 1)))
        self.master_path = os.getenv("SCRIP_MASTER_PATH", "data/scrip_master.json")
        self.entry_buffer_pct = _f("ENTRY_LIMIT_BUFFER_PCT", 1.0)     # marketable limit above LTP
        self.max_entry_dev_pct = _f("MAX_ENTRY_DEVIATION_PCT", 5.0)   # vs engine's paper entry
        self.max_quote_age_s = _f("MAX_QUOTE_AGE_S", 5.0)
        self.sl_limit_buffer_pct = _f("SL_LIMIT_BUFFER_PCT", 3.0)     # limit below SL trigger
        self.entry_timeout_s = _f("ENTRY_FILL_TIMEOUT_S", 10)
        self.exit_timeout_s = _f("EXIT_FILL_TIMEOUT_S", 15)
        self.sl_grace_s = _f("SL_GRACE_S", 3)
        self.exit_order_type = os.getenv("EXIT_ORDER_TYPE", "MARKET").upper()
        self.flatten_hhmm = os.getenv("HARD_FLATTEN_HHMM", "15:26")
        self.supervise_every_s = _f("SUPERVISE_EVERY_S", 15)
        self.product = "INTRADAY"


def _tag(signal_id: str) -> str:
    return "BK" + hashlib.sha1(signal_id.encode()).hexdigest()[:12]


# Backwards-compatible name used by the lifecycle test suite and older callers.
_order_tag = _tag


class LiveBridge:
    def __init__(self, broker=None, store=None, notifier=None, config=None, sleep=time.sleep,
                 contract_resolver=None, risk_check=None, clock=None):
        self.cfg = config or LiveConfig()
        self.broker = broker if broker is not None else AngelOneBroker()
        self.store = store
        self.notifier = notifier or self._telegram
        self.sleep = sleep
        self.clock = clock or (lambda: datetime.now(IST))
        self._resolve = contract_resolver or self._default_resolver
        self._locks: dict = {}
        self._locks_guard = threading.Lock()
        self._sup_thread = None
        self._stop = threading.Event()
        self._last_alert: dict = {}
        self.risk_check = risk_check

    # ── enablement ───────────────────────────────────────────────────────
    @property
    def enabled(self) -> bool:
        # Existing positions remain manageable after new entries are disabled.
        return (os.getenv("TRADING_MODE", "paper").strip().lower() == "live"
                and os.getenv("LIVE_TRADING_ACK", "") == "I_UNDERSTAND_REAL_ORDERS")

    @property
    def entry_enabled(self) -> bool:
        return (self.enabled
                and os.getenv("TRADING_ENABLED", "false").strip().lower() == "true"
                and os.getenv("ALLOW_ORDER_SUBMISSION", "false").strip().lower() == "true"
                and not self._kill())

    def _lock(self, signal_id):
        with self._locks_guard:
            return self._locks.setdefault(signal_id, threading.RLock())

    def _kill(self) -> bool:
        return kill_switch_on() or bool(self.store.get_flag("kill_switch"))

    # ── helpers ──────────────────────────────────────────────────────────
    def _default_resolver(self, index, expiry, strike, direction):
        path = AngelOneBroker.download_master(self.cfg.master_path)
        return AngelOneBroker.find_contract(path, index, expiry, strike, direction)

    def _telegram(self, text: str):
        tok = os.getenv("SIGNAL_BOT_TOKEN", "")
        chat = os.getenv("TRADING_TELEGRAM_CHAT_ID", os.getenv("VIP_CHANNEL_ID", ""))
        if not tok or not chat:
            return
        try:
            data = urllib.parse.urlencode({"chat_id": chat, "text": text[:3900]}).encode()
            urllib.request.urlopen(f"https://api.telegram.org/bot{tok}/sendMessage", data=data, timeout=6)
        except Exception as e:
            log.warning("Telegram notify failed: %s", e)

    def _alert(self, text, key=None, every_s=0):
        log.error("LIVE ALERT: %s", text)
        if key and every_s:
            now = time.time()
            if now - self._last_alert.get(key, 0) < every_s:
                return
            self._last_alert[key] = now
        try:
            self.notifier("🔴 LIVE: " + text)
        except Exception:
            pass

    def _info(self, text):
        log.info("LIVE: %s", text)
        try:
            self.notifier("🟢 LIVE: " + text)
        except Exception:
            pass

    def _save(self, pos):
        pos["updated"] = self.clock().isoformat()
        self.store.put(pos)

    def _poll(self, order_id, done, timeout_s, interval=0.5):
        n = max(1, int(math.ceil(timeout_s / interval)))
        info = None
        for _ in range(n):
            try:
                info = self.broker.get_order(order_id) or info
            except BrokerSafetyError as e:
                log.warning("order lookup failed: %s", e)
            if info and info["status"] in done:
                return info
            self.sleep(interval)
        try:
            return self.broker.get_order(order_id) or info
        except BrokerSafetyError:
            return info

    def _validate_pretrade_risk(self, *, signal_id, quantity, lot_size, snapshot_age_s, entry_price, stop_price):
        """Apply risk limits immediately before any live BUY."""
        if self.risk_check is not None:
            result = self.risk_check(
                signal_id=signal_id, quantity=quantity, lot_size=lot_size,
                snapshot_age_s=snapshot_age_s, entry_price=entry_price,
                stop_price=stop_price, store=self.store, broker=self.broker,
                now=self.clock(),
            )
            if result is False or (isinstance(result, dict) and not result.get("approved", True)):
                raise BrokerSafetyError("Injected pre-trade risk check rejected the order")
            return result

        from .risk_manager import RiskRejected, validate_order, validate_live_extras
        from .live_state import realized_pnl_today
        active = list_active(self.store)
        try:
            order_check = validate_order(
                quantity=int(quantity), lot_size=int(lot_size),
                open_positions=len(active), daily_realized_pnl=realized_pnl_today(self.store),
                snapshot_age_s=float(snapshot_age_s), signal_id=str(signal_id),
                existing_signal_ids={str(p.get("signal_id")) for p in active},
                now=self.clock(),
            )
            available_cash = float(self.broker.available_cash())
            money_check = validate_live_extras(
                entry_price=float(entry_price), stop_price=float(stop_price),
                quantity=int(quantity), available_cash=available_cash,
            )
        except RiskRejected as exc:
            raise BrokerSafetyError(f"Pre-trade risk rejected: {exc}") from exc
        return {"approved": True, "limits": order_check, "money": money_check,
                "available_cash": available_cash}

    # ══ ENTRY ════════════════════════════════════════════════════════════
    def open_position(self, sig: dict) -> dict | None:
        """sig: signal_id,index,direction,strike,expiry,entry_premium,sl_price,signal_type."""
        # Run read-only contract/exposure checks when live entry is requested,
        # but do not proceed to quotes/risk/claims unless every order gate is enabled.
        if (os.getenv("TRADING_MODE", "paper").strip().lower() != "live"
                or os.getenv("TRADING_ENABLED", "false").strip().lower() != "true"
                or self._kill()):
            log.info("LIVE entry blocked before preflight: mode/enable/kill-switch gate")
            return None
        sid = sig["signal_id"]
        with self._lock(sid):
            if self.store.get(sid):
                return None                                        # idempotent
            if str(sig.get("signal_type", "")).upper() not in self.cfg.allowed_types:
                log.info("LIVE skip %s: signal_type %s not allowed", sid, sig.get("signal_type"))
                return None
            pos = {"signal_id": sid, "index": sig["index"], "direction": sig["direction"],
                   "strike": float(sig["strike"]), "expiry": sig["expiry"], "status": "REJECTED",
                   "signal_type": sig.get("signal_type"), "tag": _tag(sid), "product": self.cfg.product,
                   "paper_entry": float(sig["entry_premium"]), "opened_date": today_ist(), "error": None}
            try:
                if self._kill():
                    raise BrokerSafetyError("Execution kill switch is ON")

                contract = self._resolve(sig["index"], sig["expiry"], sig["strike"], sig["direction"])

                # Account-wide guard: manual and bot positions/orders are checked
                # against the exact resolved contract before any new BUY.
                conflict = self.broker.entry_conflict(contract)
                if conflict.get("blocked"):
                    raise BrokerSafetyError(
                        "Duplicate contract exposure: "
                        f"open_positions={conflict.get('open_positions')}; "
                        f"active_orders={conflict.get('active_orders')}"
                    )
                if (os.getenv("LIVE_TRADING_ACK", "") != "I_UNDERSTAND_REAL_ORDERS"
                        or os.getenv("ALLOW_ORDER_SUBMISSION", "false").strip().lower() != "true"
                        or self._kill()):
                    raise BrokerSafetyError("Live BUY acknowledgement/order-submission gate is not enabled")

                lot = int(contract.get("lotsize") or 0)
                engine_lot = int(sig["lot_size"])
                qty = int(sig["quantity"])
                trade_lots = int(sig.get("trade_lots", self.cfg.trade_lots))
                if trade_lots != self.cfg.trade_lots or qty != lot * trade_lots:
                    raise BrokerSafetyError(
                        f"Quantity/trade-lot mismatch: engine_qty={qty}, contract_lot={lot}, "
                        f"engine_lots={trade_lots}, bridge_lots={self.cfg.trade_lots}"
                    )
                snapshot_age = float(sig["snapshot_age_s"])
                if (not math.isfinite(snapshot_age) or snapshot_age < -2
                        or snapshot_age > self.cfg.max_quote_age_s):
                    raise BrokerSafetyError(
                        f"Stale/invalid engine quote age {snapshot_age:.2f}s "
                        f"(max {self.cfg.max_quote_age_s:.2f}s)"
                    )

                if lot <= 0 or lot != engine_lot:
                    raise BrokerSafetyError(f"Contract lot mismatch: engine={engine_lot}, master={lot}")

                if qty <= 0 or qty % lot != 0:
                    raise BrokerSafetyError(f"Invalid engine quantity={qty}, verified lot={lot}")

                ltp = float(self.broker.ltp(contract))
                if not math.isfinite(ltp) or ltp <= 0:
                    raise BrokerSafetyError("Invalid Angel One option LTP")

                paper_entry = float(sig["entry_premium"])
                if not math.isfinite(paper_entry) or paper_entry <= 0:
                    raise BrokerSafetyError("Invalid engine entry premium")

                dev = abs(ltp - paper_entry) / paper_entry * 100
                if dev > self.cfg.max_entry_dev_pct:
                    raise BrokerSafetyError(
                        f"Execution price deviation {dev:.2f}% exceeds configured execution tolerance"
                    )

                sl_price = float(sig["sl_price"])
                if not math.isfinite(sl_price) or sl_price <= 0 or sl_price >= ltp:
                    raise BrokerSafetyError("Invalid engine stop price; long-option stop must be positive and below live LTP")

                if not self.cfg.shadow:
                    pos["pretrade_risk"] = self._validate_pretrade_risk(
                        signal_id=sid, quantity=qty, lot_size=lot,
                        snapshot_age_s=snapshot_age, entry_price=ltp,
                        stop_price=sl_price,
                    )

                pos.update(
                    contract=contract,
                    qty=qty,
                    requested_qty=qty,
                    engine_quantity=qty,
                    engine_sl=sl_price,
                    broker_ltp=ltp
                )
            except (ValueError, TypeError, BrokerSafetyError) as e:
                pos["error"] = str(e)
                self._save(pos)
                self._alert(f"ENTRY REJECTED {sid}: {e}")
                return pos

            if self.cfg.shadow:
                pos["status"] = "SHADOW"
                pos["shadow_payload"] = AngelOneBroker.order_payload(
                    contract, "BUY", qty, pos["product"], "LIMIT", tick(ltp * (1 + self.cfg.entry_buffer_pct / 100)), tag=pos["tag"])
                self._save(pos)
                self._info(f"SHADOW entry {contract['tradingsymbol']} qty {qty} @~{ltp} SL {sig['sl_price']} (no order sent)")
                return pos

            return self._do_entry(pos, contract, qty, ltp, float(sig["sl_price"]))

    def _do_entry(self, pos, contract, qty, ltp, sl_price):
        limit = tick(ltp * (1 + self.cfg.entry_buffer_pct / 100))
        payload = AngelOneBroker.order_payload(contract, "BUY", qty, pos["product"], "LIMIT", limit, tag=pos["tag"])
        pos.update(status="SUBMITTING", entry_limit=limit, sl_planned=sl_price)
        intent_id = None
        try:
            from . import order_manager as om
            update_intent = om.update_intent
            claim = getattr(om, "claim_entry_intent", None)
            if not callable(claim):
                raise RuntimeError("atomic claim_entry_intent is required; legacy enqueue is not safe for BUY")
            intent_payload = {
                "contract": contract, "quantity": qty, "order_payload": payload,
                "strategy": pos.get("signal_type"),
            }
            claimed = claim(pos["signal_id"], intent_payload, pos)
            if not claimed or not claimed.get("claimed"):
                pos.update(status="DUPLICATE_BLOCKED", error="durable entry intent or position already exists; reconcile existing state")
                # Do NOT overwrite the existing durable position row on a failed claim.
                self._alert(f"ENTRY DUPLICATE BLOCKED {pos['signal_id']}: existing durable intent/position must be reconciled")
                return pos
            intent_id = claimed.get("intent_id")
            self._save(pos)
            pos["entry_intent_id"] = intent_id
            if intent_id:
                update_intent(intent_id, "submitting")
            self._save(pos)

            # Re-check the account immediately before BUY. Real AngelOneBroker must
            # implement entry_conflict; test doubles may omit it when exercising _do_entry alone.
            checker = getattr(self.broker, "entry_conflict", None)
            if checker:
                conflict = checker(contract)
                if not isinstance(conflict, dict) or conflict.get("blocked") is None:
                    raise BrokerSafetyError("Manual-position/order conflict check returned an invalid result")
                if conflict["blocked"]:
                    if intent_id:
                        update_intent(intent_id, "rejected", error="same-contract broker exposure/order detected")
                    pos.update(status="ENTRY_FAILED", error="same-contract broker exposure/order detected")
                    self._save(pos)
                    self._alert(f"ENTRY BLOCKED: existing broker exposure/order for {contract['tradingsymbol']}")
                    return pos
            oid = self.broker.place_order(payload)
        except OrderOutcomeUnknown as e:
            if intent_id:
                try:
                    update_intent(intent_id, "unknown", error=str(e))
                except Exception:
                    log.exception("Could not mark ENTRY intent unknown")
            pos.update(status="UNKNOWN_ENTRY", error=str(e), sl_planned=sl_price)
            self._save(pos)
            self._alert(f"ENTRY outcome UNKNOWN for {contract['tradingsymbol']}; supervisor will reconcile by tag")
            return pos
        except BrokerSafetyError as e:
            if intent_id:
                try:
                    update_intent(intent_id, "rejected", error=str(e))
                except Exception:
                    log.exception("Could not mark ENTRY intent rejected")
            pos.update(status="ENTRY_FAILED", error=str(e))
            self._save(pos)
            self._alert(f"ENTRY failed: {e}")
            return pos
        except Exception as e:
            # Database/claim failures are fail-closed: never call place_order.
            pos.update(status="ENTRY_FAILED", error=f"durable entry claim failed: {e}")
            self._alert(f"ENTRY BLOCKED {pos['signal_id']}: durable claim failed; no BUY submitted")
            return pos

        pos["entry_order_id"] = oid
        self._save(pos)
        if intent_id:
            try:
                update_intent(intent_id, "open", broker_order_id=oid)
            except Exception:
                log.exception("Could not update ENTRY intent after broker acknowledgement")

        info = self._poll(oid, TERMINAL, self.cfg.entry_timeout_s)
        if not info or info["status"] not in TERMINAL:
            try:
                self.broker.cancel_order(oid, "NORMAL")
            except BrokerSafetyError:
                pass
            info = self._poll(oid, TERMINAL, 5)
        if not info or info["status"] not in TERMINAL:
            pos.update(status="UNKNOWN_ENTRY", error="entry order state unconfirmed after cancel")
            self._save(pos)
            self._alert(f"ENTRY {oid} state unconfirmed; check broker app NOW")
            return pos
        if info["filled"] <= 0:
            pos.update(status="ENTRY_FAILED", error=f"not filled ({info['status']}): {info.get('text','')}")
            self._save(pos)
            if intent_id:
                try:
                    update_intent(intent_id, "rejected", broker_order_id=oid, error=pos["error"])
                except Exception:
                    log.exception("Could not update rejected ENTRY intent")
            self._alert(f"ENTRY not filled ({info['status']}) {info.get('text','')}")
            return pos

        filled = int(info["filled"])
        pos.update(qty=filled, entry_avg=info["avg"] or ltp, status="OPEN")
        self._save(pos)

        # Never protect a fill based only on the order book. Confirm exact broker net
        # quantity first; mismatch or unreadable positions leaves it UNKNOWN_ENTRY.
        try:
            net_qty = int(self.broker.net_qty(contract))
        except (BrokerSafetyError, TypeError, ValueError) as e:
            pos.update(status="UNKNOWN_ENTRY", error=f"entry fill {filled} but broker net quantity unavailable: {e}")
            self._save(pos)
            self._alert(f"ENTRY {oid}: broker net quantity unavailable; protective stop NOT submitted")
            return pos
        if net_qty != filled:
            pos.update(status="UNKNOWN_ENTRY", error=f"entry fill={filled}, broker net_qty={net_qty}; protective stop NOT submitted")
            self._save(pos)
            self._alert(f"ENTRY {oid}: fill/net mismatch ({filled}/{net_qty}); stop NOT submitted")
            return pos

        if intent_id:
            try:
                update_intent(intent_id, "complete" if filled >= qty else "partial", broker_order_id=oid)
            except Exception:
                log.exception("Could not update filled ENTRY intent")
        if filled < qty:
            self._alert(f"PARTIAL ENTRY {filled}/{qty}: remainder cancelled; protecting verified filled qty only")
        lot = int(contract.get("lotsize") or 1)
        if filled % lot:
            self._alert(f"Filled qty {filled} is not a multiple of lot {lot}; protecting actual verified qty")
        return self._protect(pos, sl_price)

    def _protect(self, pos, sl_price):
        """Claim a durable STOP intent before submitting; reconcile uncertain/filled stops."""
        contract = pos["contract"]
        trigger = max(tick(float(sl_price)), 0.05)
        limit = max(tick(trigger * (1 - getattr(self.cfg, "sl_limit_buffer_pct", 3.0) / 100)), 0.05)
        payload = AngelOneBroker.order_payload(contract, "SELL", pos["qty"], pos["product"], "STOPLOSS_LIMIT",
                                               limit, trigger, variety="STOPLOSS", tag=pos["tag"])
        intent_id = None
        try:
            from . import order_manager as om
            claim = getattr(om, "claim_order_intent", None)
            if not callable(claim):
                raise RuntimeError("atomic claim_order_intent is required; legacy STOP enqueue is unsafe")
            attempt = int(pos.get("sl_attempt", 0))
            attempt_key = f"STOP:{attempt}"
            claimed = claim(pos["signal_id"], "STOP", attempt_key,
                            {"contract": contract, "quantity": int(pos["qty"]),
                             "trigger": trigger, "limit": limit, "order_payload": payload})
            if not claimed or not claimed.get("claimed"):
                pos.update(status="STOP_UNKNOWN", sl_trigger=trigger, sl_limit=limit,
                           sl_variety="STOPLOSS", error="protective STOP intent was not claimed; no broker order submitted")
                self._save(pos)
                self._alert(f"STOP CLAIM BLOCKED for {pos['signal_id']}: no STOP order submitted")
                return pos
            intent_id = claimed.get("intent_id")
            pos.update(status="STOP_UNKNOWN", sl_attempt=attempt + 1, sl_intent_id=intent_id,
                       sl_trigger=trigger, sl_limit=limit, sl_variety="STOPLOSS",
                       error="STOP intent claimed; broker acknowledgement pending")
            self._save(pos)
            om.update_intent(intent_id, "submitting")
            oid = self.broker.place_order(payload, reduce_only=True)
        except OrderOutcomeUnknown as e:
            if intent_id:
                try:
                    om.update_intent(intent_id, "unknown", error=str(e))
                except Exception:
                    log.exception("Could not mark STOP intent unknown")
            pos.update(error=f"SL placement outcome unknown: {e}", status="STOP_UNKNOWN",
                       sl_trigger=trigger, sl_limit=limit, sl_variety="STOPLOSS")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {contract['tradingsymbol']}; reconciling tagged stop before any further action")
            return pos
        except BrokerSafetyError as e:
            if intent_id:
                try:
                    om.update_intent(intent_id, "rejected", error=str(e))
                except Exception:
                    log.exception("Could not mark rejected STOP intent")
            pos.update(error=f"SL placement rejected: {e}", status="STOP_UNKNOWN",
                       sl_trigger=trigger, sl_limit=limit, sl_variety="STOPLOSS")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {contract['tradingsymbol']}: {e}; manual reconciliation required")
            return pos
        except Exception as e:
            # An unexpected exception after the claim may have happened after broker
            # acceptance. Mark UNKNOWN and never retry the placement blindly.
            if intent_id:
                try:
                    om.update_intent(intent_id, "unknown", error=str(e))
                except Exception:
                    log.exception("Could not mark uncertain STOP intent")
            pos.update(error=f"SL submission uncertain: {type(e).__name__}: {e}", status="STOP_UNKNOWN",
                       sl_trigger=trigger, sl_limit=limit, sl_variety="STOPLOSS")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {contract['tradingsymbol']}: unexpected submission error; manual reconciliation required")
            return pos

        pos.update(sl_order_id=oid, sl_trigger=trigger, sl_limit=limit,
                   sl_variety="STOPLOSS", error=None, status="STOP_UNKNOWN")
        self._save(pos)
        try:
            om.update_intent(intent_id, "open", broker_order_id=oid)
        except Exception:
            log.exception("Could not link accepted STOP order to durable intent")
        info = self._poll(oid, {"trigger_pending", "open", "complete", "rejected", "cancelled"}, 4)
        if info and info.get("status") in ("trigger_pending", "open") and int(info.get("filled") or 0) == 0:
            pos.update(status="OPEN", sl_order_id=oid, sl_trigger=trigger, sl_limit=limit,
                       sl_variety="STOPLOSS", sl_replace_count=0, error=None)
            self._save(pos)
            self._info(f"ENTERED {contract['tradingsymbol']} qty {pos['qty']} avg {pos.get('entry_avg')} | SL trigger {trigger} limit {limit}")
            return pos
        if info and info.get("status") == "complete":
            try:
                om.update_intent(intent_id, "complete", broker_order_id=oid)
            except Exception:
                log.exception("Could not mark completed STOP intent")
        elif info and int(info.get("filled") or 0) > 0:
            try:
                om.update_intent(intent_id, "partial", broker_order_id=oid)
            except Exception:
                log.exception("Could not mark partial STOP intent")
        else:
            try:
                om.update_intent(intent_id, "unknown", broker_order_id=oid,
                                 error=f"STOP order state not confirmed: {info}")
            except Exception:
                log.exception("Could not mark unconfirmed STOP intent")
        try:
            book = {str(o.get("order_id")): o for o in self.broker.orders() if o.get("order_id")}
            return self._reconcile_stop(pos, book)
        except Exception as e:
            pos.update(status="STOP_UNKNOWN", error=f"STOP accepted but order-book reconciliation failed: {e}")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {contract['tradingsymbol']}: broker state could not be reconciled")
            return pos

    def _reconcile_stop(self, pos, book):
        """Reconcile all tagged protective STOPs against actual broker net quantity."""
        rows = [o for o in book.values()
                if o.get("tag") == pos.get("tag") and o.get("side") == "SELL"
                and str(o.get("variety", "")).upper() == "STOPLOSS"]
        if not rows:
            pos.update(status="STOP_UNKNOWN", error="no tagged protective STOP found; manual reconciliation required")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
            return pos

        active = [o for o in rows if o.get("status") in ("open", "trigger_pending", "unknown")]
        if len(active) > 1:
            pos.update(status="STOP_UNKNOWN", error="multiple active tagged protective stops; manual reconciliation required")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
            return pos

        try:
            original_qty = int(pos.get("qty") or 0)
            filled_total = sum(max(0, int(o.get("filled") or 0)) for o in rows)
            net_qty = int(self.broker.net_qty(pos["contract"]))
        except (BrokerSafetyError, TypeError, ValueError, OverflowError) as e:
            pos.update(status="STOP_UNKNOWN", error=f"broker STOP/position reconciliation unavailable: {e}")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
            return pos

        if original_qty <= 0 or filled_total > original_qty:
            pos.update(status="STOP_UNKNOWN", error=f"invalid STOP fill aggregate: original={original_qty}, filled={filled_total}")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
            return pos

        expected_net = max(0, original_qty - filled_total)
        if net_qty < 0 or net_qty != expected_net:
            pos.update(status="STOP_UNKNOWN", error=f"STOP fill/net mismatch: original={original_qty}, stop_filled={filled_total}, expected_net={expected_net}, broker_net={net_qty}")
            self._save(pos)
            self._alert(f"STOP RECOVERY BLOCKED {pos['signal_id']}: {pos['error']}")
            return pos

        if filled_total > 0:
            # A partially filled stop may still have a live remainder. Cancel and
            # confirm it before exiting the remaining broker-held quantity.
            if active:
                stop = active[0]
                oid = str(stop.get("order_id") or "")
                if not oid:
                    pos.update(status="STOP_UNKNOWN", error="partially filled STOP has no broker order id")
                    self._save(pos)
                    return pos
                try:
                    self.broker.cancel_order(oid, str(stop.get("variety") or "STOPLOSS"))
                except BrokerSafetyError:
                    pass
                terminal = self._poll(oid, TERMINAL, 8)
                if not terminal or terminal.get("status") not in TERMINAL:
                    pos.update(status="STOP_UNKNOWN", error=f"partially filled STOP {oid} remainder cancellation unconfirmed")
                    self._save(pos)
                    self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
                    return pos
                try:
                    net_qty = int(self.broker.net_qty(pos["contract"]))
                except (BrokerSafetyError, TypeError, ValueError) as e:
                    pos.update(status="STOP_UNKNOWN", error=f"net quantity unavailable after STOP cancellation: {e}")
                    self._save(pos)
                    return pos
                if net_qty != expected_net:
                    pos.update(status="STOP_UNKNOWN", error=f"post-cancel STOP net mismatch: expected={expected_net}, broker_net={net_qty}")
                    self._save(pos)
                    return pos
                try:
                    if pos.get("sl_intent_id"):
                        from .order_manager import update_intent
                        update_intent(pos["sl_intent_id"], "cancelled", broker_order_id=oid)
                except Exception:
                    log.exception("Could not mark partially filled STOP intent cancelled")
            elif rows:
                try:
                    if pos.get("sl_intent_id"):
                        from .order_manager import update_intent
                        terminal_status = rows[-1].get("status")
                        if terminal_status == "complete":
                            update_intent(pos["sl_intent_id"], "complete", broker_order_id=rows[-1].get("order_id"))
                        elif terminal_status == "cancelled":
                            update_intent(pos["sl_intent_id"], "cancelled", broker_order_id=rows[-1].get("order_id"))
                except Exception:
                    log.exception("Could not close terminal STOP intent")
            pos.update(remaining_qty=net_qty, status="OPEN", sl_order_id=active[0].get("order_id") if active else pos.get("sl_order_id"),
                       error="STOP partially/fully filled; reconciling remaining quantity through exit lifecycle")
            self._save(pos)
            if net_qty == 0:
                return self._finalize(pos, "SL_FILLED")
            self._exit_locked(pos, "SL_PARTIAL")
            return pos

        if len(active) == 1:
            o = active[0]
            if o.get("status") == "unknown":
                pos.update(status="STOP_UNKNOWN", error=f"tagged STOP {o.get('order_id')} has unknown broker status; manual reconciliation required")
                self._save(pos)
                self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
                return pos
            if net_qty != original_qty:
                pos.update(status="STOP_UNKNOWN", error=f"active STOP but broker net quantity {net_qty} != expected {original_qty}")
                self._save(pos)
                self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
                return pos
            pos.update(status="OPEN", sl_order_id=o.get("order_id"),
                       sl_trigger=float(o.get("trigger") or pos.get("sl_trigger") or 0),
                       sl_limit=float(o.get("price") or pos.get("sl_limit") or 0),
                       sl_variety="STOPLOSS", error=None)
            self._save(pos)
            try:
                if pos.get("sl_intent_id"):
                    from .order_manager import update_intent
                    update_intent(pos["sl_intent_id"], "open", broker_order_id=o.get("order_id"))
            except Exception:
                log.exception("Could not reconcile active STOP intent")
            self._info(f"Recovered existing stop {pos['sl_order_id']} for {pos['signal_id']}; no duplicate stop sent")
            return pos

        pos.update(status="STOP_UNKNOWN", error="no active tagged protective stop found; manual reconciliation required")
        self._save(pos)
        self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}")
        return pos

    # ══ TRAILING ═════════════════════════════════════════════════════════
    def sync_stop(self, signal_id: str, new_sl_price: float) -> bool:
        if not self.enabled:
            return False
        with self._lock(signal_id):
            pos = self.store.get(signal_id)
            if not pos or pos["status"] != "OPEN" or not pos.get("sl_order_id"):
                return False
            new_trigger = tick(new_sl_price)
            if new_trigger <= pos["sl_trigger"] + 1e-9:
                return False                                       # never lower / no-op
            ltp = 0.0
            try:
                ltp = self.broker.ltp(pos["contract"])
            except BrokerSafetyError:
                pass
            if ltp and new_trigger >= ltp:
                log.info("LIVE sync_stop %s: new trigger %.2f >= LTP %.2f; skipped (exit logic will handle)", signal_id, new_trigger, ltp)
                return False
            new_limit = max(tick(new_trigger * (1 - self.cfg.sl_limit_buffer_pct / 100)), 0.05)
            if self.cfg.shadow:
                return False
            ok = False
            for _ in range(2):
                ok = self.broker.modify_order(pos["sl_order_id"], pos["contract"], variety=pos["sl_variety"],
                                              order_type="STOPLOSS_LIMIT", product=pos["product"],
                                              quantity=pos["qty"], price=new_limit, trigger_price=new_trigger)
                if ok:
                    break
                self.sleep(0.5)
            info = None
            try:
                info = self.broker.get_order(pos["sl_order_id"])
            except BrokerSafetyError:
                pass
            if info and (info["status"] == "complete" or info["filled"] > 0):
                self._exit_locked(pos, "SL_FILLED")
                return True
            if not ok:
                self._alert(f"SL MODIFY FAILED {signal_id}: old stop {pos['sl_trigger']} remains active", f"mod{signal_id}", 60)
                return False
            pos.update(sl_trigger=new_trigger, sl_limit=new_limit)
            self._save(pos)
            self._info(f"SL raised {pos['contract']['tradingsymbol']} -> trigger {new_trigger}")
            return True

    # ══ EXIT ═════════════════════════════════════════════════════════════
    def exit_position(self, signal_id: str, reason: str, hint_price: float = 0.0) -> bool:
        if not self.enabled and not self.store.get(signal_id):
            return False
        with self._lock(signal_id):
            pos = self.store.get(signal_id)
            if not pos or pos["status"] not in ("OPEN", "STOP_UNKNOWN", "EXITING", "EXIT_PENDING", "EXIT_BLOCKED", "UNKNOWN_ENTRY"):
                return False
            return self._exit_locked(pos, reason, hint_price)

    def _pending(self, pos, msg, key):
        pos["status"] = "EXIT_PENDING"
        pos["error"] = msg
        self._save(pos)
        self._alert(msg, f"{key}{pos['signal_id']}", 60)
        return False

    def _reconcile_entry_before_exit(self, pos) -> bool:
        """Resolve any uncertain BUY before deciding how much can safely be sold."""
        try:
            book = self.broker.orders()
        except BrokerSafetyError as exc:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: BUY order book unavailable ({exc})", "buybook")
            return False

        tag = str(pos.get("tag") or "")
        buys = [o for o in book if o.get("side") == "BUY" and
                (o.get("tag") == tag or (pos.get("entry_order_id") and
                 str(o.get("order_id")) == str(pos.get("entry_order_id"))))]
        if len(buys) > 1:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: multiple matching BUY orders; manual reconciliation required", "buymulti")
            return False
        if not buys:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: unresolved BUY is absent from broker order book; refusing to assume flat", "buymissing")
            return False

        buy = buys[0]
        oid = str(buy.get("order_id") or "")
        if not oid:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: unresolved BUY has no broker order id", "buyid")
            return False

        status = buy.get("status", "unknown")
        if status in ("open", "trigger_pending", "unknown"):
            variety = buy.get("variety") or "NORMAL"
            try:
                self.broker.cancel_order(oid, variety)
            except BrokerSafetyError:
                pass
            info = self._poll(oid, TERMINAL, 8)
            if not info or info.get("status") not in TERMINAL:
                self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: BUY {oid} cancellation/terminal status unconfirmed; no SELL submitted", "buycancel")
                return False
            buy = info
            status = info.get("status")

        if status not in TERMINAL:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: BUY {oid} has unrecognized status {status!r}", "buystatus")
            return False

        try:
            held = int(self.broker.net_qty(pos["contract"]))
        except BrokerSafetyError as exc:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: broker net quantity unavailable after BUY reconciliation ({exc})", "buynet")
            return False

        filled = int(buy.get("filled") or 0)
        if held < 0 or held != filled:
            self._pending(pos, f"EXIT BLOCKED {pos['signal_id']}: BUY fill={filled}, broker net_qty={held}; manual reconciliation required", "buyqty")
            return False

        pos.update(entry_order_id=oid, qty=held, entry_avg=buy.get("avg") or pos.get("entry_avg"),
                   entry_reconciliation_required=False, status="OPEN", error=None)
        self._save(pos)
        return True

    def _exit_locked(self, pos, reason, hint_price=0.0) -> bool:
        """Cancel/confirm all tagged SELLs -> sell only reconciled broker-held quantity -> verify flat."""
        if pos.get("status") in ("UNKNOWN_ENTRY", "SUBMITTING"):
            if not self._reconcile_entry_before_exit(pos):
                return False
        contract = pos["contract"]
        pos.update(status="EXITING", exit_reason=reason)
        self._save(pos)
        sl_id = pos.get("sl_order_id")

        if sl_id:
            wait = self.cfg.sl_grace_s if reason in ("SL", "TRAILED_SL_PROFIT") else 0
            info = self._poll(sl_id, {"complete"}, wait) if wait else self._safe_order(sl_id)
            if not info or info["status"] in ("open", "trigger_pending", "unknown"):
                try:
                    self.broker.cancel_order(sl_id, pos.get("sl_variety", "STOPLOSS"))
                except BrokerSafetyError:
                    pass
                info = self._poll(sl_id, TERMINAL, 6)
                if not info or info["status"] not in TERMINAL:
                    pos["status"] = "EXIT_BLOCKED"
                    self._save(pos)
                    self._alert(f"EXIT BLOCKED {pos['signal_id']}: SL {sl_id} state unconfirmed; NOT selling to avoid a double sell. Check broker.", f"blk{sl_id}", 120)
                    return False

        # Includes an SL whose placeOrder response was lost and any previous exit order.
        if not self._cancel_tagged_sells(pos):
            pos["status"] = "EXIT_BLOCKED"
            pos["error"] = "Tagged SELL order remains unresolved; no new SELL submitted"
            self._save(pos)
            self._alert(f"EXIT BLOCKED {pos['signal_id']}: tagged SELL order cancellation/state unconfirmed; no new SELL sent", f"tagblk{pos['signal_id']}", 120)
            return False
        if self._has_active_exit(pos):
            return False

        try:
            held = self.broker.net_qty(contract)
        except BrokerSafetyError as e:
            return self._pending(pos, f"EXIT {pos['signal_id']}: cannot read broker position ({e}); not selling blind", "nq")
        if held < 0:
            return self._pending(pos, f"EXIT {pos['signal_id']}: broker shows NET SHORT {held}; manual action required", "short")

        # A completed SELL with a stale/mismatched position response is ambiguous. Do not
        # submit another order until broker position catches up or a human reconciles it.
        prev_id = pos.get("exit_order_id")
        if prev_id:
            prev = self._safe_order(prev_id)
            pre_qty = pos.get("exit_order_pre_qty")
            if prev and prev.get("status") in TERMINAL and pre_qty is not None:
                expected = max(0, int(pre_qty) - int(prev.get("filled") or 0))
                if int(held) != expected:
                    return self._pending(pos, f"EXIT {pos['signal_id']}: broker position {held} does not match terminal SELL {prev_id} expected {expected}; waiting for reconciliation, no duplicate SELL", "lag")
        if held > 0 and not self._sell(pos, held, reason):
            return False
        return self._finalize(pos, reason)

    def _sell(self, pos, qty, reason) -> bool:
        """Submit a uniquely claimed SELL and reconcile its exact fill against broker net quantity."""
        contract = pos["contract"]
        try:
            ltp = self.broker.ltp(contract)
        except BrokerSafetyError:
            ltp = 0.0
        types = [self.cfg.exit_order_type] + (["LIMIT"] if self.cfg.exit_order_type == "MARKET" else [])
        for otype in types:
            price = tick(max((ltp or pos.get("entry_avg", 1)) * 0.97, 0.05)) if otype == "LIMIT" else 0.0
            payload = AngelOneBroker.order_payload(contract, "SELL", qty, pos["product"], otype, price, tag=pos["tag"])
            attempt = int(pos.get("exit_attempt", 0))
            attempt_key = f"EXIT:{attempt}"
            intent_id = None
            try:
                from . import order_manager as om
                claim = getattr(om, "claim_order_intent", None)
                if claim:
                    result = claim(pos["signal_id"], "EXIT", attempt_key,
                                   {"contract": contract, "quantity": int(qty), "order_payload": payload,
                                    "reason": reason})
                    if not result or not result.get("claimed"):
                        return self._pending(pos, f"EXIT blocked: intent {attempt_key} was not claimed", "exitclaim")
                    intent_id = result.get("intent_id")
                else:
                    intent_id = om.enqueue_intent(
                        pos["signal_id"],
                        {"action": "EXIT", "contract": contract, "quantity": int(qty),
                         "order_payload": payload, "reason": reason},
                        action=f"EXIT:{attempt}",
                    )
                pos["exit_attempt"] = attempt + 1
                pos["exit_intent_id"] = intent_id
                pos.setdefault("sell_tags", [])
                if pos["tag"] not in pos["sell_tags"]:
                    pos["sell_tags"].append(pos["tag"])
                if intent_id:
                    om.update_intent(intent_id, "submitting")
                self._save(pos)
                oid = self.broker.place_order(payload, reduce_only=True)
            except OrderOutcomeUnknown as e:
                try:
                    if intent_id:
                        om.update_intent(intent_id, "unknown", error=str(e))
                except Exception:
                    log.exception("Could not mark EXIT intent unknown")
                self._pending(pos, f"EXIT outcome unknown ({e}); no retry until tagged order and broker position reconcile", "unk")
                return False
            except BrokerSafetyError as e:
                try:
                    if intent_id:
                        om.update_intent(intent_id, "rejected", error=str(e))
                except Exception:
                    log.exception("Could not mark EXIT intent rejected")
                log.error("exit order %s rejected: %s", otype, e)
                continue

            pos["exit_order_id"] = oid
            pos["exit_order_pre_qty"] = int(qty)
            self._save(pos)
            try:
                if intent_id:
                    om.update_intent(intent_id, "open", broker_order_id=oid)
            except Exception:
                log.exception("Could not update EXIT intent after broker acknowledgement")
            info = self._poll(oid, TERMINAL, self.cfg.exit_timeout_s)
            if not info or info.get("status") not in TERMINAL:
                try:
                    self.broker.cancel_order(oid, "NORMAL")
                except BrokerSafetyError:
                    pass
                info = self._poll(oid, TERMINAL, 4)
                if not info or info.get("status") not in TERMINAL:
                    return self._pending(pos, f"EXIT order {oid} remains active/unconfirmed; no replacement SELL will be submitted", "active")
            try:
                held = int(self.broker.net_qty(contract))
            except (BrokerSafetyError, TypeError, ValueError):
                return self._pending(pos, "EXIT sent but broker position unreadable; no retry until reconciled", "nq2")
            if held < 0:
                return self._pending(pos, f"EXIT {pos['signal_id']}: broker reports NET SHORT {held}; manual intervention required", "short2")
            expected = max(0, int(qty) - int(info.get("filled") or 0))
            if held != expected:
                return self._pending(pos, f"EXIT order {oid} is {info.get('status')} but broker position {held} != expected {expected}; waiting for reconciliation, no duplicate SELL", "lag2")
            try:
                if intent_id:
                    om.update_intent(intent_id, "complete", broker_order_id=oid)  # terminal order; held>0 is handled by a new EXIT attempt
            except Exception:
                log.exception("Could not update terminal EXIT intent")
            if held == 0:
                return True
            if info.get("status") in ("complete", "cancelled", "rejected"):
                qty = held
                continue
        return self._pending(pos, f"EXIT NOT CONFIRMED for {contract['tradingsymbol']}: broker still holds {qty}; manual review required if unresolved", "ep")

    def _safe_order(self, oid):
        try:
            return self.broker.get_order(oid)
        except BrokerSafetyError:
            return None

    def _cancel_tagged_sells(self, pos) -> bool:
        """Preserve active EXIT sells for reconciliation; never cancel or duplicate them."""
        try:
            orders = self.broker.orders()
        except BrokerSafetyError as e:
            log.warning("tag sweep failed; refusing further SELL: %s", e)
            return False
        tags = {str(x) for x in [pos.get("tag"), pos.get("exit_tag"), *(pos.get("sell_tags") or [])] if x}
        for o in orders:
            if str(o.get("tag") or "") not in tags or o.get("side") != "SELL":
                continue
            oid = str(o.get("order_id") or "")
            status = o.get("status", "unknown")
            if not oid:
                return False
            variety = str(o.get("variety") or "").upper()
            is_stop = variety == "STOPLOSS" or oid == str(pos.get("sl_order_id") or "")
            if status in ("open", "trigger_pending", "unknown"):
                if is_stop:
                    # Protective stop state is handled by _exit_locked before this sweep.
                    return False
                # Adopt/preserve the active exit. Caller must not send another SELL.
                pos["exit_order_id"] = oid
                pos["exit_order_pre_qty"] = int(o.get("quantity") or pos.get("exit_order_pre_qty") or pos.get("qty") or 0)
                self._save(pos)
                return True
            if status not in TERMINAL:
                log.error("tagged SELL %s has unrecognized status %r; refusing new SELL", oid, status)
                return False
        return True

    def _has_active_exit(self, pos) -> bool:
        oid = str(pos.get("exit_order_id") or "")
        if not oid:
            return False
        info = self._safe_order(oid)
        if info and info.get("status") in ("open", "trigger_pending", "unknown"):
            self._pending(pos, f"Existing EXIT {oid} is still {info.get('status')}; preserving it and not submitting another SELL", "activeexit")
            return True
        return False

    def _finalize(self, pos, reason) -> bool:
        """CLOSED only if the broker confirms a flat position. P&L comes from real SELL fills in the order book."""
        contract = pos["contract"]
        try:
            held = self.broker.net_qty(contract)
        except BrokerSafetyError as e:
            return self._pending(pos, f"CLOSE of {pos['signal_id']} not verified: {e}", "fin")
        if held != 0:
            return self._pending(pos, f"{contract['tradingsymbol']} broker position is {held}, not flat after exit — NOT marking closed", "nf")
        try:
            legs = [o for o in self.broker.orders() if o["tag"] == pos["tag"] and o["side"] == "SELL" and o["filled"] > 0]
        except BrokerSafetyError:
            legs = []
        sold = sum(o["filled"] for o in legs)
        entry = float(pos.get("entry_avg") or 0)
        if legs and sold > 0 and entry > 0 and all(o["avg"] > 0 for o in legs):
            proceeds = sum(o["filled"] * o["avg"] for o in legs)
            pnl = round(proceeds - entry * sold, 2)
            exit_avg = round(proceeds / sold, 2)
            status = "CLOSED"
            if sold != pos["qty"]:
                self._alert(f"{pos['signal_id']}: sold qty {sold} != position qty {pos['qty']} (broker is flat; review fills)", f"mm{pos['signal_id']}", 300)
            if all(o["order_id"] == pos.get("sl_order_id") for o in legs):
                reason = "SL_FILLED"
        else:
            pnl, exit_avg, status = None, None, "CLOSED_UNPRICED"
            self._alert(f"{pos['signal_id']} is flat but exit fill price could not be established from the order book — "
                        f"P&L unknown; daily-loss accounting will assume the planned loss", f"up{pos['signal_id']}", 300)
        pos.update(status=status, exit_avg=exit_avg, exit_reason=reason, pnl=pnl, error=None,
                   closed_date=today_ist(), closed_at=self.clock().isoformat())
        self._save(pos)
        if pnl is not None:
            self._info(f"CLOSED {contract['tradingsymbol']} {reason} entry {entry} exit {exit_avg} qty {sold} "
                       f"P&L ₹{pnl:,.0f} (gross, before charges; broker flat ✓)")
        return True

    # ══ SUPERVISOR / RECONCILE ═══════════════════════════════════════════
    def start_supervisor(self):
        if not self.enabled or self._sup_thread:
            return
        self.reconcile_on_start()
        self._sup_thread = threading.Thread(target=self._loop, daemon=True, name="live-supervisor")
        self._sup_thread.start()
        log.info("LIVE supervisor started (shadow=%s)", self.cfg.shadow)

    def _loop(self):
        while not self._stop.is_set():
            try:
                now = self.clock()
                if now.weekday() < 5 and "09:10" <= now.strftime("%H:%M") <= "15:35":
                    self.supervise_once()
            except Exception as e:
                log.error("supervisor error: %s", e, exc_info=True)
            self._stop.wait(self.cfg.supervise_every_s)

    def reconcile_on_start(self):
        act = list_active(self.store)
        if act:
            self._alert(f"Restart: reconciling {len(act)} live position(s): " + ", ".join(p['signal_id'] for p in act))
        self.supervise_once()

    def supervise_once(self):
        # Shadow blocks new BUYs only. Existing broker positions still require reconciliation.
        flatten = self.clock().strftime("%H:%M") >= self.cfg.flatten_hhmm
        try:
            book = {o["order_id"]: o for o in self.broker.orders()}
        except BrokerSafetyError as e:
            self._alert(f"supervisor cannot read order book: {e}", "book", 300)
            return
        for pos in list_active(self.store):
            sid = pos["signal_id"]
            with self._lock(sid):
                pos = self.store.get(sid)
                if not pos:
                    continue
                st = pos["status"]
                if st in ("UNKNOWN_ENTRY", "SUBMITTING"):
                    self._reconcile_entry(pos, book)
                elif st == "STOP_UNKNOWN":
                    self._reconcile_stop(pos, book)
                elif st in ("EXIT_PENDING", "EXIT_BLOCKED", "EXITING"):
                    self._exit_locked(pos, pos.get("exit_reason") or "RETRY")
                elif st == "OPEN":
                    self._supervise_open(pos, book, flatten)

    def _supervise_open(self, pos, book, flatten):
        sl = book.get(pos.get("sl_order_id"))
        if sl and (sl["status"] == "complete" or sl["filled"] > 0):
            # stop filled fully OR partially: _exit_locked cancels any remainder, sells what the broker
            # still holds and only closes once the broker position is flat
            self._exit_locked(pos, "SL_FILLED" if sl["status"] == "complete" else "SL_PARTIAL")
            return
        if flatten:
            self._exit_locked(pos, "EOD_FLATTEN")
            return
        if not sl or sl["status"] in ("rejected", "cancelled"):
            pos.update(status="STOP_UNKNOWN",
                       error=f"Protective stop {pos.get('sl_order_id')} is missing or {((sl or {}).get('status') or 'not found')}; no automatic replacement or SELL")
            self._save(pos)
            self._alert(f"STOP UNKNOWN for {pos['signal_id']}: {pos['error']}; broker-side manual check required")
            return
        try:
            ltp = self.broker.ltp(pos["contract"])
        except BrokerSafetyError:
            return
        if ltp and ltp < pos["sl_limit"]:           # gapped through the stop-limit; it cannot fill
            self._alert(f"{pos['signal_id']} gapped below stop-limit ({ltp} < {pos['sl_limit']}) — exiting at market", f"gap{pos['signal_id']}", 120)
            self._exit_locked(pos, "SL_GAP")

    def _reconcile_entry(self, pos, book):
        entry_id = str(pos.get("entry_order_id") or "")
        tagged = [o for o in book.values() if o.get("side") == "BUY" and
                  (o.get("tag") == pos.get("tag") or
                   (entry_id and str(o.get("order_id") or "") == entry_id))]
        if len(tagged) > 1:
            pos.update(status="UNKNOWN_ENTRY", error=f"multiple tagged BUY orders found ({len(tagged)}); manual reconciliation required; no order adopted")
            self._save(pos)
            self._alert(f"ENTRY RECOVERY BLOCKED {pos['signal_id']}: multiple matching tagged BUY orders")
            return
        if not tagged:
            age = (self.clock() - datetime.fromisoformat(pos["updated"])).total_seconds() if pos.get("updated") else 999
            if age > 90:
                try:
                    flat = self.broker.net_qty(pos["contract"]) == 0
                except BrokerSafetyError:
                    return
                if flat:
                    pos.update(status="ENTRY_FAILED", error="no broker order found for tag")
                    self._save(pos)
                else:
                    self._alert(f"Position exists for {pos['contract']['tradingsymbol']} but no tagged order — manual check", f"orph{pos['signal_id']}", 300)
            return
        o = tagged[0]
        if o["status"] in ("open", "trigger_pending"):
            self.broker.cancel_order(o["order_id"], "NORMAL")
            return
        if o["filled"] > 0:
            filled = int(o["filled"])
            try:
                net_qty = int(self.broker.net_qty(pos["contract"]))
            except (BrokerSafetyError, TypeError, ValueError) as e:
                pos.update(status="UNKNOWN_ENTRY", qty=filled,
                           error=f"recovered BUY fill={filled}, broker net quantity unavailable: {e}")
                self._save(pos)
                return
            if net_qty != filled:
                pos.update(status="UNKNOWN_ENTRY", qty=filled,
                           error=f"recovered BUY fill={filled}, broker net_qty={net_qty}; stop not submitted")
                self._save(pos)
                self._alert(f"ENTRY RECOVERY BLOCKED {pos['signal_id']}: fill/net mismatch {filled}/{net_qty}")
                return
            pos.update(entry_order_id=o["order_id"], qty=filled, entry_avg=o["avg"], status="OPEN")
            self._save(pos)
            self._alert(f"Adopted filled entry {o['order_id']} for {pos['signal_id']}; broker net qty verified, placing stop")
            self._protect(pos, pos.get("sl_planned") or o["avg"] * 0.8)
        elif o["status"] in TERMINAL:
            pos.update(status="ENTRY_FAILED", error=f"reconciled: {o['status']}")
            self._save(pos)

    def stop(self):
        self._stop.set()


# ══ module-level hooks used by the engine patch ═════════════════════════
_BRIDGE = None
_BRIDGE_LOCK = threading.Lock()


def get_bridge() -> LiveBridge:
    global _BRIDGE
    with _BRIDGE_LOCK:
        if _BRIDGE is None:
            store = PgStore()
            b = LiveBridge(store=store)
            if b.enabled:
                b.broker.login()
                b.start_supervisor()
            _BRIDGE = b
        return _BRIDGE
