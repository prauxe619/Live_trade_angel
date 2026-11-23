"""Angel One SmartAPI adapter (live-capable).

Safety model
  * Entry (BUY) orders need ALL gates: TRADING_MODE=live, TRADING_ENABLED=true,
    LIVE_TRADING_ACK=I_UNDERSTAND_REAL_ORDERS, ALLOW_ORDER_SUBMISSION=true, kill switch off.
  * Risk-reducing orders (SELL / modify / cancel) need TRADING_MODE=live + the ACK only, so a
    position can still be protected or closed if you flip TRADING_ENABLED off mid-day.
  * placeOrder is NEVER auto-retried. A network error raises OrderOutcomeUnknown so the caller
    reconciles against the order book instead of risking a duplicate order.
"""
from __future__ import annotations
import os, json, time, logging, threading, urllib.request
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import time
import math
import os
import threading
import time

log = logging.getLogger("black.broker")
SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
TICK = 0.05


class BrokerSafetyError(RuntimeError):
    pass


class OrderOutcomeUnknown(BrokerSafetyError):
    """Request may or may not have reached the exchange. Reconcile before doing anything else."""


def tick(price: float) -> float:
    return round(round(float(price) / TICK) * TICK, 2)


def engine_expiry_to_master(expiry: str) -> str:
    """'27 MAY 25' / '2025-05-27' / '27MAY2025' -> '27MAY2025' (Angel scrip-master format)."""
    s = str(expiry).strip().upper()
    for fmt in ("%d %b %y", "%d %b %Y", "%Y-%m-%d", "%d%b%Y", "%d-%b-%Y", "%d%b%y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d%b%Y").upper()
        except ValueError:
            continue
    raise BrokerSafetyError(f"Unrecognised expiry format: {expiry!r}")


def kill_switch_on() -> bool:
    return os.getenv("LIVE_KILL_SWITCH", "false").strip().lower() in ("1", "true", "yes", "on")


def _require_gates(reduce_only: bool) -> None:
    if os.getenv("TRADING_MODE", "paper").lower() != "live":
        raise BrokerSafetyError("TRADING_MODE is not live")
    if os.getenv("LIVE_TRADING_ACK", "") != "I_UNDERSTAND_REAL_ORDERS":
        raise BrokerSafetyError("LIVE_TRADING_ACK confirmation missing")
    if reduce_only:
        return
    if os.getenv("TRADING_ENABLED", "false").lower() != "true":
        raise BrokerSafetyError("TRADING_ENABLED is not true")
    if os.getenv("ALLOW_ORDER_SUBMISSION", "false").lower() != "true":
        raise BrokerSafetyError("ALLOW_ORDER_SUBMISSION is not true")
    if kill_switch_on():
        raise BrokerSafetyError("LIVE_KILL_SWITCH is on")


_STATUS_MAP = {
    "complete": "complete", "completed": "complete", "executed": "complete",
    "rejected": "rejected", "cancelled": "cancelled", "canceled": "cancelled",
    "trigger pending": "trigger_pending",
    # A partially executed order is still live until its unfilled remainder is
    # cancelled/terminal. Treat it as active so recovery never protects only
    # the partial fill while the BUY remainder can still execute.
    "pending": "open", "partially executed": "open", "partially filled": "open",
    "partial": "open", "open": "open", "open pending": "open", "validation pending": "open",
    "put order req received": "open", "modified": "open", "modify pending": "open",
    "modify validation pending": "open", "after market order req received": "open",
}


def norm_status(raw) -> str:
    return _STATUS_MAP.get(str(raw or "").strip().lower(), "unknown")


def _num(v, cast=float, default=0):
    try:
        return cast(float(v))
    except (TypeError, ValueError):
        return default


class AngelOneBroker:
    MIN_INTERVAL_S = 0.4          # stay under SmartAPI per-second limits
    RELOGIN_AFTER_S = 5 * 3600
    TOKEN_ERRORS = {"AG8001", "AG8002", "AG8003", "AB8050", "AB8051"}

    def __init__(self):
        self.smart = None
        self._lock = threading.RLock()
        self._last_call = 0.0
        self._login_at = 0.0
        self._auth_provider = None
    
        # Order-book rate-limit protection
        # Order-book request manager.
        # One cache and lock per broker session.
        self._order_book_lock = threading.RLock()
        
        self._order_book_cache = None
        self._order_book_cache_at = 0.0
        
        self._order_book_cache_ttl = max(
            0.5,
            float(os.getenv("ORDER_BOOK_CACHE_TTL_SECONDS", "2"))
        )
        self._order_book_backoff_until = 0.0
        self._order_book_failures = 0

    # ── shared authentication session ────────────────────────────────────
    def set_auth_provider(self, provider):
        """Use the signal fetcher's authenticated SmartConnect session."""
        if provider is None or not callable(getattr(provider, "_ensure_auth", None)):
            raise BrokerSafetyError("Invalid shared Angel One auth provider")

        if not hasattr(provider, "smart") or not hasattr(provider, "_auth_time"):
            raise BrokerSafetyError("Shared auth provider lacks session state")

        with self._lock:
            if self._auth_provider is not None and self._auth_provider is not provider:
                raise BrokerSafetyError(
                    "Broker is already bound to a different auth provider"
                )
            self._auth_provider = provider
            self.smart = None
            self._login_at = 0.0

    def _sync_provider_session(self, force=False):
        """Refresh through the provider when needed and share its current client."""
        provider = self._auth_provider
        if provider is None:
            raise BrokerSafetyError("Shared auth provider is not configured")

        try:
            authenticated = provider._ensure_auth(force=force)
        except Exception as exc:
            raise BrokerSafetyError(
                f"Shared Angel One authentication raised {type(exc).__name__}"
            ) from exc

        if not authenticated or provider.smart is None:
            raise BrokerSafetyError(
                "Shared Angel One authentication failed; broker operation blocked"
            )

        self.smart = provider.smart
        self._login_at = provider._auth_time or time.time()
        return self.smart

    # ── session ───────────────────────────────────────────────────────────
    def login(self):
        with self._lock:

            # Shared-session mode: never generate a second session.
            if self._auth_provider is not None:
                self._sync_provider_session()
                return {
                    "authenticated": True,
                    "shared_session": True,
                    "client_id": os.environ["ANGEL_CLIENT_ID"],
                }

            # Standalone mode retained for other broker users.
            required = [
                "ANGEL_API_KEY",
                "ANGEL_CLIENT_ID",
                "ANGEL_PASSWORD",
                "ANGEL_TOTP",
            ]

            missing = [k for k in required if not os.getenv(k)]

            if missing:
                raise BrokerSafetyError(
                    "Missing trading credentials: " + ", ".join(missing)
                )

            from SmartApi import SmartConnect
            import pyotp

            smart = SmartConnect(api_key=os.environ["ANGEL_API_KEY"])
            totp = pyotp.TOTP(os.environ["ANGEL_TOTP"]).now()

            resp = smart.generateSession(
                os.environ["ANGEL_CLIENT_ID"],
                os.environ["ANGEL_PASSWORD"],
                totp,
            )

            if not resp or not resp.get("status") or not resp.get("data"):
                raise BrokerSafetyError(
                    "Angel One login failed (check logs; secrets are never printed)"
                )

            self.smart = smart
            self._login_at = time.time()

            return {
                "authenticated": True,
                "client_id": os.environ["ANGEL_CLIENT_ID"],
            }

    def _call(self, name: str, *args, retry_on_token_error: bool = True, **kw):
        with self._lock:
            if self._auth_provider is not None:
                self._sync_provider_session()
            else:
                if self.smart is None:
                    raise BrokerSafetyError("Call login() first")
    
                if time.time() - self._login_at > self.RELOGIN_AFTER_S:
                    self.login()
    
            def invoke():
                gap = self.MIN_INTERVAL_S - (time.time() - self._last_call)
    
                if gap > 0:
                    time.sleep(gap)
    
                self._last_call = time.time()
    
                try:
                    return getattr(self.smart, name)(*args, **kw)
    
                except Exception as exc:
                    error_text = str(exc).lower()
    
                    rate_limited = (
                        "exceeding access rate" in error_text
                        or "access denied" in error_text
                        or "rate limit" in error_text
                        or "too many requests" in error_text
                    )
    
                    if rate_limited:
                        log.error(
                            "Angel One API rate limit: operation=%s",
                            name
                        )
    
                        if name == "placeOrder":
                            raise OrderOutcomeUnknown(
                                "Angel One rate-limit response during order "
                                "submission; reconcile broker order book before "
                                "any further order action"
                            ) from exc
    
                        raise BrokerSafetyError(
                            f"Angel One API rate limited operation: {name}"
                        ) from exc
    
                    raise
    
            res = invoke()
    
            if (
                isinstance(res, dict)
                and res.get("errorcode") in self.TOKEN_ERRORS
                and retry_on_token_error
            ):
                log.warning(
                    "Token error on %s; refreshing session and retrying once",
                    name
                )
    
                if self._auth_provider is not None:
                    self._sync_provider_session(force=True)
                else:
                    self.login()
    
                # Never retry order submission.
                if name == "placeOrder":
                    raise OrderOutcomeUnknown(
                        "Token error during order submission; reconciliation required"
                    )
    
                res = invoke()
    
            return res

    # ── instrument master ────────────────────────────────────────────────
    @staticmethod
    def download_master(path: str, max_age_h: float = 20.0) -> str:
        p = Path(path)
        if p.is_file() and (time.time() - p.stat().st_mtime) < max_age_h * 3600:
            return str(p)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        with urllib.request.urlopen(SCRIP_MASTER_URL, timeout=60) as r:
            tmp.write_bytes(r.read())
        json.loads(tmp.read_text(encoding="utf-8"))   # validate before replacing
        tmp.replace(p)
        return str(p)

    @staticmethod
    def find_contract(master_path: str, underlying: str, expiry: str, strike: float, option_type: str) -> dict:
        """Resolve exactly one option contract from the scrip master; never guess tokens."""
        p = Path(master_path)
        if not p.is_file():
            raise BrokerSafetyError(f"Instrument master not found: {p}")
        opt = option_type.upper()
        if opt not in ("CE", "PE"):
            raise BrokerSafetyError("option_type must be CE or PE")
        want_exp = engine_expiry_to_master(expiry)
        data = json.loads(p.read_text(encoding="utf-8"))
        rows = data if isinstance(data, list) else data.get("data", [])
        matches = []
        for r in rows:
            if str(r.get("instrumenttype", "")).upper() != "OPTIDX":
                continue
            if str(r.get("name", "")).upper() != underlying.upper():
                continue
            if str(r.get("expiry", "")).replace("-", "").upper() != want_exp:
                continue
            try:
                rs = float(r.get("strike", 0) or 0) / 100.0      # master stores strike x 100
            except (TypeError, ValueError):
                continue
            if abs(rs - float(strike)) > 0.001:
                continue
            if not str(r.get("symbol", "")).upper().endswith(opt):
                continue
            matches.append(r)
        if len(matches) != 1:
            raise BrokerSafetyError(
                f"Expected exactly one contract for {underlying} {want_exp} {strike}{opt}, found {len(matches)}")
        r = matches[0]
        exchange = str(r.get("exch_seg", "")).upper()
        token, symbol = str(r.get("token", "")), str(r.get("symbol", ""))
        if not token or not symbol or exchange not in ("NFO", "BFO"):
            raise BrokerSafetyError("Contract missing token/symbol or unexpected exchange")
        return {"underlying": underlying.upper(), "expiry": want_exp, "strike": float(strike),
                "option_type": opt, "symboltoken": token, "tradingsymbol": symbol,
                "exchange": exchange, "lotsize": int(float(r.get("lotsize", 0) or 0)),
                "tick_size": float(r.get("tick_size", 5) or 5) / 100.0}

    # ── payloads ─────────────────────────────────────────────────────────
    @staticmethod
    def order_payload(contract: dict, transaction_type: str, quantity: int, product_type: str = "INTRADAY",
                      order_type: str = "LIMIT", price: float = 0.0, trigger_price: float = 0.0,
                      variety: str = "NORMAL", tag: str = "") -> dict:
        side, typ, product = transaction_type.upper(), order_type.upper(), product_type.upper()
        if side not in ("BUY", "SELL"):
            raise BrokerSafetyError("transaction_type must be BUY or SELL")
        if typ not in ("MARKET", "LIMIT", "STOPLOSS_LIMIT"):
            raise BrokerSafetyError("order_type must be MARKET, LIMIT or STOPLOSS_LIMIT")
        if product not in ("INTRADAY", "CARRYFORWARD"):
            raise BrokerSafetyError("product_type must be INTRADAY or CARRYFORWARD")
        if variety not in ("NORMAL", "STOPLOSS"):
            raise BrokerSafetyError("variety must be NORMAL or STOPLOSS")
        if int(quantity) <= 0:
            raise BrokerSafetyError("quantity must be positive")
        if contract.get("exchange") not in ("NFO", "BFO") or not contract.get("symboltoken") or not contract.get("tradingsymbol"):
            raise BrokerSafetyError("Contract has not passed instrument-master validation")
        if typ in ("LIMIT", "STOPLOSS_LIMIT") and price <= 0:
            raise BrokerSafetyError("price required for LIMIT/STOPLOSS_LIMIT")
        if typ == "STOPLOSS_LIMIT" and trigger_price <= 0:
            raise BrokerSafetyError("trigger_price required for STOPLOSS_LIMIT")
        payload = {"variety": variety, "tradingsymbol": contract["tradingsymbol"],
                   "symboltoken": contract["symboltoken"], "transactiontype": side,
                   "exchange": contract["exchange"], "ordertype": typ, "producttype": product,
                   "duration": "DAY", "price": str(tick(price)) if typ != "MARKET" else "0",
                   "squareoff": "0", "stoploss": "0", "quantity": str(int(quantity))}
        if typ == "STOPLOSS_LIMIT":
            payload["triggerprice"] = str(tick(trigger_price))
        if tag:
            payload["ordertag"] = tag[:20]
        return payload

    # ── orders ───────────────────────────────────────────────────────────
    
    def place_order(self, payload: dict, reduce_only: bool = False) -> str:
        """
        Submit one order to Angel One.
    
        Safety rules:
        - Order submission must pass the existing trading gates.
        - reduce_only is allowed only for SELL orders (long options).
        - Never automatically retry an order submission.
        - Explicit broker rejection raises BrokerSafetyError.
        - Ambiguous responses raise OrderOutcomeUnknown.
        """
    
        # Validate payload before attempting broker submission.
        if not isinstance(payload, dict):
            raise BrokerSafetyError("Order payload must be a dictionary")
    
        transaction_type = str(
            payload.get("transactiontype") or ""
        ).strip().upper()
    
        if transaction_type not in {"BUY", "SELL"}:
            raise BrokerSafetyError(
                f"Invalid transactiontype in order payload: {transaction_type!r}"
            )
    
        if reduce_only and transaction_type != "SELL":
            raise BrokerSafetyError(
                "reduce_only orders must be SELL (long options only)"
            )
    
        # Keep the existing execution gates.
        # This must remain outside the broker-call exception handler.
        _require_gates(reduce_only)
    
        # Select the broker API method supported by this SDK instance.
        fn = (
            "placeOrderFullResponse"
            if hasattr(self.smart, "placeOrderFullResponse")
            else "placeOrder"
        )
    
        try:
            # IMPORTANT:
            # Do not retry order submissions automatically.
            # A timeout or connection failure can happen after broker acceptance.
            res = self._call(
                fn,
                payload,
                retry_on_token_error=False,
            )
    
        except BrokerSafetyError:
            # Preserve the existing adapter's explicit safety/error semantics.
            raise
    
        except Exception as exc:
            # The broker may have accepted the order before the exception occurred.
            # Do not submit it again automatically.
            raise OrderOutcomeUnknown(
                f"placeOrder outcome is unknown: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
    
        # Validate the broker response.
        if isinstance(res, dict):
    
            status = res.get("status")
    
            # Only an explicit broker rejection is treated as a confirmed rejection.
            if status is False or (
                isinstance(status, str)
                and status.strip().lower() == "false"
            ):
                raise BrokerSafetyError(
                    "Order rejected by broker API: "
                    f"{res.get('message')} [{res.get('errorcode')}]"
                )
    
            data = res.get("data")
    
            if isinstance(data, dict):
                order_id = (
                    data.get("orderid")
                    or data.get("order_id")
                )
            else:
                order_id = data
    
        else:
            # Some SDK versions return the order ID directly.
            order_id = res
    
        # No confirmed order ID means the submission outcome is uncertain.
        if order_id is None or not str(order_id).strip():
            raise OrderOutcomeUnknown(
                f"No order ID returned by Angel One: {res!r}"
            )
    
        return str(order_id).strip()


    
    def modify_order(
        self,
        order_id: str,
        contract: dict,
        *,
        variety: str,
        order_type: str,
        product: str,
        quantity: int,
        price: float,
        trigger_price: float = 0.0,
        tag: str = "",
    ) -> bool:
        _require_gates(True)
    
        if not order_id:
            raise BrokerSafetyError("modify_order requires an order ID")
    
        if not isinstance(contract, dict):
            raise BrokerSafetyError("modify_order requires a valid contract")
    
        params = {
            "variety": variety,
            "orderid": str(order_id),
            "ordertype": order_type,
            "producttype": product,
            "duration": "DAY",
            "price": str(tick(price)),
            "quantity": str(int(quantity)),
            "tradingsymbol": contract["tradingsymbol"],
            "symboltoken": contract["symboltoken"],
            "exchange": contract["exchange"],
        }
    
        # Angel One requires the original ordertag on modification requests.
        if tag:
            params["ordertag"] = str(tag)[:20]
    
        if trigger_price:
            params["triggerprice"] = str(tick(trigger_price))
    
        with self._order_book_lock:
            self.invalidate_order_book_cache()
    
            try:
                res = self._call(
                    "modifyOrder",
                    params,
                    retry_on_token_error=False,
                )
    
                return bool(
                    isinstance(res, dict)
                    and res.get("status") is True
                )
    
            except Exception as exc:
                log.error(
                    "modifyOrder failed for order %s: %s",
                    order_id,
                    exc,
                )
                return False
    
            finally:
                self.invalidate_order_book_cache()
    
    
    def cancel_order(
        self,
        order_id: str,
        variety: str = "NORMAL",
    ) -> bool:
        _require_gates(True)
    
        if not order_id:
            raise BrokerSafetyError("cancel_order requires an order ID")
    
        with self._order_book_lock:
            self.invalidate_order_book_cache()
    
            try:
                res = self._call(
                    "cancelOrder",
                    str(order_id),
                    variety,
                    retry_on_token_error=False,
                )
    
                return bool(
                    isinstance(res, dict)
                    and res.get("status") is True
                )
    
            except Exception as exc:
                log.error(
                    "cancelOrder failed for order %s: %s",
                    order_id,
                    exc,
                )
                return False
    
            finally:
                self.invalidate_order_book_cache()


    # ── reads ────────────────────────────────────────────────────────────
    def invalidate_order_book_cache(self):
        """Invalidate the cached broker order book."""
        with self._order_book_lock:
            self._order_book_cache = None
            self._order_book_cache_at = 0.0
    
    
    def order_book(self, *, force_refresh=False):
        """
        Return a validated broker order book.
    
        Ordinary reads:
            Use the short-lived successful-response cache.
    
        Critical reconciliation:
            Use force_refresh=True to demand a fresh broker response.
    
        Fail closed:
            Never convert a failed or unavailable response into [].
        """
        with self._order_book_lock:
            now = time.monotonic()
    
            # Ordinary reads can reuse a recently successful response.
            if not force_refresh and self._order_book_cache is not None:
                age = now - self._order_book_cache_at
    
                if age <= self._order_book_cache_ttl:
                    return [dict(row) for row in self._order_book_cache]
    
            # Respect rate-limit and API-failure backoff.
            wall_now = time.time()
    
            if wall_now < self._order_book_backoff_until:
                remaining = int(
                    self._order_book_backoff_until - wall_now
                ) + 1
    
                raise BrokerSafetyError(
                    "Order book unavailable during broker backoff; "
                    f"retry allowed in approximately {remaining} seconds"
                )
    
            try:
                response = self._call("orderBook")
    
                if (
                    not isinstance(response, dict)
                    or response.get("status") is not True
                ):
                    raise BrokerSafetyError(
                        f"orderBook failed or status is not true: {response!r}"
                    )
    
                data = response.get("data")
    
                if not isinstance(data, list):
                    raise BrokerSafetyError(
                        "Order book response is not a confirmed list"
                    )
    
                if any(not isinstance(row, dict) for row in data):
                    raise BrokerSafetyError(
                        "Order book contains malformed rows"
                    )
    
                # Copy validated rows before caching.
                validated = [dict(row) for row in data]
    
                # Only successful, validated responses reset backoff.
                self._order_book_failures = 0
                self._order_book_backoff_until = 0.0
    
                self._order_book_cache = validated
                self._order_book_cache_at = time.monotonic()
    
                return [dict(row) for row in validated]
    
            except Exception as exc:
                self._order_book_cache = None
                self._order_book_cache_at = 0.0
    
                self._order_book_failures += 1
    
                delay = min(
                    30 * (2 ** (self._order_book_failures - 1)),
                    300,
                )
    
                self._order_book_backoff_until = time.time() + delay
    
                log.error(
                    "Order book unavailable. "
                    "Failure=%s Backoff=%ss Error=%s",
                    self._order_book_failures,
                    delay,
                    exc,
                )
    
                if isinstance(exc, BrokerSafetyError):
                    raise
    
                raise BrokerSafetyError(
                    f"Order book request failed: {type(exc).__name__}: {exc}"
                ) from exc

    
    def position_book(self) -> list:
        """
        Fetch and validate the account position book.
        Never interpret an unavailable response as an empty position book.
        """
        last_response = None
    
        for attempt in range(2):
            try:
                res = self._call("position")
                last_response = res
    
                if not isinstance(res, dict):
                    raise BrokerSafetyError(
                        f"position returned invalid response type: "
                        f"{type(res).__name__}"
                    )
    
                if res.get("status") is not True:
                    raise BrokerSafetyError(
                        "position API failed: "
                        f"message={res.get('message')!r}, "
                        f"errorcode={res.get('errorcode')!r}"
                    )
    
                data = res.get("data")
    
                if isinstance(data, list):
                    if any(not isinstance(row, dict) for row in data):
                        raise BrokerSafetyError(
                            "position response contains a malformed row"
                        )
                    return data
    
                if attempt == 0:
                    log.warning(
                        "Position API returned unexpected data type=%s; "
                        "retrying once. message=%s errorcode=%s",
                        type(data).__name__,
                        res.get("message"),
                        res.get("errorcode"),
                    )
                    time.sleep(0.6)
                    continue
    
                raise BrokerSafetyError(
                    "position data is not a confirmed list "
                    f"(received {type(data).__name__}); "
                    "manual-position check unavailable. "
                    f"message={res.get('message')!r}, "
                    f"errorcode={res.get('errorcode')!r}"
                )
    
            except BrokerSafetyError:
                if attempt == 1:
                    raise
                time.sleep(0.6)
    
            except Exception as exc:
                if attempt == 1:
                    raise BrokerSafetyError(
                        f"Position API unavailable after retry: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                time.sleep(0.6)
    
        raise BrokerSafetyError(
            f"Position API unavailable: {last_response!r}"
        )


    @staticmethod
    def _normalise(row: dict) -> dict:
        quantity = _num(row.get("quantity", row.get("orderqty")), int, 0)
        filled = _num(row.get("filledshares"), int, 0)
        return {
            "order_id": str(row.get("orderid", "")),
            "status": norm_status(row.get("orderstatus") or row.get("status")),
            "filled": filled,
            "quantity": quantity,
            "remaining": max(0, quantity - filled),
            "avg": _num(row.get("averageprice"), float, 0.0),
            "tag": str(row.get("ordertag", "") or ""),
            "side": str(row.get("transactiontype", "")).upper(),
            "symbol": str(row.get("tradingsymbol", "") or ""),
            "symboltoken": str(row.get("symboltoken", "") or ""),
            "exchange": str(row.get("exchange", "") or "").upper(),
            "product": str(row.get("producttype", "") or "").upper(),
            "text": str(row.get("text", "") or ""),
            "variety": str(row.get("variety", "") or "").upper(),
            "order_type": str(row.get("ordertype", row.get("order_type", "")) or "").upper(),
            "trigger": _num(row.get("triggerprice"), float, 0.0),
            "price": _num(row.get("price"), float, 0.0),
        }

    def orders(self, *, force_refresh=False):
        return [
            self._normalise(row)
            for row in self.order_book(force_refresh=force_refresh)
        ]
    
    
    def get_order(self, order_id: str, *, force_refresh=False):
        for order in self.orders(force_refresh=force_refresh):
            if order["order_id"] == str(order_id):
                return order
    
        return None

    @staticmethod
    def _same_contract(row: dict, contract: dict) -> bool:
        token = str(contract.get("symboltoken") or "")
        symbol = str(contract.get("tradingsymbol") or "")
        exchange = str(contract.get("exchange") or "").upper()
        row_token = str(row.get("symboltoken") or "")
        row_symbol = str(row.get("tradingsymbol") or row.get("symbol") or "")
        row_exchange = str(row.get("exchange") or "").upper()
        if exchange and row_exchange and exchange != row_exchange:
            return False
        token_match = bool(token and row_token and token == row_token)
        symbol_match = bool(symbol and row_symbol and symbol == row_symbol)
        # If both identifiers are present in the broker row, require both to agree.
        if row_token and row_symbol and token and symbol:
            return token_match and symbol_match
        return token_match or symbol_match

    def entry_conflict(self, contract: dict) -> dict:
        """Return manual/bot exposure conflicts; unknown broker data raises (fail closed)."""
        positions = self.position_book()
        orders = self.orders()
        open_positions = []
        for row in positions:
            if not self._same_contract(row, contract):
                continue
            net = _num(row.get("netqty"), int, 0)
            if net != 0:
                open_positions.append({"net_qty": net, "symbol": row.get("tradingsymbol", "")})
        active_orders = []
        terminal = {"complete", "rejected", "cancelled"}
        for order in orders:
            if not self._same_contract(order, contract):
                continue
            # Any non-terminal order for this exact contract is a conflict.
            # Do not rely on quantity fields being present in every SmartAPI row.
            if order["status"] not in terminal:
                active_orders.append(order)
        return {"open_positions": open_positions, "active_orders": active_orders,
                "blocked": bool(open_positions or active_orders)}

    def net_qty(self, contract: dict) -> int:
        total = 0
        for row in self.position_book():
            if self._same_contract(row, contract):
                total += _num(row.get("netqty"), int, 0)
        return total

    
    def ltp_quote(self, contract: dict) -> dict:
        """
        Request option LTP directly from Angel One.
    
        received_at_epoch is the API response receipt time,
        NOT the exchange tick timestamp.
        """
        requested_at = time.time()
    
        res = self._call(
            "ltpData",
            contract["exchange"],
            contract["tradingsymbol"],
            contract["symboltoken"],
        )
    
        received_at = time.time()
    
        if not isinstance(res, dict) or res.get("status") is not True:
            raise BrokerSafetyError(
                f"Angel One LTP request failed: "
                f"{res.get('message') if isinstance(res, dict) else 'invalid response'}"
            )
    
        data = res.get("data")
        if not isinstance(data, dict):
            raise BrokerSafetyError("Angel One returned invalid LTP data")
    
        ltp = _num(data.get("ltp"), float, 0.0)
    
        if not math.isfinite(ltp) or ltp <= 0:
            raise BrokerSafetyError("Angel One returned invalid option LTP")
    
        return {
            "ltp": ltp,
            "requested_at_epoch": requested_at,
            "received_at_epoch": received_at,
            "request_latency_s": received_at - requested_at,
            "exchange_feed_time": data.get("exchFeedTime"),
            "exchange_trade_time": data.get("exchTradeTime"),
            "symbol": contract["tradingsymbol"],
            "symboltoken": contract["symboltoken"],
        }
     
    def market_quote(self, contract: dict, max_age_s: float = 5.0) -> dict:
        """
        Fetch a fresh single-contract FULL quote from Angel One.

        Freshness is based on exchange exchFeedTime, not API response time.
        Missing or invalid timestamps fail closed.
        """
        requested_at = time.time()

        exchange = str(contract.get("exchange") or "").upper()
        token = str(contract.get("symboltoken") or "").strip()
        symbol = str(contract.get("tradingsymbol") or "").strip()

        if exchange not in ("NFO", "BFO") or not token or not symbol:
            raise BrokerSafetyError("Invalid contract for FULL market quote")

        try:
            max_age_s = float(max_age_s)
        except (TypeError, ValueError, OverflowError):
            raise BrokerSafetyError("Invalid MAX_QUOTE_AGE_S")

        if not math.isfinite(max_age_s) or max_age_s <= 0:
            raise BrokerSafetyError("Invalid MAX_QUOTE_AGE_S")

        res = self._call(
            "getMarketData",
            "FULL",
            {exchange: [token]},
        )

        received_at = time.time()

        if not isinstance(res, dict) or res.get("status") is not True:
            raise BrokerSafetyError("Angel One FULL market quote failed")

        data = res.get("data")
        if not isinstance(data, dict):
            raise BrokerSafetyError("Invalid Angel One FULL quote response")

        fetched = data.get("fetched")
        if not isinstance(fetched, list):
            raise BrokerSafetyError("Angel One FULL quote missing fetched records")

        matches = [
            item for item in fetched
            if isinstance(item, dict)
            and str(item.get("symbolToken") or "").strip() == token
            and str(item.get("tradingSymbol") or "").strip() == symbol
        ]

        if len(matches) != 1:
            raise BrokerSafetyError(
                "Angel One FULL quote token/symbol mismatch or duplicate"
            )

        item = matches[0]

        ltp = _num(item.get("ltp", item.get("lastPrice")), float, 0.0)

        if not math.isfinite(ltp) or ltp <= 0:
            raise BrokerSafetyError("Invalid LTP in Angel One FULL quote")

        raw_feed_time = item.get("exchFeedTime")

        if not raw_feed_time:
            raise BrokerSafetyError(
                "Angel One FULL quote missing exchange feed timestamp"
            )

        try:
            feed_dt = datetime.strptime(
                str(raw_feed_time).strip(),
                "%d-%b-%Y %H:%M:%S",
            ).replace(tzinfo=ZoneInfo("Asia/Kolkata"))

            feed_epoch = feed_dt.timestamp()

        except (TypeError, ValueError, OverflowError):
            raise BrokerSafetyError(
                f"Invalid exchange feed timestamp: {raw_feed_time!r}"
            )

        feed_age_s = received_at - feed_epoch

        if (
            not math.isfinite(feed_age_s)
            or feed_age_s < -2.0
            or feed_age_s > max_age_s
        ):
            raise BrokerSafetyError(
                f"Stale/invalid exchange quote: "
                f"feed_age={feed_age_s:.2f}s "
                f"maximum={max_age_s:.2f}s"
            )

        return {
            "ltp": ltp,
            "bid": _num(item.get("bidPrice"), float, 0.0),
            "ask": _num(item.get("askPrice"), float, 0.0),
            "requested_at_epoch": requested_at,
            "received_at_epoch": received_at,
            "request_latency_s": received_at - requested_at,
            "exchange_feed_time": raw_feed_time,
            "exchange_trade_time": item.get("exchTradeTime"),
            "feed_age_s": feed_age_s,
            "symbol": symbol,
            "symboltoken": token,
            "exchange": exchange,
        }

    
    
    def ltp(self, contract: dict) -> float:
        """Backward-compatible LTP method."""
        return float(self.ltp_quote(contract)["ltp"])


    def available_cash(self) -> float:
        res = self._call("rmsLimit")
        if isinstance(res, dict) and res.get("status") and res.get("data"):
            return _num(res["data"].get("availablecash"), float, 0.0)
        raise BrokerSafetyError("Could not read available funds")
