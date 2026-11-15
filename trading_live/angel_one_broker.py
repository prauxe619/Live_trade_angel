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
    "open": "open", "open pending": "open", "validation pending": "open",
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

    # ── session ───────────────────────────────────────────────────────────
    def login(self):
        required = ["ANGEL_API_KEY", "ANGEL_CLIENT_ID", "ANGEL_PASSWORD", "ANGEL_TOTP"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise BrokerSafetyError("Missing trading credentials: " + ", ".join(missing))
        from SmartApi import SmartConnect
        import pyotp
        with self._lock:
            smart = SmartConnect(api_key=os.environ["ANGEL_API_KEY"])
            totp = pyotp.TOTP(os.environ["ANGEL_TOTP"]).now()
            resp = smart.generateSession(os.environ["ANGEL_CLIENT_ID"], os.environ["ANGEL_PASSWORD"], totp)
            if not resp or not resp.get("status") or not resp.get("data"):
                raise BrokerSafetyError("Angel One login failed (check logs; secrets are never printed)")
            self.smart = smart
            self._login_at = time.time()
        return {"authenticated": True, "client_id": os.environ["ANGEL_CLIENT_ID"]}

    def _call(self, name: str, *args, retry_on_token_error: bool = True, **kw):
        with self._lock:
            if self.smart is None:
                raise BrokerSafetyError("Call login() first")
            if time.time() - self._login_at > self.RELOGIN_AFTER_S:
                self.login()
            gap = self.MIN_INTERVAL_S - (time.time() - self._last_call)
            if gap > 0:
                time.sleep(gap)
            self._last_call = time.time()
            res = getattr(self.smart, name)(*args, **kw)
            if (isinstance(res, dict) and res.get("errorcode") in self.TOKEN_ERRORS
                    and retry_on_token_error):
                log.warning("Token error on %s; re-login and retry once", name)
                self.login()
                self._last_call = time.time()
                res = getattr(self.smart, name)(*args, **kw)
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
        _require_gates(reduce_only)
        if reduce_only and payload.get("transactiontype") != "SELL":
            raise BrokerSafetyError("reduce_only orders must be SELL (long options only)")
        try:
            fn = "placeOrderFullResponse" if hasattr(self.smart, "placeOrderFullResponse") else "placeOrder"
            res = self._call(fn, payload, retry_on_token_error=False)
        except BrokerSafetyError:
            raise
        except Exception as e:
            raise OrderOutcomeUnknown(f"placeOrder raised {type(e).__name__}: {e}") from e
        if isinstance(res, dict):
            if res.get("status") is False or str(res.get("status")).lower() == "false":
                raise BrokerSafetyError(f"Order rejected by broker API: {res.get('message')} [{res.get('errorcode')}]")
            data = res.get("data")
            oid = data.get("orderid") if isinstance(data, dict) else data
        else:
            oid = res
        if not oid:
            raise OrderOutcomeUnknown(f"No order id in response: {res!r}")
        return str(oid)

    def modify_order(self, order_id: str, contract: dict, *, variety: str, order_type: str, product: str,
                     quantity: int, price: float, trigger_price: float = 0.0) -> bool:
        _require_gates(True)
        params = {"variety": variety, "orderid": order_id, "ordertype": order_type, "producttype": product,
                  "duration": "DAY", "price": str(tick(price)), "quantity": str(int(quantity)),
                  "tradingsymbol": contract["tradingsymbol"], "symboltoken": contract["symboltoken"],
                  "exchange": contract["exchange"]}
        if trigger_price:
            params["triggerprice"] = str(tick(trigger_price))
        try:
            res = self._call("modifyOrder", params, retry_on_token_error=False)
        except Exception as e:
            log.error("modifyOrder failed: %s", e)
            return False
        return bool(isinstance(res, dict) and res.get("status") is True)

    def cancel_order(self, order_id: str, variety: str = "NORMAL") -> bool:
        _require_gates(True)
        try:
            res = self._call("cancelOrder", order_id, variety)
        except Exception as e:
            log.error("cancelOrder failed: %s", e)
            return False
        return bool(isinstance(res, dict) and res.get("status") is True)

    # ── reads ────────────────────────────────────────────────────────────
    def order_book(self) -> list:
        res = self._call("orderBook")
        if not isinstance(res, dict) or res.get("status") is not True:
            raise BrokerSafetyError(f"orderBook failed or status is not true: {res!r}")
        data = res.get("data")
        if not isinstance(data, list):
            raise BrokerSafetyError(
                f"orderBook data is not a confirmed list (received {type(data).__name__}); "
                "manual-order check unavailable"
            )
        if any(not isinstance(row, dict) for row in data):
            raise BrokerSafetyError("orderBook contains a malformed order row")
        return data

    def position_book(self) -> list:
        res = self._call("position")
        if not isinstance(res, dict) or res.get("status") is not True:
            raise BrokerSafetyError(f"position failed or status is not true: {res!r}")
        data = res.get("data")
        if not isinstance(data, list):
            raise BrokerSafetyError(
                f"position data is not a confirmed list (received {type(data).__name__}); "
                "manual-position check unavailable"
            )
        if any(not isinstance(row, dict) for row in data):
            raise BrokerSafetyError("position response contains a malformed row")
        return data

    @staticmethod
    def _normalise(row: dict) -> dict:
        quantity = _num(row.get("quantity", row.get("orderqty")), int, 0)
        filled = _num(row.get("filledshares"), int, 0)
        return {
            "order_id": str(row.get("orderid", "")),
            "status": norm_status(row.get("orderstatus", row.get("status"))),
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

    def orders(self) -> list:
        return [self._normalise(r) for r in self.order_book()]

    def get_order(self, order_id: str):
        for o in self.orders():
            if o["order_id"] == str(order_id):
                return o
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

    def ltp(self, contract: dict) -> float:
        res = self._call("ltpData", contract["exchange"], contract["tradingsymbol"], contract["symboltoken"])
        if isinstance(res, dict) and res.get("status") and res.get("data"):
            return _num(res["data"].get("ltp"), float, 0.0)
        return 0.0

    def available_cash(self) -> float:
        res = self._call("rmsLimit")
        if isinstance(res, dict) and res.get("status") and res.get("data"):
            return _num(res["data"].get("availablecash"), float, 0.0)
        raise BrokerSafetyError("Could not read available funds")
