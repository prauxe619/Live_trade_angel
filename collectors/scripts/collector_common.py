"""Shared helpers for the NIFTY / BANKNIFTY / FINNIFTY / SENSEX collectors."""
import math
import re
import time
from scipy.stats import norm

_SQRT2 = math.sqrt(2.0)

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / _SQRT2))

def _positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0

def safe_number(value, default=0.0):
    """Convert broker values to finite floats; tolerate strings and nulls."""
    try:
        number = float(value or 0)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default
    
def normalise_spot(raw, strikes):
    """Return spot in rupees if it reconciles with the option strike range."""
    value = _positive(raw)
    if value <= 0 or strikes is None:
        return None
    try:
        clean = [float(x) for x in strikes if x is not None and math.isfinite(float(x)) and float(x) > 0]
        if not clean:
            return None
        lo, hi = min(clean), max(clean)
    except (TypeError, ValueError, OverflowError):
        return None
    for candidate in (value, value / 100.0):
        if lo <= candidate <= hi:
            return candidate
    return None



def parse_quote(item, max_spread_pct=5.0):
    """
    Separate quote-data eligibility from execution eligibility.

    quote_entry_eligible:
        Valid two-sided quote for broader option-chain analytics.

    quote_execution_eligible:
        Strict quote suitable for execution checks.
    """
    if not isinstance(item, dict):
        item = {}

    try:
        max_spread_pct = float(max_spread_pct)
    except (TypeError, ValueError, OverflowError):
        max_spread_pct = 5.0

    if not math.isfinite(max_spread_pct) or max_spread_pct < 0:
        max_spread_pct = 5.0

    depth = item.get("depth")
    buys = depth.get("buy", []) if isinstance(depth, dict) else []
    sells = depth.get("sell", []) if isinstance(depth, dict) else []

    buys = [x for x in buys if isinstance(x, dict)]
    sells = [x for x in sells if isinstance(x, dict)]

    def level_values(level):
        price = next(
            (
                _positive(level.get(k))
                for k in ("price", "buyPrice", "sellPrice", "bestPrice", "rate")
                if _positive(level.get(k)) > 0
            ),
            0.0,
        )

        qty = next(
            (
                _positive(level.get(k))
                for k in ("quantity", "qty", "totalQuantity", "totalQty", "volume")
                if _positive(level.get(k)) > 0
            ),
            0.0,
        )

        return price, qty

    def first_level(levels):
        for level in levels:
            price, qty = level_values(level)
            if price > 0:
                return price, qty
        return 0.0, 0.0

    depth_bid, depth_bid_qty = first_level(buys)
    depth_ask, depth_ask_qty = first_level(sells)

    bid = (
        _positive(item.get("bidPrice"))
        or _positive(item.get("bestBid"))
        or depth_bid
    )

    ask = (
        _positive(item.get("askPrice"))
        or _positive(item.get("bestAsk"))
        or depth_ask
    )

    bid_qty = (
        depth_bid_qty
        or _positive(item.get("bestBidQty"))
        or _positive(item.get("totBuyQuan"))
        or _positive(item.get("totalBuyQty"))
        or _positive(item.get("buyQty"))
    )

    ask_qty = (
        depth_ask_qty
        or _positive(item.get("bestAskQty"))
        or _positive(item.get("totSellQuan"))
        or _positive(item.get("totalSellQty"))
        or _positive(item.get("sellQty"))
    )

    ltp = (
        _positive(item.get("ltp"))
        or _positive(item.get("lastPrice"))
    )

    quote_valid = bool(bid > 0 and ask > bid)

    spread_pct = (
        (ask - bid) / ask * 100.0
        if quote_valid else None
    )

    reasons = []

    if not quote_valid:
        reasons.append("invalid_or_locked_bid_ask")

    if ltp <= 0:
        reasons.append("invalid_ltp")
    elif quote_valid and not (bid <= ltp <= ask):
        reasons.append("ltp_outside_bid_ask")

    if spread_pct is None:
        reasons.append("spread_unavailable")
    elif spread_pct > max_spread_pct:
        reasons.append("spread_above_limit")

    if bid_qty <= 0:
        reasons.append("missing_bid_quantity")

    if ask_qty <= 0:
        reasons.append("missing_ask_quantity")

    # Broad eligibility for option-chain analytics.
    quote_entry_eligible = quote_valid

    # Strict eligibility for execution-quality checks.
    quote_execution_eligible = bool(
        quote_valid
        and bid_qty > 0
        and ask_qty > 0
        and spread_pct is not None
        and spread_pct <= max_spread_pct
    )

    return {
        "ltp": ltp,
        "bid": bid,
        "ask": ask,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "quote_valid": quote_valid,
        "spread_pct": spread_pct,
        "quote_entry_eligible": quote_entry_eligible,
        "quote_execution_eligible": quote_execution_eligible,
        "quote_quality_reason": (
            ",".join(reasons) if reasons else "eligible"
        ),
        "ltp_outside_quote": bool(
            quote_valid
            and ltp > 0
            and not (bid <= ltp <= ask)
        ),
    }

def _bs(spot, strike, t, rate, vol, is_call):
    sd = vol * math.sqrt(t)
    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * t) / sd
    d2 = d1 - sd
    if is_call:
        return spot * _norm_cdf(d1) - strike * math.exp(-rate * t) * _norm_cdf(d2)
    return strike * math.exp(-rate * t) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def implied_vol(spot, strike, dte, price, otype, rate=0.065):
    """Annualized Black-Scholes IV; returns 0 when inputs or solution are invalid."""
    try:
        spot, strike, price = float(spot), float(strike), float(price)
        dte, rate = float(dte), float(rate)
        if not all(math.isfinite(x) for x in (spot, strike, price, dte, rate)):
            return 0.0
        if min(spot, strike, price) <= 0 or dte < 0 or otype not in ("CE", "PE"):
            return 0.0
        t = max(dte, 0.5) / 365.0
        is_call = otype == "CE"
        intrinsic = max(spot - strike, 0.0) if is_call else max(strike - spot, 0.0)
        if price + max(0.5, spot * 0.00002) < intrinsic:
            return 0.0
        lo, hi = 0.01, 5.0
        if price < _bs(spot, strike, t, rate, lo, is_call) - 0.5:
            return 0.0
        if price > _bs(spot, strike, t, rate, hi, is_call) + 0.5:
            return 0.0
        for _ in range(40):
            mid = (lo + hi) * 0.5
            model_price = _bs(spot, strike, t, rate, mid, is_call)
            if abs(model_price - price) < 1e-3 or (hi - lo) < 1e-4:
                return mid if 0.01 <= mid <= 5.0 and math.isfinite(mid) else 0.0
            if model_price < price:
                lo = mid
            else:
                hi = mid
        result = (lo + hi) * 0.5
        return result if 0.01 <= result <= 5.0 and math.isfinite(result) else 0.0
    except (ValueError, TypeError, OverflowError, ZeroDivisionError):
        return 0.0


def compute_pcr(call_oi, put_oi, rows, min_cov=0.5):
    """Return (PCR, valid); malformed, negative, or incomplete OI is invalid."""
    try:
        call_oi, put_oi, min_cov = float(call_oi), float(put_oi), float(min_cov)
        if not all(math.isfinite(x) for x in (call_oi, put_oi, min_cov)):
            return 1.0, False
        if call_oi <= 0 or put_oi <= 0 or not 0 <= min_cov <= 1:
            return 1.0, False
        rows = list(rows or [])
        if not rows:
            return 1.0, False
        valid_rows = 0
        for row in rows:
            try:
                oi = float(row.get("oi", 0) or 0)
                if math.isfinite(oi) and oi > 0:
                    valid_rows += 1
            except (AttributeError, TypeError, ValueError, OverflowError):
                continue
        coverage = valid_rows / len(rows)
        valid = coverage >= min_cov
        return (put_oi / call_oi if valid else 1.0), valid
    except (TypeError, ValueError, OverflowError):
        return 1.0, False


class AlertThrottle:
    """Sends the same numeric-masked alert at most once per cooldown seconds."""
    def __init__(self, cooldown=1800):
        self.cooldown = max(0.0, float(cooldown))
        self._last = {}

    def send(self, msg, sender):
        key = re.sub(r"[-+]?\d+(?:\.\d+)?", "#", str(msg))
        now = time.time()
        if now - self._last.get(key, 0) >= self.cooldown:
            sender(msg)
            self._last[key] = now
