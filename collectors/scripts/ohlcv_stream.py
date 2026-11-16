"""Angel One WebSocket index-tick aggregation into genuine one-minute OHLC bars.

Index spot instruments do not have traded volume. Their volume is deliberately
stored as NULL rather than fabricating volume or mixing futures volume with spot OHLC.
Closed bars are persisted to market_ohlcv_candles through shared_snapshots and then
included in each collector's market_snapshots payload for the hosted signal engine.

CHANGES vs previous version
---------------------------
1. SmartWebSocketV2 delivers index prices in PAISE (x100). Every price is now
   converted to rupees (_to_rupees) before it reaches the candle aggregator.
2. The WS price is validated against the collector's last REST spot (spot_hint);
   ticks more than 10% away are dropped instead of corrupting candles.
3. The stream NO LONGER writes owner.last_spot. That attribute belongs to the
   collector's REST tick. The WS price is stored in owner.ws_spot instead.
4. update_credentials() lets the collector hand over a fresh JWT/feed token after
   its 09:14 reconnect, so the stream can re-subscribe with valid credentials.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _as_ist_minute(value):
    if not isinstance(value, datetime):
        raise ValueError("exchange tick timestamp is missing or invalid")
    dt = value
    if dt.tzinfo is None:
        # Angel timestamps without offset are interpreted as exchange-local IST.
        dt = dt.replace(tzinfo=IST)
    dt = dt.astimezone(IST)
    now = datetime.now(IST)
    age = (now - dt).total_seconds()
    if age < -5 or age > 180:
        raise ValueError(f"exchange tick timestamp outside allowed window: age={age:.1f}s")
    return dt.replace(second=0, microsecond=0)



class MinuteOHLCVAggregator:
    """Aggregates price ticks and optional genuine traded-volume deltas."""

    def __init__(self, symbol: str, publisher=None, on_price=None,
                 source="angelone_websocket_index_spot"):
        self.symbol = str(symbol).upper()
        self.publisher = publisher
        self.on_price = on_price
        self.source = str(source)
        self.current = None
        self.lock = threading.RLock()
        self.pending = []
        self.last_persist_attempt = 0.0

    def add_tick(self, price, timestamp=None, volume_delta=None):
        try:
            price = float(price)
        except (TypeError, ValueError, OverflowError):
            return None

        if not math.isfinite(price) or price <= 0:
            return None

        if volume_delta is not None:
            try:
                volume_delta = float(volume_delta)
            except (TypeError, ValueError, OverflowError):
                return None
            if not math.isfinite(volume_delta) or volume_delta < 0:
                return None

        try:
            minute = _as_ist_minute(timestamp)
        except ValueError as exc:
            logger.warning("%s rejected tick timestamp: %s", self.symbol, exc)
            return None

        closed = None

        with self.lock:
            current = self.current

            if current is None:
                self.current = self._new_bar(minute, price, volume_delta)

            elif minute < current["_minute"]:
                return None

            elif minute == current["_minute"]:
                current["high"] = max(current["high"], price)
                current["low"] = min(current["low"], price)
                current["close"] = price
                current["tick_count"] += 1

                if volume_delta is not None:
                    if current["volume"] is None:
                        current["volume"] = 0.0
                    current["volume"] += volume_delta

            else:
                closed = self._public_bar(current)
                self.pending.append(closed)
                self.current = self._new_bar(minute, price, volume_delta)

        if self.on_price:
            try:
                self.on_price(price, timestamp or datetime.now(IST))
            except Exception:
                logger.exception("%s on_price callback failed", self.symbol)

        if closed:
            self._try_persist()
        else:
            self._try_persist_pending()

        return closed

    def _new_bar(self, minute, price, volume_delta=None):
        return {
            "_minute": minute,
            "timestamp": minute.isoformat(),
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": None if volume_delta is None else float(volume_delta),
            "source": self.source,
            "tick_count": 1,
        }

    @staticmethod
    def _public_bar(bar):
        public = {k: v for k, v in bar.items() if k != "_minute"}
        public["is_complete"] = True
        public["tick_count"] = int(public.get("tick_count") or 0)
        public["quality_status"] = (
            "VALID" if public["tick_count"] >= 3
            else "INVALID_LOW_TICK_COUNT"
        )
        return public

    def flush(self):
        """Persist completed bars; discard the forming candle."""
        with self.lock:
            if self.current is not None:
                logger.info(
                    "%s discarding incomplete shutdown candle %s",
                    self.symbol,
                    self.current.get("timestamp"),
                )
                self.current = None

        self._try_persist_pending(force=True)

        with self.lock:
            return len(self.pending) == 0

    def _try_persist(self):
        if self.publisher:
            self._try_persist_pending(force=True)

    def _try_persist_pending(self, force=False):
        if not self.publisher:
            return

        now = time.monotonic()

        with self.lock:
            if not self.pending:
                return
            if not force and now - self.last_persist_attempt < 10:
                return
            batch = list(self.pending)
            self.last_persist_attempt = now

        try:
            self.publisher(self.symbol, batch, strict=True)
            with self.lock:
                del self.pending[:len(batch)]
            logger.info(
                "%s persisted %d completed candle(s)",
                self.symbol, len(batch)
            )
        except Exception as exc:
            logger.warning(
                "%s candle persistence failed; %d bars queued: %s",
                self.symbol, len(batch), exc
            )



class AngelOneIndexOHLCVStream:
    """Reconnectable SmartWebSocketV2 client for one index spot token."""
    def __init__(self, symbol, token, exchange_type, jwt_token, api_key,
                 client_code, feed_token, on_price=None,
                 price_divisor=100.0, spot_hint=None):
        self.symbol = str(symbol).upper()
        self.token = str(token)
        self.exchange_type = int(exchange_type)
        self.jwt_token = jwt_token
        self.api_key = api_key
        self.client_code = client_code
        self.feed_token = feed_token
        self.price_divisor = float(price_divisor)   # Angel WS sends paise
        self.spot_hint = spot_hint                  # callable -> last good REST spot (rupees)
        self.stop_event = threading.Event()
        self.thread = None
        self.ws = None
        self.aggregator = MinuteOHLCVAggregator(
            self.symbol, publisher=self._publish_candles, on_price=on_price
        )
        self.option_aggregators = {}
        self.option_contracts = {}
        self.option_lock = threading.RLock()
        self.option_refresh_thread = None
        self._option_master_cache = None

        self.futures_token = None
        self.futures_expiry = None
        self.futures_exchange_type = None
        self.futures_aggregator = None
        self.futures_lock = threading.RLock()
        self._last_futures_cumulative_volume = None
        self._last_futures_timestamp = None
        self._ws_open = False


    @staticmethod
    def _publish_candles(symbol, rows, strict=False):
        from collectors.scripts.shared_snapshots import publish_candles
        return publish_candles(symbol, rows, strict=strict)

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, name=f"ohlcv-ws-{self.symbol}", daemon=True)
        self.thread.start()
        self.option_refresh_thread = threading.Thread(
            target=self._option_refresh_loop,
            name=f"option-ohlcv-refresh-{self.symbol}",
            daemon=True,
        )
        self.option_refresh_thread.start()

    def stop(self, join_timeout: float = 5.0):
        self.stop_event.set()
        ws = self.ws
        if ws is not None:
            try:
                ws.close_connection()
            except Exception:
                logger.exception("%s websocket close failed", self.symbol)
        if self.thread and self.thread.is_alive() and self.thread is not threading.current_thread():
            self.thread.join(timeout=max(0.0, float(join_timeout)))
        if (self.option_refresh_thread and self.option_refresh_thread.is_alive()
                and self.option_refresh_thread is not threading.current_thread()):
            self.option_refresh_thread.join(timeout=max(0.0, float(join_timeout)))
        flushed = self.aggregator.flush()
        with self.futures_lock:
            futures_aggregator = self.futures_aggregator

        futures_flushed = True
        if futures_aggregator is not None:
            futures_flushed = futures_aggregator.flush()
        with self.option_lock:
            option_aggregators = list(self.option_aggregators.values())
        for option_aggregator in option_aggregators:
            option_aggregator.flush()
        for _ in range(4):
            if all(not agg.pending for agg in option_aggregators):
                break
            time.sleep(1)
            for option_aggregator in option_aggregators:
                option_aggregator._try_persist_pending(force=True)
        for _ in range(4):
            if flushed:
                break
            time.sleep(1)
            self.aggregator._try_persist_pending(force=True)
            with self.aggregator.lock:
                flushed = not self.aggregator.pending
        if not flushed:
            logger.critical("%s OHLCV shutdown flush incomplete; %d candle(s) remain queued",
                            self.symbol, len(self.aggregator.pending))
        if not futures_flushed:
            logger.critical(
                "%s futures OHLCV shutdown flush incomplete",
                self.symbol
            )

    def update_credentials(self, jwt_token, feed_token):
        """Swap in fresh tokens (e.g. after the collector's 09:14 reconnect) and
        force the websocket to drop so the retry loop re-subscribes with them."""
        if jwt_token:
            self.jwt_token = jwt_token
        if feed_token:
            self.feed_token = feed_token
        ws = self.ws
        if ws is not None:
            try:
                ws.close_connection()
            except Exception:
                pass
        logger.info("%s WebSocket credentials refreshed; reconnecting", self.symbol)

    
    def set_futures_contract(self, token, exchange_type, expiry):
        """Configure the active futures contract and subscribe in SNAP_QUOTE mode."""

        token = str(token).strip()
        if not token:
            raise ValueError("Futures token cannot be empty")

        expiry = str(expiry)[:10]

        with self.futures_lock:
            if self.futures_token == token and self.futures_aggregator is not None:
                return

            self.futures_token = token
            self.futures_expiry = expiry
            self.futures_exchange_type = int(exchange_type)
            self._last_futures_cumulative_volume = None
            self._last_futures_timestamp = None

            self.futures_aggregator = MinuteOHLCVAggregator(
                f"{self.symbol}:FUTURES",
                publisher=lambda _symbol, rows, strict=False,
                    tok=token, exp=expiry:
                    self._publish_futures_candles(tok, exp, rows),
                source="angelone_websocket_futures_snap_quote",
            )

        logger.info(
            "%s futures contract configured token=%s expiry=%s exchange_type=%s",
            self.symbol, token, expiry, exchange_type
        )

        if self._ws_open:
            self._subscribe_futures()

    def _subscribe_futures(self):
        ws = self.ws

        with self.futures_lock:
            token = self.futures_token
            exchange_type = self.futures_exchange_type

        if ws is None or not self._ws_open or not token or exchange_type is None:
            return

        try:
            ws.subscribe(
                f"{self.symbol.lower()}-fut",
                3,
                [{
                    "exchangeType": int(exchange_type),
                    "tokens": [str(token)],
                }],
            )
            logger.info(
                "%s futures SNAP_QUOTE subscribed token=%s",
                self.symbol, token
            )
        except Exception:
            logger.exception(
                "%s futures WebSocket subscription failed",
                self.symbol
            )

    def _publish_futures_candles(self, token, expiry, rows):
        from collectors.scripts.shared_snapshots import publish_futures_candles

        return publish_futures_candles(
            self.symbol,
            token,
            expiry,
            rows,
            strict=True,
        )


    
    def _run(self):
       from SmartApi.smartWebSocketV2 import SmartWebSocketV2
   
       # Compatibility adapter for websocket-client 1.9.x.
       # SmartAPI's original _on_close accepts only wsapp, while
       # websocket-client supplies wsapp, status_code and close_msg.
       class CompatibleSmartWebSocketV2(SmartWebSocketV2):
           def _on_close(self, wsapp, *args):
               callback = getattr(self, "on_close", None)
               if callback:
                   return callback(wsapp)
   
       while not self.stop_event.is_set():
           try:
               ws = CompatibleSmartWebSocketV2(
                   self.jwt_token,
                   self.api_key,
                   self.client_code,
                   self.feed_token
               )
               self.ws = ws
   
               def on_open(wsapp):
                   logger.info(
                       "%s index OHLCV WebSocket connected; subscribing token %s",
                       self.symbol, self.token
                   )
                   ws.subscribe(
                       f"{self.symbol.lower()}-ohlcv",
                       1,
                       [{
                           "exchangeType": self.exchange_type,
                           "tokens": [self.token]
                       }]
                   )
   
                   self._subscribe_all_option_tokens(ws)
                   self._ws_open = True
                   self._subscribe_futures()

               def on_data(wsapp, message):
                   self._on_data(message)
   
               def on_error(wsapp, error):
                   logger.warning(
                       "%s WebSocket error: %s",
                       self.symbol, error
                   )
   
               def on_close(wsapp):
                   self._ws_open = False
                   logger.warning(
                       "%s WebSocket closed",
                       self.symbol
                   )
   
               ws.on_open = on_open
               ws.on_data = on_data
               ws.on_error = on_error
               ws.on_close = on_close
               self._ws_open = False
   
               ws.connect()
   
           except Exception as exc:
               if not self.stop_event.is_set():
                   logger.exception(
                       "%s WebSocket connection failed: %s",
                       self.symbol, exc
                   )
   
           finally:
               self._ws_open = False
               self.ws = None
   
           if not self.stop_event.is_set():
               self.stop_event.wait(5)


    def _to_rupees(self, raw):
        """Convert a WS price (paise) to rupees and validate against the REST spot.

        Returns None when the price is inconsistent with the collector's last
        known spot, so a bad tick can never reach the candle aggregator.
        """
        candidates = [raw / self.price_divisor, raw]      # paise->rupees first, then as-is
        hint = None
        try:
            hint = self.spot_hint() if self.spot_hint else None
            hint = float(hint) if hint else None
        except Exception:
            hint = None

        if hint and hint > 0:
            best = min(candidates, key=lambda c: abs(c - hint))
            if abs(best - hint) / hint > 0.10:            # >10% away from REST spot
                logger.warning("%s WS price %s inconsistent with REST spot %s; dropped",
                               self.symbol, raw, hint)
                return None
            return best
        return candidates[0]                              # no REST spot yet: trust the divisor

    
    def _on_data(self, message):
        if isinstance(message, str):
            try:
                message = json.loads(message)
            except json.JSONDecodeError:
                return

        if not isinstance(message, dict):
            return

        raw_token = message.get("token")
        if raw_token is None or not str(raw_token).strip():
            return

        tick_token = str(raw_token).split("-")[0]

        try:
            raw_price = float(
                message.get(
                    "last_traded_price",
                    message.get("ltp", message.get("lastPrice"))
                )
            )
        except (TypeError, ValueError, OverflowError):
            return

        raw_ts = message.get("exchange_timestamp") or message.get("exchangeTimestamp")
        if raw_ts is None:
            logger.warning(
                "%s rejected tick with missing exchange timestamp",
                self.symbol
            )
            return

        try:
            numeric_ts = float(raw_ts)
            if numeric_ts > 1e12:
                numeric_ts /= 1000.0

            timestamp = datetime.fromtimestamp(
                numeric_ts, tz=timezone.utc
            ).astimezone(IST)
        except (TypeError, ValueError, OSError, OverflowError):
            return

        # ----------------------------------------------------------
        # FUTURES: SNAP_QUOTE mode, cumulative daily traded volume.
        # ----------------------------------------------------------
        with self.futures_lock:
            futures_token = self.futures_token
            futures_aggregator = self.futures_aggregator

        if tick_token == futures_token and futures_aggregator is not None:
            try:
                cumulative = float(message["volume_trade_for_the_day"])
            except (KeyError, TypeError, ValueError, OverflowError):
                logger.warning(
                    "%s futures tick missing cumulative traded volume",
                    self.symbol
                )
                return

            if not math.isfinite(cumulative) or cumulative < 0:
                return

            with self.futures_lock:
                previous = self._last_futures_cumulative_volume
                previous_ts = self._last_futures_timestamp

                if previous_ts is not None and timestamp < previous_ts:
                    return

                if previous is None:
                    # Establish baseline; do not incorrectly count the whole
                    # day's accumulated volume as this minute's volume.
                    volume_delta = 0.0
                elif (timestamp - previous_ts).total_seconds() > 90:
                    # A reconnect gap can contain many minutes of cumulative
                    # volume. Re-baseline instead of assigning that whole
                    # amount to the first post-reconnect candle.
                    logger.warning(
                        "%s futures volume baseline reset after %.1fs tick gap",
                        self.symbol, (timestamp - previous_ts).total_seconds()
                    )
                    volume_delta = 0.0
                elif cumulative >= previous:
                    volume_delta = cumulative - previous
                else:
                    # Cumulative-volume reset: establish a new baseline.
                    logger.warning(
                        "%s futures cumulative volume reset: %.0f -> %.0f",
                        self.symbol, previous, cumulative
                    )
                    volume_delta = 0.0

                self._last_futures_cumulative_volume = cumulative
                self._last_futures_timestamp = timestamp

            price = raw_price / 100.0

            if math.isfinite(price) and price > 0:
                futures_aggregator.add_tick(
                    price,
                    timestamp,
                    volume_delta=volume_delta,
                )

            return

        # ----------------------------------------------------------
        # SPOT INDEX: existing LTP stream; no traded volume.
        # ----------------------------------------------------------
        if tick_token == self.token:
            price = self._to_rupees(raw_price)

            if price is not None and price > 0:
                self.aggregator.add_tick(price, timestamp)

            return

        # ----------------------------------------------------------
        # OPTIONS: existing option LTP candle stream.
        # ----------------------------------------------------------
        with self.option_lock:
            option_aggregator = self.option_aggregators.get(tick_token)

        if option_aggregator is None:
            return

        option_price = raw_price / 100.0

        if math.isfinite(option_price) and option_price > 0:
            option_aggregator.add_tick(option_price, timestamp)


    def _publish_option_candles(self, token, rows):
        from collectors.scripts import market_store
        with self.option_lock:
            contract = dict(self.option_contracts.get(str(token)) or {})
        if not contract:
            raise RuntimeError(f"Missing option metadata for token {token}")
        payload = [{
            **row, "token": str(token), "symbol": contract["symbol"],
            "exchange_segment": contract["exchange_segment"],
            "expiry": contract["expiry"], "strike": contract["strike"],
            "opt_type": contract["opt_type"],
            "source": "angelone_websocket_option_ltp",
        } for row in rows]
        market_store.insert_option_ohlcv_candles(self.symbol, payload)

    def _resolve_active_option_contracts(self):
        from collectors.scripts import market_store
        from collectors.scripts.instrument_master_store import get_today_master
        import pandas as pd

        records = market_store.load_open_signal_payloads(self.symbol)
        if not records:
            return []
        if self._option_master_cache is None:
            self._option_master_cache = get_today_master()
        master = self._option_master_cache
        exchange = "BFO" if self.symbol == "SENSEX" else "NFO"
        name_aliases = {
            "NIFTY": {"NIFTY"},
            "BANKNIFTY": {"BANKNIFTY", "NIFTY BANK"},
            "FINNIFTY": {"FINNIFTY", "NIFTY FIN SERVICE"},
            "SENSEX": {"SENSEX", "BSE SENSEX"},
        }.get(self.symbol, {self.symbol})
        contracts = master[
            (master["name"].astype(str).str.upper().isin(name_aliases))
            & (master["exch_seg"].astype(str).str.upper() == exchange)
            & (master["instrumenttype"].astype(str).str.upper() == "OPTIDX")
        ].copy()
        if contracts.empty:
            return []
        contracts["strike_norm"] = pd.to_numeric(contracts["strike"], errors="coerce")
        if contracts["strike_norm"].max() > 1_000_000:
            contracts["strike_norm"] = contracts["strike_norm"] / 100.0
        contracts["expiry_norm"] = pd.to_datetime(
            contracts["expiry"], errors="coerce", dayfirst=True
        ).dt.date
        contracts["symbol_norm"] = contracts["symbol"].astype(str).str.upper()
        resolved = []
        for record in records:
            try:
                direction = str(record.get("direction", "")).upper()
                strike = float(record["strike"])
                expiry = pd.to_datetime(str(record["expiry"]), errors="coerce", dayfirst=True)
                if direction not in {"CE", "PE"} or pd.isna(expiry):
                    continue
                expiry_date = expiry.date()
                match = contracts[
                    (contracts["strike_norm"].sub(strike).abs() < 0.001)
                    & (contracts["expiry_norm"] == expiry_date)
                    & contracts["symbol_norm"].str.endswith(direction)
                ]
                if match.empty:
                    logger.warning("%s cannot resolve active signal contract %s",
                                   self.symbol, record.get("signal_id"))
                    continue
                row = match.iloc[0]
                resolved.append({
                    "token": str(row["token"]), "symbol": str(row["symbol"]),
                    "exchange_segment": exchange,
                    "exchange_type": 4 if exchange == "BFO" else 2,
                    "expiry": expiry_date.isoformat(), "strike": strike,
                    "opt_type": direction,
                })
            except Exception:
                logger.exception("%s option contract resolution failed", self.symbol)
        return resolved

    def _subscribe_all_option_tokens(self, ws):
        with self.option_lock:
            grouped = {}
            for token, contract in self.option_contracts.items():
                grouped.setdefault(int(contract["exchange_type"]), []).append(str(token))
        for exchange_type, tokens in grouped.items():
            if tokens:
                ws.subscribe(
                    f"{self.symbol.lower()}-options-{exchange_type}",
                    1, [{"exchangeType": exchange_type, "tokens": tokens}],
                )

    def _option_refresh_loop(self):
        while not self.stop_event.is_set():
            try:
                resolved = self._resolve_active_option_contracts()
                added = []
                with self.option_lock:
                    for contract in resolved:
                        token = contract["token"]
                        if token in self.option_aggregators:
                            continue
                        self.option_contracts[token] = contract
                        self.option_aggregators[token] = MinuteOHLCVAggregator(
                            f"{self.symbol}:{contract['symbol']}",
                            publisher=lambda _symbol, rows, strict=False, tok=token:
                                self._publish_option_candles(tok, rows),
                            source="angelone_websocket_option_ltp",
                        )
                        added.append(contract)
                ws = self.ws
                if ws is not None and added:
                    grouped = {}
                    for contract in added:
                        grouped.setdefault(int(contract["exchange_type"]), []).append(contract["token"])
                    for exchange_type, tokens in grouped.items():
                        ws.subscribe(
                            f"{self.symbol.lower()}-options-refresh-{int(time.time())}",
                            1, [{"exchangeType": exchange_type, "tokens": tokens}],
                        )
                    logger.info("%s registered %d active option contract(s)",
                                self.symbol, len(added))
            except Exception as exc:
                logger.warning("%s active option refresh failed: %s", self.symbol, exc)
            self.stop_event.wait(10)


def start_index_stream(owner, token, exchange_type, api_key, client_code):
    """Start a stream for owner.index_key.

    The latest WS price (rupees) is stored in owner.ws_spot. owner.last_spot is
    NOT touched: it belongs to the collector's REST tick. The collector's
    owner.tick_spot is used as the sanity reference for incoming WS prices.
    """
    jwt_token = getattr(owner, "jwt_token", None)
    feed_token = getattr(owner, "feed_token", None)
    if not all((jwt_token, feed_token, api_key, client_code, token)):
        logger.error("%s WebSocket not started: JWT/feed/API/client/token missing",
                     getattr(owner, "index_key", "INDEX"))
        return None
    stream = AngelOneIndexOHLCVStream(
        owner.index_key, token, exchange_type, jwt_token, api_key, client_code,
        feed_token,
        on_price=lambda price, ts: setattr(owner, "ws_spot", price),
        spot_hint=lambda: getattr(owner, "tick_spot", None),
    )
    stream.start()
    return stream
