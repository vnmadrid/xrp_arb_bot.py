import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import re
import signal
import sqlite3
import sys
import time
import base64
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import requests
import websockets

# – Optional rich dashboard –––––––––––––––––––––––––

try:
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
RICH = True
except ImportError:
RICH = False

# —————————————————————————–

# CONFIG

# —————————————————————————–

@dataclass
class Config:
gemini_api_key:         str   = “”
gemini_api_secret:      str   = “”
gemini_sandbox:         bool  = True

```
# Strategy thresholds
lag_threshold_pct:      float = 3.0   # min % lag to flag opportunity
min_edge_pct:           float = 5.0   # min % edge to execute
max_position_pct:       float = 8.0   # max % of NAV per position
min_confidence:         float = 0.85  # min composite confidence 0-1
kelly_fraction:         float = 0.5   # half-Kelly

# Risk controls
max_daily_drawdown_pct: float = 40.0
max_open_positions:     int   = 5

# Operational
paper_trading:          bool  = True
poll_interval_sec:      float = 2.0
binance_ws_url:         str   = "wss://ws.kraken.com"
db_path:                str   = "xrp_arb_bot.db"
log_level:              str   = "INFO"
paper_portfolio_usd:    float = 10_000.0
```

# —————————————————————————–

# DATABASE

# —————————————————————————–

DB_SCHEMA = “””
CREATE TABLE IF NOT EXISTS trades (
id              INTEGER PRIMARY KEY AUTOINCREMENT,
ts              TEXT    NOT NULL,
symbol          TEXT    NOT NULL,
contract_type   TEXT    NOT NULL,
direction       TEXT    NOT NULL,
outcome         TEXT    NOT NULL,
size_usd        REAL    NOT NULL,
quantity        INTEGER NOT NULL,
entry_price     REAL    NOT NULL,
gemini_odds     REAL    NOT NULL,
cex_price       REAL    NOT NULL,
edge_pct        REAL    NOT NULL,
confidence      REAL    NOT NULL,
kelly_frac      REAL    NOT NULL,
paper           INTEGER NOT NULL,
gemini_order_id TEXT,
status          TEXT    NOT NULL DEFAULT ‘open’,
exit_price      REAL,
pnl             REAL,
closed_ts       TEXT,
resolution_side TEXT
);

CREATE TABLE IF NOT EXISTS daily_stats (
date          TEXT PRIMARY KEY,
starting_nav  REAL    NOT NULL,
current_nav   REAL    NOT NULL,
drawdown_pct  REAL    NOT NULL DEFAULT 0,
trades_taken  INTEGER NOT NULL DEFAULT 0,
wins          INTEGER NOT NULL DEFAULT 0,
losses        INTEGER NOT NULL DEFAULT 0,
killed        INTEGER NOT NULL DEFAULT 0
);
“””

def init_db(path: str) -> sqlite3.Connection:
conn = sqlite3.connect(path, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.executescript(DB_SCHEMA)
conn.commit()
return conn

# —————————————————————————–

# GEMINI CLIENT  –  real Prediction Markets API endpoints

# —————————————————————————–

class GeminiClient:
“””
Wrapper around the Gemini Prediction Markets REST API.
Docs: https://docs.gemini.com/prediction-markets/
“””

```
BASE_LIVE    = "https://api.gemini.com"
BASE_SANDBOX = "https://api.sandbox.gemini.com"

def __init__(self, api_key: str, api_secret: str, sandbox: bool = True):
    self.api_key    = api_key
    self.api_secret = api_secret.encode() if isinstance(api_secret, str) else api_secret
    self.base_url   = self.BASE_SANDBOX if sandbox else self.BASE_LIVE
    self._lock      = threading.Lock()
    self._last_req  = 0.0
    self._min_gap   = 0.15   # ~6 req/s max
    self.logger     = logging.getLogger("GeminiClient")

# -- Rate limiting ---------------------------------------------------------

def _rate_limit(self):
    with self._lock:
        gap = time.time() - self._last_req
        if gap < self._min_gap:
            time.sleep(self._min_gap - gap)
        self._last_req = time.time()

# -- Auth ------------------------------------------------------------------

def _sign(self, payload: dict) -> dict:
    """Gemini HMAC-SHA384 signature for authenticated endpoints."""
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    sig = hmac.new(self.api_secret, encoded.encode(), hashlib.sha384).hexdigest()
    return {
        "X-GEMINI-APIKEY":    self.api_key,
        "X-GEMINI-PAYLOAD":   encoded,
        "X-GEMINI-SIGNATURE": sig,
        "Content-Type":       "text/plain",
    }

# -- HTTP helpers ----------------------------------------------------------

def _get(self, path: str, params: dict = None, retries: int = 3) -> Optional[dict]:
    url = self.base_url + path
    for attempt in range(retries):
        try:
            self._rate_limit()
            r = requests.get(url, params=params, timeout=8)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            self.logger.warning(f"GET {path} HTTP {r.status_code}: {e} (attempt {attempt+1})")
            if r.status_code in (429, 500, 502, 503):
                time.sleep(2 ** attempt)
            else:
                break
        except Exception as e:
            self.logger.warning(f"GET {path} error: {e} (attempt {attempt+1})")
            time.sleep(1)
    return None

def _post_signed(self, path: str, payload: dict, retries: int = 3) -> Optional[dict]:
    """POST with Gemini HMAC auth."""
    url = self.base_url + path
    payload["request"] = path
    payload["nonce"]   = str(int(time.time() * 1000))
    headers = self._sign(payload)
    for attempt in range(retries):
        try:
            self._rate_limit()
            r = requests.post(url, headers=headers, timeout=8)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            self.logger.warning(
                f"POST {path} HTTP {r.status_code}: {e} (attempt {attempt+1})"
            )
            if r.status_code in (429, 500, 502, 503):
                time.sleep(2 ** attempt)
            else:
                break
        except Exception as e:
            self.logger.warning(f"POST {path} error: {e} (attempt {attempt+1})")
            time.sleep(1)
    return None

# =========================================================================
# PUBLIC MARKET ENDPOINTS
# https://docs.gemini.com/prediction-markets/markets
# =========================================================================

def get_xrp_contracts(self) -> list[dict]:
    """
    GET /v1/prediction-markets/events
      ?status=active&category=crypto&search=XRP&limit=100

    Returns all active XRP price-threshold prediction market contracts,
    normalised into a flat list of contract dicts for the strategy engine.

    Real Gemini ticker format (from docs):
      GEMI-{Asset}[{Duration}]{Expiry}-HI{Price}
      e.g. GEMI-XRP05M2604031950-HI2D20
           GEMI-XRP15M2604031950-HI2D20
           GEMI-XRP2604031500-HI2D20   (daily, no duration marker)

    Contract type is ALWAYS "HI" (price >= strike).
    We trade YES when we think the price will be >= strike,
    and NO when we think the price will be < strike.

    Contract dict fields:
      symbol        - e.g. "GEMI-XRP05M2604031950-HI2D20"
      event_ticker  - e.g. "XRP05M2604031950"
      strike        - XRP/USD strike price parsed from ticker
      minutes       - 5, 15, or 1440 (daily)
      expiry        - unix timestamp parsed from ticker
      implied_prob  - Gemini last trade price (0-1), probability of HI resolving YES
      best_bid      - best bid probability
      best_ask      - best ask probability
      contract_id   - Gemini internal contract ID
    """
    data = self._get(
        "/v1/prediction-markets/events",
        params={
            "status":   "active",
            "category": "crypto",
            "search":   "XRP",
            "limit":    100,
        },
    )

    if not data:
        self.logger.warning("get_xrp_contracts: no response - using synthetic stubs")
        return self._synthetic_contracts()

    contracts = []
    for event in data.get("data", []):
        title  = event.get("title",  "").upper()
        ticker = event.get("ticker", "").upper()
        if "XRP" not in title and "XRP" not in ticker:
            continue

        minutes = self._parse_minutes(event.get("ticker", ""))

        for c in event.get("contracts", []):
            instrument = c.get("instrumentSymbol", "")
            cticker    = c.get("ticker", "").upper()

            # Real Gemini crypto contracts use HI{PRICE} suffix only.
            # Skip anything that is not a price-threshold contract.
            if not cticker.startswith("HI"):
                continue

            # Parse strike price from contract ticker: HI2D20 -> 2.20, HI105000 -> 105000
            strike = self._parse_hi_price(cticker)
            if strike is None:
                # Fall back to strike field from API response if present
                strike_info = c.get("strike") or {}
                sv = strike_info.get("value")
                strike = float(sv) if sv else 0.0

            last_price = c.get("prices", {}).get("lastTradePrice")
            if last_price is None:
                continue
            try:
                implied_prob = float(last_price)
            except (ValueError, TypeError):
                continue

            prices   = c.get("prices", {})
            best_bid = float(prices.get("bestBid") or implied_prob)
            best_ask = float(prices.get("bestAsk") or implied_prob)

            # Parse expiry from event ticker string (YYMMDDHHmm)
            expiry_ts = self._parse_expiry_from_ticker(event.get("ticker", ""))
            if expiry_ts == 0:
                expiry_ts = self._parse_iso(c.get("expiryDate", ""))

            contracts.append({
                "symbol":       instrument,
                "event_ticker": event.get("ticker", ""),
                "strike":       strike,
                "minutes":      minutes,
                "expiry":       expiry_ts,
                "implied_prob": implied_prob,
                "best_bid":     best_bid,
                "best_ask":     best_ask,
                "contract_id":  c.get("id", ""),
            })

    if not contracts:
        self.logger.info("No live XRP contracts found - using synthetic stubs")
        return self._synthetic_contracts()

    self.logger.info(f"Found {len(contracts)} live XRP HI-threshold contracts")
    return contracts

def get_strike_price(self, event_ticker: str) -> Optional[float]:
    """
    GET /v1/prediction-markets/events/{eventTicker}/strike

    Returns the reference XRP/USD strike price for an up/down event.
    Returns None if strike not yet available (set ~5 min before expiry).
    """
    data = self._get(f"/v1/prediction-markets/events/{event_ticker}/strike")
    if data and data.get("value"):
        try:
            return float(data["value"])
        except (ValueError, TypeError):
            pass
    return None

def get_event(self, event_ticker: str) -> Optional[dict]:
    """
    GET /v1/prediction-markets/events/{eventTicker}
    Full event detail with all contract prices.
    """
    return self._get(f"/v1/prediction-markets/events/{event_ticker}")

# =========================================================================
# AUTHENTICATED TRADING ENDPOINTS
# https://docs.gemini.com/prediction-markets/trading
# =========================================================================

def place_prediction_order(
    self,
    symbol:        str,    # "GEMI-XRP5M2604031950-UP"
    outcome:       str,    # "yes" or "no"
    quantity:      int,    # number of contracts
    price:         float,  # limit price in 0-1 range
    time_in_force: str = "immediate-or-cancel",
) -> Optional[dict]:
    """
    POST /v1/prediction-markets/order

    Place a limit buy order on a prediction market contract.

    Key facts:
      - price is a probability (0.01 to 0.99), NOT a dollar amount
      - quantity is number of contracts (shares)
      - each contract pays $1.00 if it resolves in your favour
      - your dollar cost = price * quantity
      - outcome "yes" = betting the event happens (direction is UP for up contracts)
      - requires NewOrder permission on your API key
    """
    payload = {
        "symbol":      symbol,
        "orderType":   "limit",
        "side":        "buy",
        "quantity":    str(quantity),
        "price":       f"{price:.4f}",
        "outcome":     outcome,
        "timeInForce": time_in_force,
    }
    self.logger.debug(f"place_prediction_order: {payload}")
    return self._post_signed("/v1/prediction-markets/order", payload)

def cancel_order(self, order_id: int) -> Optional[dict]:
    """
    POST /v1/prediction-markets/order/cancel
    Cancel an open prediction market order by orderId.
    """
    return self._post_signed(
        "/v1/prediction-markets/order/cancel",
        {"orderId": order_id},
    )

# =========================================================================
# AUTHENTICATED POSITION ENDPOINTS
# https://docs.gemini.com/prediction-markets/positions
# =========================================================================

def get_active_orders(self, symbol: str = None) -> list[dict]:
    """
    POST /v1/prediction-markets/orders/active
    Returns currently open (unfilled) prediction market orders.
    Optionally filter by contract symbol.
    """
    body = {"limit": 100, "offset": 0}
    if symbol:
        body["symbol"] = symbol
    data = self._post_signed("/v1/prediction-markets/orders/active", body)
    return (data or {}).get("orders", [])

def get_order_history(self, symbol: str = None, status: str = "filled") -> list[dict]:
    """
    POST /v1/prediction-markets/orders/history
    Returns filled or cancelled historical orders.
    status: "filled" | "cancelled"
    """
    body = {"status": status, "limit": 100, "offset": 0}
    if symbol:
        body["symbol"] = symbol
    data = self._post_signed("/v1/prediction-markets/orders/history", body)
    return (data or {}).get("orders", [])

def get_positions(self) -> list[dict]:
    """
    POST /v1/prediction-markets/positions
    Returns all current open (filled) positions for the authenticated user.

    Response fields per position:
      symbol         - contract instrument symbol
      instrumentId   - internal ID
      totalQuantity  - number of contracts held
      avgPrice       - average fill price
      outcome        - "yes" or "no"
      contractMetadata.resolvedAt - set when contract settles
    """
    data = self._post_signed("/v1/prediction-markets/positions", {})
    return (data or {}).get("positions", [])

def check_order_settled(self, order_id: int) -> tuple[bool, Optional[str]]:
    """
    Poll /v1/prediction-markets/orders/history to check if a specific
    order has been filled and whether its underlying contract has resolved.

    Returns:
      (settled: bool, resolution_side: "yes"|"no"|None)
    """
    orders = self.get_order_history(status="filled")
    for o in orders:
        if o.get("orderId") == order_id:
            meta            = o.get("contractMetadata", {})
            resolved_at     = meta.get("resolvedAt")
            resolution_side = o.get("resolutionSide")
            if resolved_at:
                return True, resolution_side
    return False, None

# -- Legacy spot balance for NAV tracking in live mode ---------------------

def get_usd_balance(self) -> float:
    """POST /v1/balances - returns available USD from spot account."""
    data = self._post_signed("/v1/balances", {})
    if not data:
        return 0.0
    for b in data:
        if b.get("currency") == "USD":
            return float(b.get("available", 0))
    return 0.0

# -- Helpers ---------------------------------------------------------------

@staticmethod
def _parse_minutes(ticker: str) -> int:
    """
    Parse contract duration from event ticker string.
    "XRP05M2604031950" -> 5
    "XRP15M2604031950" -> 15
    "XRP2604031500"    -> 1440  (daily, no duration marker)
    Duration marker must be zero-padded: 05M not 5M.
    """
    m = re.search(r"(0?5M|15M)", ticker.upper())
    if m:
        marker = m.group(1).replace("0", "")   # "05M" -> "5M"
        return int(marker.replace("M", ""))
    return 1440   # daily contract

@staticmethod
def _parse_hi_price(cticker: str) -> Optional[float]:
    """
    Parse the strike price from a HI-format contract ticker.
    Gemini uses 'D' as the decimal delimiter instead of '.'.

    Examples:
      HI2D20    -> 2.20
      HI0D50    -> 0.50
      HI105000  -> 105000.0
      HI3500D25 -> 3500.25
    """
    m = re.match(r"^HI(\d+)(?:D(\d+))?$", cticker.upper())
    if not m:
        return None
    integer_part  = m.group(1)
    decimal_part  = m.group(2) or "0"
    return float(f"{integer_part}.{decimal_part}")

@staticmethod
def _parse_expiry_from_ticker(ticker: str) -> float:
    """
    Parse expiry unix timestamp from event ticker string.
    Format: {UNDERLYING}[{DURATION}]{YYMMDDHHmm}
    e.g. XRP05M2604031950 -> expiry at 2026-04-03 19:50 UTC
         XRP2604031500    -> expiry at 2026-04-03 15:00 UTC
    """
    # Strip leading asset chars and optional duration marker
    m = re.search(r"(\d{10})$", ticker.upper())
    if not m:
        return 0.0
    expiry_str = m.group(1)   # YYMMDDHHmm
    try:
        dt = datetime(
            year   = 2000 + int(expiry_str[0:2]),
            month  = int(expiry_str[2:4]),
            day    = int(expiry_str[4:6]),
            hour   = int(expiry_str[6:8]),
            minute = int(expiry_str[8:10]),
            tzinfo = timezone.utc,
        )
        return dt.timestamp()
    except (ValueError, OverflowError):
        return 0.0

@staticmethod
def _parse_iso(ts: str) -> float:
    """Convert ISO 8601 datetime string to unix timestamp."""
    if not ts:
        return time.time() + 300
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time() + 300

def _synthetic_contracts(self) -> list[dict]:
    """
    Fallback synthetic contract stubs for paper trading when no live
    XRP contracts are currently listed on Gemini.

    Generates realistic HI-format (price-threshold) contracts mirroring
    Gemini's real ticker structure:
      GEMI-XRP05M{expiry}-HI{price}
      GEMI-XRP15M{expiry}-HI{price}

    Creates contracts at current price and +/- 1% price levels so the
    bot has a spread of strikes to evaluate edge against.
    """
    ticker = self._get("/v1/pubticker/xrpusd")
    price  = float(ticker["last"]) if ticker else 0.55
    now    = time.time()
    stubs  = []

    for minutes in (5, 15):
        duration = "05M" if minutes == 5 else "15M"
        expiry_ts = now + minutes * 60
        # Format expiry as YYMMDDHHmm UTC
        dt_exp = datetime.fromtimestamp(expiry_ts, tz=timezone.utc)
        expiry_str = dt_exp.strftime("%y%m%d%H%M")

        # Generate 3 strike levels: at-the-money, 1% above, 1% below
        for pct_offset in (0.0, 0.01, -0.01):
            strike = round(price * (1 + pct_offset), 4)

            # Encode strike in Gemini HI format (D as decimal delimiter)
            strike_int = int(strike)
            strike_dec = round((strike - strike_int) * 100)
            if strike_dec > 0:
                hi_str = f"HI{strike_int}D{strike_dec:02d}"
            else:
                hi_str = f"HI{strike_int}"

            symbol = f"GEMI-XRP{duration}{expiry_str}-{hi_str}"

            # Implied probability: ATM ~50%, above strike lower, below higher
            # P(price >= strike) given current price
            if strike <= price:
                implied_prob = 0.52 - pct_offset * 2   # slightly favours HI
            else:
                implied_prob = 0.48 - pct_offset * 2

            implied_prob = max(0.05, min(0.95, implied_prob))

            stubs.append({
                "symbol":       symbol,
                "event_ticker": f"XRP{duration}{expiry_str}",
                "strike":       strike,
                "minutes":      minutes,
                "expiry":       expiry_ts,
                "implied_prob": implied_prob,
                "best_bid":     round(implied_prob - 0.01, 4),
                "best_ask":     round(implied_prob + 0.01, 4),
                "contract_id":  f"stub-{duration}-{hi_str}",
            })

    self.logger.info(f"Generated {len(stubs)} synthetic HI-format stubs for paper trading")
    return stubs
```

# —————————————————————————–

# BINANCE WEBSOCKET PRICE FEED

# —————————————————————————–

@dataclass
class BinanceTick:
price:          float = 0.0
bid:            float = 0.0
ask:            float = 0.0
ts:             float = field(default_factory=time.time)
volume_24h:     float = 0.0
change_24h_pct: float = 0.0

class BinanceFeed:
“””
Real-time XRP/USD price feed from Kraken WebSocket.
Kraken has no geo-restrictions unlike Binance.
Uses the same BinanceTick dataclass for compatibility with the rest of the bot.
Kraken WS docs: https://docs.kraken.com/websockets/
“””

```
SUBSCRIBE_MSG = json.dumps({
    "event": "subscribe",
    "pair":  ["XRP/USD"],
    "subscription": {"name": "ticker"},
})

def __init__(self, url: str):
    self.url      = url
    self._tick    = BinanceTick()
    self._lock    = asyncio.Lock()
    self._running = True
    self.logger   = logging.getLogger("BinanceFeed")

async def get_tick(self) -> BinanceTick:
    async with self._lock:
        return BinanceTick(**asdict(self._tick))

async def run(self):
    backoff = 1
    while self._running:
        try:
            async with websockets.connect(
                self.url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                await ws.send(self.SUBSCRIBE_MSG)
                self.logger.info("Kraken WebSocket connected")
                backoff = 1
                async for raw in ws:
                    if not self._running:
                        break
                    try:
                        msg = json.loads(raw)
                        # Kraken ticker format:
                        # [channelID, {"c":["price","qty"], "b":["bid","qty"],
                        #              "a":["ask","qty"], "p":["today","24h"]}, "ticker", "XRP/USD"]
                        if not isinstance(msg, list) or len(msg) < 4:
                            continue
                        if msg[-1] != "XRP/USD" or msg[-2] != "ticker":
                            continue
                        data = msg[1]
                        price   = float(data["c"][0])   # last trade price
                        bid     = float(data["b"][0])   # best bid
                        ask     = float(data["a"][0])   # best ask
                        # 24h change: Kraken gives opening price in "o"
                        open_24 = float(data["o"][0]) if "o" in data else price
                        change_pct = ((price - open_24) / open_24 * 100) if open_24 > 0 else 0.0
                        volume  = float(data["v"][1]) if "v" in data else 0.0  # 24h volume

                        async with self._lock:
                            self._tick = BinanceTick(
                                price          = price,
                                bid            = bid,
                                ask            = ask,
                                ts             = time.time(),
                                volume_24h     = volume,
                                change_24h_pct = change_pct,
                            )
                    except (KeyError, ValueError, IndexError,
                            json.JSONDecodeError) as e:
                        self.logger.debug(f"Tick parse error: {e}")
        except (websockets.ConnectionClosed, OSError) as e:
            self.logger.warning(
                f"Kraken WS disconnected: {e}. Reconnecting in {backoff}s"
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            self.logger.error(f"Kraken WS unexpected error: {e}")
            await asyncio.sleep(backoff)

def stop(self):
    self._running = False
```

# —————————————————————————–

# OPPORTUNITY DETECTION & SIZING

# —————————————————————————–

@dataclass
class Opportunity:
contract:    dict
strike:      float  # XRP/USD price threshold from ticker (HI = price >= strike)
outcome:     str    # “yes” (price will be >= strike) or “no” (price will be < strike)
gemini_prob: float  # Gemini last trade price (0-1) = P(price >= strike)
cex_prob:    float  # CEX-derived P(price >= strike) from Binance
edge:        float  # cex_prob - gemini_prob (positive = we have edge on YES)
lag_pct:     float  # abs(edge) * 100
confidence:  float  # composite score 0-1
cex_price:   float  # XRP/USD on Binance at detection time
minutes:     int    # contract duration
ts:          float  = field(default_factory=time.time)

def implied_prob_from_price_action(
tick: BinanceTick, strike: float, minutes: int
) -> float:
“””
Estimate P(XRP_expiry >= strike) from Binance real-time price.

```
Gemini HI contracts resolve YES if price >= strike at expiry.
Uses a simplified log-normal / Black-Scholes d2 approximation:
  P(S_T >= K) = N(d2)  where d2 = (ln(S/K)) / sigma_T

When current price > strike: probability > 0.5 (in the money)
When current price < strike: probability < 0.5 (out of the money)
"""
if strike <= 0 or tick.price <= 0:
    return 0.5
log_ret   = math.log(tick.price / strike)    # positive if price > strike (ITM)
daily_vol = abs(tick.change_24h_pct / 100) or 0.03
t_years   = (minutes / 60) / 8760
sigma_t   = daily_vol * math.sqrt(t_years * 365)
if sigma_t < 1e-9:
    # No time value - purely intrinsic
    return 1.0 if tick.price >= strike else 0.0
d2 = log_ret / sigma_t
# Logistic CDF approximation of N(d2)
return 1 / (1 + math.exp(-1.7 * d2))
```

def confidence_score(opp: Opportunity, tick: BinanceTick) -> float:
“””
Composite confidence (0-1):
50% edge magnitude
30% Gemini contract spread tightness (liquidity proxy)
20% Binance data freshness
“””
edge_score   = min(opp.edge / 0.20, 1.0)
contract     = opp.contract
bid          = float(contract.get(“best_bid”, opp.gemini_prob))
ask          = float(contract.get(“best_ask”, opp.gemini_prob))
mid          = (bid + ask) / 2 if (bid + ask) > 0 else opp.gemini_prob
spread       = (ask - bid) / mid if mid > 0 else 0.05
spread_score = max(0.0, 1 - spread / 0.05)
freshness    = max(0.0, 1 - (time.time() - tick.ts) / 5.0)
return round(0.5 * edge_score + 0.3 * spread_score + 0.2 * freshness, 4)

def kelly_size(
edge: float, win_prob: float, kelly_fraction: float,
portfolio_usd: float, max_pct: float
) -> float:
“”“Half-Kelly position size in USD, capped at max_pct of portfolio.”””
if win_prob <= 0 or win_prob >= 1 or edge <= 0:
return 0.0
b      = (1 / (1 - win_prob)) - 1
q      = 1 - win_prob
f_full = (b * win_prob - q) / b
f_half = kelly_fraction * f_full
f_cap  = min(max(f_half, 0.0), max_pct / 100)
return round(f_cap * portfolio_usd, 2)

def usd_to_contracts(size_usd: float, price: float) -> int:
“””
Convert a dollar risk budget into prediction market contract quantity.

```
Gemini prediction markets:
  - Each contract costs `price` USD (probability, 0-1)
  - Each contract pays $1.00 on win
  - quantity = floor(budget / price)
"""
if price <= 0:
    return 0
return max(1, int(size_usd / price))
```

# —————————————————————————–

# POSITION / TRADE BOOK

# —————————————————————————–

@dataclass
class Position:
id:              int
symbol:          str
strike:          float  # HI price threshold from ticker
outcome:         str    # “yes” (betting price >= strike) or “no”
minutes:         int    # contract duration in minutes
size_usd:        float
quantity:        int    # number of contracts
entry_price:     float  # probability paid per contract (0-1)
gemini_odds:     float
cex_price:       float  # XRP/USD at time of entry
edge_pct:        float
confidence:      float
kelly_frac:      float
paper:           bool
gemini_order_id: Optional[str] = None
opened_ts:       float   = field(default_factory=time.time)
expiry:          float   = 0.0

class TradeBook:
def **init**(self, conn: sqlite3.Connection, paper: bool):
self.conn   = conn
self.paper  = paper
self._lock  = threading.Lock()
self.logger = logging.getLogger(“TradeBook”)

```
def record_open(
    self, opp: Opportunity, size_usd: float, quantity: int,
    kelly_frac: float, gemini_order_id: str = None
) -> int:
    with self._lock:
        cur = self.conn.execute(
            """INSERT INTO trades
               (ts, symbol, contract_type, direction, outcome, size_usd,
                quantity, entry_price, gemini_odds, cex_price, edge_pct,
                confidence, kelly_frac, paper, gemini_order_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                opp.contract["symbol"],
                f"{opp.contract.get('minutes','?')}m",
                f"HI{opp.strike}",        # store as "HI2.20" for readability
                opp.outcome,
                size_usd,
                quantity,
                opp.gemini_prob,
                opp.gemini_prob,
                opp.cex_price,
                opp.edge * 100,
                opp.confidence,
                kelly_frac,
                int(self.paper),
                str(gemini_order_id) if gemini_order_id else None,
            ),
        )
        self.conn.commit()
        return cur.lastrowid

def record_close(
    self, trade_id: int, exit_price: float, pnl: float,
    win: bool, resolution_side: str = None
):
    with self._lock:
        self.conn.execute(
            """UPDATE trades
               SET status=?, exit_price=?, pnl=?, closed_ts=?, resolution_side=?
               WHERE id=?""",
            (
                "win" if win else "loss",
                exit_price,
                pnl,
                datetime.now(timezone.utc).isoformat(),
                resolution_side,
                trade_id,
            ),
        )
        self.conn.commit()

def recent_trades(self, n: int = 10) -> list:
    with self._lock:
        return self.conn.execute(
            "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (n,)
        ).fetchall()

def daily_pnl(self) -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with self._lock:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE closed_ts LIKE ?",
            (f"{today}%",),
        ).fetchone()
    return row[0] if row else 0.0

def win_rate(self) -> tuple[int, int, float]:
    with self._lock:
        row = self.conn.execute(
            "SELECT COUNT(*) total, "
            "SUM(CASE WHEN status='win' THEN 1 ELSE 0 END) wins "
            "FROM trades WHERE status IN ('win','loss')"
        ).fetchone()
    total = row["total"] or 0
    wins  = row["wins"]  or 0
    return wins, total, (wins / total if total else 0.0)
```

# —————————————————————————–

# KILL SWITCH

# —————————————————————————–

class KillSwitch:
def **init**(self, max_drawdown_pct: float, starting_nav: float):
self.max_drawdown_pct = max_drawdown_pct
self.peak_nav         = starting_nav
self.triggered        = False
self.logger           = logging.getLogger(“KillSwitch”)

```
def update(self, current_nav: float) -> float:
    self.peak_nav = max(self.peak_nav, current_nav)
    drawdown = (
        (self.peak_nav - current_nav) / self.peak_nav * 100
        if self.peak_nav > 0 else 0.0
    )
    if drawdown >= self.max_drawdown_pct and not self.triggered:
        self.triggered = True
        self.logger.critical(
            f"KILL SWITCH TRIGGERED -- drawdown {drawdown:.1f}% "
            f">= {self.max_drawdown_pct}%"
        )
    return drawdown
```

# —————————————————————————–

# TERMINAL DASHBOARD

# —————————————————————————–

def build_dashboard(state: dict) -> str:
mode = “PAPER” if state[“paper”] else “LIVE”
aggr = “ | AGGRESSIVE” if state.get(“aggressive”) else “”
ks   = “TRIGGERED” if state[“killed”] else “OK”
lines = [
“=” * 72,
f”  XRP LATENCY ARB BOT  |  {mode}{aggr}  |  {datetime.now().strftime(’%H:%M:%S’)}”,
“=” * 72,
f”  NAV:           ${state[‘nav’]:>12,.2f}”,
f”  Daily P&L:     ${state[‘daily_pnl’]:>+12,.2f}”,
f”  Drawdown:       {state[‘drawdown’]:>11.2f}%”,
f”  Win Rate:       {state[‘win_rate’]*100:>10.1f}%  ({state[‘wins’]}/{state[‘total’]})”,
f”  Open Positions: {state[‘open_positions’]:>10d}”,
f”  XRP/USD:        ${state[‘xrp_price’]:>10.4f}”,
f”  Kill Switch:    {ks}”,
“-” * 72,
“  LAST 10 TRADES”,
f”  {‘ID’:<5} {‘Symbol’:<32} {‘Side’:<5} {‘Strike’:>7} {‘Qty’:>5} {‘Cost’:>8} {‘P&L’:>9} {‘Status’}”,
“-” * 72,
]
for t in state.get(“recent_trades”, []):
pnl_str    = f”${t[‘pnl’]:+.2f}” if t[“pnl”] is not None else “open”
# direction column now shows YES/NO outcome
side       = t.get(“outcome”, t.get(“direction”, “?”)).upper()[:3]
strike_str = t.get(“direction”, “?”)   # stored as “HI2.20” in direction col
lines.append(
f”  {t[‘id’]:<5} {t[‘symbol’]:<32} {side:<5} {strike_str:>7} “
f”{t.get(‘quantity’,’?’):>5} ${t[‘size_usd’]:>7.2f} “
f”{pnl_str:>9} {t[‘status’]}”
)
lines.append(”=” * 72)
return “\n”.join(lines)

# —————————————————————————–

# CORE BOT

# —————————————————————————–

class XRPArbBot:

```
def __init__(self, cfg: Config):
    self.cfg          = cfg
    self.logger       = logging.getLogger("XRPArbBot")
    self.conn         = init_db(cfg.db_path)
    self.gemini       = GeminiClient(
        cfg.gemini_api_key, cfg.gemini_api_secret, sandbox=cfg.gemini_sandbox
    )
    self.binance_feed = BinanceFeed(cfg.binance_ws_url)
    self.trade_book   = TradeBook(self.conn, cfg.paper_trading)
    self.open_positions: dict[int, Position] = {}

    self._paper_nav  = cfg.paper_portfolio_usd
    self.kill_switch = KillSwitch(cfg.max_daily_drawdown_pct, self._paper_nav)
    self._stop_event = asyncio.Event()

    signal.signal(signal.SIGINT,  self._handle_signal)
    signal.signal(signal.SIGTERM, self._handle_signal)

def _handle_signal(self, *_):
    self.logger.info("Shutdown signal received.")
    self._stop_event.set()

# -- NAV ------------------------------------------------------------------

def _get_nav(self) -> float:
    if self.cfg.paper_trading:
        return self._paper_nav
    # Live: USD balance + mark-to-cost of open positions
    usd      = self.gemini.get_usd_balance()
    open_val = sum(p.quantity * p.entry_price for p in self.open_positions.values())
    return usd + open_val

# -- Opportunity detection -------------------------------------------------

async def _find_opportunities(self, tick: BinanceTick) -> list[Opportunity]:
    contracts = await asyncio.get_event_loop().run_in_executor(
        None, self.gemini.get_xrp_contracts
    )
    opps = []
    for contract in contracts:
        strike      = float(contract.get("strike") or 0)
        minutes     = int(contract.get("minutes", 5))
        gemini_prob = float(contract.get("implied_prob", 0.5))

        if not (0.01 <= gemini_prob <= 0.99):
            continue

        # Strike is always known from ticker for HI contracts.
        # If somehow missing, skip - we can't evaluate without it.
        if strike <= 0:
            self.logger.debug(f"Skipping {contract['symbol']} - no strike price")
            continue

        # CEX-implied P(XRP_expiry >= strike)
        cex_prob = implied_prob_from_price_action(tick, strike, minutes)

        # Edge on YES side: cex thinks higher probability than Gemini quotes
        edge_yes = cex_prob - gemini_prob

        # Edge on NO side: cex thinks lower probability (P(price < strike) higher)
        edge_no  = (1 - cex_prob) - (1 - gemini_prob)   # = -edge_yes

        # Take whichever side has positive edge
        if edge_yes >= edge_no and edge_yes > 0:
            edge    = edge_yes
            outcome = "yes"
        elif edge_no > edge_yes and edge_no > 0:
            edge    = edge_no
            outcome = "no"
            # For NO side: gemini_prob for display is P(NO) = 1 - implied_prob
            gemini_prob = 1 - gemini_prob
            cex_prob    = 1 - cex_prob
        else:
            continue   # no edge on either side

        lag_pct = abs(edge) * 100
        if lag_pct < self.cfg.lag_threshold_pct:
            continue
        if edge * 100 < self.cfg.min_edge_pct:
            continue

        opp = Opportunity(
            contract    = contract,
            strike      = strike,
            outcome     = outcome,
            gemini_prob = gemini_prob,
            cex_prob    = cex_prob,
            edge        = edge,
            lag_pct     = lag_pct,
            confidence  = 0.0,
            cex_price   = tick.price,
            minutes     = minutes,
        )
        opp.confidence = confidence_score(opp, tick)

        if opp.confidence >= self.cfg.min_confidence:
            opps.append(opp)

    return sorted(opps, key=lambda o: o.edge * o.confidence, reverse=True)

# -- Execution -------------------------------------------------------------

async def _execute_opportunity(self, opp: Opportunity):
    if self.kill_switch.triggered:
        self.logger.warning("Kill switch active -- skipping.")
        return
    if len(self.open_positions) >= self.cfg.max_open_positions:
        self.logger.info("Max open positions reached.")
        return

    nav      = self._get_nav()
    size_usd = kelly_size(
        opp.edge, opp.cex_prob, self.cfg.kelly_fraction,
        nav, self.cfg.max_position_pct
    )
    if size_usd < 1.0:
        return

    # Contracts cost entry_price each (probability), pay $1 on win
    entry_price = opp.gemini_prob
    quantity    = usd_to_contracts(size_usd, entry_price)
    actual_cost = round(quantity * entry_price, 2)

    self.logger.info(
        f"{'[PAPER]' if self.cfg.paper_trading else '[LIVE]'} "
        f"{opp.contract['symbol']} {opp.outcome.upper()} (strike=${opp.strike:.4f}) | "
        f"{quantity} contracts @ {entry_price:.3f} = ${actual_cost:.2f} | "
        f"edge={opp.edge*100:.1f}% conf={opp.confidence:.2f}"
    )

    gemini_order_id = None

    if not self.cfg.paper_trading:
        # Place real order via POST /v1/prediction-markets/order
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self.gemini.place_prediction_order(
                symbol        = opp.contract["symbol"],
                outcome       = opp.outcome,   # "yes"
                quantity      = quantity,
                price         = entry_price,
                time_in_force = "immediate-or-cancel",
            )
        )
        if not result:
            self.logger.error("Order placement failed.")
            return

        gemini_order_id = result.get("orderId")
        filled_qty      = int(result.get("filledQuantity", 0))

        if filled_qty == 0:
            self.logger.warning(
                f"Order {gemini_order_id}: 0 contracts filled (IOC cancelled)."
            )
            return

        # Adjust to actual fill
        quantity    = filled_qty
        avg_price   = float(result.get("avgExecutionPrice") or entry_price)
        actual_cost = round(quantity * avg_price, 2)
        entry_price = avg_price
        self.logger.info(
            f"Order {gemini_order_id} filled: {quantity} contracts @ ${actual_cost:.2f}"
        )

    trade_id = self.trade_book.record_open(
        opp, actual_cost, quantity, self.cfg.kelly_fraction, gemini_order_id
    )

    pos = Position(
        id              = trade_id,
        symbol          = opp.contract["symbol"],
        strike          = opp.strike,
        outcome         = opp.outcome,
        minutes         = opp.minutes,
        size_usd        = actual_cost,
        quantity        = quantity,
        entry_price     = entry_price,
        gemini_odds     = opp.gemini_prob,
        cex_price       = opp.cex_price,
        edge_pct        = opp.edge * 100,
        confidence      = opp.confidence,
        kelly_frac      = self.cfg.kelly_fraction,
        paper           = self.cfg.paper_trading,
        gemini_order_id = str(gemini_order_id) if gemini_order_id else None,
        expiry          = opp.contract.get("expiry", time.time() + 300),
    )
    self.open_positions[trade_id] = pos

    if self.cfg.paper_trading:
        self._paper_nav -= actual_cost

# -- Position management ---------------------------------------------------

async def _manage_positions(self, tick: BinanceTick):
    now      = time.time()
    to_close = [pid for pid, p in self.open_positions.items() if now >= p.expiry]
    for pid in to_close:
        pos = self.open_positions.pop(pid)
        await self._close_position(pos, tick)

async def _close_position(self, pos: Position, tick: BinanceTick):
    resolution_side = None

    if self.cfg.paper_trading:
        # HI contracts: YES wins if XRP price >= strike at expiry
        # Use current Binance price as settlement proxy
        if pos.outcome == "yes":
            win = tick.price >= pos.strike
        else:
            win = tick.price < pos.strike
        resolution_side = "yes" if tick.price >= pos.strike else "no"
        exit_price      = 1.0 if win else 0.0

    else:
        # Live: poll POST /v1/prediction-markets/orders/history for settlement
        if pos.gemini_order_id:
            settled, resolution_side = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.gemini.check_order_settled(int(pos.gemini_order_id))
            )
            if not settled:
                # Not yet resolved -- re-queue for next cycle
                self.logger.debug(f"Position {pos.id} not yet settled, rechecking next cycle.")
                self.open_positions[pos.id] = pos
                return
            # resolution_side "yes" means the "yes" outcome won
            win        = (resolution_side == pos.outcome)
            exit_price = 1.0 if win else 0.0
        else:
            # Fallback: price-based estimate
            win             = (pos.direction == "up" and tick.price > pos.cex_price)
            exit_price      = 1.0 if win else 0.0
            resolution_side = "yes" if win else "no"

    # P&L:
    #   Win:  earn (1.0 - entry_price) per contract
    #   Loss: lose entry_price per contract (stake)
    pnl = (pos.quantity * (1.0 - pos.entry_price)) if win else -(pos.quantity * pos.entry_price)

    self.trade_book.record_close(pos.id, exit_price, round(pnl, 4), win, resolution_side)

    if self.cfg.paper_trading:
        # Return win proceeds (lose nothing extra -- stake already debited on open)
        self._paper_nav += pos.quantity * exit_price

    self.logger.info(
        f"Closed: {pos.symbol} {pos.outcome.upper()} strike=${pos.strike:.4f} "
        f"{'WIN' if win else 'LOSS'} | settle={tick.price:.4f} | "
        f"{pos.quantity} contracts | P&L ${pnl:+.4f}"
    )

# -- Dashboard state dict --------------------------------------------------

def _build_state(self, tick: BinanceTick) -> dict:
    wins, total, rate = self.trade_book.win_rate()
    nav               = self._get_nav()
    daily_pnl         = self.trade_book.daily_pnl()
    drawdown          = self.kill_switch.update(nav)
    recent            = self.trade_book.recent_trades(10)
    return {
        "paper":          self.cfg.paper_trading,
        "aggressive":     self.cfg.poll_interval_sec < 2.0,
        "nav":            nav,
        "daily_pnl":      daily_pnl,
        "drawdown":       drawdown,
        "win_rate":       rate,
        "wins":           wins,
        "total":          total,
        "open_positions": len(self.open_positions),
        "xrp_price":      tick.price,
        "killed":         self.kill_switch.triggered,
        "recent_trades":  [dict(r) for r in recent],
    }

# -- Main loop -------------------------------------------------------------

async def run(self):
    self.logger.info(
        f"Bot starting -- mode={'PAPER' if self.cfg.paper_trading else 'LIVE'} "
        f"nav=${self._get_nav():,.2f}"
    )

    ws_task = asyncio.create_task(self.binance_feed.run())
    await asyncio.sleep(2)  # allow WS first tick

    iteration = 0
    while not self._stop_event.is_set():
        if self.kill_switch.triggered:
            self.logger.critical("Kill switch active -- bot halted.")
            break

        tick = await self.binance_feed.get_tick()
        if tick.price == 0:
            await asyncio.sleep(1)
            continue

        await self._manage_positions(tick)

        opps = await self._find_opportunities(tick)
        for opp in opps:
            await self._execute_opportunity(opp)
            if self.kill_switch.triggered:
                break

        if iteration % 5 == 0:
            os.system("cls" if os.name == "nt" else "clear")
            print(build_dashboard(self._build_state(tick)))

        iteration += 1
        try:
            await asyncio.wait_for(
                self._stop_event.wait(), timeout=self.cfg.poll_interval_sec
            )
        except asyncio.TimeoutError:
            pass

    self.binance_feed.stop()
    ws_task.cancel()
    self.logger.info("Bot stopped cleanly.")
```

# —————————————————————————–

# ENTRY POINT

# —————————————————————————–

def parse_args():
p = argparse.ArgumentParser(
description=“XRP Latency Arbitrage Bot – Gemini Prediction Markets vs Binance”
)
p.add_argument(”–api-key”,    default=os.getenv(“GEMINI_API_KEY”, “”))
p.add_argument(”–api-secret”, default=os.getenv(“GEMINI_API_SECRET”, “”))
p.add_argument(”–paper-nav”,  type=float, default=10_000)
p.add_argument(”–db”,         default=“xrp_arb_bot.db”)
p.add_argument(”–log-level”,  default=“INFO”)
p.add_argument(”–lag-threshold”,  type=float, default=3.0)
p.add_argument(”–min-edge”,       type=float, default=5.0)
p.add_argument(”–max-position”,   type=float, default=8.0)
p.add_argument(”–min-confidence”, type=float, default=0.85)
p.add_argument(”–max-drawdown”,   type=float, default=40.0)

```
# Aggressive mode: higher frequency, lower quality bar
# Overrides lag-threshold->2.0, min-edge->3.0, min-confidence->0.75,
# max-position->10.0, poll interval->1s
# Any individually specified flag still takes precedence over aggressive defaults.
p.add_argument("--aggressive", action="store_true",
               help="Higher frequency mode (2-3x more trades, lower average edge).")

# All three required for live trading
p.add_argument("--live",               action="store_true")
p.add_argument("--confirm-live",       action="store_true")
p.add_argument("--i-understand-risks", action="store_true")

return p.parse_args()
```

def main():
args = parse_args()

```
logging.basicConfig(
    level    = getattr(logging, args.log_level.upper(), logging.INFO),
    format   = "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("xrp_arb_bot.log"),
    ],
)

live_trading = args.live and args.confirm_live and args.i_understand_risks
paper        = not live_trading

# -- Aggressive mode defaults -----------------------------------------------
# Applied BEFORE individual flag overrides so explicit flags always win.
# Tracks which args were explicitly set by the user via sys.argv so we
# don't clobber intentional overrides.
AGGRESSIVE_DEFAULTS = {
    "lag_threshold":  2.0,
    "min_edge":       3.0,
    "min_confidence": 0.75,
    "max_position":   10.0,
    "poll_interval":  1.0,
}
explicit_flags = {a.lstrip("-").replace("-", "_") for a in sys.argv if a.startswith("--")}

if args.aggressive:
    if "lag_threshold"  not in explicit_flags: args.lag_threshold  = AGGRESSIVE_DEFAULTS["lag_threshold"]
    if "min_edge"       not in explicit_flags: args.min_edge       = AGGRESSIVE_DEFAULTS["min_edge"]
    if "min_confidence" not in explicit_flags: args.min_confidence = AGGRESSIVE_DEFAULTS["min_confidence"]
    if "max_position"   not in explicit_flags: args.max_position   = AGGRESSIVE_DEFAULTS["max_position"]
    poll_interval = AGGRESSIVE_DEFAULTS["poll_interval"]
    print(
        f"\n[AGGRESSIVE MODE]\n"
        f"  lag-threshold  : {args.lag_threshold}%\n"
        f"  min-edge       : {args.min_edge}%\n"
        f"  min-confidence : {args.min_confidence}\n"
        f"  max-position   : {args.max_position}%\n"
        f"  poll-interval  : {poll_interval}s\n"
        f"  Expected trades: ~10-25/day on normal volatility\n"
    )
else:
    poll_interval = 2.0

if (args.live or args.confirm_live or args.i_understand_risks) and not live_trading:
    missing = [
        flag for flag, present in [
            ("--live",               args.live),
            ("--confirm-live",       args.confirm_live),
            ("--i-understand-risks", args.i_understand_risks),
        ] if not present
    ]
    print(f"Live trading requires ALL THREE flags. Missing: {', '.join(missing)}")
    print("Defaulting to paper trading.\n")

if not paper:
    print("\n" + "!" * 62)
    print("  LIVE TRADING ENABLED -- REAL FUNDS AT RISK")
    print("!" * 62 + "\n")
    if not args.api_key or not args.api_secret:
        print("ERROR: --api-key and --api-secret required for live trading.")
        sys.exit(1)

cfg = Config(
    gemini_api_key         = args.api_key,
    gemini_api_secret      = args.api_secret,
    gemini_sandbox         = paper,
    paper_trading          = paper,
    paper_portfolio_usd    = args.paper_nav,
    db_path                = args.db,
    lag_threshold_pct      = args.lag_threshold,
    min_edge_pct           = args.min_edge,
    max_position_pct       = args.max_position,
    min_confidence         = args.min_confidence,
    max_daily_drawdown_pct = args.max_drawdown,
    poll_interval_sec      = poll_interval,
)

bot = XRPArbBot(cfg)
asyncio.run(bot.run())
```

if **name** == “**main**”:
main()
