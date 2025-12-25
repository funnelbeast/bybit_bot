#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BYBIT SIGNAL BOT v2.0 - Produktionsreif mit Hedging & Guards
=============================================================

FEATURES:
- Bewährte Basis aus working script (Quantity-Berechnung, Parsing, API)
- Vollständige Guards-Integration (Validierung, Housekeeping)
- Hedging erlaubt (gegengesetzte Richtungen im gleichen Paar)
- Positionsvergrößerung bei gleicher Richtung (+DEFAULT_ORDER_USDT)
- Sequentielle TP-Orders (30%/30%/20%/10%) mit Teilverkäufen
- Breakeven mit Fees (0.055% × 2) nach TP1 erreicht
- Dynamischer Trailing Stop basierend auf ungehebeltem Profit
- TP-Status-Updates werden ignoriert (keine falschen Entries)
- Robuste Error-Handling (kein Hard-Crash)

ENV VARIABLEN:
- BYBIT_API_KEY, BYBIT_API_SECRET
- DEFAULT_ORDER_USDT=60 (Budget pro Position/Erweiterung)
- DEFAULT_LEVERAGE=20
- TEST_MODE=false/true
- DEBUG=true/false (erweiterte Logs)
"""

import os
import re
import time
import hmac
import json
import hashlib
import logging
import requests
import asyncio
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from dotenv import load_dotenv
from telethon import TelegramClient, events

# =========================
# KONFIGURATION & LOGGING
# =========================

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ENV laden
BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# Bybit API
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")

if not BYBIT_API_KEY or not BYBIT_API_SECRET:
    raise RuntimeError("Bybit API Key/Secret fehlen in .env")

# Trading Parameter
DEFAULT_ORDER_USDT = float(os.getenv("DEFAULT_ORDER_USDT", "60"))
DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "20"))
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
DEBUG_MODE = os.getenv("DEBUG", "false").lower() == "true"

# Telegram
TG_API_ID = int(os.getenv("TG_API_ID", "21513355"))
TG_API_HASH = os.getenv("TG_API_HASH", "e083f81b9a7d9379b9650158cbdf65b5")
SIGNAL_SOURCE = int(os.getenv("SIGNAL_SOURCE", "-1001717037581"))

SESSION_FILE = BASE_DIR / "bybit_signal_session"
client = TelegramClient(str(SESSION_FILE), TG_API_ID, TG_API_HASH)

# Trailing Stop Thresholds (UNGEHEBELTE Performance)
TRAILING_THRESHOLDS = [
    (5.0, 12.0),   # +5%  ungehebelt → TS 12%
    (10.0, 10.0),  # +10% ungehebelt → TS 10%
    (15.0, 7.0),   # +15% ungehebelt → TS 7%
    (20.0, 5.0),   # +20% ungehebelt → TS 5%
    (25.0, 3.0),   # +25% ungehebelt → TS 3%
    (30.0, 2.0),   # +30% ungehebelt → TS 2%
    (35.0, 1.5),   # +35% ungehebelt → TS 1.5%
    (40.0, 1.0),   # +40% ungehebelt → TS 1.0%
]

# Trading Fee (Bybit linear 0.055% × 2 für Round-Trip)
TRADING_FEE_PERCENT = 0.00055  # 0.055%
BREAKEVEN_MULTIPLIER = 1 + (TRADING_FEE_PERCENT * 2)  # 1.0011

# Global State
positions = {}  # {symbol: {"long": {...}, "short": {...}}}
active_watchers = {}  # {symbol: {"trailing": task, "breakeven": task}}

# =========================
# BYBIT API HELPERS
# =========================

def _canon_json(body: dict) -> str:
    return json.dumps(body, separators=(",", ":"), sort_keys=True)

def _sign_headers(body_str: str) -> dict:
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    sign_payload = ts + BYBIT_API_KEY + recv_window + body_str
    sign = hmac.new(BYBIT_API_SECRET.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": sign,
    }

def _sign_get_request(params: dict = None) -> dict:
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    if params:
        sorted_params = sorted(params.items())
        param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
    else:
        param_str = ""
    signature = hmac.new(
        BYBIT_API_SECRET.encode("utf-8"),
        (ts + BYBIT_API_KEY + recv_window + param_str).encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
    }

def _post_v5(path: str, body: dict) -> requests.Response:
    url = f"{BYBIT_BASE_URL}{path}"
    body_str = _canon_json(body)
    headers = _sign_headers(body_str)
    return requests.post(url, headers=headers, data=body_str, timeout=10)

def _get_v5(path: str, params: dict = None) -> requests.Response:
    url = f"{BYBIT_BASE_URL}{path}"
    headers = _sign_get_request(params)
    return requests.get(url, params=params, headers=headers, timeout=10)

# =========================
# BYBIT ADAPTER FÜR GUARDS
# =========================

class BybitAdapter:
    """Adapter für Guards-System mit Bybit v5 API."""
    
    def __init__(self):
        self._instrument_cache = {}
        self._price_cache = {}
        self._cache_time = {}
        
    def get_instrument(self, symbol: str) -> dict:
        """Holt Instrument-Details mit Cache und Fallbacks."""
        if symbol in self._instrument_cache and time.time() - self._cache_time.get(symbol, 0) < 300:
            return self._instrument_cache[symbol]
            
        try:
            response = _get_v5("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
            data = response.json()
            
            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                item = data["result"]["list"][0]
                
                # Extrahiere Filter mit Fallbacks
                pf = item.get("priceFilter", {})
                lf = item.get("lotSizeFilter", {})
                
                instrument_data = {
                    "priceFilter": {
                        "tickSize": pf.get("tickSize", "0.0001"),
                    },
                    "lotSizeFilter": {
                        "qtyStep": lf.get("qtyStep", "0.001"),
                        "minOrderQty": lf.get("minOrderQty", "0.001"),
                        "maxOrderQty": lf.get("maxOrderQty", "999999"),
                    },
                    "minNotionalValue": item.get("minNotionalValue", "5"),
                }
                
                self._instrument_cache[symbol] = instrument_data
                self._cache_time[symbol] = time.time()
                
                if DEBUG_MODE:
                    log.debug(f"[{symbol}] Instrument cached: {instrument_data}")
                    
                return instrument_data
                
        except Exception as e:
            log.warning(f"[{symbol}] Instrument fetch failed: {e}")
            
        # Fallback für Fehlerfälle
        return {
            "priceFilter": {"tickSize": "0.0001"},
            "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001", "maxOrderQty": "999999"},
            "minNotionalValue": "5"
        }
    
    def place_order(self, **params) -> dict:
        """Platziert Order über Bybit v5 API."""
        try:
            response = _post_v5("/v5/order/create", params)
            return response.json()
        except Exception as e:
            log.error(f"Order placement failed: {e}")
            return {"retCode": -1, "retMsg": str(e)}
    
    def cancel_all_orders(self, symbol: str) -> dict:
        """Storniert alle Orders für ein Symbol."""
        try:
            response = _post_v5("/v5/order/cancel-all", {
                "category": "linear",
                "symbol": symbol,
                "orderFilter": "Order"
            })
            return response.json()
        except Exception as e:
            log.error(f"[{symbol}] Cancel all orders failed: {e}")
            return {"retCode": -1, "retMsg": str(e)}
    
    def get_position(self, symbol: str) -> dict:
        """Holt Positionen für ein Symbol (long/short separat)."""
        try:
            response = _get_v5("/v5/position/list", {
                "category": "linear",
                "symbol": symbol
            })
            data = response.json()
            
            if data.get("retCode") == 0:
                positions_list = data.get("result", {}).get("list", [])
                
                long_pos = {"size": 0.0, "entryPrice": None, "side": "Buy"}
                short_pos = {"size": 0.0, "entryPrice": None, "side": "Sell"}
                
                for pos in positions_list:
                    size = float(pos.get("size", 0))
                    if abs(size) > 0:
                        side = "Buy" if size > 0 else "Sell"
                        entry = float(pos.get("avgPrice", 0))
                        
                        if side == "Buy":
                            long_pos = {"size": abs(size), "entryPrice": entry, "side": side}
                        else:
                            short_pos = {"size": abs(size), "entryPrice": entry, "side": side}
                
                return {"long": long_pos, "short": short_pos}
                
        except Exception as e:
            log.error(f"[{symbol}] Get position failed: {e}")
            
        return {"long": {"size": 0.0, "entryPrice": None, "side": "Buy"}, 
                "short": {"size": 0.0, "entryPrice": None, "side": "Sell"}}
    
    def get_last_price(self, symbol: str) -> float:
        """Holt aktuellen Preis mit Cache."""
        if symbol in self._price_cache and time.time() - self._cache_time.get(f"price_{symbol}", 0) < 5:
            return self._price_cache[symbol]
            
        try:
            response = _get_v5("/v5/market/tickers", {
                "category": "linear",
                "symbol": symbol
            })
            data = response.json()
            
            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                price = float(data["result"]["list"][0]["lastPrice"])
                self._price_cache[symbol] = price
                self._cache_time[f"price_{symbol}"] = time.time()
                return price
                
        except Exception as e:
            log.warning(f"[{symbol}] Price fetch failed: {e}")
            
        return 0.0

# =========================
# TRADING FUNKTIONEN
# =========================

def get_qty_step_info(symbol: str) -> Tuple[float, float, float]:
    """Holt qtyStep, minOrderQty, maxOrderQty für ein Symbol."""
    try:
        response = _get_v5("/v5/market/instruments-info", {
            "category": "linear",
            "symbol": symbol
        })
        data = response.json()
        
        if data.get("retCode") == 0 and data.get("result", {}).get("list"):
            lf = data["result"]["list"][0].get("lotSizeFilter", {})
            min_qty = float(lf.get("minOrderQty", 0))
            max_qty = float(lf.get("maxOrderQty", 999999))
            qty_step = float(lf.get("qtyStep", 0.001))
            return (min_qty, max_qty, qty_step)
            
    except Exception as e:
        log.warning(f"[{symbol}] Qty step info failed: {e}")
        
    return (0.0, 999999.0, 0.001)

def adjust_qty_to_step(symbol: str, qty: float) -> float:
    """Rundet Quantity auf gültigen Step."""
    min_qty, max_qty, step = get_qty_step_info(symbol)
    
    if qty <= 0:
        qty = min_qty if min_qty > 0 else step
    
    # Auf Step runterrunden
    if step > 0:
        qty = (int(qty / step)) * step
    
    # Grenzen prüfen
    if qty < min_qty:
        qty = min_qty
    if qty > max_qty:
        qty = max_qty
    
    # Sauber runden
    qty = round(qty, 6 if step < 0.001 else 3)
    return max(qty, 0.001)

def calculate_qty(entry_price: float, symbol: str = None, current_price: float = None) -> float:
    """Berechnet Quantity aus USDT Budget (robuste Methode)."""
    qty = DEFAULT_ORDER_USDT / entry_price
    
    # Min Notional Check (5 USDT)
    if current_price is not None:
        order_value = qty * current_price
        if order_value < 5:
            qty = 5 / current_price
            log.info(f"[{symbol}] Order Value auf Min 5 USDT angehoben: {qty:.6f}")
    
    # Auf Step runden
    if symbol:
        qty = adjust_qty_to_step(symbol, qty)
    
    qty = max(qty, 0.001)
    
    if DEBUG_MODE:
        log.debug(f"[{symbol}] Quantity: {qty} (Entry: {entry_price}, USDT: {DEFAULT_ORDER_USDT})")
    
    return qty

def calculate_breakeven_price(entry_price: float, side: str) -> float:
    """Berechnet Breakeven-Preis mit Trading Fees."""
    if side == "Buy":
        # Long: Entry × (1 + (0.055% × 2))
        return round(entry_price * BREAKEVEN_MULTIPLIER, 6)
    else:
        # Short: Entry × (1 - (0.055% × 2))
        return round(entry_price * (2 - BREAKEVEN_MULTIPLIER), 6)

def calculate_ungehebelter_profit(entry_price: float, current_price: float, side: str) -> float:
    """Berechnet ungehebelten Profit in Prozent."""
    if side == "Buy":
        return ((current_price / entry_price) - 1) * 100
    else:
        return ((entry_price / current_price) - 1) * 100

def get_trailing_stop_for_profit(profit_pct: float) -> float:
    """Gibt Trailing-Stop % basierend auf ungehebeltem Profit zurück."""
    for threshold, ts_pct in TRAILING_THRESHOLDS:
        if profit_pct >= threshold:
            return ts_pct
    return 14.0  # Default vor 5% Profit

# =========================
# ORDER FUNKTIONEN
# =========================

def place_order_with_retry(symbol: str, side: str, qty: float, 
                          reduce_only: bool = False, order_type: str = "Market",
                          price: float = None, max_retries: int = 2) -> Optional[dict]:
    """Platziert Order mit Retry-Mechanismus."""
    for attempt in range(max_retries):
        try:
            position_idx = 1 if side == "Buy" else 2
            
            body = {
                "category": "linear",
                "symbol": symbol,
                "side": side,
                "positionIdx": position_idx,
                "orderType": order_type,
                "qty": str(qty),
                "reduceOnly": bool(reduce_only),
                "timeInForce": "IOC" if order_type == "Market" else "GTC",
            }
            
            if order_type == "Limit" and price is not None:
                body["price"] = str(price)
            
            if TEST_MODE:
                log.info(f"[TEST] Order {side} {order_type} qty={qty} price={price}")
                return {"retCode": 0, "result": {"orderId": "TEST"}}
            
            response = _post_v5("/v5/order/create", body)
            resp = response.json()
            
            if resp.get("retCode") == 0:
                log.info(f"[{symbol}] {order_type} Order {side} qty={qty} erfolgreich")
                return resp
            else:
                err = resp.get("retMsg", "Unknown error")
                
                # Qty anpassen und retry
                if "Qty invalid" in err or "qty" in err.lower():
                    new_qty = adjust_qty_to_step(symbol, qty)
                    if new_qty != qty and new_qty > 0:
                        qty = new_qty
                        log.warning(f"[{symbol}] Qty invalid → passe an: {qty}")
                        continue
                
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    log.warning(f"[{symbol}] Order fehlgeschlagen, retry {attempt+1}: {err}")
                    time.sleep(wait)
                    continue
                    
                log.error(f"[{symbol}] Order endgültig fehlgeschlagen: {err}")
                return None
                
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                log.warning(f"[{symbol}] Order Exception, retry {attempt+1}: {e}")
                time.sleep(wait)
                continue
            log.error(f"[{symbol}] Order Exception endgültig fehlgeschlagen: {e}")
            return None
    
    return None

def set_leverage(symbol: str, leverage: int = DEFAULT_LEVERAGE) -> bool:
    """Setzt Leverage für ein Symbol."""
    body = {
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": str(leverage),
        "sellLeverage": str(leverage)
    }
    
    if TEST_MODE:
        log.info(f"[TEST] Leverage x{leverage} für {symbol}")
        return True
        
    try:
        response = _post_v5("/v5/position/set-leverage", body)
        resp = response.json()
        
        if resp.get("retCode") == 0:
            log.info(f"[{symbol}] Leverage x{leverage} gesetzt")
            return True
        else:
            # Error 110043 ignorieren (harmlos)
            if resp.get("retCode") != 110043:
                log.warning(f"[{symbol}] Leverage Fehler: {resp.get('retMsg')}")
            return False
            
    except Exception as e:
        log.error(f"[{symbol}] Leverage Exception: {e}")
        return False

def set_trailing_stop(symbol: str, trailing_pct: float, side: str) -> bool:
    """Setzt Trailing Stop als Distanz (nicht Rate!)."""
    position_idx = 1 if side == "Buy" else 2
    
    # Distanz berechnen
    current_price = bybit_adapter.get_last_price(symbol)
    if current_price <= 0:
        log.warning(f"[{symbol}] Kann Trailing nicht setzen (Preis=0)")
        return False
    
    trailing_distance = current_price * (trailing_pct / 100)
    trailing_distance = round(trailing_distance, 6)
    
    body = {
        "category": "linear",
        "symbol": symbol,
        "positionIdx": position_idx,
        "tpslMode": "Full",
        "trailingStop": str(trailing_distance)
    }
    
    if TEST_MODE:
        log.info(f"[TEST] Trailing Stop {trailing_pct}% ({trailing_distance}) für {symbol}")
        return True
        
    try:
        response = _post_v5("/v5/position/trading-stop", body)
        resp = response.json()
        
        if resp.get("retCode") == 0:
            log.info(f"[{symbol}] Trailing Stop {trailing_pct}% gesetzt")
            return True
        else:
            # Bestimmte Fehler ignorieren
            if resp.get("retCode") not in [34040, 10001]:
                log.warning(f"[{symbol}] Trailing Fehler: {resp.get('retMsg')}")
            return False
            
    except Exception as e:
        log.error(f"[{symbol}] Trailing Exception: {e}")
        return False

def set_stop_loss(symbol: str, price: float, side: str) -> bool:
    """Setzt Stop Loss."""
    position_idx = 1 if side == "Buy" else 2
    
    body = {
        "category": "linear",
        "symbol": symbol,
        "positionIdx": position_idx,
        "tpslMode": "Full",
        "stopLoss": str(price),
        "slTriggerBy": "LastPrice"
    }
    
    if TEST_MODE:
        log.info(f"[TEST] Stop Loss {price} für {symbol}")
        return True
        
    try:
        response = _post_v5("/v5/position/trading-stop", body)
        resp = response.json()
        
        if resp.get("retCode") == 0:
            log.info(f"[{symbol}] Stop Loss {price} gesetzt")
            return True
        else:
            if resp.get("retCode") not in [34040, 10001]:
                log.warning(f"[{symbol}] SL Fehler: {resp.get('retMsg')}")
            return False
            
    except Exception as e:
        log.error(f"[{symbol}] SL Exception: {e}")
        return False

def place_sequential_tps(symbol: str, total_qty: float, side: str, tp_levels: dict) -> int:
    """Platziert sequentielle TP-Orders (30%/30%/20%/10%)."""
    close_side = "Sell" if side == "Buy" else "Buy"
    steps = [(40, 0.30), (60, 0.30), (80, 0.20), (100, 0.10)]
    success_count = 0
    
    for perc, pct_qty in steps:
        price = tp_levels.get(perc)
        if not price:
            log.warning(f"[{symbol}] TP{perc}% Preis fehlt. Stoppe Sequenz.")
            break
        
        # Menge berechnen
        step_qty = total_qty * pct_qty
        adj_qty = adjust_qty_to_step(symbol, step_qty)
        
        # Prüfungen
        if adj_qty <= 0:
            log.warning(f"[{symbol}] TP{perc}% qty zu klein. Stoppe Sequenz.")
            break
        
        order_value = adj_qty * price
        if order_value < 5:
            log.warning(f"[{symbol}] TP{perc}% Value {order_value:.2f} < 5 USDT. Stoppe Sequenz.")
            break
        
        # Order platzieren
        log.info(f"[{symbol}] Platziere TP{perc}%: {adj_qty} @ {price}")
        result = place_order_with_retry(
            symbol=symbol,
            side=close_side,
            qty=adj_qty,
            reduce_only=True,  # WICHTIG: Reduce-Only!
            order_type="Limit",
            price=price
        )
        
        if result and result.get("retCode") == 0:
            success_count += 1
            oid = result.get("result", {}).get("orderId", "N/A")
            log.info(f"[{symbol}] ✓ TP{perc}% platziert (ID: {oid})")
        else:
            log.error(f"[{symbol}] ✗ TP{perc}% fehlgeschlagen")
            break  # Sequenz stoppen bei Fehler
    
    log.info(f"[{symbol}] {success_count}/4 TP-Orders erfolgreich")
    return success_count

# =========================
# SIGNAL PARSER
# =========================

SIGNAL_RE = re.compile(
    r"#(?P<base>[A-Z0-9]+)\/USDT.*?(Short|Long).*\nEntry\s*-\s*(?P<entry>[0-9.]+).*?Take-Profit:\s*(?:\n.+?(?P<tp1>[0-9.]+).*\(40%\).*?\n.+?(?P<tp2>[0-9.]+).*\(60%\).*?\n.+?(?P<tp3>[0-9.]+).*\(80%\).*?\n.+?(?P<tp4>[0-9.]+).*\(100%\))",
    re.S
)

def parse_entry_signal(text: str) -> Optional[dict]:
    """Parst Entry-Signal aus Telegram Nachricht."""
    # Zuerst Regex versuchen
    try:
        match = SIGNAL_RE.search(text)
        if match:
            coin = match.group("base").upper()
            symbol = f"{coin}USDT"
            entry_price = float(match.group("entry"))
            
            # Side erkennen
            matched_side = match.group(2)
            side_word = matched_side.lower()
            side = "Buy" if side_word == "long" else "Sell"
            
            # TP-Levels
            tp_levels = {}
            if match.group("tp1"):
                tp_levels[40] = float(match.group("tp1"))
            if match.group("tp2"):
                tp_levels[60] = float(match.group("tp2"))
            if match.group("tp3"):
                tp_levels[80] = float(match.group("tp3"))
            if match.group("tp4"):
                tp_levels[100] = float(match.group("tp4"))
            
            return {
                "symbol": symbol,
                "side": side,
                "entry": entry_price,
                "tp_levels": tp_levels,
                "side_word": side_word,
                "parsed_with_regex": True
            }
    except Exception as e:
        if DEBUG_MODE:
            log.debug(f"Regex parsing failed: {e}")
    
    # Fallback: Einfaches Parsing
    normalized_text = text.replace("**", "")
    if "Entry" not in normalized_text:
        return None
        
    lines = [l.strip() for l in normalized_text.splitlines() if l.strip()]
    if not lines:
        return None
    
    first = lines[0]
    m_pair = re.search(r"#([A-Z0-9]+)", first, re.IGNORECASE)
    if not m_pair:
        return None
        
    coin = m_pair.group(1).upper()
    symbol = f"{coin}USDT"
    
    # Side erkennen
    if "Long" in first or "📈" in first:
        side_word = "long"
        side = "Buy"
    elif "Short" in first or "📉" in first:
        side_word = "short"
        side = "Sell"
    else:
        m_side = re.search(r"\((Long|Short)", first, re.IGNORECASE)
        if not m_side:
            return None
        side_word = m_side.group(1).lower()
        side = "Buy" if side_word == "long" else "Sell"
    
    # Entry Preis
    entry_price = None
    for line in lines:
        if "Entry" in line:
            nums = re.findall(r'[-+]?\d*\.\d+|\d+', line)
            if nums:
                try:
                    entry_price = float(nums[0])
                    break
                except:
                    continue
                    
    if entry_price is None:
        return None
    
    # TP-Levels
    tp_levels = {}
    for line in lines:
        m_tp = re.search(r'(\d+\.\d+|\d+)\s*\((\d+)%\s*of\s*profit\)', line, re.IGNORECASE)
        if m_tp:
            price = float(m_tp.group(1))
            perc = int(m_tp.group(2))
            tp_levels[perc] = price
    
    return {
        "symbol": symbol,
        "side": side,
        "entry": entry_price,
        "tp_levels": tp_levels,
        "side_word": side_word,
        "parsed_with_regex": False
    }

def parse_tp_signal(text: str) -> Optional[dict]:
    """Erkennt TP-Status-Updates (werden ignoriert)."""
    normalized_text = text.replace("**", "")
    
    # TP-Update erkennen (✅ Price - X und Profit - X%)
    if "✅" in normalized_text and "Profit" in normalized_text and "Entry" not in normalized_text:
        if DEBUG_MODE:
            log.debug("TP-Status Update erkannt (wird ignoriert)")
        return {"is_tp_update": True}
    
    # Normales TP-Signal (älteres Format)
    if "✅" not in normalized_text or "Profit" not in normalized_text:
        return None
        
    lines = [l.strip() for l in normalized_text.splitlines() if l.strip()]
    if len(lines) < 2:
        return None
        
    first = lines[0]
    m_pair = re.search(r"#([A-Z0-9]+)", first, re.IGNORECASE)
    if not m_pair:
        return None
        
    coin = m_pair.group(1).upper()
    symbol = f"{coin}USDT"
    
    profit = None
    for line in lines:
        if "Profit" in line:
            nums = re.findall(r'\d+', line)
            if nums:
                try:
                    profit = int(nums[0])
                    break
                except:
                    continue
                    
    if profit is None:
        return None
        
    return {"symbol": symbol, "profit": profit, "is_tp_update": False}

# =========================
# WATCHER FUNKTIONEN
# =========================

async def breakeven_watcher(symbol: str, entry_price: float, side: str, tp1_price: float):
    """Setzt SL auf Breakeven wenn TP1 Preis erreicht wird."""
    log.info(f"[{symbol}] Breakeven Watcher gestartet (TP1: {tp1_price})")
    
    breakeven_price = calculate_breakeven_price(entry_price, side)
    deadline = time.time() + (6 * 60 * 60)  # 6 Stunden
    
    while time.time() < deadline:
        try:
            # Prüfe ob Position noch aktiv
            pos_data = bybit_adapter.get_position(symbol)
            if side == "Buy":
                pos_size = pos_data["long"]["size"]
            else:
                pos_size = pos_data["short"]["size"]
                
            if pos_size <= 0:
                log.info(f"[{symbol}] Position geschlossen, Breakeven Watcher beendet")
                return
            
            # Aktuellen Preis holen
            current_price = bybit_adapter.get_last_price(symbol)
            if current_price <= 0:
                await asyncio.sleep(2)
                continue
            
            # TP1 erreicht?
            tp1_reached = False
            if side == "Buy":
                tp1_reached = current_price >= tp1_price
            else:
                tp1_reached = current_price <= tp1_price
            
            if tp1_reached:
                log.info(f"[{symbol}] TP1 erreicht ({current_price}) → setze SL auf Breakeven ({breakeven_price})")
                set_stop_loss(symbol, breakeven_price, side)
                
                # In State speichern
                if symbol in positions:
                    if side.lower() in positions[symbol]:
                        positions[symbol][side.lower()]["be_set"] = True
                
                log.info(f"[{symbol}] ✅ Breakeven gesetzt, Watcher beendet")
                return
                
        except Exception as e:
            log.error(f"[{symbol}] Breakeven Watcher Fehler: {e}")
        
        await asyncio.sleep(2)
    
    log.info(f"[{symbol}] Breakeven Watcher Timeout (6h)")

async def trailing_stop_watcher(symbol: str, side: str):
    """Passt Trailing Stop basierend auf ungehebeltem Profit an."""
    log.info(f"[{symbol}] Trailing Stop Watcher gestartet ({side})")
    
    last_trailing_pct = 14.0
    
    while True:
        try:
            # Prüfe ob Position noch aktiv
            pos_data = bybit_adapter.get_position(symbol)
            if side == "Buy":
                pos_info = pos_data["long"]
            else:
                pos_info = pos_data["short"]
                
            entry_price = pos_info.get("entryPrice")
            pos_size = pos_info.get("size", 0)
            
            if pos_size <= 0 or entry_price is None:
                log.info(f"[{symbol}] Position geschlossen, Trailing Watcher beendet")
                break
            
            # Aktuellen Preis und Profit berechnen
            current_price = bybit_adapter.get_last_price(symbol)
            if current_price <= 0:
                await asyncio.sleep(30)
                continue
            
            profit_pct = calculate_ungehebelter_profit(entry_price, current_price, side)
            trailing_pct = get_trailing_stop_for_profit(profit_pct)
            
            # Trailing Stop aktualisieren wenn besser
            if trailing_pct < last_trailing_pct:
                log.info(f"[{symbol}] Profit: {profit_pct:.1f}% → Trailing: {trailing_pct}% (vorher: {last_trailing_pct}%)")
                if set_trailing_stop(symbol, trailing_pct, side):
                    last_trailing_pct = trailing_pct
            
            # State aktualisieren
            if symbol in positions:
                if side.lower() in positions[symbol]:
                    positions[symbol][side.lower()]["current_profit"] = profit_pct
                    positions[symbol][side.lower()]["current_trailing"] = trailing_pct
            
        except Exception as e:
            log.error(f"[{symbol}] Trailing Watcher Fehler: {e}")
        
        await asyncio.sleep(30)  # Alle 30 Sekunden prüfen
    
    log.info(f"[{symbol}] Trailing Stop Watcher beendet")

# =========================
# TRADE EXECUTION
# =========================

async def execute_trade(symbol: str, side: str, entry_price: float, tp_levels: dict):
    """Führt einen Trade aus (Entry, TP, Trailing, Watcher)."""
    log.info(f"[{symbol}] Starte Trade Execution: {side} @ {entry_price}")
    
    # Leverage setzen
    set_leverage(symbol)
    
    # Quantity berechnen
    current_price = bybit_adapter.get_last_price(symbol)
    qty = calculate_qty(entry_price, symbol, current_price)
    
    # Entry mit Toleranz (±0.5%)
    tolerance = entry_price * 0.005
    if side == "Buy":
        if current_price > (entry_price + tolerance):
            log.warning(f"[{symbol}] Preis zu hoch: {current_price} > {entry_price + tolerance}")
            return False
    else:
        if current_price < (entry_price - tolerance):
            log.warning(f"[{symbol}] Preis zu niedrig: {current_price} < {entry_price - tolerance}")
            return False
    
    # Position prüfen (Hedging/Erweiterung)
    pos_data = bybit_adapter.get_position(symbol)
    existing_position = pos_data["long" if side == "Buy" else "short"]
    
    if existing_position["size"] > 0:
        # Positionsvergrößerung
        log.info(f"[{symbol}] Bestehende {side} Position gefunden: {existing_position['size']}")
        log.info(f"[{symbol}] Vergrößere um {DEFAULT_ORDER_USDT} USDT")
    
    # Market Order platzieren
    log.info(f"[{symbol}] Platziere Market Order: {side} {qty}")
    order_result = place_order_with_retry(
        symbol=symbol,
        side=side,
        qty=qty,
        reduce_only=False,
        order_type="Market"
    )
    
    if not order_result or order_result.get("retCode") != 0:
        log.error(f"[{symbol}] Market Order fehlgeschlagen")
        return False
    
    log.info(f"[{symbol}] ✓ Market Order erfolgreich")
    
    # Kurze Pause für Fill
    await asyncio.sleep(3)
    
    # Initialen Trailing Stop setzen (14% Distanz)
    log.info(f"[{symbol}] Setze initialen Trailing Stop (14%)")
    set_trailing_stop(symbol, 14.0, side)
    
    # TP-Orders sequentiell platzieren
    if tp_levels:
        log.info(f"[{symbol}] Platziere sequentielle TP-Orders")
        
        # Gesamtmenge für TPs (inklusive bestehender Position)
        total_qty_for_tps = qty
        if existing_position["size"] > 0:
            total_qty_for_tps += existing_position["size"]
        
        tp_success = place_sequential_tps(symbol, total_qty_for_tps, side, tp_levels)
        log.info(f"[{symbol}] {tp_success}/4 TP-Orders platziert")
    else:
        log.warning(f"[{symbol}] Keine TP-Levels verfügbar")
    
    # State aktualisieren
    if symbol not in positions:
        positions[symbol] = {}
    
    positions[symbol][side.lower()] = {
        "entry": entry_price,
        "qty": qty,
        "tp_levels": tp_levels,
        "tp1_hit": False,
        "be_set": False,
        "entry_time": datetime.now().isoformat()
    }
    
    # Watcher starten
    if tp_levels.get(40):
        # Breakeven Watcher
        be_task = asyncio.create_task(
            breakeven_watcher(symbol, entry_price, side, tp_levels[40])
        )
        
        # Trailing Stop Watcher
        ts_task = asyncio.create_task(
            trailing_stop_watcher(symbol, side)
        )
        
        # Watcher speichern
        if symbol not in active_watchers:
            active_watchers[symbol] = {}
        active_watchers[symbol][side.lower()] = {
            "breakeven": be_task,
            "trailing": ts_task
        }
    
    log.info(f"[{symbol}] ✅ Trade komplett eingerichtet")
    return True

# =========================
# TELEGRAM HANDLER
# =========================

@client.on(events.NewMessage(chats=SIGNAL_SOURCE))
async def on_message(event):
    """Verarbeitet Telegram Signale."""
    try:
        text = (event.raw_text or "").strip()
        if not text:
            return
            
        log.info(f"📨 Nachricht empfangen: {text[:100]}...")
        
        # TP-Status Update erkennen und ignorieren
        tp_update = parse_tp_signal(text)
        if tp_update:
            if tp_update.get("is_tp_update"):
                log.info("TP-Status Update ignoriert")
            elif "symbol" in tp_update and "profit" in tp_update:
                log.info(f"TP {tp_update['profit']}% für {tp_update['symbol']} (alte Logik)")
            return
        
        # Entry Signal parsen
        entry_data = parse_entry_signal(text)
        if not entry_data:
            log.info("Kein Entry Signal erkannt")
            return
        
        symbol = entry_data["symbol"]
        side = entry_data["side"]
        entry_price = entry_data["entry"]
        tp_levels = entry_data["tp_levels"]
        
        # Guards-Validierung
        if not symbol or not side or entry_price is None or not tp_levels:
            log.warning(f"Signal unvollständig: symbol={symbol}, side={side}, entry={entry_price}, tps={tp_levels}")
            return
        
        # Mindestens TP1 muss vorhanden sein
        if 40 not in tp_levels:
            log.error(f"[{symbol}] TP1 (40%) fehlt")
            return
        
        log.info(f"[{symbol}] Signal erkannt: {side} @ {entry_price}")
        log.info(f"[{symbol}] TP-Levels: {tp_levels}")
        
        # Trade ausführen
        await execute_trade(symbol, side, entry_price, tp_levels)
        
    except Exception as e:
        log.exception(f"Fehler in on_message: {e}")

# =========================
# INITIALISIERUNG
# =========================

async def initialize_bot():
    """Initialisiert den Bot und stellt Verbindungen her."""
    log.info("=" * 50)
    log.info("BYBIT SIGNAL BOT v2.0 - START")
    log.info("=" * 50)
    
    log.info(f"Bybit API: {BYBIT_BASE_URL}")
    log.info(f"Budget pro Position: {DEFAULT_ORDER_USDT} USDT")
    log.info(f"Leverage: {DEFAULT_LEVERAGE}x")
    log.info(f"Test Mode: {TEST_MODE}")
    log.info(f"Debug Mode: {DEBUG_MODE}")
    log.info("")
    log.info("FEATURES:")
    log.info("- Hedging erlaubt (gegengesetzte Richtungen)")
    log.info("- Positionsvergrößerung bei gleicher Richtung")
    log.info("- Sequentielle TPs (30%/30%/20%/10%)")
    log.info("- Breakeven mit Fees (0.11%) nach TP1")
    log.info("- Dynamischer Trailing Stop (performance-basiert)")
    log.info("- TP-Status Updates werden ignoriert")
    log.info("=" * 50)
    
    # Telegram verbinden
    await client.start()
    me = await client.get_me()
    log.info(f"Telegram verbunden als: {me.username or me.id}")
    
    # Bybit Adapter initialisieren
    global bybit_adapter
    bybit_adapter = BybitAdapter()
    
    log.info("✅ Bot ist bereit. Warte auf Signale...")

# =========================
# HAUPTPROGRAMM
# =========================

async def main():
    """Hauptprogramm."""
    try:
        await initialize_bot()
        await client.run_until_disconnected()
        
    except KeyboardInterrupt:
        log.info("Bot manuell gestoppt")
    except Exception as e:
        log.exception(f"Kritischer Fehler: {e}")
    finally:
        # Cleanup
        log.info("Bot wird beendet...")
        for symbol, watchers in active_watchers.items():
            for side, tasks in watchers.items():
                for task_name, task in tasks.items():
                    if not task.done():
                        task.cancel()
        log.info("Bot beendet")

# =========================
# PROGRAMMSTART
# =========================

if __name__ == "__main__":
    # Globaler Bybit Adapter
    bybit_adapter = None
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot gestoppt")
    except Exception as e:
        log.exception(f"Startfehler: {e}")