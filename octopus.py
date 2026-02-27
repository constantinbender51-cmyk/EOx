#!/usr/bin/env python3
"""
Hourly Strategy Executor for try3btc.up.railway.app
- Scrapes the web app for targets.
- Allocates 1/16th of Capital * Leverage per asset.
- Executes position deltas using 5-min Limit Chase -> Market.
- Monitors price every minute to locally trigger and trail stop losses.
"""

import os
import sys
import time
import logging
import re
import json
import math
import threading
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("ERROR: 'kraken_futures.py' not found in the same directory.")
    sys.exit(1)

# --- Configuration ---
load_dotenv()
API_KEY = os.getenv("KRAKEN_FUTURES_KEY")
API_SEC = os.getenv("KRAKEN_FUTURES_SECRET")

WEB_APP_URL = "https://try3btc.up.railway.app"
LEVERAGE = 3.0
MAX_CHASE_MINUTES = 5
CHASE_TICK_SECONDS = 5

STATE_FILE = "executor_state.json"
LOG_FILE = "executor.log"

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AppExecutor")

# Binance Spot -> Kraken Futures Mapping
SYMBOL_MAP = {
    'BTC': 'PF_XBTUSD',
    'ETH': 'PF_ETHUSD',
    'XRP': 'PF_XRPUSD',
    'SOL': 'PF_SOLUSD',
    'DOGE': 'PF_DOGEUSD',
    'ADA': 'PF_ADAUSD',
    'BCH': 'PF_BCHUSD',
    'LINK': 'PF_LINKUSD',
    'XLM': 'PF_XLMUSD',
    'SUI': 'PF_SUIUSD',
    'AVAX': 'PF_AVAXUSD',
    'LTC': 'PF_LTCUSD',
    'HBAR': 'PF_HBARUSD', 
    'SHIB': 'PF_SHIBUSD', # Typically 1000SHIB, bot will auto-adjust if contract specs exist
    'TON': 'PF_TONUSD',
    'PEPE': 'PF_PEPEUSD'
}

class AppExecutor:
    def __init__(self):
        if not API_KEY or not API_SEC:
            logger.error("Missing Kraken API Keys in .env")
            sys.exit(1)
            
        self.api = KrakenFuturesApi(API_KEY, API_SEC)
        self.specs = {}
        self.state = {}
        
        self.load_state()
        self.fetch_specs()
        
    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    self.state = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                self.state = {}
        else:
            self.state = {}

    def save_state(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            logger.error(f"Save State Failed: {e}")

    def fetch_specs(self):
        logger.info("Fetching Instrument Specs...")
        try:
            res = self.api.get_instruments()
            if "instruments" in res:
                for inst in res["instruments"]:
                    sym = inst["symbol"].upper()
                    self.specs[sym] = {
                        "tickSize": float(inst.get("tickSize", 0.000001)),
                        "contractValue": float(inst.get("contractValue", 1.0)),
                        "precision": int(inst.get("contractValueTradePrecision", 0)),
                        "qty_step": 10 ** (-int(inst.get("contractValueTradePrecision", 0)))
                    }
        except Exception as e:
            logger.error(f"Spec Fetch Error: {e}")

    def get_account_equity(self):
        try:
            res = self.api.get_accounts()
            if "accounts" in res and "flex" in res["accounts"]:
                return float(res["accounts"]["flex"].get("marginEquity", 0))
        except Exception as e:
            logger.error(f"Equity Fetch Error: {e}")
        return 0.0

    def get_current_prices(self):
        prices = {}
        try:
            res = self.api.get_tickers()
            if "tickers" in res:
                for t in res["tickers"]:
                    prices[t["symbol"].upper()] = float(t["markPrice"])
        except Exception as e:
            logger.error(f"Get Prices Error: {e}")
        return prices

    def get_open_positions(self):
        positions = {}
        try:
            res = self.api.get_open_positions()
            for p in res.get("openPositions", []):
                positions[p["symbol"].upper()] = float(p["size"])
        except Exception as e:
            logger.error(f"Get Positions Error: {e}")
        return positions

    def cancel_symbol_orders(self, symbol):
        try:
            self.api.cancel_all_orders({"symbol": symbol})
        except Exception as e:
            logger.error(f"Cancel Orders Error for {symbol}: {e}")

    def scrape_web_app(self):
        logger.info(f"Scraping targets from {WEB_APP_URL}...")
        targets = {}
        try:
            resp = requests.get(WEB_APP_URL, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            rows = soup.find_all('tr')
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 7 and cols[0].text.strip() in SYMBOL_MAP:
                    base_sym = cols[0].text.strip()
                    k_sym = SYMBOL_MAP[base_sym]
                    status = cols[1].text.strip().upper()
                    
                    try:
                        ep_text = cols[3].text.replace('$', '').replace(',', '').strip()
                        ep = float(ep_text) if ep_text != '-' else 0.0
                    except: ep = 0.0
                    
                    params_text = cols[6].text
                    c_match = re.search(r'c:([\d.]+)%', params_text)
                    sl_match = re.search(r'sl:([\d.]+)%', params_text)
                    
                    tsl_pct = float(c_match.group(1))/100.0 if c_match else 0.02
                    sl_pct = float(sl_match.group(1))/100.0 if sl_match else 0.02
                    
                    target_dir = 0
                    if "ACTIVE LONG" in status: target_dir = 1
                    elif "ACTIVE SHORT" in status: target_dir = -1
                    
                    targets[k_sym] = {
                        "direction": target_dir,
                        "ep": ep,
                        "tsl_pct": tsl_pct,
                        "sl_pct": sl_pct
                    }
        except Exception as e:
            logger.error(f"Scraper Error: {e}")
            
        return targets

    def calculate_qty(self, symbol, target_usd, price):
        if symbol not in self.specs or price == 0: return 0.0
        c_val = self.specs[symbol]["contractValue"]
        q_step = max(self.specs[symbol]["qty_step"], 1.0)
        
        raw_qty = target_usd / (price * c_val)
        final_qty = max(round(raw_qty / q_step) * q_step, q_step)
        
        if self.specs[symbol]["qty_step"] >= 1.0:
            return int(final_qty)
        return final_qty

    def round_price(self, symbol, price):
        if symbol not in self.specs: return price
        ts = self.specs[symbol]["tickSize"]
        return round(price / ts) * ts

    def execute_chase(self, symbol, target_size):
        """Places a Limit order, adjusts it for 5 mins to catch Mark Price, then Markets."""
        start_time = time.time()
        logger.info(f"[{symbol}] Starting Chase execution to target size: {target_size}")
        
        while time.time() - start_time < (MAX_CHASE_MINUTES * 60):
            current_pos = self.get_open_positions().get(symbol, 0.0)
            delta = target_size - current_pos
            
            # If we are within 1 qty_step of target, consider it filled
            q_step = self.specs.get(symbol, {}).get("qty_step", 1.0)
            if abs(delta) < q_step:
                logger.info(f"[{symbol}] Chase filled target. Current Pos: {current_pos}")
                return current_pos
                
            side = "buy" if delta > 0 else "sell"
            size = abs(delta)
            
            # Get latest price to peg limit order
            prices = self.get_current_prices()
            px = prices.get(symbol)
            if not px:
                time.sleep(5)
                continue
                
            limit_px = self.round_price(symbol, px)
            
            self.cancel_symbol_orders(symbol) # Clear existing chase limits
            
            try:
                self.api.send_order({
                    "orderType": "lmt",
                    "symbol": symbol,
                    "side": side,
                    "size": size,
                    "limitPrice": limit_px
                })
            except Exception as e:
                logger.error(f"[{symbol}] Chase Lmt Error: {e}")
                
            time.sleep(CHASE_TICK_SECONDS)
            
        # 5 mins passed, dump remaining to market
        current_pos = self.get_open_positions().get(symbol, 0.0)
        delta = target_size - current_pos
        if abs(delta) >= self.specs.get(symbol, {}).get("qty_step", 1.0):
            side = "buy" if delta > 0 else "sell"
            logger.warning(f"[{symbol}] Chase Timeout! Sending MARKET for {abs(delta)} ({side})")
            self.cancel_symbol_orders(symbol)
            try:
                self.api.send_order({
                    "orderType": "mkt",
                    "symbol": symbol,
                    "side": side,
                    "size": abs(delta)
                })
            except Exception as e:
                logger.error(f"[{symbol}] Market Dump Error: {e}")
                
        return self.get_open_positions().get(symbol, 0.0)

    def place_stop_loss(self, symbol, position, target_data):
        """Places the initial Stop Market order."""
        if position == 0: return
        
        ep = target_data["ep"]
        sl_pct = target_data["sl_pct"]
        side = "sell" if position > 0 else "buy"
        
        raw_sl = ep * (1 - sl_pct) if position > 0 else ep * (1 + sl_pct)
        stop_px = self.round_price(symbol, raw_sl)
        
        try:
            resp = self.api.send_order({
                "orderType": "stp",
                "symbol": symbol,
                "side": side,
                "size": abs(position),
                "stopPrice": stop_px
            })
            if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                order_id = resp["sendStatus"]["order_id"]
                
                # Setup local tracking state
                self.state[symbol] = {
                    "pos": position,
                    "ep": ep,
                    "highest": ep,
                    "lowest": ep,
                    "act_price": ep * (1 + target_data["tsl_pct"]) if position > 0 else ep * (1 - target_data["tsl_pct"]),
                    "tsl_pct": target_data["tsl_pct"],
                    "current_sl_price": stop_px,
                    "sl_order_id": order_id
                }
                self.save_state()
                logger.info(f"[{symbol}] Initial SL placed at {stop_px}. ID: {order_id}")
        except Exception as e:
            logger.error(f"[{symbol}] SL Placement Error: {e}")

    def hourly_sync(self):
        """Scrapes app, calculates deltas, executes orders."""
        logger.info("=== STARTING HOURLY SYNC ===")
        targets = self.scrape_web_app()
        if not targets:
            logger.warning("No targets scraped. Aborting sync.")
            return

        equity = self.get_account_equity()
        if equity <= 0:
            logger.error("Equity is 0 or failed to fetch. Aborting.")
            return
            
        usd_per_asset = (equity / 16.0) * LEVERAGE
        logger.info(f"Equity: ${equity:.2f} | Allocation per asset: ${usd_per_asset:.2f} (Lev: {LEVERAGE}x)")
        
        prices = self.get_current_prices()
        current_positions = self.get_open_positions()
        
        threads = []
        
        for symbol, t_data in targets.items():
            if symbol not in self.specs: continue
            
            px = prices.get(symbol)
            if not px: continue
            
            target_qty = self.calculate_qty(symbol, usd_per_asset, px) * t_data["direction"]
            current_qty = current_positions.get(symbol, 0.0)
            
            q_step = self.specs[symbol]["qty_step"]
            delta = target_qty - current_qty
            
            # If position changed or needs closing
            if abs(delta) >= max(q_step, 1.0) or target_qty == 0 and current_qty != 0:
                logger.info(f"[{symbol}] Delta detected! Target: {target_qty} | Current: {current_qty}")
                
                # Clear tracking state and exchange stops before execution
                if symbol in self.state:
                    del self.state[symbol]
                    self.save_state()
                self.cancel_symbol_orders(symbol)
                
                # Spin up chase execution thread to prevent blocking other assets
                def run_trade(sym, t_qty, t_dta):
                    final_pos = self.execute_chase(sym, t_qty)
                    if final_pos != 0:
                        self.place_stop_loss(sym, final_pos, t_dta)
                
                t = threading.Thread(target=run_trade, args=(symbol, target_qty, t_data))
                t.start()
                threads.append(t)
                
        # Wait for all executions to finish
        for t in threads:
            t.join()
            
        logger.info("=== HOURLY SYNC COMPLETE ===")

    def minute_sl_monitor(self):
        """Locally monitors high/lows and trails the Kraken stop loss via edit_order."""
        if not self.state: return
        
        prices = self.get_current_prices()
        
        for symbol, track in list(self.state.items()):
            px = prices.get(symbol)
            if not px: continue
            
            pos = track["pos"]
            act_price = track["act_price"]
            tsl_pct = track["tsl_pct"]
            sl_id = track.get("sl_order_id")
            
            needs_update = False
            new_stop_px = track["current_sl_price"]
            
            if pos > 0:
                if px > track["highest"]:
                    track["highest"] = px
                    if px >= act_price:
                        calc_sl = self.round_price(symbol, px * (1 - tsl_pct))
                        if calc_sl > track["current_sl_price"]:
                            new_stop_px = calc_sl
                            needs_update = True
                            
            elif pos < 0:
                if px < track["lowest"]:
                    track["lowest"] = px
                    if px <= act_price:
                        calc_sl = self.round_price(symbol, px * (1 + tsl_pct))
                        if calc_sl < track["current_sl_price"]:
                            new_stop_px = calc_sl
                            needs_update = True
                            
            if needs_update and sl_id:
                try:
                    resp = self.api.edit_order({
                        "orderId": sl_id,
                        "stopPrice": new_stop_px
                    })
                    if "editStatus" in resp and resp["editStatus"].get("status") == "edited":
                        # Updated successfully or received new ID
                        new_id = resp["editStatus"].get("orderId", sl_id)
                        track["current_sl_price"] = new_stop_px
                        track["sl_order_id"] = new_id
                        self.save_state()
                        logger.info(f"[{symbol}] Trailed SL updated to {new_stop_px}. ID: {new_id}")
                    else:
                        logger.warning(f"[{symbol}] SL Edit failed (might have triggered): {resp}")
                        # If failed, the order might be gone (filled). We should let the hourly sync clean it up, 
                        # but we can pop it locally so we don't spam API requests.
                        self.state.pop(symbol, None)
                        self.save_state()
                        
                except Exception as e:
                    logger.error(f"[{symbol}] Edit Order Exception: {e}")

    def run_forever(self):
        logger.info("Executor Bot Started.")
        last_hour = -1
        
        while True:
            try:
                now = datetime.utcnow()
                
                # 1. Run Hourly Sync precisely at minute 1 to allow web app to update
                if now.minute == 1 and now.hour != last_hour:
                    self.hourly_sync()
                    last_hour = now.hour
                
                # 2. Run SL monitor the rest of the time
                else:
                    self.minute_sl_monitor()
                    
                time.sleep(60) # Wake up every minute
                
            except KeyboardInterrupt:
                logger.info("Bot Shutdown manually.")
                break
            except Exception as e:
                logger.error(f"Main Loop Error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    executor = AppExecutor()
    executor.run_forever()