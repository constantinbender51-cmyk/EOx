#!/usr/bin/env python3
"""
PEPE Hunter Bot (Kraken Futures)
- Logic: Sell $40/day until Day 60 OR BTC < 51k -> Then Buy phase.
- Startup: Places order immediately if not done today, then repeats at 00:00 UTC.
- Safety: Shutdown if BTC < 50.4k OR BTC > 67k.
- Execution: Limit (1% offset, 3hr wait) -> Chase (0.01% offset, 5min duration) -> Market.
"""

import os
import sys
import time
import logging
import requests
import json
import hmac
import hashlib
import base64
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

API_KEY = os.getenv("KEY")
API_SEC = os.getenv("SECRET")
NTFY_TOPIC = os.getenv("NTFY_TOPIC")

# Symbols
SYMBOL_PEPE = "PF_PEPEUSD" 
SYMBOL_BTC = "PF_XBTUSD"

# Logic Settings
DAILY_SIZE_USD = 40.0
BTC_LO_SHUTDOWN = 50400.0
BTC_HI_SHUTDOWN = 67000.0
BTC_PHASE_SWITCH = 51000.0
BTC_NOTIFY_LEVEL = 51400.0
MAX_PHASE_DAYS = 60

# Execution Settings
OFFSET_INITIAL = 0.01    # 1%
OFFSET_CHASE = 0.0001    # 0.01%
TIME_LIMIT_INITIAL = 3 * 60 * 60 # 3 hours
TIME_LIMIT_CHASE = 5 * 60        # 5 minutes
CHASE_UPDATE_FREQ = 5            # 5 seconds

STATE_FILE = "pepe_state.json"
LOG_FILE = "pepe_hunter.log"

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PepeBot")

class KrakenFuturesMinimal:
    """Minimal robust wrapper for Kraken Futures V3"""
    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self.url = "https://futures.kraken.com"

    def _sign(self, endpoint, post_data=""):
        if endpoint.startswith("/derivatives"): endpoint = endpoint[12:]
        postdata = post_data + endpoint
        encoded = (postdata).encode('utf-8')
        message = hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(self.secret), message, hashlib.sha512)
        return base64.b64encode(signature.digest()).decode()

    def request(self, method, endpoint, params=None):
        try:
            full_url = self.url + endpoint
            headers = {"APIKey": self.key, "Authent": self._sign(endpoint, "")}
            
            if method == "GET":
                if params:
                    q = urllib.parse.urlencode(params)
                    full_url += f"?{q}"
                    headers["Authent"] = self._sign(endpoint + "?" + q, "")
                resp = requests.get(full_url, headers=headers, timeout=10)
            else:
                json_str = json.dumps(params) if params else ""
                headers["Authent"] = self._sign(endpoint, json_str)
                headers["Content-Type"] = "application/json"
                resp = requests.post(full_url, headers=headers, data=json_str, timeout=10)

            if resp.status_code >= 400:
                logger.error(f"API Error {resp.status_code}: {resp.text}")
                return {"error": resp.text}
            return resp.json()
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return {"error": str(e)}

    def get_tickers(self):
        return self.request("GET", "/derivatives/api/v3/tickers")

    def get_equity(self):
        res = self.request("GET", "/derivatives/api/v3/accounts")
        try:
            if "accounts" in res:
                flex = res["accounts"].get("flex", {})
                return float(flex.get("marginEquity", 0))
            first_acc = list(res.get("accounts", {}).values())[0]
            return float(first_acc.get("marginEquity", 0))
        except: pass
        return 0.0

    def get_position(self, symbol):
        res = self.request("GET", "/derivatives/api/v3/openpositions")
        try:
            for p in res.get("openPositions", []):
                if p["symbol"].upper() == symbol.upper():
                    return float(p["size"])
        except: pass
        return 0.0

    def cancel_all(self, symbol):
        return self.request("POST", "/derivatives/api/v3/cancelallorders", {"symbol": symbol})

    def cancel_order(self, order_id):
        return self.request("POST", "/derivatives/api/v3/cancelorder", {"order_id": order_id})

    def send_order(self, payload):
        return self.request("POST", "/derivatives/api/v3/sendorder", payload)

class PepeHunter:
    def __init__(self):
        if not API_KEY or not API_SEC:
            logger.error("Missing API Keys")
            sys.exit(1)
            
        self.api = KrakenFuturesMinimal(API_KEY, API_SEC)
        
        self.tick_size = 0.000001
        self.qty_step = 1.0
        self.contract_val = 1.0
        
        self.load_state()
        self.fetch_specs()
        
        # Runtime variables
        self.btc_price = 0.0
        self.pepe_price = 0.0
        self.last_notify_ts = 0
        self.execution_active = False
        self.execution_start_ts = 0
        self.execution_stage = "IDLE" 
        self.active_order_id = None
        self.chase_last_update = 0
        self.chase_start_ts = 0

    def ntfy(self, message):
        logger.info(f"NOTIFY: {message}")
        if NTFY_TOPIC:
            try:
                requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", 
                              data=message.encode('utf-8'),
                              headers={"Title": "PEPE Bot Update"})
            except Exception as e:
                logger.error(f"Ntfy failed: {e}")

    def load_state(self):
        default_state = {
            "inception_ts": time.time(),
            "phase": "sell", 
            "days_active": 0,
            "last_trade_date": "",
            "initial_equity": 0.0,
            "notified_51400": False
        }
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    self.state = json.load(f)
            except:
                self.state = default_state
        else:
            self.state = default_state
            self.state["initial_equity"] = self.api.get_equity()
            self.save_state()

    def save_state(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            logger.error(f"Save State Failed: {e}")

    def fetch_specs(self):
        res = self.api.request("GET", "/derivatives/api/v3/instruments")
        if "instruments" in res:
            for inst in res["instruments"]:
                if inst["symbol"].upper() == SYMBOL_PEPE:
                    self.tick_size = float(inst.get("tickSize", 0.000001))
                    self.contract_val = float(inst.get("contractValue", 1.0))
                    
                    # Precision Handling (Negative means large integer steps)
                    precision = inst.get("contractValueTradePrecision", 0)
                    self.qty_step = 10 ** (-int(precision))
                    
                    logger.info(f"SPECS | Tick: {self.tick_size} | Val: {self.contract_val} | "
                                f"Prec: {precision} -> Step: {self.qty_step}")
                    return

    def get_prices(self):
        tickers = self.api.get_tickers()
        if "tickers" in tickers:
            for t in tickers["tickers"]:
                if t["symbol"].upper() == SYMBOL_BTC:
                    self.btc_price = float(t["markPrice"])
                if t["symbol"].upper() == SYMBOL_PEPE:
                    self.pepe_price = float(t["markPrice"])

    def emergency_shutdown(self, reason):
        msg = f"EMERGENCY SHUTDOWN: {reason}. Closing positions."
        logger.critical(msg)
        self.ntfy(msg)
        
        self.api.cancel_all(SYMBOL_PEPE)
        time.sleep(2)
        
        pos_size = self.api.get_position(SYMBOL_PEPE)
        if pos_size != 0:
            side = "sell" if pos_size > 0 else "buy"
            self.api.send_order({
                "orderType": "mkt",
                "symbol": SYMBOL_PEPE,
                "side": side,
                "size": abs(pos_size)
            })
            
        sys.exit(0)

    def round_qty(self, raw_qty):
        if self.qty_step == 0: return raw_qty
        steps = round(raw_qty / self.qty_step)
        rounded = steps * self.qty_step
        return max(rounded, self.qty_step)

    def calculate_qty(self, usd_amount, price):
        if price == 0 or self.contract_val == 0: return 0
        raw_qty = usd_amount / (price * self.contract_val)
        return self.round_qty(raw_qty)

    def round_price(self, price):
        steps = round(price / self.tick_size)
        return steps * self.tick_size

    def perform_execution_logic(self):
        target_side = "sell" if self.state["phase"] == "sell" else "buy"
        
        # 1. Start Logic
        if self.execution_stage == "START":
            qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
            if qty <= 0: 
                logger.warning("Calculated Qty is 0. Skipping.")
                self.execution_active = False
                return

            offset = 1 + OFFSET_INITIAL if target_side == "sell" else 1 - OFFSET_INITIAL
            limit_px = self.round_price(self.pepe_price * offset)
            
            logger.info(f"EXEC: Placing Initial {target_side.upper()} {qty} @ {limit_px}")
            
            resp = self.api.send_order({
                "orderType": "lmt",
                "symbol": SYMBOL_PEPE,
                "side": target_side,
                "size": qty,
                "limitPrice": limit_px
            })
            
            if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                self.active_order_id = resp["sendStatus"]["order_id"]
                self.execution_start_ts = time.time()
                self.execution_stage = "WAIT_INITIAL"
            else:
                logger.error(f"Exec Start Failed: {resp}")
                self.execution_active = False # Stop on API error to prevent loop spam
                
        # 2. Wait Logic (3 Hours)
        elif self.execution_stage == "WAIT_INITIAL":
            elapsed = time.time() - self.execution_start_ts
            if elapsed > TIME_LIMIT_INITIAL:
                logger.info("EXEC: 3 Hours passed. Switching to CHASE.")
                self.api.cancel_order(self.active_order_id)
                self.execution_stage = "SETUP_CHASE"

        # 3. Setup Chase
        elif self.execution_stage == "SETUP_CHASE":
            qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
            offset = 1 + OFFSET_CHASE if target_side == "sell" else 1 - OFFSET_CHASE
            limit_px = self.round_price(self.pepe_price * offset)
            
            resp = self.api.send_order({
                "orderType": "lmt",
                "symbol": SYMBOL_PEPE,
                "side": target_side,
                "size": qty,
                "limitPrice": limit_px
            })
            if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                self.active_order_id = resp["sendStatus"]["order_id"]
                self.chase_last_update = time.time()
                self.chase_start_ts = time.time()
                self.execution_stage = "CHASE"
            else:
                logger.warning("Chase Setup Failed. Dumping to Market.")
                self.execution_stage = "MARKET_DUMP"

        # 4. Chase Loop (5 Mins)
        elif self.execution_stage == "CHASE":
            total_chase_time = time.time() - self.chase_start_ts
            time_since_update = time.time() - self.chase_last_update
            
            if total_chase_time > TIME_LIMIT_CHASE:
                logger.info("EXEC: Chase timeout. Dumping to MARKET.")
                self.api.cancel_order(self.active_order_id)
                self.execution_stage = "MARKET_DUMP"
                return

            if time_since_update > CHASE_UPDATE_FREQ:
                self.api.cancel_order(self.active_order_id)
                offset = 1 + OFFSET_CHASE if target_side == "sell" else 1 - OFFSET_CHASE
                limit_px = self.round_price(self.pepe_price * offset)
                qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
                
                resp = self.api.send_order({
                    "orderType": "lmt",
                    "symbol": SYMBOL_PEPE,
                    "side": target_side,
                    "size": qty,
                    "limitPrice": limit_px
                })
                
                if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                    self.active_order_id = resp["sendStatus"]["order_id"]
                    self.chase_last_update = time.time()
                    logger.info(f"EXEC: Chasing... {limit_px}")
                else:
                    self.execution_stage = "MARKET_DUMP"

        # 5. Market Dump
        elif self.execution_stage == "MARKET_DUMP":
            qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
            self.api.send_order({
                "orderType": "mkt",
                "symbol": SYMBOL_PEPE,
                "side": target_side,
                "size": qty
            })
            logger.info("EXEC: Market Order Sent. Cycle Complete.")
            self.execution_active = False
            self.execution_stage = "IDLE"

    def run(self):
        logger.info("--- PEPE Hunter Started ---")
        self.ntfy(f"Bot Started. Phase: {self.state['phase']}")
        self.fetch_specs()

        while True:
            try:
                # 1. Update Prices
                self.get_prices()
                utc_now = datetime.utcnow()
                current_date = utc_now.strftime("%Y-%m-%d")

                # 2. Safety Checks
                if self.btc_price < BTC_LO_SHUTDOWN and self.btc_price > 0:
                    self.emergency_shutdown(f"BTC {self.btc_price} < {BTC_LO_SHUTDOWN}")
                
                if self.btc_price > BTC_HI_SHUTDOWN:
                    self.emergency_shutdown(f"BTC {self.btc_price} > {BTC_HI_SHUTDOWN}")

                # 3. Alerts
                if self.btc_price < BTC_NOTIFY_LEVEL and not self.state["notified_51400"] and self.btc_price > 0:
                    self.ntfy(f"BTC Breach Alert: {self.btc_price} < {BTC_NOTIFY_LEVEL}")
                    self.state["notified_51400"] = True
                    self.save_state()

                # 4. Phase Switching
                if self.state["phase"] == "sell" and self.btc_price < BTC_PHASE_SWITCH and self.btc_price > 0:
                    self.ntfy(f"PHASE SWITCH: Selling -> Buying (BTC {self.btc_price})")
                    self.state["phase"] = "buy"
                    self.save_state()

                # 5. Execution Check (Startup OR New Day)
                # If we haven't traded for the current UTC date, trigger immediately
                if self.state["last_trade_date"] != current_date and not self.execution_active:
                    
                    # Ensure we have valid prices before starting
                    if self.btc_price > 0 and self.pepe_price > 0:
                        
                        # Phase Limits
                        if self.state["phase"] == "sell" and self.state["days_active"] >= MAX_PHASE_DAYS:
                            self.state["phase"] = "buy"
                            self.ntfy("Day 60 Reached. Switching Sell -> Buy.")
                            self.save_state()
                        
                        logger.info(f"Triggering Daily Trade. Date: {current_date} (Last: {self.state['last_trade_date']})")
                        
                        self.execution_active = True
                        self.execution_stage = "START"
                        self.state["last_trade_date"] = current_date
                        self.state["days_active"] += 1
                        self.save_state()
                    else:
                        logger.warning("Prices not yet available, waiting...")

                # 6. Run Execution Logic
                if self.execution_active:
                    self.perform_execution_logic()

                # 7. Notifications (every 6 hours)
                if utc_now.hour % 6 == 0 and utc_now.minute == 0:
                    if time.time() - self.last_notify_ts > 3600:
                        cur_eq = self.api.get_equity()
                        pnl = cur_eq - self.state["initial_equity"]
                        pos = self.api.get_position(SYMBOL_PEPE)
                        msg = (f"REPORT | Day: {self.state["days_active"]} | Phase: {self.state["phase"]}\n"
                               f"PnL: ${pnl:.2f} | Pos: {pos} | BTC: {self.btc_price}")
                        self.ntfy(msg)
                        self.last_notify_ts = time.time()

                time.sleep(5)

            except KeyboardInterrupt:
                logger.info("Manual Stop.")
                sys.exit(0)
            except Exception as e:
                logger.error(f"Loop Error: {e}")
                time.sleep(30)

if __name__ == "__main__":
    bot = PepeHunter()
    bot.run()