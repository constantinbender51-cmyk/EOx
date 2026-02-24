#!/usr/bin/env python3
"""
PEPE Hunter Bot (Kraken Futures)
- Uses local 'kraken_futures.py' for correct API authentication.
- Logic: Sell $40/day until Day 60 OR BTC < 51k -> Then Buy phase.
- Startup: Places order immediately if not done today, then repeats at 00:00 UTC.
- Safety: Shutdown if BTC < 50.4k OR BTC > 67k.
"""

import os
import sys
import time
import logging
import json
import uuid
import requests
from datetime import datetime
from dotenv import load_dotenv

# Import the provided library
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("ERROR: 'kraken_futures.py' not found in the same directory.")
    sys.exit(1)

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

class PepeHunter:
    def __init__(self):
        if not API_KEY or not API_SEC:
            logger.error("Missing API Keys in .env")
            sys.exit(1)
            
        # Initialize the imported client
        self.api = KrakenFuturesApi(API_KEY, API_SEC)
        
        # Specs defaults
        self.tick_size = 0.000001
        self.qty_step = 1.0
        self.contract_val = 1.0
        self.is_integer_qty = True 
        
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
            # Try to get initial equity
            try:
                acc = self.api.get_accounts()
                if "accounts" in acc and "flex" in acc["accounts"]:
                    self.state["initial_equity"] = float(acc["accounts"]["flex"].get("marginEquity", 0))
            except Exception: pass
            self.save_state()

    def save_state(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            logger.error(f"Save State Failed: {e}")

    def fetch_specs(self):
        try:
            res = self.api.get_instruments()
            if "instruments" in res:
                for inst in res["instruments"]:
                    if inst["symbol"].upper() == SYMBOL_PEPE:
                        self.tick_size = float(inst.get("tickSize", 0.000001))
                        self.contract_val = float(inst.get("contractValue", 1.0))
                        
                        precision = inst.get("contractValueTradePrecision", 0)
                        self.qty_step = 10 ** (-int(precision))
                        self.is_integer_qty = (self.qty_step >= 1.0)

                        logger.info(f"SPECS | Tick: {self.tick_size} | Val: {self.contract_val} | "
                                    f"Prec: {precision} -> Step: {self.qty_step}")
                        return
        except Exception as e:
            logger.error(f"Spec Fetch Error: {e}")

    def get_prices(self):
        try:
            res = self.api.get_tickers()
            if "tickers" in res:
                for t in res["tickers"]:
                    if t["symbol"].upper() == SYMBOL_BTC:
                        self.btc_price = float(t["markPrice"])
                    if t["symbol"].upper() == SYMBOL_PEPE:
                        self.pepe_price = float(t["markPrice"])
        except Exception as e:
            logger.error(f"Get Prices Error: {e}")

    def get_account_equity(self):
        try:
            res = self.api.get_accounts()
            if "accounts" in res:
                if "flex" in res["accounts"]:
                    return float(res["accounts"]["flex"].get("marginEquity", 0))
                # Fallback
                first = list(res["accounts"].values())[0]
                return float(first.get("marginEquity", 0))
        except Exception: pass
        return 0.0

    def get_pos_size(self, symbol):
        try:
            res = self.api.get_open_positions()
            for p in res.get("openPositions", []):
                if p["symbol"].upper() == symbol.upper():
                    return float(p["size"])
        except Exception: pass
        return 0.0

    def emergency_shutdown(self, reason):
        msg = f"EMERGENCY SHUTDOWN: {reason}. Closing positions."
        logger.critical(msg)
        self.ntfy(msg)
        
        try:
            self.api.cancel_all_orders({"symbol": SYMBOL_PEPE})
            time.sleep(2)
            
            pos_size = self.get_pos_size(SYMBOL_PEPE)
            if pos_size != 0:
                side = "sell" if pos_size > 0 else "buy"
                self.api.send_order({
                    "orderType": "mkt",
                    "symbol": SYMBOL_PEPE,
                    "side": side,
                    "size": int(abs(pos_size)) if self.is_integer_qty else abs(pos_size),
                    "cliOrdId": str(uuid.uuid4())[:18]
                })
        except Exception as e:
            logger.error(f"Shutdown Exec Error: {e}")
            
        sys.exit(0)

    def calculate_qty(self, usd_amount, price):
        if price == 0 or self.contract_val == 0: return 0
        raw_qty = usd_amount / (price * self.contract_val)
        
        if self.qty_step == 0:
            final_qty = raw_qty
        else:
            steps = round(raw_qty / self.qty_step)
            final_qty = max(steps * self.qty_step, self.qty_step)
            
        return int(final_qty) if self.is_integer_qty else final_qty

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
            
            try:
                resp = self.api.send_order({
                    "orderType": "lmt",
                    "symbol": SYMBOL_PEPE,
                    "side": target_side,
                    "size": qty,
                    "limitPrice": limit_px,
                    "cliOrdId": str(uuid.uuid4())[:18]
                })
                
                if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                    self.active_order_id = resp["sendStatus"]["order_id"]
                    self.execution_start_ts = time.time()
                    self.execution_stage = "WAIT_INITIAL"
                else:
                    logger.error(f"Exec Start Failed: {resp}")
                    self.execution_active = False
            except Exception as e:
                logger.error(f"Exec API Exception: {e}")
                self.execution_active = False
                
        # 2. Wait Logic (3 Hours)
        elif self.execution_stage == "WAIT_INITIAL":
            elapsed = time.time() - self.execution_start_ts
            if elapsed > TIME_LIMIT_INITIAL:
                logger.info("EXEC: 3 Hours passed. Switching to CHASE.")
                try:
                    self.api.cancel_order({"order_id": self.active_order_id})
                except: pass
                self.execution_stage = "SETUP_CHASE"

        # 3. Setup Chase
        elif self.execution_stage == "SETUP_CHASE":
            qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
            offset = 1 + OFFSET_CHASE if target_side == "sell" else 1 - OFFSET_CHASE
            limit_px = self.round_price(self.pepe_price * offset)
            
            try:
                resp = self.api.send_order({
                    "orderType": "lmt",
                    "symbol": SYMBOL_PEPE,
                    "side": target_side,
                    "size": qty,
                    "limitPrice": limit_px,
                    "cliOrdId": str(uuid.uuid4())[:18]
                })
                if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                    self.active_order_id = resp["sendStatus"]["order_id"]
                    self.chase_last_update = time.time()
                    self.chase_start_ts = time.time()
                    self.execution_stage = "CHASE"
                else:
                    logger.warning(f"Chase Setup Failed: {resp}. Dumping to Market.")
                    self.execution_stage = "MARKET_DUMP"
            except:
                 self.execution_stage = "MARKET_DUMP"

        # 4. Chase Loop (5 Mins)
        elif self.execution_stage == "CHASE":
            total_chase_time = time.time() - self.chase_start_ts
            time_since_update = time.time() - self.chase_last_update
            
            if total_chase_time > TIME_LIMIT_CHASE:
                logger.info("EXEC: Chase timeout. Dumping to MARKET.")
                try:
                    self.api.cancel_order({"order_id": self.active_order_id})
                except: pass
                self.execution_stage = "MARKET_DUMP"
                return

            if time_since_update > CHASE_UPDATE_FREQ:
                try:
                    self.api.cancel_order({"order_id": self.active_order_id})
                except: pass
                
                offset = 1 + OFFSET_CHASE if target_side == "sell" else 1 - OFFSET_CHASE
                limit_px = self.round_price(self.pepe_price * offset)
                qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
                
                try:
                    resp = self.api.send_order({
                        "orderType": "lmt",
                        "symbol": SYMBOL_PEPE,
                        "side": target_side,
                        "size": qty,
                        "limitPrice": limit_px,
                        "cliOrdId": str(uuid.uuid4())[:18]
                    })
                    
                    if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                        self.active_order_id = resp["sendStatus"]["order_id"]
                        self.chase_last_update = time.time()
                        logger.info(f"EXEC: Chasing... {limit_px}")
                    else:
                        self.execution_stage = "MARKET_DUMP"
                except:
                     self.execution_stage = "MARKET_DUMP"

        # 5. Market Dump
        elif self.execution_stage == "MARKET_DUMP":
            qty = self.calculate_qty(DAILY_SIZE_USD, self.pepe_price)
            try:
                self.api.send_order({
                    "orderType": "mkt",
                    "symbol": SYMBOL_PEPE,
                    "side": target_side,
                    "size": qty,
                    "cliOrdId": str(uuid.uuid4())[:18]
                })
                logger.info("EXEC: Market Order Sent. Cycle Complete.")
            except Exception as e:
                logger.error(f"Market Dump Failed: {e}")
            
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
                if self.state["last_trade_date"] != current_date and not self.execution_active:
                    
                    if self.btc_price > 0 and self.pepe_price > 0:
                        
                        if self.state["phase"] == "sell" and self.state["days_active"] >= MAX_PHASE_DAYS:
                            self.state["phase"] = "buy"
                            self.ntfy("Day 60 Reached. Switching Sell -> Buy.")
                            self.save_state()
                        
                        logger.info(f"Triggering Daily Trade. Date: {current_date}")
                        self.execution_active = True
                        self.execution_stage = "START"
                        self.state["last_trade_date"] = current_date
                        self.state["days_active"] += 1
                        self.save_state()
                    else:
                        logger.warning("Waiting for price feed...")

                # 6. Run Execution Logic
                if self.execution_active:
                    self.perform_execution_logic()

                # 7. Notifications (every 6 hours)
                if utc_now.hour % 6 == 0 and utc_now.minute == 0:
                    if time.time() - self.last_notify_ts > 3600:
                        cur_eq = self.get_account_equity()
                        pnl = cur_eq - self.state["initial_equity"]
                        pos = self.get_pos_size(SYMBOL_PEPE)
                        msg = (f"REPORT | Day: {self.state['days_active']} | Phase: {self.state['phase']}\n"
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