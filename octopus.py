#!/usr/bin/env python3
"""
Equal Opportunity: Dual Account Grid System
- Logic: Long 60-64k / Short 66-70k.
- Sizing: Min(Equity1, Equity2) / 10 per level.
- Integrity: Verifies Order IDs, Exact Prices, and Size Tolerance (>5%).
"""

import os
import sys
import time
import logging
import requests
import json
from kraken_futures import KrakenFuturesApi
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

KEYS = {
    "LONG": {
        "key": os.getenv("KEY1"),
        "secret": os.getenv("KEY1SEC"),
        "levels": [60000, 61000, 62000, 63000, 64000],
        "stop": 59000,
        "side": "buy",
        "stop_side": "sell"
    },
    "SHORT": {
        "key": os.getenv("KEY2"),
        "secret": os.getenv("KEY2SEC"),
        "levels": [66000, 67000, 68000, 69000, 70000],
        "stop": 71000,
        "side": "sell",
        "stop_side": "buy"
    }
}

SYMBOL = "FF_XBTUSD_260227".upper()
UPDATE_INTERVAL = 600
STATE_FILE = "order_state.json"
SIZE_TOLERANCE = 0.05  # Reset if size drifts > 5% from ideal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("equal_opportunity.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("EqualOpp")

class EqualOpportunityBot:
    def __init__(self):
        self.clients = {}
        for name, creds in KEYS.items():
            if not creds["key"] or not creds["secret"]:
                logger.error(f"Missing keys for {name} account.")
                sys.exit(1)
            self.clients[name] = KrakenFuturesApi(creds["key"], creds["secret"])
        
        self.state = self.load_state()
        
        # Default Specs (Updated on start)
        self.tick_size = 0.5
        self.qty_step = 0.0001
        self.min_qty = 0.0001
        self.fetch_specs()

    def fetch_specs(self):
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    if inst["symbol"].upper() == SYMBOL:
                        self.tick_size = float(inst.get("tickSize", 0.5))
                        precision = inst.get("contractValueTradePrecision", 3)
                        self.qty_step = 10 ** (-int(precision))
                        self.min_qty = self.qty_step
                        logger.info(f"SPECS | Tick: {self.tick_size} | QtyStep: {self.qty_step}")
                        return
        except Exception as e:
            logger.warning(f"Spec Fetch Failed, using defaults: {e}")

    # --- Persistence ---
    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save_state(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            logger.error(f"State Save Failed: {e}")

    # --- Utils ---
    def round_price(self, price):
        steps = round(price / self.tick_size)
        return steps * self.tick_size

    def round_qty(self, qty):
        steps = round(qty / self.qty_step)
        rounded = steps * self.qty_step
        return max(rounded, self.min_qty)

    def get_equity(self, client):
        try:
            acc = client.get_accounts()
            if "flex" in acc.get("accounts", {}):
                return float(acc["accounts"]["flex"].get("marginEquity", 0))
            first = list(acc.get("accounts", {}).values())[0]
            return float(first.get("marginEquity", 0))
        except Exception:
            return 0.0

    def cancel_all(self, client):
        try:
            client.cancel_all_orders(SYMBOL)
            logger.info("Orders flushed.")
        except Exception as e:
            logger.error(f"Cancel Failed: {e}")

    # --- Core Logic ---
    def place_grid(self, name, client, config, base_equity):
        if base_equity <= 0: return

        base_value = base_equity / 10.0
        total_qty = 0.0
        new_orders_state = []
        orders_payload = []

        # 1. Prepare Limit Orders
        for raw_price in config["levels"]:
            price = self.round_price(raw_price)
            qty = self.round_qty(base_value / price)
            total_qty += qty
            
            orders_payload.append({
                "orderType": "lmt",
                "symbol": SYMBOL,
                "side": config["side"],
                "size": qty,
                "limitPrice": price,
                "meta_type": "limit"
            })

        # 2. Prepare Stop Loss
        stop_price = self.round_price(config["stop"])
        stop_qty = self.round_qty(total_qty)
        orders_payload.append({
            "orderType": "stp",
            "symbol": SYMBOL,
            "side": config["stop_side"],
            "size": stop_qty,
            "stopPrice": stop_price,
            "reduceOnly": True,
            "meta_type": "stop"
        })

        # 3. Execute & Record IDs
        logger.info(f"{name} | Placing {len(orders_payload)} orders (Eq: {base_equity:.2f})")
        
        for order in orders_payload:
            try:
                meta_type = order.pop("meta_type")
                resp = client.send_order(order)
                
                if "sendStatus" in resp:
                    status = resp["sendStatus"]
                    if "order_id" in status:
                        # Save State with exact values sent
                        new_orders_state.append({
                            "id": status["order_id"],
                            "type": meta_type,
                            "price": order.get("limitPrice", order.get("stopPrice")),
                            "size": order["size"]
                        })
                    else:
                        logger.error(f"Order Rejected: {status}")
            except Exception as e:
                logger.error(f"Order Excep: {e}")

        # 4. Update State
        self.state[name] = new_orders_state
        self.save_state()

    def check_grid_integrity(self, name, open_orders, min_equity, config):
        """
        1. Checks if saved IDs exist on exchange.
        2. Checks if live price == saved price (Exact).
        3. Checks if live size == ideal size (Tolerance 5%).
        """
        saved_orders = self.state.get(name, [])
        if not saved_orders:
            logger.info(f"{name}: No saved state found.")
            return False

        # Map Live Orders by ID
        live_map = {}
        if "openOrders" in open_orders:
            for o in open_orders["openOrders"]:
                live_map[o["order_id"]] = o

        # Recalculate Ideal Sizing
        base_value = min_equity / 10.0
        total_ideal_qty = 0.0

        # --- LIMIT ORDERS CHECK ---
        for saved in saved_orders:
            if saved["type"] == "limit":
                oid = saved["id"]
                
                # 1. Existence Check
                if oid not in live_map:
                    logger.info(f"{name}: Limit {oid} missing (Filled/Closed). Resetting.")
                    return False
                
                live_order = live_map[oid]
                saved_price = float(saved["price"])
                live_price = float(live_order.get("limitPrice", 0))
                
                # 2. Price Check (Exact)
                if live_price != saved_price:
                    logger.info(f"{name}: Price Mismatch. Live: {live_price}, Saved: {saved_price}. Resetting.")
                    return False

                # 3. Size Check (Drift)
                ideal_qty = self.round_qty(base_value / saved_price)
                total_ideal_qty += ideal_qty
                
                current_qty = float(live_order["size"])
                if abs(current_qty - ideal_qty) / current_qty > SIZE_TOLERANCE:
                    logger.info(f"{name}: Limit Size Drift > 5%. Live: {current_qty}, Ideal: {ideal_qty}. Resetting.")
                    return False

        # --- STOP LOSS CHECK ---
        for saved in saved_orders:
            if saved["type"] == "stop":
                oid = saved["id"]
                
                if oid not in live_map:
                    logger.info(f"{name}: Stop {oid} missing. Resetting.")
                    return False
                
                live_order = live_map[oid]
                saved_price = float(saved["price"])
                live_price = float(live_order.get("stopPrice", 0))

                # Price Check
                if live_price != saved_price:
                    logger.info(f"{name}: Stop Price Mismatch. Live: {live_price}, Saved: {saved_price}. Resetting.")
                    return False
                
                # Size Check (Total Aggregated)
                ideal_stop_qty = self.round_qty(total_ideal_qty)
                current_stop_qty = float(live_order["size"])
                
                if abs(current_stop_qty - ideal_stop_qty) / current_stop_qty > SIZE_TOLERANCE:
                    logger.info(f"{name}: Stop Size Drift. Live: {current_stop_qty}, Ideal: {ideal_stop_qty}. Resetting.")
                    return False

        return True

    def run(self):
        logger.info(f"Engine Started. Symbol: {SYMBOL}")
        
        while True:
            eq_long = self.get_equity(self.clients["LONG"])
            eq_short = self.get_equity(self.clients["SHORT"])
            min_equity = min(eq_long, eq_short)
            
            logger.info(f"Equities | LONG: {eq_long:.2f} | SHORT: {eq_short:.2f} | MIN: {min_equity:.2f}")

            for name, config in KEYS.items():
                client = self.clients[name]
                try:
                    open_orders = client.get_open_orders()
                    
                    if self.check_grid_integrity(name, open_orders, min_equity, config):
                        logger.info(f"{name}: Grid Valid & Healthy.")
                    else:
                        logger.info(f"{name}: Grid Invalid/Drifted. Rebuilding.")
                        
                        # Safety: Cancel existing before placing new
                        count = 0
                        if "openOrders" in open_orders:
                            count = sum(1 for o in open_orders["openOrders"] if o["symbol"].upper() == SYMBOL)
                        
                        if count > 0:
                            self.cancel_all(client)
                        
                        self.state[name] = [] # Clear old state
                        self.place_grid(name, client, config, min_equity)

                except Exception as e:
                    logger.error(f"{name} Loop Error: {e}")

            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    bot = EqualOpportunityBot()
    bot.run()
