#!/usr/bin/env python3
"""
Equal Opportunity: Dual Account Grid System
- Account 1 (Long): Buys 60-64k, Stop 59k.
- Account 2 (Short): Sells 66-70k, Stop 71k.
- Symbol: FF_XBTUSD_260227
- Sizing: Min(Equity1, Equity2) / 10 per level.
- Interval: 10 Minutes
- Fix: Dynamic Precision (tickSize & tradePrecision)
"""

import os
import sys
import time
import logging
import requests
import math
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
        
        # Default Specs (Updated on start)
        self.tick_size = 0.5
        self.qty_step = 0.0001
        self.min_qty = 0.0001
        self.fetch_specs()

    def fetch_specs(self):
        """Fetch instrument precision settings to avoid invalidSize/Price errors."""
        try:
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            resp = requests.get(url).json()
            if "instruments" in resp:
                for inst in resp["instruments"]:
                    if inst["symbol"].upper() == SYMBOL:
                        # Price Step
                        self.tick_size = float(inst.get("tickSize", 0.5))
                        
                        # Quantity Step (Precision)
                        precision = inst.get("contractValueTradePrecision", 3)
                        self.qty_step = 10 ** (-int(precision))
                        
                        # Min Order Size (Sometimes in separate field, safe fallback to step)
                        self.min_qty = self.qty_step
                        
                        logger.info(f"SPECS | Tick: {self.tick_size} | QtyStep: {self.qty_step}")
                        return
        except Exception as e:
            logger.warning(f"Spec Fetch Failed, using defaults: {e}")

    def round_price(self, price):
        """Round price to nearest tickSize."""
        steps = round(price / self.tick_size)
        return steps * self.tick_size

    def round_qty(self, qty):
        """Round quantity to contractValueTradePrecision."""
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
        except Exception as e:
            logger.error(f"Equity Fetch Fail: {e}")
            return 0.0

    def cancel_all(self, client):
        try:
            client.cancel_all_orders(SYMBOL)
            logger.info("Orders flushed.")
        except Exception as e:
            logger.error(f"Cancel Failed: {e}")

    def place_grid(self, name, client, config, base_equity):
        if base_equity <= 0:
            logger.error(f"{name}: Base Equity 0. Skipping.")
            return

        base_value = base_equity / 10.0
        total_qty = 0.0
        orders = []

        # 2. Limit Orders
        for raw_price in config["levels"]:
            price = self.round_price(raw_price)
            
            raw_qty = base_value / price
            qty = self.round_qty(raw_qty)
            
            total_qty += qty
            orders.append({
                "orderType": "lmt",
                "symbol": SYMBOL,
                "side": config["side"],
                "size": qty,
                "limitPrice": price
            })

        # 3. Stop Loss (Must match sum of open limits)
        stop_price = self.round_price(config["stop"])
        stop_qty = self.round_qty(total_qty)
        
        orders.append({
            "orderType": "stp",
            "symbol": SYMBOL,
            "side": config["stop_side"],
            "size": stop_qty,
            "stopPrice": stop_price,
            "reduceOnly": True
        })

        logger.info(f"{name} | BaseEq: {base_equity:.2f} | Placing {len(orders)} orders.")
        
        for order in orders:
            try:
                resp = client.send_order(order)
                
                if "error" in resp:
                    logger.error(f"Order Fail: {resp}")
                elif "sendStatus" in resp:
                    status = resp.get("sendStatus", {})
                    # Check for explicit API status failure inside success packet
                    if status.get("status") != "placed" and "order_id" not in status:
                         logger.error(f"Order Rejected: {status}")
                    else:
                        logger.info(f"Placed {order['orderType']} | ID: {status.get('order_id', 'Unknown')}")
            except Exception as e:
                logger.error(f"Order Excep: {e}")

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
                    
                    current_count = 0
                    if "openOrders" in open_orders:
                        current_count = sum(1 for o in open_orders["openOrders"] 
                                          if o["symbol"].upper() == SYMBOL)
                    
                    if current_count != 6:
                        logger.info(f"{name}: Count {current_count}/6. Resetting.")
                        if current_count > 0:
                            self.cancel_all(client)
                        self.place_grid(name, client, config, min_equity)
                    else:
                        logger.info(f"{name}: Grid intact.")
                        
                except Exception as e:
                    logger.error(f"{name} Loop Fail: {e}")

            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    bot = EqualOpportunityBot()
    bot.run()
