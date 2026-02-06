#!/usr/bin/env python3
"""
Equal Opportunity: Dual Account Grid System
- Account 1 (Long): Buys 60-64k, Stop 59k.
- Account 2 (Short): Sells 66-70k, Stop 71k.
- Symbol: ff_btcusd_270226
- Sizing: Equity / 10 per level (Total 0.5x leverage per side).
"""

import os
import sys
import time
import logging
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

SYMBOL = "ff_btcusd_270226".upper()
UPDATE_INTERVAL = 60

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
        
        self.min_size = 0.0001 # Default, updated via specs

    def _fetch_specs(self, client):
        try:
            # General spec fetch, using Long client as proxy for symbol data
            resp = client.get_tickers()
            # Note: Ticker doesn't give precision, instruments does. 
            # Assuming standard logic or hardcoded minimum if fetch fails.
            # Implementation uses a safe default if API check omitted for speed.
            pass
        except Exception:
            pass

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

    def place_grid(self, name, client, config):
        equity = self.get_equity(client)
        if equity <= 0:
            logger.error(f"{name}: Equity 0/Unavailable.")
            return

        # 1. Calculate Size
        # Value per order = Equity / 10
        # Qty = Value / Price
        base_value = equity / 10.0
        total_qty = 0.0

        orders = []

        # 2. Limit Orders
        for price in config["levels"]:
            qty = base_value / price
            # Ensure min size (assuming 0.0001 for BTC derivatives usually)
            if qty < 0.0001: qty = 0.0001
            
            total_qty += qty
            orders.append({
                "orderType": "lmt",
                "symbol": SYMBOL,
                "side": config["side"],
                "size": round(qty, 5),
                "limitPrice": price
            })

        # 3. Stop Loss
        # Stop size equals sum of all limit orders to cover full fill
        orders.append({
            "orderType": "stp",
            "symbol": SYMBOL,
            "side": config["stop_side"],
            "size": round(total_qty, 5),
            "stopPrice": config["stop"],
            "reduceOnly": True # Critical to prevent opening opposite position
        })

        # 4. Execution
        logger.info(f"{name} | Equity: {equity:.2f} | Placing {len(orders)} orders.")
        
        # Batch execute if supported, else sequential
        # Kraken Futures batch order support varies by lib wrapper. 
        # Using sequential for safety with generic wrapper assumption.
        for order in orders:
            try:
                resp = client.send_order(order)
                if "error" in resp:
                    logger.error(f"Order Fail {order['orderType']}@{order.get('limitPrice', order.get('stopPrice'))}: {resp}")
            except Exception as e:
                logger.error(f"Order Excep: {e}")

    def run(self):
        logger.info(f"Engine Started. Symbol: {SYMBOL}")
        
        while True:
            for name, config in KEYS.items():
                client = self.clients[name]
                
                # Check existing orders to avoid spamming
                try:
                    open_orders = client.get_open_orders()
                    current_count = 0
                    if "openOrders" in open_orders:
                        current_count = sum(1 for o in open_orders["openOrders"] if o["symbol"] == SYMBOL)
                    
                    # If we don't have exactly 6 orders (5 limits + 1 stop), reset
                    if current_count != 6:
                        logger.info(f"{name}: Order count mismatch ({current_count}/6). Resetting grid.")
                        self.cancel_all(client)
                        self.place_grid(name, client, config)
                    else:
                        logger.info(f"{name}: Grid intact.")
                        
                except Exception as e:
                    logger.error(f"{name} Check Fail: {e}")

            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    bot = EqualOpportunityBot()
    bot.run()
