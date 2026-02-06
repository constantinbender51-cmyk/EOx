#!/usr/bin/env python3
"""
Equal Opportunity: Dual Account Grid System
- Logic: Long 60-64k / Short 66-70k.
- Sizing: Min(Equity1, Equity2) / 10 per level.
- Fix: Corrected API Payload Formats (Dict vs String) for Cancellation.
"""

import os
import sys
import time
import logging
import requests
import json
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
STATE_FILE = "order_state.json"
SIZE_TOLERANCE = 0.05

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
        self.tick_size = 0.5
        self.qty_step = 0.0001
        self.min_qty = 0.0001
        self.fetch_specs()
        
        # --- Startup Cleanup ---
        logger.info("--- STARTUP: Wiping Orders & Positions ---")
        for name, client in self.clients.items():
            self.force_flush(name, client)
            self.close_open_position(name, client)
            self.state[name] = []
        self.save_state()

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
        except Exception:
            pass

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

    def get_position(self, client):
        try:
            pos = client.get_open_positions()
            for p in pos.get("openPositions", []):
                if p["symbol"].upper() == SYMBOL:
                    return float(p["size"])
            return 0.0
        except Exception:
            return 0.0

    def close_open_position(self, name, client):
        try:
            pos_data = client.get_open_positions()
            if "openPositions" not in pos_data: return

            for p in pos_data["openPositions"]:
                if p["symbol"].upper() == SYMBOL:
                    size = float(p["size"])
                    if size > 0:
                        direction = p["side"]
                        exit_side = "sell" if direction == "long" else "buy"
                        
                        logger.info(f"{name}: Closing {direction.upper()} position of {size}...")
                        resp = client.send_order({
                            "orderType": "mkt",
                            "symbol": SYMBOL,
                            "side": exit_side,
                            "size": size
                        })
                        logger.info(f"{name}: Close Position Resp: {resp}")
                        time.sleep(2)
        except Exception as e:
            logger.error(f"{name}: Position Close Fail: {e}")

    def force_flush(self, name, client):
        """
        Modified to use Dictionary payloads for cancellation to avoid TypeErrors.
        """
        # 1. Blanket Cancel (Try sending explicit Dict)
        try:
            resp = client.cancel_all_orders({"symbol": SYMBOL})
            logger.info(f"{name}: Cancel All Resp: {resp}")
        except Exception as e:
            logger.error(f"{name}: Cancel All Fail: {e}")
        
        time.sleep(1.0)

        # 2. Sniper Cancel (Iterate and kill survivors)
        try:
            open_orders = client.get_open_orders()
            if "openOrders" in open_orders:
                survivors = [o for o in open_orders["openOrders"] if o["symbol"].upper() == SYMBOL]
                if survivors:
                    logger.info(f"{name}: Sniping {len(survivors)} stuck orders...")
                    for o in survivors:
                        try:
                            # Try passing Dict payload
                            resp = client.cancel_order({"order_id": o["order_id"]})
                            
                            # Fallback logging
                            if "error" in str(resp):
                                logger.warning(f"Sniper Retry {o['order_id']}")
                            else:
                                logger.info(f"{name}: Cancelled {o['order_id']}")
                                
                        except Exception as inner_e:
                            logger.error(f"{name}: Sniper ID {o['order_id']} Error: {inner_e}")
        except Exception as e:
            logger.error(f"{name}: Sniper Check Fail: {e}")

    def place_grid(self, name, client, config, base_equity):
        if base_equity <= 0: return

        current_pos = self.get_position(client)
        base_value_per_level = base_equity / 10.0
        
        levels = sorted(config["levels"], reverse=(config["side"] == "buy"))
        
        limit_payloads = []
        pending_limit_size = 0.0
        
        filled_qty_tracker = current_pos

        for raw_price in levels:
            price = self.round_price(raw_price)
            ideal_qty = self.round_qty(base_value_per_level / price)
            
            if filled_qty_tracker >= (ideal_qty * 0.9): 
                filled_qty_tracker -= ideal_qty
            else:
                limit_payloads.append({
                    "orderType": "lmt",
                    "symbol": SYMBOL,
                    "side": config["side"],
                    "size": ideal_qty,
                    "limitPrice": price,
                    "meta_type": "limit"
                })
                pending_limit_size += ideal_qty

        stop_price = self.round_price(config["stop"])
        total_risk_size = self.round_qty(current_pos + pending_limit_size)
        
        orders_to_send = limit_payloads
        orders_to_send.append({
            "orderType": "stp",
            "symbol": SYMBOL,
            "side": config["stop_side"],
            "size": total_risk_size,
            "stopPrice": stop_price,
            "reduceOnly": True,
            "meta_type": "stop"
        })

        logger.info(f"{name} | Pos: {current_pos:.4f} | Placing {len(limit_payloads)} limits + Stop {total_risk_size:.4f}")

        new_state = []
        for order in orders_to_send:
            try:
                meta = order.pop("meta_type")
                resp = client.send_order(order)
                if "sendStatus" in resp and "order_id" in resp["sendStatus"]:
                    new_state.append({
                        "id": resp["sendStatus"]["order_id"],
                        "type": meta,
                        "price": order.get("limitPrice", order.get("stopPrice")),
                        "size": order["size"]
                    })
            except Exception as e:
                logger.error(f"Order Excep: {e}")
        
        self.state[name] = new_state
        self.save_state()

    def check_integrity(self, name, open_orders, current_pos, min_equity, config):
        saved = self.state.get(name, [])
        if not saved: return False
        
        live_map = {o["order_id"]: o for o in open_orders.get("openOrders", [])}
        
        limit_size_sum = 0.0
        for s in saved:
            if s["type"] == "limit":
                if s["id"] not in live_map:
                    logger.info(f"{name}: Limit {s['id']} filled/gone. Resetting.")
                    return False
                limit_size_sum += float(live_map[s["id"]]["size"])

        target_stop_size = self.round_qty(current_pos + limit_size_sum)
        
        stop_found = False
        for s in saved:
            if s["type"] == "stop":
                if s["id"] not in live_map:
                    logger.info(f"{name}: Stop {s['id']} missing. Resetting.")
                    return False
                
                live_size = float(live_map[s["id"]]["size"])
                if abs(live_size - target_stop_size) / live_size > SIZE_TOLERANCE:
                     logger.info(f"{name}: Stop Size Drift. Live: {live_size}, Target: {target_stop_size}. Resetting.")
                     return False
                stop_found = True
        
        if not stop_found:
            return False

        return True

    def run(self):
        logger.info(f"Engine Running. Symbol: {SYMBOL}")
        while True:
            eq_long = self.get_equity(self.clients["LONG"])
            eq_short = self.get_equity(self.clients["SHORT"])
            min_equity = min(eq_long, eq_short)
            
            logger.info(f"Status | MIN EQ: {min_equity:.2f}")

            for name, config in KEYS.items():
                client = self.clients[name]
                try:
                    open_orders = client.get_open_orders()
                    pos = self.get_position(client)
                    
                    if self.check_integrity(name, open_orders, pos, min_equity, config):
                        logger.info(f"{name}: OK. Pos: {pos:.4f}")
                    else:
                        logger.info(f"{name}: State Invalid. Rebuilding...")
                        self.force_flush(name, client)
                        self.place_grid(name, client, config, min_equity)

                except Exception as e:
                    logger.error(f"{name} Error: {e}")

            time.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    bot = EqualOpportunityBot()
    bot.run()
