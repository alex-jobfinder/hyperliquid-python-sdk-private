import asyncio
from asyncio import run_coroutine_threadsafe
import os
import json
from datetime import datetime
from typing import Any
import example_utils
from hyperliquid.utils import constants
from examples.rate_limiter import HyperliquidRateLimiter

# Import our Kafka callbacks
from hyperliquid.utils.kafka_callbacks import (
    ClickHouseFillKafka,
    ClickHouseOrderKafka
)

# Set up your Kafka config
KAFKA_SERVER = "cthki8qfdq8asdnsm9gg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092"
USERNAME = "alexei.jobfinder@gmail.com"
PASSWORD = "y0obC7dFiU3CJxcsCH4RwXtwEhaauf"

# Instantiate the fill/order Kafka callbacks
fill_kafka_cb = ClickHouseFillKafka(
    bootstrap=KAFKA_SERVER,
    username=USERNAME,
    password=PASSWORD
)

order_kafka_cb = ClickHouseOrderKafka(
    bootstrap=KAFKA_SERVER,
    username=USERNAME,
    password=PASSWORD
)

rate_limiter = HyperliquidRateLimiter()
event_loop = asyncio.get_event_loop()

# async def handle_user_fills(msg, address):
#     # Rate limit for each fill event.
#     await rate_limiter.acquire_address_action(address)

#     fills = msg.get("data", {}).get("fills", [])
#     for f in fills:
#         # We still retrieve Hyperliquid keys, but we'll log them under Cryptofeed-style names
#         known_data = [
#             address,
#             f.get("coin"),       # "symbol" in CSV
#             f.get("side"),       # "side"
#             f.get("px"),         # "price"
#             f.get("sz"),         # "amount"
#             f.get("time"),       # "timestamp"
#             f.get("hash"),       # "id"
#             f.get("oid"),        # "order_id"
#             f.get("crossed"),    # "crossed"
#             f.get("closedPnl"),  # "closedPnl"
#             f.get("dir"),        # "type"
#             f.get("startPosition"),
#             f.get("fee"),
#             f.get("feeToken"),
#             f.get("tid"),
#         ]

#         # Keep the known Hyperliquid keys for filtering unknown fields
#         known_keys = {
#             "coin",
#             "side",
#             "px",
#             "sz",
#             "time",
#             "hash",
#             "oid",
#             "crossed",
#             "closedPnl",
#             "dir",
#             "startPosition",
#             "fee",
#             "feeToken",
#             "tid"
#         }
#         unknown_data = {}
#         for key, val in f.items():
#             if key not in known_keys:
#                 unknown_data[key] = val

#         known_data.append(json.dumps(unknown_data))
#         user_fills_logger.log(known_data)

# async def handle_order_updates(msg, address):
#     # Rate limit for each order update.
#     await rate_limiter.acquire_address_action(address)

#     for update in msg.get("data", []):
#         order = update.get("order", {})
#         # We still retrieve Hyperliquid keys, but log them under Cryptofeed-style names
#         cloid_val = order.get("cloid")
#         known_data = [
#             address,
#             order.get("coin"),       # "symbol"
#             order.get("side"),
#             order.get("limitPx"),    # "price"
#             order.get("sz"),         # "amount"
#             order.get("oid"),        # "order_id"
#             order.get("timestamp"),  # "timestamp"
#             order.get("origSz"),
#             order.get("reduceOnly", False),
#             update.get("status"),
#             update.get("statusTimestamp"),
#             cloid_val,
#         ]

#         known_order_keys = {
#             "coin",
#             "side",
#             "limitPx",
#             "sz",
#             "oid",
#             "timestamp",
#             "origSz",
#             "reduceOnly",
#             "cloid",
#         }
#         known_update_keys = {"status", "statusTimestamp", "order"}
#         unknown_data = {}

#         # Gather unknown fields in 'order'
#         for key, val in order.items():
#             if key not in known_order_keys:
#                 unknown_data[key] = val

#         # Gather unknown fields in 'update'
#         for key, val in update.items():
#             if key not in known_update_keys:
#                 unknown_data[key] = val

#         known_data.append(json.dumps(unknown_data))
#         order_updates_logger.log(known_data)


async def handle_user_fills(msg, address):
    # Rate limit for each fill event.
    await rate_limiter.acquire_address_action(address)

    fills = msg.get("data", {}).get("fills", [])
    for f in fills:
        # Prepare fill data for Kafka
        fill_data = {
            "exchange": "hyperliquid",
            "symbol": f.get("coin"),         # was "coin" in the original fill
            "side": f.get("side"),
            "price": f.get("px"),
            "amount": f.get("sz"),
            "fee": f.get("fee"),
            "id": f.get("hash"),
            "order_id": f.get("oid"),
            "liquidity": f.get("crossed"),   # optional choice if you'd like to store "crossed" under liquidity
            "type": f.get("dir"),
            "account": address,
            "timestamp": f.get("time"),
            # If desired, you can provide a receipt_timestamp. Here, we omit it or set to None.
            "raw": f,                       # store the individual fill as raw
            "raw_data": msg                 # store the entire message for debugging
        }
        # Send fill data to Kafka
        await fill_kafka_cb.write(fill_data)

async def handle_order_updates(msg, address):
    # Rate limit for each order update.
    await rate_limiter.acquire_address_action(address)

    for update in msg.get("data", []):
        order = update.get("order", {})
        # Prepare order data for Kafka
        order_data = {
            "exchange": "hyperliquid",
            "symbol": order.get("coin"),
            "id": order.get("oid"),
            "client_order_id": order.get("cloid"),
            "side": order.get("side"),
            "status": update.get("status"),
            "type": "limit" if order.get("limitPx") else "unknown",
            "price": order.get("limitPx"),
            "amount": order.get("sz"),
            # "remaining" not directly provided; omitted or set to None
            "timestamp": order.get("timestamp"),
            "account": address,
            "raw": order,        # store the order portion of the update
            "raw_data": msg      # store the entire message for debugging
        }
        # Send order data to Kafka
        await order_kafka_cb.write(order_data)

async def subscribe_to_address(address: str):
    try:
        _, info, _ = example_utils.setup(constants.MAINNET_API_URL)

        # Initial rate limit for address before subscribing.
        await rate_limiter.acquire_address_action(address)

        info.subscribe({"type": "userFills", "user": address},
                       lambda msg: run_coroutine_threadsafe(handle_user_fills(msg, address), event_loop))
        info.subscribe({"type": "orderUpdates", "user": address},
                       lambda msg: run_coroutine_threadsafe(handle_order_updates(msg, address), event_loop))

        print(f"[{address}] Subscribed. Listening...")

        while True:
            await asyncio.sleep(5)

    except Exception as e:
        print(f"[{address}] ERROR: {e}")
        await asyncio.sleep(3)
        return await subscribe_to_address(address)  # Auto-retry

async def main():
    addresses = [
        "0x0000a0ab1b620e79fa089a16c89c5eeee490f2da",
        "0x007d76eec0ba411ce873a8819df50dd443d967a0",
        "0x023a3d058020fb76cca98f01b3c48c8938a22355",
        "0x03ca2d85b85dc1d61243cc2932382dbc6285fbda",
        "0x06cecfbac34101ae41c88ebc2450f8602b3d164b",
        "0x091144e651b334341eabdbbbfed644ad0100023e",
        "0x0a170cdb6eb5a46c15cffd727c8659f2f371f478",
        "0x0a97692d91fa9195a52da81cca2051c032c5347d",
    ]

    await asyncio.gather(*[subscribe_to_address(addr) for addr in addresses])

if __name__ == "__main__":
    asyncio.run(main())
