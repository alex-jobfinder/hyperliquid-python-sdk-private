import asyncio
import os
import csv
import json
from datetime import datetime
from typing import Any
# import example_utils
from hyperliquid.utils import constants

import asyncio
import time
from collections import defaultdict, deque
from math import floor
from typing import Optional

"""
### WebSocket & Rate Limit Constraints for Hyperliquid

**WebSocket Limits:**
- Max **1000 subscriptions** total  
- Max **10 unique users** for user-specific subscriptions  
- Max **2000 messages/minute** sent across all WebSocket connections  
- Max **100 inflight POST messages** across all WebSocket connections  

> Use WebSockets for lowest-latency real-time data. See the Python SDK for examples.

---

### Rate Limits

#### **Address-Based Limits**
- Each address can send **1 request per 1 USDC** traded since inception
- Each address starts with a **10,000 request buffer**
- When limited:
  - Allowed **1 request every 10 seconds**
- Using a **100 USDC** order value only requires a **1% fill rate** to sustain usage

#### **Cancel Requests**
- Cancel limit = `min(default_limit + 100,000, default_limit * 2)`
- Ensures cancels are allowed even when action limits are hit

> These address-based limits apply only to *actions*, not info requests.

---

### Batched Requests
- Batched actions (e.g. `n` orders/cancels):
  - Count as **1 request** for **IP-based limits**
  - Count as **n requests** for **address-based limits**

---
"""

class HyperliquidRateLimiter:
    def __init__(self):
        self.lock = asyncio.Lock()

        # IP-based rate limits
        self.rest_weight_used = 0
        self.rest_window_start = time.time()
        self.ws_messages = 0
        self.ws_window_start = time.time()
        self.inflight_posts = 0

        # Address-based rate limits
        self.address_last_used = defaultdict(float)  # address -> last request timestamp
        self.address_limit_buffer = defaultdict(lambda: 10000)  # address -> remaining requests

        # Constants
        self.REST_LIMIT = 1200
        self.WS_LIMIT = 2000
        self.WS_WINDOW = 60
        self.REST_WINDOW = 60
        self.INFLIGHT_POST_LIMIT = 100
        self.ADDRESS_RATE_LIMIT_DELAY = 10
        self.ADDRESS_RATE_LIMIT_WINDOW = 86400  # 1 day

    def _reset_ip_windows(self):
        now = time.time()
        if now - self.rest_window_start > self.REST_WINDOW:
            self.rest_window_start = now
            self.rest_weight_used = 0
        if now - self.ws_window_start > self.WS_WINDOW:
            self.ws_window_start = now
            self.ws_messages = 0

    async def acquire_rest(self, weight: int):
        async with self.lock:
            while True:
                self._reset_ip_windows()
                if self.rest_weight_used + weight <= self.REST_LIMIT:
                    self.rest_weight_used += weight
                    return
                await asyncio.sleep(0.1)

    async def acquire_ws(self):
        async with self.lock:
            while True:
                self._reset_ip_windows()
                if self.ws_messages + 1 <= self.WS_LIMIT:
                    self.ws_messages += 1
                    return
                await asyncio.sleep(0.1)

    async def acquire_post_slot(self):
        async with self.lock:
            while self.inflight_posts >= self.INFLIGHT_POST_LIMIT:
                await asyncio.sleep(0.05)
            self.inflight_posts += 1

    def release_post_slot(self):
        self.inflight_posts = max(0, self.inflight_posts - 1)

    async def acquire_address_action(self, address: str, batch_len: int = 1):
        now = time.time()

        if self.address_limit_buffer[address] > 0:
            self.address_limit_buffer[address] -= batch_len
            self.address_last_used[address] = now
            return

        # Rate-limited: one request every 10 seconds
        last_used = self.address_last_used[address]
        if now - last_used >= self.ADDRESS_RATE_LIMIT_DELAY:
            self.address_last_used[address] = now
            return

        delay = self.ADDRESS_RATE_LIMIT_DELAY - (now - last_used)
        await asyncio.sleep(delay)
        self.address_last_used[address] = time.time()

    @staticmethod
    def calculate_rest_weight(request_type: str, batch_len: Optional[int] = 1) -> int:
        if request_type in {"l2Book", "allMids", "clearinghouseState", "orderStatus", "spotClearinghouseState", "exchangeStatus"}:
            return 2
        elif request_type == "userRole":
            return 60
        elif request_type.startswith("explorer"):
            return 40
        elif request_type == "action":
            return 1 + floor((batch_len or 1) / 40)
        else:
            return 20

