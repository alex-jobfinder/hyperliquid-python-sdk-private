import asyncio
import ssl
from collections import OrderedDict
from decimal import Decimal, InvalidOperation
from collections import OrderedDict
import orjson
from aiokafka import AIOKafkaProducer
import base64
from typing import Optional
import base64
import yaml



def make_my_print(source_name):
    async def _my_print(data, _receipt_time):
        print(f"[{source_name}] {data}")
    return _my_print
    # pass

def ensure_decimal(value):
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"Invalid decimal value: {value}")

# def partition_key(self, data: dict) -> Optional[bytes]:
#     return f"{data['symbol']}".encode('utf-8')
def partition_key(symbol: str) -> Optional[bytes]:
    return symbol.encode("utf-8") if symbol else b"unknown"


MAX_INT64 = 9223372036854775807

def to_ns(ts):
    try:
        ns = int(ts * 1_000_000_000)
        return min(ns, MAX_INT64)
    except Exception:
        return None


class KafkaCallback:
    def __init__(self, bootstrap, topic=None, numeric_type=float, none_to=None, sasl_mechanism="SCRAM-SHA-256",
                 username=None, password=None, security_protocol="SASL_SSL", ssl_cafile=None, ssl_certfile=None, ssl_keyfile=None, exchange_index=None, **kwargs):
        """
        bootstrap: str
            The bootstrap server(s). e.g. 'yourcluster:9092'
        """
        self.bootstrap = bootstrap
        self.topic = topic if topic else self.default_topic
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.username = username
        self.password = password
        self.sasl_mechanism = sasl_mechanism
        self.security_protocol = security_protocol
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.producer = None
        self.exchange_index = exchange_index or {}

    async def __call__(self, dtype, receipt_timestamp: float):
        if isinstance(dtype, dict):
            data = dtype
        else:
            data = dtype.to_dict(numeric_type=self.numeric_type, none_to=self.none_to)
            if not dtype.timestamp:
                data['timestamp'] = receipt_timestamp
            data['receipt_timestamp'] = receipt_timestamp
        await self.write(data)

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            ssl_context = None
            if self.security_protocol in ("SSL", "SASL_SSL"):
                ssl_context = ssl.create_default_context(cafile=self.ssl_cafile)
                if self.ssl_certfile and self.ssl_keyfile:
                    ssl_context.load_cert_chain(certfile=self.ssl_certfile, keyfile=self.ssl_keyfile)

            self.producer = AIOKafkaProducer(
                # acks=0,
                acks=1,
                request_timeout_ms=10000,
                connections_max_idle_ms=20000,
                loop=loop,
                bootstrap_servers=self.bootstrap,
                client_id='cryptofeed',
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.username,
                sasl_plain_password=self.password,
                ssl_context=ssl_context
            )
            await self.producer.start()

    async def write(self, data: dict):
        await self.__connect()
        await self.producer.send_and_wait(self.topic, orjson.dumps(data))
   
   
   
"""

cdef class Order:
    cdef readonly str exchange
    cdef readonly str symbol
    cdef readonly str client_order_id
    cdef readonly str side
    cdef readonly str type
    cdef readonly object price
    cdef readonly object amount
    cdef readonly str account
    cdef readonly object timestamp

    def __init__(self, symbol, client_order_id, side, type, price, amount, timestamp, account=None, exchange=None):
        assert isinstance(price, Decimal)
        assert isinstance(amount, Decimal)
        assert timestamp is None or isinstance(timestamp, float)

        self.symbol = symbol
        self.client_order_id = client_order_id
        self.side = side
        self.type = type
        self.price = price
        self.amount = amount
        self.account = account
        self.exchange = exchange
        self.timestamp = timestamp

    @staticmethod
    def from_dict(data: dict) -> Order:
        return Order(
            data['symbol'],
            data['client_order_id'],
            data['side'],
            data['type'],
            Decimal(data['price']),
            Decimal(data['amount']),
            data['timestamp'],
            account=data['account'],
            exchange=data['exchange']
        )

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'client_order_id': self.client_order_id, 'side': self.side, 'type': self.type, 'price': self.price, 'amount': self.amount, 'account': self.account, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'client_order_id': self.client_order_id, 'side': self.side, 'type': self.type, 'price': numeric_type(self.price), 'amount': numeric_type(self.amount), 'account': self.account, 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} symbol: {self.symbol} client_order_id: {self.client_order_id} side: {self.side} type: {self.type} price: {self.price} amount: {self.amount} account: {self.account} timestamp: {self.timestamp}'

    def __eq__(self, cmp):
        return self.exchange == cmp.exchange and self.symbol == cmp.symbol and self.type == cmp.type and self.price == cmp.price and self.amount == cmp.amount and self.timestamp == cmp.timestamp and self.account == cmp.account and self.client_order_id == cmp.client_order_id

    def __hash__(self):
        return hash(self.__repr__())


"""

class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trades'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            # if not all(k in data for k in ("price", "amount", "timestamp")):
            #     raise ValueError(f"Incomplete trade data: {data}")

            symbol = data.get("symbol", "unknown")
            payload = {
                "exchange": data.get("exchange", "unknown"),
                "symbol": symbol,
                "side": data.get("side", "unknown"),
                "amount": ensure_decimal(data["amount"]),
                "price": ensure_decimal(data["price"]),
                "timestamp": int(data["timestamp"] * 1_000_000_000),
                "id": data.get("id"),
                "type": data.get("type"),
                "receipt_timestamp": int(data["receipt_timestamp"] * 1_000_000_000) if "receipt_timestamp" in data else None,
                "raw":orjson.dumps(getattr(data, "raw", data.get("raw", {}))).decode() if data.get("raw") else None,
                "raw_data": orjson.dumps(data, default=str).decode()
            }

            key = partition_key(symbol)
            await self.producer.send_and_wait(self.topic, orjson.dumps(payload, default=str), key=key)
        except Exception as e:
            print(f"[WARN] ClickHouseTradeKafka.write() failed: {e}")
            
        
"""

cdef class OrderInfo:
    cdef readonly str exchange
    cdef readonly str symbol
    cdef readonly str id
    cdef readonly str client_order_id
    cdef readonly str side
    cdef readonly str status
    cdef readonly str type
    cdef readonly object price
    cdef readonly object amount
    cdef readonly object remaining
    cdef readonly str account
    cdef readonly object timestamp
    cdef readonly object raw  # Can be dict or list

    def __init__(self, exchange, symbol, id, side, status, type, price, amount, remaining, timestamp, client_order_id=None, account=None, raw=None):
        assert isinstance(price, Decimal)
        assert isinstance(amount, Decimal)
        assert remaining is None or isinstance(remaining, Decimal)
        assert timestamp is None or isinstance(timestamp, float)

        self.exchange = exchange
        self.symbol = symbol
        self.id = id
        self.client_order_id = client_order_id
        self.side = side
        self.status = status
        self.type = type
        self.price = price
        self.amount = amount
        self.remaining = remaining
        self.account = account
        self.timestamp = timestamp
        self.raw = raw

    cpdef set_status(self, status: str):
        self.status = status

    @staticmethod
    def from_dict(data: dict) -> OrderInfo:
        return OrderInfo(
            data['exchange'],
            data['symbol'],
            data['id'],
            data['side'],
            data['status'],
            data['type'],
            Decimal(data['price']),
            Decimal(data['amount']),
            Decimal(data['remaining']) if data['remaining'] else data['remaining'],
            data['timestamp'],
            account=data['account'],
            client_order_id=data['client_order_id']
        )

    cpdef dict to_dict(self, numeric_type=None, none_to=False):
        if numeric_type is None:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'client_order_id': self.client_order_id, 'side': self.side, 'status': self.status, 'type': self.type, 'price': self.price, 'amount': self.amount, 'remaining': self.remaining, 'account': self.account, 'timestamp': self.timestamp}
        else:
            data = {'exchange': self.exchange, 'symbol': self.symbol, 'id': self.id, 'client_order_id': self.client_order_id, 'side': self.side, 'status': self.status, 'type': self.type, 'price': numeric_type(self.price), 'amount': numeric_type(self.amount), 'remaining': numeric_type(self.remaining), 'account': self.account, 'timestamp': self.timestamp}
        return data if not none_to else convert_none_values(data, none_to)

    def __repr__(self):
        return f'exchange: {self.exchange} symbol: {self.symbol} id: {self.id} client_order_id: {self.client_order_id} side: {self.side} status: {self.status} type: {self.type} price: {self.price} amount: {self.amount} remaining: {self.remaining} account: {self.account} timestamp: {self.timestamp}'

    def __eq__(self, cmp):
        return self.exchange == cmp.exchange and self.symbol == cmp.symbol and self.id == cmp.id and self.status == cmp.status and self.type == cmp.type and self.price == cmp.price and self.amount == cmp.amount and self.remaining == cmp.remaining and self.timestamp == cmp.timestamp and self.account == cmp.account and self.client_order_id == cmp.client_order_id

    def __hash__(self):
        return hash(self.__repr__())
"""

class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trades'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            # if not all(k in data for k in ("price", "amount", "timestamp")):
            #     raise ValueError(f"Incomplete trade data: {data}")

            symbol = data.get("symbol", "unknown")
            payload = {
                "exchange": data.get("exchange", "unknown"),
                "symbol": symbol,
                "side": data.get("side", "unknown"),
                "amount": ensure_decimal(data["amount"]),
                "price": ensure_decimal(data["price"]),
                "timestamp": int(data["timestamp"] * 1_000_000_000),
                "id": data.get("id"),
                "type": data.get("type"),
                "receipt_timestamp": int(data["receipt_timestamp"] * 1_000_000_000) if "receipt_timestamp" in data else None,
                "raw":orjson.dumps(getattr(data, "raw", data.get("raw", {}))).decode() if data.get("raw") else None,
                "raw_data": orjson.dumps(data, default=str).decode()
            }

            key = partition_key(symbol)
            await self.producer.send_and_wait(self.topic, orjson.dumps(payload, default=str), key=key)
        except Exception as e:
            print(f"[WARN] ClickHouseTradeKafka.write() failed: {e}")
            

"""
cdef class Fill:
    cdef readonly str exchange
    cdef readonly str symbol
    cdef readonly object price
    cdef readonly object amount
    cdef readonly str side
    cdef readonly object fee
    cdef readonly str id
    cdef readonly str order_id
    cdef readonly str liquidity
    cdef readonly str type
    cdef readonly str account
    cdef readonly double timestamp
    cdef readonly object raw  # can be dict or list

    def __init__(self, exchange, symbol, side, amount, price, fee, id, order_id, type, liquidity, timestamp, account=None, raw=None):
        assert isinstance(price, Decimal)
        assert isinstance(amount, Decimal)
        assert fee is None or isinstance(fee, Decimal)

        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.amount = amount
        self.price = price
        self.fee = fee
        self.id = id
        self.order_id = order_id
        self.type = type
        self.liquidity = liquidity
        self.account = account
        self.timestamp = timestamp
        self.raw = raw
"""

class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trades'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            # if not all(k in data for k in ("price", "amount", "timestamp")):
            #     raise ValueError(f"Incomplete trade data: {data}")

            symbol = data.get("symbol", "unknown")
            payload = {
                "exchange": data.get("exchange", "unknown"),
                "symbol": symbol,
                "side": data.get("side", "unknown"),
                "amount": ensure_decimal(data["amount"]),
                "price": ensure_decimal(data["price"]),
                "timestamp": int(data["timestamp"] * 1_000_000_000),
                "id": data.get("id"),
                "type": data.get("type"),
                "receipt_timestamp": int(data["receipt_timestamp"] * 1_000_000_000) if "receipt_timestamp" in data else None,
                "raw":orjson.dumps(getattr(data, "raw", data.get("raw", {}))).decode() if data.get("raw") else None,
                "raw_data": orjson.dumps(data, default=str).decode()
            }

            key = partition_key(symbol)
            await self.producer.send_and_wait(self.topic, orjson.dumps(payload, default=str), key=key)
        except Exception as e:
            print(f"[WARN] ClickHouseTradeKafka.write() failed: {e}")

class ClickHouseOrderKafka(KafkaCallback):
    """
    A Kafka callback for streaming order data (based on OrderInfo fields)
    to a ClickHouse-compatible topic.
    """
    default_topic = 'orders'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            # Obtain the 'symbol' from data for partitioning; default to 'unknown'
            symbol = data.get("symbol", "unknown")

            # Convert the 'timestamp' to nanoseconds if present
            ts = data.get("timestamp")
            timestamp_ns = to_ns(ts)

            payload = {
                "exchange": data.get("exchange", "unknown"),
                "symbol": symbol,
                "id": data.get("id"),
                "client_order_id": data.get("client_order_id"),
                "side": data.get("side", "unknown"),
                "status": data.get("status", "unknown"),
                "type": data.get("type", "unknown"),
                "price": Decimal(data["price"]) if "price" in data else None,
                "amount": Decimal(data["amount"]) if "amount" in data else None,
                "remaining": Decimal(data["remaining"]) if "remaining" in data and data["remaining"] else None,
                "timestamp": timestamp_ns,
                "account": data.get("account"),
                "raw": orjson.dumps(getattr(data, "raw", data.get("raw", {}))).decode() if data.get("raw") else None,
                "raw_data": orjson.dumps(data, default=str).decode(),
            }

            key = partition_key(symbol)
            await self.producer.send_and_wait(self.topic, orjson.dumps(payload, default=str), key=key)
        except Exception as e:
            print(f"[WARN] ClickHouseOrderKafka.write() failed: {e}")


class ClickHouseFillKafka(KafkaCallback):
    """
    A Kafka callback for streaming fill data (based on Fill fields)
    to a ClickHouse-compatible topic.
    """
    default_topic = 'fills'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            # Obtain the 'symbol' from data for partitioning; default to 'unknown'
            symbol = data.get("symbol", "unknown")

            # Convert the 'timestamp' to nanoseconds if present
            ts = data.get("timestamp")
            timestamp_ns = to_ns(ts)

            payload = {
                "exchange": data.get("exchange", "unknown"),
                "symbol": symbol,
                "side": data.get("side", "unknown"),
                "amount": Decimal(data["amount"]) if "amount" in data else None,
                "price": Decimal(data["price"]) if "price" in data else None,
                "fee": Decimal(data["fee"]) if data.get("fee") else None,
                "id": data.get("id"),
                "order_id": data.get("order_id"),
                "liquidity": data.get("liquidity"),
                "type": data.get("type", "unknown"),
                "account": data.get("account"),
                "timestamp": timestamp_ns,
                "receipt_timestamp": int(data["receipt_timestamp"] * 1_000_000_000) 
                    if "receipt_timestamp" in data else None,
                "raw": orjson.dumps(getattr(data, "raw", data.get("raw", {}))).decode() if data.get("raw") else None,
                "raw_data": orjson.dumps(data, default=str).decode()
            }

            key = partition_key(symbol)
            await self.producer.send_and_wait(self.topic, orjson.dumps(payload, default=str), key=key)
        except Exception as e:
            print(f"[WARN] ClickHouseFillKafka.write() failed: {e}")