import asyncio
import json
import os
import yaml
from aiokafka import AIOKafkaProducer
from datetime import datetime, timedelta
from typing import Any, Optional, List, Dict
import ssl
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

from hyperliquid.utils import constants
import example_utils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        topic: str,
        max_batch_size: int = 1000
    ):
        self.config = {
            "SERVER": server,
            "USERNAME": username,
            "PASSWORD": password,
            "TOPIC": topic
        }
        self.max_batch_size = max_batch_size
        self.producer: Optional[AIOKafkaProducer] = None
        self.last_message_time = None
        self.health_check_interval = timedelta(minutes=5)

    def ensure_topic(self):
        """Ensure the Kafka topic exists, create it if it doesn't."""
        admin = KafkaAdminClient(
            bootstrap_servers=self.config["SERVER"],
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=self.config["USERNAME"],
            sasl_plain_password=self.config["PASSWORD"],
        )

        try:
            existing_topics = admin.list_topics()
            if self.config["TOPIC"] not in existing_topics:
                new_topic = NewTopic(
                    name=self.config["TOPIC"],
                    num_partitions=1,
                    replication_factor=3
                )
                admin.create_topics([new_topic])
                logger.info(f"Created topic: {self.config['TOPIC']}")
            else:
                logger.info(f"Topic {self.config['TOPIC']} already exists")
        except TopicAlreadyExistsError:
            logger.info(f"Topic {self.config['TOPIC']} already exists")
        except Exception as e:
            logger.error(f"Error ensuring topic: {e}")
        finally:
            admin.close()

    async def create_producer(self, retries=3, delay=5):
        """Create and start the Kafka producer with retries."""
        for attempt in range(retries):
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.config["SERVER"],
                    security_protocol="SASL_SSL",
                    sasl_mechanism="SCRAM-SHA-256",
                    sasl_plain_username=self.config["USERNAME"],
                    sasl_plain_password=self.config["PASSWORD"],
                    ssl_context=ssl_context
                )
                await self.producer.start()
                return
            except Exception as e:
                if attempt == retries - 1:
                    raise
                logger.warning(f"Failed to create producer (attempt {attempt + 1}/{retries}): {e}")
                await asyncio.sleep(delay)

    async def send_batch(self, messages: List[bytes]):
        """Send a batch of messages to Kafka."""
        if not messages:
            return
        
        batch_size = sum(len(msg) for msg in messages)
        logger.info(f"Sending batch of {len(messages)} messages, total size: {batch_size} bytes")
        
        await asyncio.gather(*[
            self.producer.send_and_wait(self.config["TOPIC"], msg)
            for msg in messages
        ])
        self.last_message_time = datetime.now()

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def health_check(self):
        """Monitor the health of the Kafka connection."""
        while True:
            await asyncio.sleep(300)  # Check every 5 minutes
            if self.last_message_time and datetime.now() - self.last_message_time > self.health_check_interval:
                logger.warning("No messages received in the last 5 minutes")


class HyperliquidETL:
    def __init__(self, kafka_client: KafkaClient):
        self.kafka_client = kafka_client
        self.info = None
        self.config = {}
        self.coins = []
        self.loop = None  # Store the event loop

    def load_config(self, file_path: str) -> None:
        """Load configuration from YAML file."""
        try:
            with open(file_path, 'r') as file:
                self.config = yaml.safe_load(file)
                self.validate_config()
                self.coins = self.config.get("assets", [])
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            self.config = {}
            self.coins = []

    def validate_config(self) -> None:
        """Validate the configuration."""
        required_fields = ["addresses", "assets"]
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"Missing required config field: {field}")

    def _enrich_trade(self, trade: Dict) -> Dict:
        """Enrich a single trade with additional information."""
        px = float(trade["px"])
        sz = float(trade["sz"])
        volume = px * sz

        users = trade.get("users", [])
        initiator, counterparty = (
            ("missing", "missing") if len(users) < 2
            else (users[0], users[1]) if trade["side"] == "B"
            else (users[1], users[0])
        )

        whale_address, is_whale = None, 0
        whale_is_aggressor = False

        for address in self.config.get("addresses", []):
            if initiator == address:
                is_whale = 1
                whale_address = initiator
                whale_is_aggressor = True
                break
            elif counterparty == address:
                is_whale = 1
                whale_address = counterparty
                break

        buy_or_sell = (
            "buy" if trade["side"] == "B" else "sell"
            if whale_is_aggressor else
            "sell" if trade["side"] == "B" else "buy"
        )

        return {
            "coin": trade["coin"],
            "side": trade["side"],
            "px": px,
            "sz": sz,
            "time": trade["time"],
            "hash": trade["hash"],
            "tid": trade["tid"],
            "initiator": initiator,
            "counterparty": counterparty,
            "buy_or_sell": buy_or_sell,
            "volume": volume,
            "is_whale": is_whale,
            "whale_address": whale_address
        }

    async def handle_trade_message(self, message: Dict) -> None:
        """Handle incoming trade messages."""
        if not message or not isinstance(message, dict):
            logger.error(f"Invalid message format: {message}")
            return

        batch = []
        try:
            for trade in message.get("data", []):
                try:
                    enriched = self._enrich_trade(trade)
                    batch.append(json.dumps(enriched).encode("utf-8"))
                except Exception as e:
                    logger.error(f"Error enriching trade {trade}: {e}")
                    continue
            
            if batch:
                try:
                    await self.kafka_client.send_batch(batch)
                except Exception as e:
                    logger.error(f"Error sending batch to Kafka: {e}")
        except Exception as e:
            logger.error(f"Error handling trade message: {e}")

    def trade_message_wrapper(self, message: Dict) -> None:
        """Thread-safe wrapper for the websocket callback."""
        try:
            if self.loop is None or not self.loop.is_running():
                logger.error("No running event loop available")
                return
            
            future = asyncio.run_coroutine_threadsafe(
                self.handle_trade_message(message), 
                self.loop
            )
            
            # Add callback to handle any errors
            def handle_future_result(fut):
                try:
                    fut.result()
                except Exception as e:
                    logger.error(f"Async execution failed: {e}")
            
            future.add_done_callback(handle_future_result)
        except Exception as e:
            logger.error(f"Error in trade message wrapper: {e}")

    async def start(self) -> None:
        """Start the ETL process."""
        # Store the event loop
        self.loop = asyncio.get_running_loop()
        
        # Initialize Kafka
        self.kafka_client.ensure_topic()
        await self.kafka_client.create_producer()

        # Set up Hyperliquid connection
        _, self.info, _ = example_utils.setup(constants.MAINNET_API_URL)

        # Subscribe to trades
        for coin in self.coins:
            self.info.subscribe({"type": "trades", "coin": coin}, self.trade_message_wrapper)
        
        logger.info("Subscribed to trades. Ctrl+C to exit.")

    async def stop(self) -> None:
        """Stop the ETL process."""
        logger.info("Shutting down gracefully...")
        for coin in self.coins:
            self.info.unsubscribe({"type": "trades", "coin": coin})
        await self.kafka_client.stop()


async def main():
    # Initialize Kafka client
    kafka_client = KafkaClient(
        server="cthki8qfdq8asdnsm9gg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
        username="alexei.jobfinder@gmail.com",
        password="y0obC7dFiU3CJxcsCH4RwXtwEhaauf",
        topic="hyperliquid_trades"
    )

    # Initialize ETL
    etl = HyperliquidETL(kafka_client)
    
    # Load configuration
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.getcwd())),
        'private_repos/hyperliquid-python-sdk-private/config_whales.yml'
    )
    etl.load_config(config_path)

    # Start ETL process
    await etl.start()

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await etl.stop()

if __name__ == "__main__":
    asyncio.run(main())


"""
poetry run python kafka_trading/sql_trades_writer.py
"""