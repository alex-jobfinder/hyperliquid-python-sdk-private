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
from decimal import Decimal
import requests
from pydantic import BaseModel

from hyperliquid.utils import constants

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Pydantic models for data validation
class CumFunding(BaseModel):
    allTime: Decimal
    sinceOpen: Decimal
    sinceChange: Decimal


class Leverage(BaseModel):
    type: str
    value: int
    rawUsd: Optional[Decimal] = None


class Position(BaseModel):
    coin: str
    szi: Decimal
    leverage: Leverage
    entryPx: Decimal
    positionValue: Decimal
    unrealizedPnl: Decimal
    returnOnEquity: Decimal
    liquidationPx: Optional[Decimal]
    marginUsed: Decimal
    maxLeverage: int
    cumFunding: CumFunding


class AssetPositionEntry(BaseModel):
    type: str
    position: Position


class Summary(BaseModel):
    accountValue: Decimal
    totalMarginUsed: Decimal
    totalNtlPos: Decimal
    totalRawUsd: Decimal


class ClearinghouseState(BaseModel):
    user_id: str
    time: int
    marginSummary: Summary
    crossMarginSummary: Summary
    crossMaintenanceMarginUsed: Decimal
    withdrawable: Decimal
    assetPositions: List[AssetPositionEntry]


class KafkaClient:
    def __init__(
        self,
        server: str,
        username: str,
        password: str,
        topics: List[str],
        max_batch_size: int = 1000
    ):
        self.config = {
            "SERVER": server,
            "USERNAME": username,
            "PASSWORD": password,
            "TOPICS": topics
        }
        self.max_batch_size = max_batch_size
        self.producer: Optional[AIOKafkaProducer] = None
        self.last_message_time = {}  # Track last message time per topic
        self.health_check_interval = timedelta(minutes=5)

    def ensure_topics(self):
        """Ensure all Kafka topics exist, create them if they don't."""
        admin = KafkaAdminClient(
            bootstrap_servers=self.config["SERVER"],
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=self.config["USERNAME"],
            sasl_plain_password=self.config["PASSWORD"],
        )

        try:
            existing_topics = admin.list_topics()
            for topic in self.config["TOPICS"]:
                if topic not in existing_topics:
                    new_topic = NewTopic(
                        name=topic,
                        num_partitions=1,
                        replication_factor=3
                    )
                    admin.create_topics([new_topic])
                    logger.info(f"Created topic: {topic}")
                else:
                    logger.info(f"Topic {topic} already exists")
        except Exception as e:
            logger.error(f"Error ensuring topics: {e}")
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

    async def send_batch(self, messages: List[bytes], topic: str):
        """Send a batch of messages to specified Kafka topic."""
        if not messages:
            return
        
        if topic not in self.config["TOPICS"]:
            logger.error(f"Invalid topic: {topic}")
            return
        
        batch_size = sum(len(msg) for msg in messages)
        logger.info(f"Sending batch of {len(messages)} messages to topic {topic}, total size: {batch_size} bytes")
        
        try:
            await asyncio.gather(*[
                self.producer.send_and_wait(topic, msg)
                for msg in messages
            ])
            self.last_message_time[topic] = datetime.now()
            logger.info(f"Successfully sent batch to topic {topic}")
        except Exception as e:
            logger.error(f"Error sending batch to topic {topic}: {e}")
            raise

    async def health_check(self):
        """Monitor the health of the Kafka connection for all topics."""
        while True:
            await asyncio.sleep(300)  # Check every 5 minutes
            now = datetime.now()
            for topic in self.config["TOPICS"]:
                last_time = self.last_message_time.get(topic)
                if last_time and now - last_time > self.health_check_interval:
                    logger.warning(f"No messages received for topic {topic} in the last 5 minutes")

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None


class PositionsETL:
    def __init__(self, kafka_client: KafkaClient, debug_logging: bool = False):
        self.kafka_client = kafka_client
        self.info = None
        self.config = {}
        self.debug_logging = debug_logging
        self.loop = None
        self.api_url = "https://api.hyperliquid.xyz/info"

    def load_config(self, file_path: str) -> None:
        """Load configuration from YAML file."""
        try:
            with open(file_path, 'r') as file:
                self.config = yaml.safe_load(file)
                self.validate_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            self.config = {}

    def validate_config(self) -> None:
        """Validate the configuration."""
        if not self.config.get("addresses"):
            raise ValueError("Missing required config field: addresses")

    async def fetch_clearinghouse_state(self, user_id: str) -> Dict:
        """Fetch clearinghouse state for a user with debug logging."""
        try:
            payload = {
                "type": "clearinghouseState",
                "user": user_id
            }
            
            if self.debug_logging:
                logger.info(f"Sending API request for user {user_id}:")
                logger.info(json.dumps(payload, indent=2))

            response = requests.post(
                self.api_url,
                headers={"Content-Type": "application/json"},
                json=payload
            )
            response.raise_for_status()
            data = response.json()
            
            if self.debug_logging:
                logger.info(f"API Response for user {user_id}:")
                logger.info(json.dumps(data, indent=2))
            
            return data
        except Exception as e:
            logger.error(f"Error fetching clearinghouse state for user {user_id}: {e}")
            if self.debug_logging and 'response' in locals():
                logger.error(f"Response status code: {response.status_code}")
                logger.error(f"Response text: {response.text}")
            return None

    async def start(self) -> None:
        """Start the ETL process."""
        # Initialize Kafka
        self.kafka_client.ensure_topics()
        await self.kafka_client.create_producer()

        logger.info("Starting ETL process...")
        while True:
            try:
                # Process all addresses from config
                addresses = self.config.get("addresses", [])
                logger.info(f"Processing {len(addresses)} addresses")
                
                for address in addresses:
                    try:
                        data = await self.fetch_clearinghouse_state(address)
                        if data:
                            # Process positions
                            unix_time = data.get("time")
                            positions = []
                            for position_entry in data.get("assetPositions", []):
                                position = position_entry.get("position", {})
                                leverage = position.get("leverage", {})
                                cum_funding = position.get("cumFunding", {})
                                
                                enriched_position = {
                                    "user_id": address,
                                    "timestamp": datetime.now().isoformat(),
                                    "time": unix_time,
                                    # Position fields
                                    "coin": position.get("coin"),
                                    "szi": position.get("szi"),
                                    # Leverage fields
                                    "leverage_type": leverage.get("type"),
                                    "leverage_value": leverage.get("value"),
                                    # Other position fields
                                    "entry_px": position.get("entryPx"),
                                    "position_value": position.get("positionValue"),
                                    "unrealized_pnl": position.get("unrealizedPnl"),
                                    "return_on_equity": position.get("returnOnEquity"),
                                    "liquidation_px": position.get("liquidationPx"),
                                    "margin_used": position.get("marginUsed"),
                                    "max_leverage": position.get("maxLeverage"),
                                    # Funding fields
                                    "funding_all_time": cum_funding.get("allTime"),
                                    "funding_since_open": cum_funding.get("sinceOpen"),
                                    "funding_since_change": cum_funding.get("sinceChange")
                                }
                                positions.append(json.dumps(enriched_position).encode("utf-8"))

                            # Process margin
                            if data.get("marginSummary"):
                                margin_data = {
                                    "user_id": address,
                                    "timestamp": datetime.now().isoformat(),
                                    "time": unix_time,
                                    # Regular margin summary
                                    "account_value": data["marginSummary"]["accountValue"],
                                    "total_ntl_pos": data["marginSummary"]["totalNtlPos"],
                                    "total_raw_usd": data["marginSummary"]["totalRawUsd"],
                                    "total_margin_used": data["marginSummary"]["totalMarginUsed"],
                                    # Cross margin summary (from root level)
                                    "cross_account_value": data.get("crossMarginSummary", {}).get("accountValue"),
                                    "cross_total_ntl_pos": data.get("crossMarginSummary", {}).get("totalNtlPos"),
                                    "cross_total_raw_usd": data.get("crossMarginSummary", {}).get("totalRawUsd"),
                                    "cross_total_margin_used": data.get("crossMarginSummary", {}).get("totalMarginUsed"),
                                    # Additional margin fields (from root level)
                                    "cross_maintenance_margin_used": data.get("crossMaintenanceMarginUsed"),
                                    "withdrawable": data.get("withdrawable")
                                }
                                await self.kafka_client.send_batch([json.dumps(margin_data).encode("utf-8")], "hyperliquid_margin")

                            # Send positions batch
                            if positions:
                                await self.kafka_client.send_batch(positions, "hyperliquid_positions")
                    except Exception as e:
                        logger.error(f"Error processing address {address}: {e}")
                        continue

                # Wait before next update
                await asyncio.sleep(60)  # Update every minute
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(5)  # Wait before retry

    async def stop(self) -> None:
        """Stop the ETL process."""
        logger.info("Shutting down gracefully...")
        await self.kafka_client.stop()


async def main():
    # Initialize Kafka client with both topics
    kafka_client = KafkaClient(
        server="cthki8qfdq8asdnsm9gg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
        username="alexei.jobfinder@gmail.com",
        password="y0obC7dFiU3CJxcsCH4RwXtwEhaauf",
        topics=["hyperliquid_positions", "hyperliquid_margin"]
    )

    # Initialize ETL with debug logging enabled
    etl = PositionsETL(kafka_client, debug_logging=True)
    
    # Load configuration
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.getcwd())),
        'private_repos/hyperliquid-python-sdk-private/config_whales.yml'
    )
    etl.load_config(config_path)

    try:
        await etl.start()
    except KeyboardInterrupt:
        await etl.stop()

if __name__ == "__main__":
    asyncio.run(main())


"""
poetry run python kafka_trading/positions_writer.py

curl -X POST https://api.hyperliquid.xyz/info -H "Content-Type: application/json" -d '{"type": "clearinghouseState", "user": "0xa44481a6454f4FD0899e261Aa941323f2b11A09b"}'
"""