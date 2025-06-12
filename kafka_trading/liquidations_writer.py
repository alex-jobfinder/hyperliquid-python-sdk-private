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
import glob
from pathlib import Path

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
        except Exception as e:
            logger.error(f"Error ensuring topic: {e}")
        finally:
            admin.close()

    async def create_producer(self):
        """Create and start the Kafka producer."""
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

    async def send_message(self, message: Dict):
        """Send a single message to Kafka."""
        if not message:
            return
        
        try:
            msg_bytes = json.dumps(message).encode("utf-8")
            await self.producer.send_and_wait(self.config["TOPIC"], msg_bytes)
            self.last_message_time = datetime.now()
            logger.debug(f"Sent message to topic {self.config['TOPIC']}")
        except Exception as e:
            logger.error(f"Error sending message to topic {self.config['TOPIC']}: {e}")
            raise

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            await self.producer.stop()
            self.producer = None

class LiquidationsETL:
    def __init__(self, kafka_client: KafkaClient, debug_logging: bool = False):
        self.kafka_client = kafka_client
        self.debug_logging = debug_logging
        self.data_dir = os.path.expanduser("~/hl/data")
        self.misc_events_dir = os.path.join(self.data_dir, "misc_events/hourly")
        self.processed_files = set()

    def get_new_files(self) -> List[str]:
        """Get list of new misc_events files that haven't been processed."""
        pattern = os.path.join(self.misc_events_dir, "*", "*")
        all_files = glob.glob(pattern)
        new_files = [f for f in all_files if f not in self.processed_files]
        return sorted(new_files)  # Sort to process in chronological order

    async def process_file(self, file_path: str):
        """Process a single misc_events file for liquidation events."""
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    try:
                        event = json.loads(line)
                        if "inner" in event and "LedgerUpdate" in event["inner"]:
                            ledger_update = event["inner"]["LedgerUpdate"]
                            if "delta" in ledger_update and "Liquidation" in ledger_update["delta"]:
                                liquidation = ledger_update["delta"]["Liquidation"]
                                enriched_liquidation = {
                                    "timestamp": event["time"],
                                    "hash": event["hash"],
                                    "users": ledger_update["users"],
                                    "liquidated_ntl_pos": liquidation["liquidatedNtlPos"],
                                    "account_value": liquidation["accountValue"],
                                    "leverage_type": liquidation["leverageType"],
                                    "liquidated_positions": [
                                        {
                                            "coin": pos["coin"],
                                            "size": pos["szi"]
                                        }
                                        for pos in liquidation["liquidatedPositions"]
                                    ]
                                }
                                if self.debug_logging:
                                    logger.info(f"Processing liquidation: {json.dumps(enriched_liquidation, indent=2)}")
                                await self.kafka_client.send_message(enriched_liquidation)
                    except json.JSONDecodeError:
                        logger.error(f"Error decoding JSON line in file {file_path}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing line in file {file_path}: {e}")
                        continue
            
            self.processed_files.add(file_path)
            logger.info(f"Processed file: {file_path}")
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

    async def start(self):
        """Start the ETL process."""
        logger.info("Starting Liquidations ETL process...")
        
        # Initialize Kafka
        self.kafka_client.ensure_topic()
        await self.kafka_client.create_producer()

        while True:
            try:
                # Get new files
                new_files = self.get_new_files()
                if new_files:
                    logger.info(f"Found {len(new_files)} new files to process")
                    for file_path in new_files:
                        await self.process_file(file_path)
                
                # Wait before next check
                await asyncio.sleep(60)  # Check for new files every minute
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(5)  # Wait before retry

    async def stop(self):
        """Stop the ETL process."""
        logger.info("Shutting down gracefully...")
        await self.kafka_client.stop()

async def main():
    # Initialize Kafka client
    kafka_client = KafkaClient(
        server="cthki8qfdq8asdnsm9gg.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
        username="alexei.jobfinder@gmail.com",
        password="y0obC7dFiU3CJxcsCH4RwXtwEhaauf",
        topic="hyperliquid_liquidations"
    )

    # Initialize ETL with debug logging enabled
    etl = LiquidationsETL(kafka_client, debug_logging=True)

    try:
        await etl.start()
    except KeyboardInterrupt:
        await etl.stop()

if __name__ == "__main__":
    asyncio.run(main())

"""
# First run the node with misc events enabled:
~/hl-visor run-non-validator --write-misc-events

# Then in another terminal, run this script:
poetry run python kafka_trading/liquidations_writer.py
""" 