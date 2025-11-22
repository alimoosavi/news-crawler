# broker_manager.py
import json
import logging
from dataclasses import asdict, is_dataclass
# FIX: Import datetime and timezone for robust deserialization
from datetime import datetime, timezone
from typing import List, Optional, Type, Generator, Union

from confluent_kafka import Producer, Consumer, KafkaException, admin

from config import RedpandaConfig
from schema import NewsLinkData, NewsData


class BrokerManager:
    """
    Redpanda broker manager with produce/consume functionality
    and topic creation support (Kafka-compatible API).
    """

    def __init__(self, redpanda_config: RedpandaConfig, logger: logging.Logger):
        self.config = redpanda_config
        self.logger = logger
        self.producer: Optional[Producer] = None
        self.consumer: Optional[Consumer] = None
        self.admin_client: Optional[admin.AdminClient] = None

    def __enter__(self):
        try:
            # Use the external port to connect from outside the Docker network
            bootstrap_servers = f"localhost:{self.config.external_port}"
            self.producer = Producer({'bootstrap.servers': bootstrap_servers})
            self.admin_client = admin.AdminClient({'bootstrap.servers': bootstrap_servers})
            self.logger.info("Connected to Redpanda via Kafka API (producer + admin client).")
            return self
        except KafkaException as e:
            self.logger.error(f"Error connecting to Redpanda: {e}")
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if self.producer:
            self.logger.info("Flushing outstanding messages...")
            self.producer.flush()
            self.logger.info("Producer closed.")
        if self.consumer:
            self.consumer.close()
            self.logger.info("Consumer closed.")
        # Returning False here allows exceptions to propagate if they occur
        return False

    # -----------------------
    # Topic Management
    # -----------------------
    def create_topics(self):
        if not self.admin_client:
            self.logger.warning("Admin client not initialized. Cannot create topics.")
            return

        topics = [
            admin.NewTopic(self.config.news_links_topic, num_partitions=1, replication_factor=1),
            admin.NewTopic(self.config.news_content_topic, num_partitions=1, replication_factor=1)
        ]

        fs = self.admin_client.create_topics(topics, request_timeout=10)
        for topic, f in fs.items():
            try:
                f.result()
                self.logger.info(f"Topic '{topic}' created successfully.")
            except KafkaException as e:
                if "TopicAlreadyExists" in str(e):
                    self.logger.info(f"Topic '{topic}' already exists.")
                else:
                    self.logger.error(f"Failed to create topic '{topic}': {e}")

    # -----------------------
    # Producer Methods
    # -----------------------
    def delivery_report(self, err, msg):
        """Callback function for message delivery status."""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(
                f"Message delivered to '{msg.topic()}' [partition {msg.partition()}] at offset {msg.offset()}")

    def _produce(self, topic: str, items: List, cls_name: str):
        """Generic method to produce a list of dataclass objects to a Kafka topic."""
        if not self.producer:
            self.logger.warning(f"Producer not initialized. Cannot produce {cls_name}.")
            return

        self.logger.info(f"Producing {len(items)} {cls_name} items to topic '{topic}'...")
        for item in items:
            try:
                if not is_dataclass(item):
                    raise ValueError(f"Expected dataclass, got {type(item)}")

                # Convert dataclass to JSON, ensuring datetime objects are converted to strings
                payload = json.dumps(asdict(item), default=str).encode("utf-8")

                self.producer.produce(topic, value=payload, on_delivery=self.delivery_report)
            except Exception as e:
                self.logger.error(f"Failed to produce message for {item}: {e}")

    def produce_links(self, links: List[NewsLinkData]):
        """Produce a batch of NewsLinkData objects."""
        self._produce(self.config.news_links_topic, links, "links")

    def produce_content(self, content: List[NewsData]):
        """Produce a batch of NewsData objects."""
        self._produce(self.config.news_content_topic, content, "content")

    # -----------------------
    # Consumer Methods
    # -----------------------
    def init_consumer(self, group_id: str, topics: List[str]):
        """Initialize and subscribe consumer once."""
        bootstrap_servers = f"localhost:{self.config.external_port}"
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # manual commit
        })
        self.consumer.subscribe(topics)
        self.logger.info(f"Consumer subscribed to {topics} with group '{group_id}'.")

    def _deserialize_datetime(self, data_dict: dict, field_name: str = 'published_datetime') -> datetime:
        """Helper to convert an ISO 8601 string back to a timezone-aware datetime object (UTC)."""
        dt_str = data_dict[field_name]
        try:
            # 1. Parse from ISO format string
            dt_obj = datetime.fromisoformat(dt_str)

            # 2. Ensure timezone awareness (set naive dates to UTC for consistency)
            if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            else:
                # Convert to UTC for consistent comparison across the pipeline
                dt_obj = dt_obj.astimezone(timezone.utc)

            return dt_obj
        except Exception as e:
            self.logger.error(f"Failed to deserialize datetime string '{dt_str}': {e}")
            # Return current UTC time as a safe fallback if parsing fails
            return datetime.now(timezone.utc)

    def consume_batch(
            self,
            topic: str,
            schema: Type[Union[NewsData, NewsLinkData]],  # Updated Type hint for clarity
            batch_size: int = 10,
            timeout: float = 1.0,
            group_id: str = "default-group"
    ) -> Generator[List[Union[NewsData, NewsLinkData]], None, None]:  # Updated Type hint for clarity
        """
        Yield batches of messages as dataclass objects.
        Initializes the consumer if it hasn't been done yet.
        """
        if not self.consumer:
            self.init_consumer(group_id=group_id, topics=[topic])

        buffer = []
        while True:
            # Poll for a single message
            msg = self.consumer.poll(timeout=timeout)

            if msg is None:
                # If a timeout occurs, yield any messages in the buffer before continuing
                if buffer:
                    yield buffer
                    buffer = []
                continue

            if msg.error():
                self.logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data_dict = json.loads(msg.value().decode("utf-8"))

                # CORE FIX: Check for the date field and convert its type from string to datetime
                # This ensures the dataclass constructor receives the correct type.
                if issubclass(schema, (NewsData, NewsLinkData)) and 'published_datetime' in data_dict:
                    data_dict['published_datetime'] = self._deserialize_datetime(data_dict)

                # Now the schema can be constructed with the correct datetime object
                obj = schema(**data_dict)
                buffer.append(obj)

                # Yield the batch when buffer size is reached
                if len(buffer) >= batch_size:
                    yield buffer
                    buffer = []
            except Exception as e:
                self.logger.error(f"Error decoding or constructing dataclass object: {e}")

    def commit_offsets(self):
        """
        Commit offsets manually after processing a batch.
        Includes defensive logging for the common 'No offset stored' error (code -168).
        """
        if self.consumer:
            try:
                # Commit sync, which will raise KafkaException if the commit fails
                self.consumer.commit(asynchronous=False)
                self.logger.debug("Offsets committed")
            except KafkaException as e:
                # Specifically handle the _NO_OFFSET code to prevent error logs on harmless timeouts.
                _NO_OFFSET = -168

                if e.args[0].code() == _NO_OFFSET:
                    # This warning happens when commit is called but no new message was successfully polled/processed
                    self.logger.warning("Attempted to commit offsets when no commit-ready offsets were available.")
                else:
                    self.logger.error(f"Failed to commit offsets: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error during offset commit: {e}")