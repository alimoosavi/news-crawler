# broker_manager.py
import json
import logging
from dataclasses import asdict, is_dataclass
from typing import List, Optional, Type, Generator

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
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(
                f"Message delivered to '{msg.topic()}' [partition {msg.partition()}] at offset {msg.offset()}")

    def _produce(self, topic: str, items: List, cls_name: str):
        if not self.producer:
            self.logger.warning(f"Producer not initialized. Cannot produce {cls_name}.")
            return
        self.logger.info(f"Producing {len(items)} {cls_name} items to topic '{topic}'...")
        for item in items:
            try:
                if not is_dataclass(item):
                    raise ValueError(f"Expected dataclass, got {type(item)}")
                payload = json.dumps(asdict(item), default=str).encode("utf-8")
                self.producer.produce(topic, value=payload, on_delivery=self.delivery_report)
            except Exception as e:
                self.logger.error(f"Failed to produce message for {item}: {e}")

    def produce_links(self, links: List[NewsLinkData]):
        self._produce(self.config.news_links_topic, links, "links")

    def produce_content(self, content: List[NewsData]):
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

    def consume_batch(
        self,
        topic: str,
        schema: Type,
        batch_size: int = 10,
        timeout: float = 1.0,
        group_id: str = "default-group"
    ) -> Generator[List, None, None]:
        """Yield batches of messages as dataclass objects."""
        if not self.consumer:
            self.init_consumer(group_id=group_id, topics=[topic])

        buffer = []
        while True:
            msg = self.consumer.poll(timeout=timeout)
            if msg is None:
                if buffer:
                    yield buffer
                    buffer = []
                continue

            if msg.error():
                self.logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data_dict = json.loads(msg.value().decode("utf-8"))
                obj = schema(**data_dict)
                buffer.append(obj)
                if len(buffer) >= batch_size:
                    yield buffer
                    buffer = []
            except Exception as e:
                self.logger.error(f"Error decoding message: {e}")

    def commit_offsets(self):
        """Commit offsets manually after processing a batch."""
        if self.consumer:
            try:
                self.consumer.commit(asynchronous=False)
                self.logger.debug("Offsets committed")
            except Exception as e:
                self.logger.error(f"Failed to commit offsets: {e}")
