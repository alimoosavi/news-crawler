import json
import logging
from dataclasses import asdict, is_dataclass
from typing import List, Optional

from confluent_kafka import Producer, Consumer, KafkaException, admin

from config import KafkaConfig
from schema import NewsLinkData, NewsData


class BrokerManager:
    """
    Kafka broker manager with produce/consume functionality
    and topic creation support.
    """

    def __init__(self, kafka_config: KafkaConfig, logger: logging.Logger):
        self.kafka_config = kafka_config
        self.logger = logger
        self.producer: Optional[Producer] = None
        self.consumer: Optional[Consumer] = None
        self.admin_client: Optional[admin.AdminClient] = None

    def __enter__(self):
        try:
            self.producer = Producer({'bootstrap.servers': self.kafka_config.advertised_listeners})
            self.admin_client = admin.AdminClient({'bootstrap.servers': self.kafka_config.advertised_listeners})
            self.logger.info("Confluent Kafka producer and admin client connected.")
            return self
        except KafkaException as e:
            self.logger.error(f"Error connecting to Kafka: {e}")
            self.producer = None
            self.admin_client = None
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if self.producer:
            self.logger.info("Flushing outstanding messages...")
            self.producer.flush()
            self.logger.info("Confluent Kafka producer closed.")
        if self.consumer:
            self.consumer.close()
            self.logger.info("Confluent Kafka consumer closed.")
        return False

    # -----------------------
    # Topic Management
    # -----------------------
    def create_topics(self):
        """
        Create Kafka topics if they do not exist already.
        """
        if not self.admin_client:
            self.logger.warning("Admin client not initialized. Cannot create topics.")
            return

        topics = [
            admin.NewTopic(
                topic=self.kafka_config.news_links_topic,
                num_partitions=1,
                replication_factor=self.kafka_config.offsets_topic_replication_factor
            ),
            admin.NewTopic(
                topic=self.kafka_config.news_content_topic,
                num_partitions=1,
                replication_factor=self.kafka_config.offsets_topic_replication_factor
            )
        ]

        fs = self.admin_client.create_topics(topics, request_timeout=10)

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.logger.info(f"Topic '{topic}' created successfully.")
            except KafkaException as e:
                if "TopicAlreadyExistsError" in str(e):
                    self.logger.info(f"Topic '{topic}' already exists. Skipping creation.")
                else:
                    self.logger.error(f"Failed to create topic '{topic}': {e}")

    # -----------------------
    # Producer Methods
    # -----------------------
    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info(f"Message delivered to topic '{msg.topic()}' at offset {msg.offset()}")

    def produce_links(self, links: List[NewsLinkData]):
        if not self.producer:
            self.logger.warning("Producer not initialized. Cannot produce links.")
            return
        topic = self.kafka_config.news_links_topic
        self.logger.info(f"Producing {len(links)} links to topic '{topic}'...")
        for link in links:
            try:
                if not is_dataclass(link):
                    raise ValueError("produce_links expects NewsLinkData instances")
                payload = json.dumps(asdict(link), default=str).encode("utf-8")
                self.producer.produce(topic, value=payload, on_delivery=self.delivery_report)
            except Exception as e:
                self.logger.error(f"Failed to produce message for {link}: {e}")

    def produce_content(self, content: List[NewsData]):
        if not self.producer:
            self.logger.warning("Producer not initialized. Cannot produce content.")
            return
        topic = self.kafka_config.news_content_topic
        self.logger.info(f"Producing {len(content)} content items to topic '{topic}'...")
        for item in content:
            try:
                if not is_dataclass(item):
                    raise ValueError("produce_content expects NewsData instances")
                payload = json.dumps(asdict(item), default=str).encode("utf-8")
                self.producer.produce(topic, value=payload, on_delivery=self.delivery_report)
            except Exception as e:
                self.logger.error(f"Failed to produce message for {item}: {e}")

    # -----------------------
    # Consumer Methods
    # -----------------------
    def consume_messages(self, topic: str, schema: str = "link"):
        self.logger.info(f"Starting consumer for topic '{topic}'...")
        try:
            self.consumer = Consumer({
                'bootstrap.servers': self.kafka_config.advertised_listeners,
                'group.id': 'my-consumer-group',
                'auto.offset.reset': 'earliest'
            })
            self.consumer.subscribe([topic])
            cls = NewsLinkData if schema == "link" else NewsData

            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        break
                try:
                    data_dict = json.loads(msg.value().decode("utf-8"))
                    data_obj = cls(**data_dict)
                    self.logger.info(f"Received {cls.__name__}: {data_obj}")
                except Exception as e:
                    self.logger.error(f"Error decoding message: {e}")
        except Exception as e:
            self.logger.error(f"Error consuming messages: {e}")
