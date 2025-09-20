#!/usr/bin/env python3
"""
Scheduler for generating embeddings from news contents using LangChain + OpenAI.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from langchain.embeddings import OpenAIEmbeddings

from config import settings
from broker_manager import BrokerManager
from schema import NewsData

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("EmbeddingsScheduler")

# OpenAI Embeddings model
EMBEDDINGS_MODEL = "text-embedding-3-large"
openai_api_key = settings.openai.api_key
embeddings_model = OpenAIEmbeddings(model=EMBEDDINGS_MODEL, openai_api_key=openai_api_key)

# Kafka topic for news content
NEWS_CONTENT_TOPIC = settings.redpanda.news_content_topic

BATCH_SIZE = 10


def process_news_batch(batch: list[NewsData]):
    """
    Convert each news item to a single string (title + body) and generate embeddings.
    """
    try:
        texts = [f"{news.title}\n{news.content}" for news in batch]
        vectors = embeddings_model.embed_documents(texts)
        for text, vec in zip(texts, vectors):
            logger.info(f"Text: {text[:50]}... | Embedding length: {len(vec)}")
    except Exception as e:
        logger.exception(f"Error generating embeddings: {e}")


def consume_and_embed(broker_manager: BrokerManager):
    """
    Consume batches of news content from Kafka and generate embeddings.
    """
    group_id = "embeddings_workers"
    logger.info(f"Starting consumer for topic '{NEWS_CONTENT_TOPIC}' with group '{group_id}'...")

    for batch in broker_manager.consume_batch(
        topic=NEWS_CONTENT_TOPIC,
        schema=NewsData,
        batch_size=BATCH_SIZE,
        group_id=group_id
    ):
        if not batch:
            time.sleep(1)
            continue

        logger.info(f"Received {len(batch)} news items")
        process_news_batch(batch)
        broker_manager.commit_offsets()


def main():
    with BrokerManager(settings.redpanda, logger) as broker_manager:
        # Run consumer in a separate thread
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(consume_and_embed, broker_manager)

            logger.info("Embedding consumer started. Running indefinitely...")
            try:
                while True:
                    time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                logger.info("Shutting down embedding consumer...")


if __name__ == "__main__":
    main()
