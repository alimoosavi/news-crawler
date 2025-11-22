#!/usr/bin/env python3
"""
News Processor
Consumes news content from Redpanda, generates semantic embeddings in BATCHES,
and persists enriched data to a vector database.
"""
import logging
import time

from prometheus_client import Gauge, Counter, Summary, Info, start_http_server

# Assuming these modules are available in your environment
from broker_manager import BrokerManager
from config import settings
from schema import NewsData
# Import the fixed manager
from vector_db_manager import VectorDBManager

# --------------------
# Logging setup
# --------------------
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("NewsProcessor")

# --------------------
# Prometheus Metrics
# --------------------
SERVICE_INFO = Info(
    'news_processor_info',
    'General information about the news processor service'
)
PROCESSING_BATCH_SIZE = Gauge(
    'processor_processing_batch_size',
    'Number of news articles in the current batch being processed'
)
CONTENT_CONSUMED_TOTAL = Counter(
    'processor_content_consumed_total',
    'Total number of news articles consumed from the Kafka topic'
)
ARTICLES_PERSISTED_TOTAL = Counter(
    'processor_articles_persisted_total',
    'Total number of news articles successfully persisted to the vector database'
)
PROCESSOR_ERRORS_TOTAL = Counter(
    'processor_errors_total',
    'Total number of errors encountered during news processing',
    ['error_type']
)
BATCH_PROCESSING_DURATION_SECONDS = Summary(
    'processor_batch_processing_duration_seconds',
    'Time taken to process a full batch of news articles'
)

BROKER_GROUP_ID = "news-embedding-processor"


# --------------------
# Main Loop
# --------------------
def main():
    """
    Main function to consume, process, and persist enriched news data in batches.
    """
    # Start the scheduler's Prometheus server on its designated port (e.g., 9090)
    try:
        start_http_server(settings.prom.port)
        logger.info(f"Prometheus metrics server started on port {settings.prom.port}")
    except OSError as e:
        logger.warning(f"Failed to start Prometheus server, port {settings.prom.port} might be in use: {e}")

    SERVICE_INFO.info({
        'version': '1.0.0',
        'pipeline_stage': 'news_processor'
    })

    try:
        vector_db_manager = VectorDBManager(
            qdrant_config=settings.qdrant,
            openai_config=settings.openai,
            logger=logger
        )
        logger.info("VectorDBManager initialized successfully using OpenAI API.")
    except Exception as e:
        logger.critical(f"Failed to initialize VectorDBManager: {e}")
        return

    with BrokerManager(settings.redpanda, logger) as broker:

        # Consumer loop
        for batch in broker.consume_batch(
                topic=settings.redpanda.news_content_topic,
                schema=NewsData,
                batch_size=50,  # This batch_size is now the embedding batch size too
                timeout=2.0,
                group_id=BROKER_GROUP_ID,
        ):
            batch_count = len(batch)
            PROCESSING_BATCH_SIZE.set(batch_count)

            if not batch:
                time.sleep(1)
                continue

            CONTENT_CONSUMED_TOTAL.inc(batch_count)  # Increment consumption metric immediately

            with BATCH_PROCESSING_DURATION_SECONDS.time():
                persisted_count = 0
                try:
                    # CORE LOGIC CHANGE: Persist the entire batch in one go
                    persisted_count = vector_db_manager.persist_news_batch(batch)

                    ARTICLES_PERSISTED_TOTAL.inc(persisted_count)

                    if persisted_count > 0:
                        logger.info(f"Processed and BATCH-persisted {persisted_count} news articles.")
                        broker.commit_offsets()
                    else:
                        logger.warning("Batch processing finished but 0 articles were persisted.")


                except Exception as e:
                    logger.exception(
                        f"Critical error during batch processing. Error: {e}")
                    PROCESSOR_ERRORS_TOTAL.labels(error_type="batch_processing_error").inc()
                finally:
                    PROCESSING_BATCH_SIZE.set(0)


if __name__ == "__main__":
    main()
