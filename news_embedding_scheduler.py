#!/usr/bin/env python3
"""
News Embedding Scheduler (Real-time)

Consumes news content from Redpanda, generates semantic embeddings in batches,
and persists to Qdrant vector database.

Uses embedding provider configured in .env (OpenAI or Ollama).
"""
import logging
import time
from prometheus_client import Gauge, Counter, Summary, Info, start_http_server

from broker_manager import BrokerManager
from config import settings
from schema import NewsData
from vector_db_manager import VectorDBManager

# Logging
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("NewsProcessor")

# Metrics
SERVICE_INFO = Info('news_processor_info', 'Service information')
PROCESSING_BATCH_SIZE = Gauge('processor_processing_batch_size', 'Batch size')
CONTENT_CONSUMED_TOTAL = Counter('processor_content_consumed_total', 'Articles consumed')
ARTICLES_PERSISTED_TOTAL = Counter('processor_articles_persisted_total', 'Articles persisted')
PROCESSOR_ERRORS_TOTAL = Counter('processor_errors_total', 'Processing errors', ['error_type'])
BATCH_PROCESSING_DURATION_SECONDS = Summary('processor_batch_processing_duration_seconds', 'Batch processing time')

BROKER_GROUP_ID = "news-embedding-processor"


def main():
    """Main processing loop"""
    # Start Prometheus
    try:
        start_http_server(settings.prom.port)
        logger.info(f"Prometheus metrics server started on port {settings.prom.port}")
    except OSError as e:
        logger.warning(f"Failed to start Prometheus server: {e}")

    # Set service info
    SERVICE_INFO.info({
        'version': '1.0.0',
        'pipeline_stage': 'news_processor',
        'embedding_provider': settings.embedding.provider,
        'embedding_model': (
            settings.embedding.openai_model if settings.embedding.provider == 'openai'
            else settings.embedding.ollama_model
        ),
        'embedding_dimension': str(settings.embedding.embedding_dim)
    })

    # Initialize VectorDBManager
    try:
        vector_db_manager = VectorDBManager(
            qdrant_config=settings.qdrant,
            embedding_config=settings.embedding,
            logger=logger
        )
        logger.info(f"✅ VectorDBManager ready with {settings.embedding.provider}")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize VectorDBManager: {e}")
        return

    # Consume and process
    with BrokerManager(settings.redpanda, logger) as broker:
        for batch in broker.consume_batch(
                topic=settings.redpanda.news_content_topic,
                schema=NewsData,
                batch_size=50,
                timeout=2.0,
                group_id=BROKER_GROUP_ID,
        ):
            batch_count = len(batch)
            PROCESSING_BATCH_SIZE.set(batch_count)

            if not batch:
                time.sleep(1)
                continue

            CONTENT_CONSUMED_TOTAL.inc(batch_count)

            with BATCH_PROCESSING_DURATION_SECONDS.time():
                try:
                    persisted_count = vector_db_manager.persist_news_batch(batch)
                    ARTICLES_PERSISTED_TOTAL.inc(persisted_count)

                    if persisted_count > 0:
                        logger.info(f"✅ Persisted {persisted_count} articles")
                        broker.commit_offsets()
                    else:
                        logger.warning("⚠️  0 articles persisted")

                except Exception as e:
                    logger.exception(f"❌ Batch processing error: {e}")
                    PROCESSOR_ERRORS_TOTAL.labels(error_type="batch_processing_error").inc()
                finally:
                    PROCESSING_BATCH_SIZE.set(0)


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("News Embedding Processor Starting")
    logger.info(f"Provider: {settings.embedding.provider}")
    logger.info(f"Model: {settings.embedding.openai_model if settings.embedding.provider == 'openai' else settings.embedding.ollama_model}")
    logger.info(f"Dimension: {settings.embedding.embedding_dim}")
    logger.info("=" * 80)
    main()