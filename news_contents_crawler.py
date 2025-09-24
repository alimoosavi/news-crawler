#!/usr/bin/env python3
"""
Scheduler for crawling news pages.
Each PageCrawler (e.g., IRNAPageCrawler) consumes its own Kafka topic stream,
fetches pages, and produces processed news content back to Kafka.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from prometheus_client import Gauge, Counter, Summary, Info, start_http_server

from config import settings
from broker_manager import BrokerManager
from crawlers.irna.pages_crawler import IRNAPageCrawler

# from crawlers.isna.pages_crawler import ISNAPageCrawler  # implement later

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PagesScheduler")

# -------------------------------
# Prometheus Metrics
# -------------------------------
# General info about the service
SERVICE_INFO = Info(
    'news_content_crawler_info',
    'General information about the news content crawler service'
)

# Gauges
CRAWL_ACTIVE = Gauge(
    'page_crawler_active',
    'Indicates if a page crawl job is currently running (1 for active, 0 for inactive)',
    ['source']
)

# Counters
CRAWLER_PROCESS_ERRORS = Counter(
    'page_crawler_errors_total',
    'Total number of errors encountered during the page crawling process',
    ['source', 'error_type']
)

# Summaries
CRAWLER_PROCESS_DURATION = Summary(
    'page_crawler_process_duration_seconds',
    'Duration of a single page crawler run (consume, process, and produce)',
    ['source']
)

# -------------------------------
# News sources and crawlers
# -------------------------------
NEWS_SOURCES = ["IRNA"]

PAGE_CRAWLERS = {
    "IRNA": IRNAPageCrawler,
    # "ISNA": ISNAPageCrawler,
}


def crawl_pages_for_source(source: str, broker_manager: BrokerManager):
    """
    Start the page crawler for a given source.
    Each crawler runs its own consume/produce loop.
    """
    CRAWL_ACTIVE.labels(source=source).set(1)
    with CRAWLER_PROCESS_DURATION.labels(source=source).time():
        try:
            logger.info(f"[{source}] Starting page crawler")
            crawler_cls = PAGE_CRAWLERS.get(source)
            if not crawler_cls:
                logger.warning(f"No page crawler implemented for {source}")
                CRAWLER_PROCESS_ERRORS.labels(source=source, error_type="no_crawler").inc()
                return

            with crawler_cls(broker_manager=broker_manager) as crawler:
                crawler.run()  # infinite consume/produce loop
        except Exception as e:
            logger.exception(f"[{source}] Page crawl failed: {e}")
            CRAWLER_PROCESS_ERRORS.labels(source=source, error_type="unexpected").inc()
        finally:
            CRAWL_ACTIVE.labels(source=source).set(0)


def main():
    # Start Prometheus metrics server
    try:
        start_http_server(settings.prom.port)
        logger.info(f"Prometheus metrics server started on port {settings.prom.port}")
    except OSError as e:
        logger.warning(f"Failed to start Prometheus server, port {settings.prom.port} might be in use: {e}")

    # Set service info
    SERVICE_INFO.info({
        'version': '1.0.0',
        'pipeline_stage': 'news_content_crawler',
        'sources': ','.join(NEWS_SOURCES)
    })

    with BrokerManager(settings.redpanda, logger) as broker_manager:
        # Run one crawler per source, each in its own thread
        with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
            for source in NEWS_SOURCES:
                executor.submit(crawl_pages_for_source, source, broker_manager)

            logger.info("All page crawlers started. Running indefinitely...")

            try:
                while True:
                    time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                logger.info("Shutting down page crawlers...")


if __name__ == "__main__":
    main()
