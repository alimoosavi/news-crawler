#!/usr/bin/env python3
"""
Scheduler for crawling news pages.
Refactored: Uses a single consumer to dispatch links to proper PageCrawler utility classes.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any

from prometheus_client import Gauge, Counter, Summary, Info, start_http_server

from config import settings
from broker_manager import BrokerManager
from schema import NewsLinkData, NewsData
from collectors.irna.pages_collector import IRNAPageCollector
from news_publishers import IRNA, DONYAYE_EQTESAD
from collectors.donyaye_eghtesad.pages_collector import DonyaEqtesadPageCollector

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PagesScheduler")

# -------------------------------
# Prometheus Metrics (Unchanged)
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
# News sources and collectors
# -------------------------------
BROKER_GROUP_ID = "news-content-crawler"

# Instantiate collectors ONCE at startup to reuse resources (especially important for Playwright)
CRAWLER_INSTANCES = {
    IRNA: IRNAPageCollector(fetch_timeout=settings.crawler.retry_delay),
    DONYAYE_EQTESAD: DonyaEqtesadPageCollector(fetch_timeout=settings.crawler.retry_delay)
}

# Use the keys for the list of sources
NEWS_SOURCES = list(CRAWLER_INSTANCES.keys())


def dispatch_and_crawl(source: str, links: List[NewsLinkData]) -> List[NewsData]:
    """
    Calls the unified crawl_batch method on the specific crawler instance.
    Returns a list of NewsData objects.
    """
    crawler = CRAWLER_INSTANCES.get(source)
    if not crawler:
        logger.warning(f"No crawler instance found for source: {source}")
        CRAWLER_PROCESS_ERRORS.labels(source=source, error_type="no_instance").inc()
        return []

    CRAWL_ACTIVE.labels(source=source).set(1)

    # Run the crawl_batch and time it for Prometheus
    with CRAWLER_PROCESS_DURATION.labels(source=source).time():
        try:
            # crawl_batch returns Dict[str, NewsData], we convert it to List[NewsData]
            results_dict = crawler.crawl_batch(links)
            logger.info(f"[{source}] Crawled {len(results_dict)} of {len(links)} links.")
            return list(results_dict.values())
        except Exception as e:
            logger.exception(f"[{source}] Crawl batch failed: {e}")
            CRAWLER_PROCESS_ERRORS.labels(source=source, error_type="crawl_error").inc()
            return []
        finally:
            # Set this back to 0 immediately as the thread finishes its job
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
        logger.info(f"Scheduler starting single consumer on topic '{settings.redpanda.news_links_topic}'...")

        # Max workers should be high enough to handle all sources + extra concurrency
        max_workers = len(NEWS_SOURCES) * 2

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            try:
                # Centralized Kafka consumer loop
                for batch in broker_manager.consume_batch(
                        settings.redpanda.news_links_topic,
                        NewsLinkData,
                        batch_size=settings.crawler.bulk_size,
                        group_id=BROKER_GROUP_ID
                ):
                    if not batch:
                        time.sleep(1)
                        continue

                    logger.info(f"Consumed batch of {len(batch)} links. Dispatching...")

                    # 1. Group links by source
                    links_by_source: Dict[str, List[NewsLinkData]] = {}
                    for link_data in batch:
                        links_by_source.setdefault(link_data.source, []).append(link_data)

                    # 2. Dispatch work to threads using the executor
                    all_futures = []
                    for source, links_group in links_by_source.items():
                        future = executor.submit(dispatch_and_crawl, source, links_group)
                        all_futures.append(future)

                    # 3. Collect results from all threads
                    all_news_data: List[NewsData] = []
                    for future in all_futures:
                        # get() blocks and retrieves the result (List[NewsData]) or raises an exception
                        news_data_list = future.result()
                        all_news_data.extend(news_data_list)

                    # 4. Produce all collected content to Kafka
                    if all_news_data:
                        broker_manager.produce_content(all_news_data)
                        logger.info(f"Produced total of {len(all_news_data)} news contents to Kafka.")

                    # 5. Commit offsets after successful processing of the entire batch
                    broker_manager.commit_offsets()

            except (KeyboardInterrupt, SystemExit):
                logger.info("Scheduler shutting down...")
            except Exception as e:
                logger.exception(f"Unexpected fatal error in scheduler: {e}")


if __name__ == "__main__":
    main()
