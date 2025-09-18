#!/usr/bin/env python3
"""
Scheduler for crawling news links with Kafka
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.background import BackgroundScheduler

from broker_manager import BrokerManager
from cache_manager import CacheManager
from config import settings
from crawlers.irna.links_crawler import IRNALinksCrawler

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("LinksScheduler")

# -------------------------------
# News sources and crawlers
# -------------------------------
NEWS_SOURCES = ["IRNA"]

LINK_CRAWLERS = {
    "IRNA": IRNALinksCrawler
}


# -------------------------------
# Cache Manager
# -------------------------------
def get_cache_manager() -> CacheManager:
    cache_manager = CacheManager(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db
    )
    return cache_manager


# -------------------------------
# Crawl links for a single source
# -------------------------------
def crawl_links_for_source(source: str, broker_manager: BrokerManager, cache_manager: CacheManager):
    """
    Run the link crawler for a single source, pushing results to Kafka.
    """
    try:
        logger.info(f"[{source}] Starting link crawl")
        last_link = cache_manager.get_last_link(source)

        # Inject broker manager into crawler
        crawler = LINK_CRAWLERS[source](broker_manager)
        fresh_last_link = crawler.crawl_recent_links(last_link)

        logger.info(f"[{source}] Link crawl finished")
        if fresh_last_link is not None:
            cache_manager.update_last_link(source, fresh_last_link)

    except Exception as e:
        logger.exception(f"[{source}] Link crawl failed: {e}")


# -------------------------------
# Schedule crawling
# -------------------------------
def schedule_news_links(broker_manager: BrokerManager):
    """
    Schedule crawling for all configured news sources using a thread pool.
    """
    cache_manager = get_cache_manager()
    with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
        for source in NEWS_SOURCES:
            executor.submit(crawl_links_for_source, source, broker_manager, cache_manager)


# -------------------------------
# Main
# -------------------------------
def main():
    # Initialize broker manager and create topics if missing
    with BrokerManager(settings.redpanda, logger) as broker_manager:
        broker_manager.create_topics()  # ✅ ensure topics exist

        # Start scheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(schedule_news_links, 'interval', seconds=15, args=[broker_manager])
        scheduler.start()
        logger.info("Scheduler started for news links")

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            logger.info("Scheduler shut down gracefully")


if __name__ == "__main__":
    main()
