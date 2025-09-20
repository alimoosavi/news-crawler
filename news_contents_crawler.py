#!/usr/bin/env python3
"""
Scheduler for crawling news pages.
Each PageCrawler (e.g., IRNAPageCrawler) consumes its own Kafka topic stream,
fetches pages, and produces processed news content back to Kafka.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from config import settings
from broker_manager import BrokerManager
from crawlers.irna.pages_crawler import IRNAPageCrawler
# from crawlers.isna.pages_crawler import ISNAPageCrawler  # implement later

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PagesScheduler")

# Sources & their crawlers
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
    try:
        logger.info(f"[{source}] Starting page crawler")
        crawler_cls = PAGE_CRAWLERS.get(source)
        if not crawler_cls:
            logger.warning(f"No page crawler implemented for {source}")
            return

        with crawler_cls(broker_manager=broker_manager) as crawler:
            crawler.run()  # infinite consume/produce loop
    except Exception as e:
        logger.exception(f"[{source}] Page crawl failed: {e}")


def main():
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
