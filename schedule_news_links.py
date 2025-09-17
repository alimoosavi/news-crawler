#!/usr/bin/env python3
"""
Scheduler for crawling news links
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.background import BackgroundScheduler

# Import cache manager
from cache_manager import CacheManager
# Import your settings and crawlers
from config import settings
from crawlers.irna.links_crawler import IRNALinksCrawler
from database_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("LinksScheduler")

NEWS_SOURCES = ["IRNA"]

LINK_CRAWLERS = {
    "IRNA": IRNALinksCrawler
}


def get_db_manager() -> DatabaseManager:
    return DatabaseManager(
        host=settings.database.host,
        port=settings.database.port,
        db_name=settings.database.db_name,
        user=settings.database.user,
        password=settings.database.password,
        min_conn=settings.database.min_conn,
        max_conn=settings.database.max_conn
    )


def get_cache_manager() -> CacheManager:
    cache_manager = CacheManager()
    cache_manager.load_cache()
    return cache_manager


def crawl_links_for_source(source: str, db_manager: DatabaseManager, cache_manager: CacheManager):
    try:
        logger.info(f"[{source}] Starting link crawl")
        last_link = cache_manager.get_last_link(source)
        crawler = LINK_CRAWLERS[source](db_manager=db_manager, headless=True)
        fresh_last_link = crawler.crawl_recent_links(last_link)
        logger.info(f"[{source}] Link crawl finished")
        cache_manager.update_last_link(source, fresh_last_link)
    except Exception as e:
        logger.exception(f"[{source}] Link crawl failed: {e}")


def schedule_news_links():
    db_manager = get_db_manager()
    cache_manager = get_cache_manager()
    with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
        for source in NEWS_SOURCES:
            executor.submit(crawl_links_for_source, source,
                            db_manager, cache_manager)


def main():
    scheduler = BackgroundScheduler()
    scheduler.add_job(schedule_news_links, 'interval', seconds=15)
    scheduler.start()
    logger.info("Scheduler started for news links")
    while True:
        try:
            time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            logger.info("Scheduler shut down gracefully")


if __name__ == "__main__":
    main()
