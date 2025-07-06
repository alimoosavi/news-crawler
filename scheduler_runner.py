#!/usr/bin/env python3
"""
Scheduler for crawling news_links and news_pages independently every 15 minutes
"""

import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor

# Import cache manager
from cache_manager import CacheManager

# Import your settings and crawlers
from config import settings
from database_manager import DatabaseManager
from crawlers.isna.links_crawler import ISNALinksCrawler
from crawlers.isna.pages_crawler import ISNAPageCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("Scheduler")

NEWS_SOURCES = ["ISNA"]

LINK_CRAWLERS = {
    "ISNA": ISNALinksCrawler,
}

PAGE_CRAWLERS = {
    "ISNA": ISNAPageCrawler,
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
        
        # or whatever method fetches new links
        fresh_last_link = crawler.crawl_recent_links(last_link)
        logger.info(f"[{source}] Link crawl finished")
        cache_manager.update_last_link(source, fresh_last_link)
        
    except Exception as e:
        logger.exception(f"[{source}] Link crawl failed: {e}")


def crawl_pages_for_source(source: str, db_manager: DatabaseManager):
    try:
        logger.info(f"[{source}] Starting page crawl")
        crawler = PAGE_CRAWLERS[source](db_manager=db_manager, headless=True)
        crawler.crawl_unprocessed_links()  # should process uncrawled links
        logger.info(f"[{source}] Page crawl finished")
    except Exception as e:
        logger.exception(f"[{source}] Page crawl failed: {e}")


def schedule_news_links():
    db_manager = get_db_manager()
    cache_manager = get_cache_manager()

    with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
        for source in NEWS_SOURCES:
            executor.submit(crawl_links_for_source, source,
                            db_manager, cache_manager)


def schedule_news_pages():
    db_manager = get_db_manager()
    with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
        for source in NEWS_SOURCES:
            executor.submit(crawl_pages_for_source, source, db_manager)


def main():
    scheduler = BackgroundScheduler()
    scheduler.add_job(schedule_news_links, 'interval', minutes=5)
    scheduler.add_job(schedule_news_pages, 'interval', minutes=5)

    scheduler.start()
    logger.info("Scheduler started for both news_links and news_pages")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler shut down gracefully")


if __name__ == "__main__":
    main()
