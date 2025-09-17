#!/usr/bin/env python3
"""
Scheduler for crawling news pages
"""

import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor

# Import your settings and crawlers
from config import settings
from database_manager import DatabaseManager
from crawlers.isna.pages_crawler import ISNAPageCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("PagesScheduler")

NEWS_SOURCES = ["IRNA"]

PAGE_CRAWLERS = {
    # "ISNA": ISNAPageCrawler,
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


def crawl_pages_for_source(source: str, db_manager: DatabaseManager):
    try:
        logger.info(f"[{source}] Starting page crawl")
        crawler = PAGE_CRAWLERS[source](db_manager=db_manager, headless=True)
        crawler.crawl_unprocessed_links()
        logger.info(f"[{source}] Page crawl finished")
    except Exception as e:
        logger.exception(f"[{source}] Page crawl failed: {e}")


def schedule_news_pages():
    db_manager = get_db_manager()
    with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
        for source in NEWS_SOURCES:
            executor.submit(crawl_pages_for_source, source, db_manager)


def main():
    scheduler = BackgroundScheduler()
    scheduler.add_job(schedule_news_pages, 'interval', seconds=15)
    scheduler.start()
    logger.info("Scheduler started for news pages")
    while True:
        try:
            time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            logger.info("Scheduler shut down gracefully")


if __name__ == "__main__":
    main()