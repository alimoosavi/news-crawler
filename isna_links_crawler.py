#!/usr/bin/env python
"""
Example usage of ISNA Links Crawler with Shamsi date support
"""

import logging
from datetime import datetime
import pytz
from crawlers.isna.links_crawler import ISNALinksCrawler
from database_manager import DatabaseManager
from config import settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Crawl ISNA archive pages and persist news items to the database"""

    # Initialize DatabaseManager
    db_manager = DatabaseManager(
        host=settings.database.host,
        port=settings.database.port,
        db_name=settings.database.db_name,
        user=settings.database.user,
        password=settings.database.password,
        min_conn=settings.database.min_conn,
        max_conn=settings.database.max_conn
    )
    logger.info("DatabaseManager initialized successfully")

    # Initialize ISNALinksCrawler
    crawler = ISNALinksCrawler(db_manager=db_manager, headless=True)
    logger.info("ISNALinksCrawler initialized successfully")

    # Define crawling parameters (e.g., specific Shamsi date and page)
    shamsi_year = 1404
    shamsi_month = 3  # Khordad
    shamsi_day = 13

    crawler.crawl_archive(year=shamsi_year, month=shamsi_month, day=shamsi_day)


if __name__ == "__main__":
    main()
