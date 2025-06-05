#!/usr/bin/env python3
"""
Example usage of ISNA Links Crawler with Shamsi date support
"""

import logging
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
    """Example usage"""

    # Initialize crawler
    db_manager = DatabaseManager(
        host=settings.database.host,
        port=settings.database.port,
        db_name=settings.database.db_name,
        user=settings.database.user,
        password=settings.database.password,
        min_conn=settings.database.min_conn,
        max_conn=settings.database.max_conn)
    crawler = ISNALinksCrawler(db_manager=db_manager)

    print(crawler.crawl_archive_page(year=1404, month=3, day=13, page_index=2))
    # try:
    #     # Crawl specific Shamsi date
    #     logger.info("🚀 Starting ISNA link crawling for specific date")
    #
    #     # Crawl 10 Khordad 1404 (example date)
    #     links_count = crawler.crawl_shamsi_date(
    #         shamsi_year=1404,
    #         shamsi_month=3,  # Khordad
    #         shamsi_day=10
    #     )
    #
    #     logger.info(f"✅ Found {links_count} links for 10 Khordad 1404")
    #
    #     # Show some statistics
    #     db_stats = crawler._db_manager.get_processing_statistics()
    #     logger.info(f"📊 Database stats: {db_stats}")
    #
    # except Exception as e:
    #     logger.error(f"Error in crawler: {str(e)}")
    #
    # finally:
    #     crawler.close()


if __name__ == "__main__":
    main()
