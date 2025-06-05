#!/usr/bin/env python3
"""
News Crawler - Multi-Source Edition

Distributed crawler that supports multiple news sources
"""

import logging
import sys
import time

from config import settings
from crawlers.page_crawler.orchestrator import PageCrawlerOrchestrator
from database_manager import DatabaseManager
from news_sources.factory import get_news_source

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.app.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('news_crawler.log')
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point for news crawler"""

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

    source = "ISNA"
    try:
        news_source = get_news_source(source)
    except ValueError as e:
        logger.error(f"Error: {e}")
        return

    logger.info(f"🗞️  News Crawler - {news_source.source_name} Edition")
    logger.info("=" * 50)
    logger.info(f"📋 Configuration:")
    logger.info(f"   Source: {news_source.source_name}")
    logger.info(f"   Bulk Size: {settings.crawler.bulk_size}")
    logger.info(f"   Max Workers: {min(settings.crawler.max_workers, 4)}")
    logger.info(f"   Sleep Interval: {settings.crawler.sleep_interval}s")
    logger.info("=" * 50)

    # Create and start orchestrator with injected news source
    orchestrator = PageCrawlerOrchestrator(news_source=news_source, db_manager=db_manager)

    try:
        # Start the system
        orchestrator.start()

        # Keep main thread alive
        while orchestrator.running:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
    finally:
        orchestrator.stop()


if __name__ == "__main__":
    main()
