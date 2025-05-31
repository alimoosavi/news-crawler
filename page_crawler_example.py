#!/usr/bin/env python3
"""
News Crawler - Multi-Source Edition

Distributed crawler that supports multiple news sources
"""

import logging
import sys
import argparse
from crawlers.orchestrator import CrawlerOrchestrator
from news_sources.factory import NewsSourceFactory, get_news_source
from config import settings

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
    parser = argparse.ArgumentParser(description='Multi-Source News Crawler')
    parser.add_argument(
        '--source', 
        type=str, 
        default='ISNA',
        choices=NewsSourceFactory.get_available_sources(),
        help='News source to crawl'
    )
    parser.add_argument(
        '--list-sources',
        action='store_true',
        help='List available news sources'
    )
    
    args = parser.parse_args()
    
    # List sources if requested
    if args.list_sources:
        print("Available news sources:")
        for source in NewsSourceFactory.get_available_sources():
            print(f"  - {source}")
        return
    
    # Get the news source
    try:
        news_source = get_news_source(args.source)
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
    orchestrator = CrawlerOrchestrator(news_source)
    
    try:
        # Start the system
        orchestrator.start()
        
        # Keep main thread alive
        while orchestrator.running:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
    finally:
        orchestrator.stop()

if __name__ == "__main__":
    main() 