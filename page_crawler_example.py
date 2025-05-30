#!/usr/bin/env python3
"""
ISNA News Crawler - Laptop Edition

Lightweight distributed crawler optimized for laptop resources
"""

import logging
import sys
from crawlers.orchestrator import CrawlerOrchestrator
from config import settings

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.app.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('laptop_crawler.log')
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for laptop crawler"""
    logger.info("💻 ISNA News Crawler - Laptop Edition")
    logger.info("=" * 50)
    logger.info(f"📋 Configuration:")
    logger.info(f"   Bulk Size: {settings.crawler.bulk_size}")
    logger.info(f"   Max Workers: {min(settings.crawler.max_workers, 4)}")
    logger.info(f"   Sleep Interval: {settings.crawler.sleep_interval}s")
    logger.info("=" * 50)
    
    # Create and start orchestrator
    orchestrator = CrawlerOrchestrator()
    
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