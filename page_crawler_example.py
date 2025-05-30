#!/usr/bin/env python3
"""
ISNA News Page Crawler - Continuous Bulk Processing

This script runs continuously, processing unprocessed news links in batches.
It fetches pages concurrently, extracts article data, and persists to database.

Configuration is managed through environment variables and config.py
"""

import time
import logging
import signal
import sys
from datetime import datetime
from crawlers.isna.page_crawler import ISNAPageCrawler
from database_manager import DatabaseManager
from config import settings

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.app.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('page_crawler.log')
    ]
)

logger = logging.getLogger(__name__)

class ContinuousPageCrawler:
    """Continuous page crawler with bulk processing"""
    
    def __init__(self):
        self.running = True
        self.page_crawler = None
        self.db_manager = None
        self.stats = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'batches_completed': 0,
            'start_time': datetime.now()
        }
        
        # Configuration from settings
        self.bulk_size = settings.crawler.bulk_size
        self.max_workers = settings.crawler.max_workers
        self.sleep_interval = settings.crawler.sleep_interval
        self.max_retries = settings.crawler.max_retries
        self.retry_delay = settings.crawler.retry_delay
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.running = False
    
    def _setup_resources(self):
        """Setup database and crawler resources"""
        try:
            # Initialize database manager
            self.db_manager = DatabaseManager()
            self.db_manager.connect()
            
            # Initialize page crawler
            self.page_crawler = ISNAPageCrawler(db_manager=self.db_manager)
            
            logger.info("Resources initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup resources: {str(e)}")
            return False
    
    def _cleanup_resources(self):
        """Cleanup resources"""
        try:
            if self.page_crawler:
                self.page_crawler.cleanup()
                
            if self.db_manager:
                self.db_manager.close()
                
            logger.info("Resources cleaned up successfully")
            
        except Exception as e:
            logger.warning(f"Error during cleanup: {str(e)}")
    
    def _get_unprocessed_batch(self):
        """Get a batch of unprocessed links"""
        try:
            unprocessed_links = self.db_manager.get_unprocessed_links(
                source='ISNA', 
                limit=self.bulk_size
            )
            
            logger.info(f"Retrieved {len(unprocessed_links)} unprocessed links for batch")
            return unprocessed_links
            
        except Exception as e:
            logger.error(f"Error retrieving unprocessed links: {str(e)}")
            return []
    
    def _process_batch(self, links_batch):
        """Process a batch of links with retry logic"""
        if not links_batch:
            return {'successful': 0, 'failed': 0, 'total_processed': 0}
        
        logger.info(f"Processing batch of {len(links_batch)} links with {self.max_workers} workers")
        
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Process the batch
                result = self.page_crawler.crawl_unprocessed_links(
                    source='ISNA',
                    limit=len(links_batch),
                    max_workers=self.max_workers
                )
                
                summary = result['summary']
                
                # Log batch results
                logger.info(f"Batch completed - Processed: {summary['total_processed']}, "
                           f"Successful: {summary['successful']}, Failed: {summary['failed']}")
                
                # Update global stats
                self.stats['total_processed'] += summary['total_processed']
                self.stats['total_successful'] += summary['successful']
                self.stats['total_failed'] += summary['failed']
                self.stats['batches_completed'] += 1
                
                return summary
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Batch processing failed (attempt {retry_count}/{self.max_retries}): {str(e)}")
                
                if retry_count < self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Max retries reached. Batch processing failed.")
                    return {'successful': 0, 'failed': len(links_batch), 'total_processed': len(links_batch)}
        
        return {'successful': 0, 'failed': 0, 'total_processed': 0}
    
    def _log_progress_stats(self):
        """Log current progress statistics"""
        runtime = datetime.now() - self.stats['start_time']
        
        logger.info("=" * 60)
        logger.info("📊 CRAWLER PROGRESS STATISTICS")
        logger.info("=" * 60)
        logger.info(f"🕐 Runtime: {runtime}")
        logger.info(f"📦 Batches completed: {self.stats['batches_completed']}")
        logger.info(f"📄 Total pages processed: {self.stats['total_processed']}")
        logger.info(f"✅ Successful extractions: {self.stats['total_successful']}")
        logger.info(f"❌ Failed extractions: {self.stats['total_failed']}")
        
        if self.stats['total_processed'] > 0:
            success_rate = (self.stats['total_successful'] / self.stats['total_processed']) * 100
            logger.info(f"📈 Success rate: {success_rate:.1f}%")
            
            # Calculate processing speed
            total_seconds = runtime.total_seconds()
            if total_seconds > 0:
                pages_per_hour = (self.stats['total_processed'] / total_seconds) * 3600
                logger.info(f"⚡ Processing speed: {pages_per_hour:.1f} pages/hour")
        
        logger.info("=" * 60)
    
    def _check_database_status(self):
        """Check database status and log statistics"""
        try:
            stats = self.db_manager.get_news_statistics()
            
            logger.info(f"📊 Database Status:")
            logger.info(f"   Total links: {stats['total_links']}")
            logger.info(f"   Processed links: {stats['processed_links']}")
            logger.info(f"   Unprocessed links: {stats['unprocessed_links']}")
            logger.info(f"   Total articles: {stats['total_articles']}")
            
            return stats['unprocessed_links']
            
        except Exception as e:
            logger.error(f"Error checking database status: {str(e)}")
            return 0
    
    def run(self):
        """Main continuous processing loop"""
        logger.info("🚀 Starting ISNA News Page Crawler - Continuous Mode")
        logger.info("=" * 60)
        logger.info(f"📋 Configuration:")
        logger.info(f"   Bulk size: {self.bulk_size}")
        logger.info(f"   Max workers: {self.max_workers}")
        logger.info(f"   Sleep interval: {self.sleep_interval}s")
        logger.info(f"   Max retries: {self.max_retries}")
        logger.info("=" * 60)
        
        # Setup resources
        if not self._setup_resources():
            logger.error("Failed to setup resources. Exiting.")
            return False
        
        try:
            # Main processing loop
            while self.running:
                logger.info(f"🔄 Starting new processing cycle...")
                
                # Check database status
                unprocessed_count = self._check_database_status()
                
                if unprocessed_count == 0:
                    logger.info("✅ No unprocessed links found. Waiting for new links...")
                    self._log_progress_stats()
                    
                    # Wait before checking again
                    for i in range(self.sleep_interval):
                        if not self.running:
                            break
                        time.sleep(1)
                    continue
                
                # Get batch of unprocessed links
                links_batch = self._get_unprocessed_batch()
                
                if not links_batch:
                    logger.warning("No links retrieved despite unprocessed count > 0. Waiting...")
                    time.sleep(self.sleep_interval)
                    continue
                
                # Process the batch
                batch_result = self._process_batch(links_batch)
                
                # Log progress every 10 batches or if no successful processing
                if (self.stats['batches_completed'] % 10 == 0 or 
                    batch_result['successful'] == 0):
                    self._log_progress_stats()
                
                # Sleep between batches if still running
                if self.running and unprocessed_count > len(links_batch):
                    logger.info(f"😴 Sleeping for {self.sleep_interval} seconds before next batch...")
                    for i in range(self.sleep_interval):
                        if not self.running:
                            break
                        time.sleep(1)
        
        except Exception as e:
            logger.error(f"💥 Critical error in main loop: {str(e)}")
            return False
        
        finally:
            # Final statistics
            logger.info("🏁 Crawler stopping...")
            self._log_progress_stats()
            self._cleanup_resources()
            logger.info("👋 Crawler stopped gracefully")
        
        return True

def main():
    """Main entry point"""
    crawler = ContinuousPageCrawler()
    
    try:
        success = crawler.run()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 