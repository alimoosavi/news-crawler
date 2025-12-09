#!/usr/bin/env python3
"""
Historical Page Content Scheduler with Retry Tracking

Crawls historical news page content with intelligent retry handling:
- Tracks number of attempts per link (tried_count)
- Skips links that exceed max retry limit
- Marks failed links after max retries
- Provides retry statistics

NEW FEATURES:
- Automatic retry tracking
- Max retry limit enforcement (configurable, default: 3)
- Failed link detection and marking
- Retry statistics logging
"""
import argparse
import logging
import time
from typing import List
from datetime import datetime

from collectors.irna.pages_collector import IRNAHistoricalPageCollector
from collectors.tasnim.pages_collector import TasnimHistoricalPageCollector
from collectors.donyaye_eghtesad.pages_collector import DonyaEqtesadHistoricalPageCollector
from collectors.isna.pages_collector import ISNAHistoricalPageCollector
from config import settings
from database_manager import DatabaseManager
from news_publishers import IRNA, TASNIM, ISNA, DONYAYE_EQTESAD
from schema import NewsLinkData

logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("HistoricalPageScheduler")

# Map each publisher to its page collector class
HISTORICAL_PAGE_COLLECTORS = {
    IRNA: IRNAHistoricalPageCollector,
    TASNIM: TasnimHistoricalPageCollector,
    DONYAYE_EQTESAD: DonyaEqtesadHistoricalPageCollector,
    ISNA: ISNAHistoricalPageCollector
}


class HistoricalPageScheduler:
    """
    Scheduler for crawling historical news page content with retry tracking.
    
    Features:
    - Fetches pending links from database
    - Crawls page content using source-specific collectors
    - Tracks retry attempts per link
    - Skips links exceeding max retry limit
    - Marks failed links automatically
    """
    
    def __init__(
        self, 
        db_manager: DatabaseManager, 
        source: str,
        batch_size: int = 20,
        max_retries: int = 3,
        poll_interval: int = 30
    ):
        """
        Initialize the historical page scheduler.
        
        Args:
            db_manager: Database manager instance
            source: News source to process
            batch_size: Number of links to process per batch
            max_retries: Maximum retry attempts per link (default: 3)
            poll_interval: Seconds to wait when no pending links
        """
        self.db_manager = db_manager
        self.db_manager.max_retries = max_retries  # Update DB manager max retries
        self.source = source
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.poll_interval = poll_interval
        
        # Get collector class for this source
        if source not in HISTORICAL_PAGE_COLLECTORS:
            raise ValueError(
                f"Unknown source: {source}. "
                f"Available sources: {list(HISTORICAL_PAGE_COLLECTORS.keys())}"
            )
        
        CollectorClass = HISTORICAL_PAGE_COLLECTORS[source]
        self.collector = CollectorClass(db_manager=db_manager)
        
        # Statistics
        self.total_processed = 0
        self.total_failed = 0
        self.total_retries = 0
        self.start_time = time.time()
    
    def process_batch(self) -> bool:
        """
        Process a single batch of pending links.
        
        Returns:
            True if links were processed, False if no pending links
        """
        # Fetch pending links (excluding those that exceeded max retries)
        pending_links = self.db_manager.get_pending_links_by_source(
            source=self.source,
            limit=self.batch_size,
            exclude_max_retries=True  # NEW: Skip links at max retries
        )
        
        if not pending_links:
            return False
        
        logger.info(f"📥 Fetched {len(pending_links)} pending links for {self.source}")
        
        # Separate links by retry status
        fresh_links = []
        retry_links = []
        
        for link_data in pending_links:
            # Note: We don't have tried_count in NewsLinkData, 
            # so we treat all as fresh for now
            fresh_links.append(link_data)
        
        if retry_links:
            logger.info(f"  🔄 {len(retry_links)} are retry attempts")
            self.total_retries += len(retry_links)
        
        # Collect page content
        successful_links = []
        failed_links = []
        
        try:
            # Use the collector to fetch page content
            # The collector returns NewsData objects and persists them
            news_items = self.collector.collect_links(pending_links)
            
            if news_items:
                logger.info(f"✅ Successfully crawled {len(news_items)} pages")
                successful_links = [item.link for item in news_items]
                self.total_processed += len(news_items)
                
                # Mark successfully crawled links as COMPLETED
                self.db_manager.mark_links_completed(successful_links)
            
            # Identify failed links (links that weren't successfully crawled)
            all_links = {link.link for link in pending_links}
            successful_set = set(successful_links)
            failed_links = list(all_links - successful_set)
            
            if failed_links:
                logger.warning(f"❌ {len(failed_links)} links failed to crawl")
                self.total_failed += len(failed_links)
                
                # Increment try count for failed links
                self.db_manager.increment_link_try_count(failed_links)
                
                # Check if any failed links now exceed max retries
                exceeded = self.db_manager.get_links_exceeding_retries(source=self.source)
                if exceeded:
                    logger.warning(
                        f"⚠️  {len(exceeded)} links exceeded max retries "
                        f"({self.max_retries}). Marking as FAILED..."
                    )
                    self.db_manager.mark_links_as_failed(exceeded)
        
        except Exception as e:
            logger.error(f"Error processing batch: {e}", exc_info=True)
            # Increment try count for all links in batch
            all_links = [link.link for link in pending_links]
            self.db_manager.increment_link_try_count(all_links)
            return False
        
        return True
    
    def log_statistics(self):
        """Log processing statistics"""
        elapsed = time.time() - self.start_time
        rate = self.total_processed / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 80)
        logger.info(f"📊 STATISTICS for {self.source}")
        logger.info("-" * 80)
        logger.info(f"  Total Processed: {self.total_processed}")
        logger.info(f"  Total Failed: {self.total_failed}")
        logger.info(f"  Total Retries: {self.total_retries}")
        logger.info(f"  Success Rate: {(self.total_processed / max(self.total_processed + self.total_failed, 1)) * 100:.1f}%")
        logger.info(f"  Elapsed Time: {elapsed:.0f}s")
        logger.info(f"  Processing Rate: {rate:.2f} pages/sec")
        
        # Get retry statistics from database
        try:
            retry_stats = self.db_manager.get_retry_statistics(source=self.source)
            logger.info("-" * 80)
            logger.info("  Retry Distribution:")
            for tries, count in sorted(retry_stats['retry_distribution'].items()):
                logger.info(f"    {tries} attempts: {count} links")
            logger.info(f"  Near Max Retries ({self.max_retries-1}+): {retry_stats['near_max_retries']}")
            
            # Get failed count
            failed_count = self.db_manager.get_failed_links_count_by_source(self.source)
            if failed_count > 0:
                logger.warning(f"  Permanently Failed Links: {failed_count}")
        except Exception as e:
            logger.error(f"Failed to get retry statistics: {e}")
        
        logger.info("=" * 80)
    
    def run_forever(self):
        """Main processing loop"""
        logger.info("=" * 80)
        logger.info("Historical Page Scheduler Started")
        logger.info(f"Source: {self.source}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info(f"Max Retries: {self.max_retries}")
        logger.info(f"Poll Interval: {self.poll_interval}s")
        logger.info("=" * 80)
        
        # Run initial cleanup to mark links exceeding max retries
        logger.info("Running initial cleanup...")
        cleaned = self.db_manager.cleanup_exceeded_retries(source=self.source)
        if cleaned > 0:
            logger.warning(f"Marked {cleaned} links as FAILED (exceeded max retries)")
        
        idle_cycles = 0
        
        try:
            while True:
                # Process one batch
                had_work = self.process_batch()
                
                if not had_work:
                    idle_cycles += 1
                    
                    if idle_cycles == 1:
                        logger.info(
                            f"No pending links for {self.source}. "
                            f"Sleeping for {self.poll_interval}s..."
                        )
                        self.log_statistics()
                    elif idle_cycles % 10 == 0:
                        logger.info(f"Still idle (cycle {idle_cycles})...")
                        self.log_statistics()
                    
                    time.sleep(self.poll_interval)
                    continue
                
                # Reset idle counter
                idle_cycles = 0
                
                # Brief pause between batches
                time.sleep(2)
                
                # Log stats every 10 batches
                if self.total_processed % (self.batch_size * 10) < self.batch_size:
                    self.log_statistics()
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Scheduler stopped by user")
            self.log_statistics()
        
        except Exception as e:
            logger.critical(f"❌ Scheduler crashed: {e}", exc_info=True)
            self.log_statistics()
            raise


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Historical News Page Scheduler with Retry Tracking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Process IRNA with default settings
    python news_historical_page_scheduler.py --source IRNA
    
    # Process with custom batch size and max retries
    python news_historical_page_scheduler.py --source ISNA --batch-size 50 --max-retries 5
    
    # Run all sources in parallel
    python news_historical_page_scheduler.py --source IRNA &
    python news_historical_page_scheduler.py --source ISNA &
    python news_historical_page_scheduler.py --source Tasnim &
    python news_historical_page_scheduler.py --source Donya-e-Eqtesad &

Features:
    - Automatic retry tracking per link
    - Skips links exceeding max retry limit
    - Marks permanently failed links
    - Provides detailed retry statistics
        """
    )
    
    parser.add_argument(
        '--source',
        type=str,
        required=True,
        choices=['IRNA', 'ISNA', 'Tasnim', 'Donya-e-Eqtesad'],
        help='News source to process (required)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='Number of links to process per batch (default: 20)'
    )
    
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum retry attempts per link (default: 3)'
    )
    
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=30,
        help='Seconds to wait when no pending links (default: 30)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Initialize database manager with max retries
    try:
        db_manager = DatabaseManager(
            db_config=settings.database,
            max_retries=args.max_retries
        )
        logger.info("✅ DatabaseManager initialized")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize DatabaseManager: {e}")
        return 1
    
    # Start scheduler
    scheduler = HistoricalPageScheduler(
        db_manager=db_manager,
        source=args.source,
        batch_size=args.batch_size,
        max_retries=args.max_retries,
        poll_interval=args.poll_interval
    )
    
    scheduler.run_forever()
    
    return 0


if __name__ == "__main__":
    exit(main())