#!/usr/bin/env python3
"""
Historical Embedding Scheduler with Optional Source Filtering

Processes historical news from PostgreSQL database and generates embeddings.
Uses embedding provider configured in .env (OpenAI or Ollama).

DEFAULT BEHAVIOR (no --source):
    Processes ALL news sources together in a single scheduler.
    This is the backward-compatible mode.

OPTIONAL SOURCE FILTERING (with --source):
    Processes only articles from a specific source.
    Useful for running multiple parallel schedulers (one per source).

Key Features:
- Source-specific processing (optional)
- Batch processing with configurable size
- Automatic retry on failures
- Comprehensive logging and statistics

Usage Examples:
    # DEFAULT: Process all sources together (backward compatible)
    python news_historical_embedding_scheduler.py
    
    # OPTIONAL: Process specific source for parallel processing
    python news_historical_embedding_scheduler.py --source IRNA
    
    # OPTIONAL: Custom batch size
    python news_historical_embedding_scheduler.py --source ISNA --batch-size 50
    
    # PARALLEL: Run multiple instances for maximum throughput
    python news_historical_embedding_scheduler.py --source IRNA &
    python news_historical_embedding_scheduler.py --source ISNA &
    python news_historical_embedding_scheduler.py --source Tasnim &
    python news_historical_embedding_scheduler.py --source Donya-e-Eqtesad &
"""
import argparse
import logging
import time
import sys
from typing import Optional

from database_manager import DatabaseManager
from vector_db_manager import VectorDBManager
from config import settings

logger = logging.getLogger("HistoricalEmbeddingScheduler")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class EmbeddingScheduler:
    """
    Scheduler for processing news articles and generating embeddings.
    
    Modes:
    1. ALL SOURCES (default): source=None processes all news sources
    2. SINGLE SOURCE: source='IRNA' processes only that source
    """
    
    def __init__(
        self,
        db_manager: DatabaseManager,
        vector_manager: VectorDBManager,
        batch_size: int = 20,
        poll_interval: int = 30,
        source: Optional[str] = None
    ):
        """
        Initialize the embedding scheduler.
        
        Args:
            db_manager: Database manager instance
            vector_manager: Vector database manager instance
            batch_size: Number of articles to process per batch
            poll_interval: Seconds to wait when no pending articles
            source: Optional source filter (default=None processes ALL sources)
                   Examples: 'IRNA', 'ISNA', 'Tasnim', 'Donya-e-Eqtesad'
        """
        self.db_manager = db_manager
        self.vector_manager = vector_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.source = source  # None = ALL sources (default)
        
        # Statistics
        self.total_processed = 0
        self.total_errors = 0
        self.start_time = time.time()

    def _fetch_pending_batch(self):
        """
        Fetch pending news batch based on source filter.
        
        If source is None (default): Fetches from ALL sources
        If source is set: Fetches only from that specific source
        """
        if self.source:
            # Source-specific mode
            return self.db_manager.get_pending_news_batch_by_source(
                source=self.source,
                limit=self.batch_size
            )
        else:
            # Default mode: ALL sources
            return self.db_manager.get_pending_news_batch(
                limit=self.batch_size
            )

    def _log_statistics(self):
        """Log processing statistics"""
        elapsed = time.time() - self.start_time
        rate = self.total_processed / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 80)
        logger.info("STATISTICS:")
        logger.info(f"  Total Processed: {self.total_processed}")
        logger.info(f"  Total Errors: {self.total_errors}")
        logger.info(f"  Success Rate: {(self.total_processed - self.total_errors) / max(self.total_processed, 1) * 100:.1f}%")
        logger.info(f"  Elapsed Time: {elapsed:.0f}s")
        logger.info(f"  Processing Rate: {rate:.2f} articles/sec")
        logger.info("=" * 80)

    def run_forever(self):
        """Main processing loop"""
        logger.info("=" * 80)
        logger.info("Historical Embedding Scheduler Started")
        logger.info(f"Provider: {settings.embedding.provider}")
        
        if settings.embedding.provider == 'openai':
            logger.info(f"Model: {settings.embedding.openai_model}")
        else:
            logger.info(f"Model: {settings.embedding.ollama_model}")
            
        logger.info(f"Dimension: {settings.embedding.embedding_dim}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info(f"Poll Interval: {self.poll_interval}s")
        
        # Log source filter mode
        if self.source:
            # Source-specific mode
            logger.info(f"Mode: SOURCE-SPECIFIC")
            logger.info(f"Source Filter: {self.source}")
            
            # Check pending count for this source
            try:
                pending_count = self.db_manager.get_pending_count_by_source(self.source)
                logger.info(f"Pending Articles for {self.source}: {pending_count:,}")
            except Exception as e:
                logger.warning(f"Could not get pending count: {e}")
        else:
            # Default mode: all sources
            logger.info(f"Mode: ALL SOURCES (default)")
            logger.info(f"Source Filter: NONE (processing all sources)")
            
            # Check total pending count
            try:
                total_pending = self.db_manager.get_total_pending_count()
                logger.info(f"Total Pending Articles (all sources): {total_pending:,}")
            except Exception as e:
                logger.warning(f"Could not get pending count: {e}")
            
        logger.info("=" * 80)
        
        idle_cycles = 0
        
        try:
            while True:
                # Fetch pending news
                news_batch = self._fetch_pending_batch()
                
                if not news_batch:
                    idle_cycles += 1
                    
                    if idle_cycles == 1:
                        if self.source:
                            logger.info(f"No pending news for source '{self.source}'. Sleeping for {self.poll_interval}s...")
                        else:
                            logger.info(f"No pending news (all sources). Sleeping for {self.poll_interval}s...")
                    elif idle_cycles % 10 == 0:
                        logger.info(f"Still idle (cycle {idle_cycles}). Waiting for new articles...")
                        self._log_statistics()
                    
                    time.sleep(self.poll_interval)
                    continue
                
                # Reset idle counter
                idle_cycles = 0
                
                # Log what we're processing
                if self.source:
                    # Source-specific mode
                    logger.info(f"📥 Fetched {len(news_batch)} pending news from {self.source}")
                else:
                    # Show sources in this batch
                    sources_in_batch = set(n.source for n in news_batch)
                    sources_str = ", ".join(sorted(sources_in_batch))
                    logger.info(f"📥 Fetched {len(news_batch)} pending news from sources: {sources_str}")

                # Persist into Qdrant
                try:
                    inserted = self.vector_manager.persist_news_batch(news_batch)
                    logger.info(f"✅ Inserted {inserted} items into Qdrant")

                    # Mark as COMPLETED
                    if inserted > 0:
                        links = [n.link for n in news_batch]
                        updated = self.db_manager.mark_news_completed(links)
                        logger.info(f"✅ Marked {updated} news as COMPLETED")
                        
                        self.total_processed += inserted
                    else:
                        logger.warning("⚠️  No items inserted")
                        self.total_errors += len(news_batch)
                        
                except Exception as e:
                    logger.error(f"❌ Error processing batch: {e}")
                    self.total_errors += len(news_batch)
                    time.sleep(5)
                    continue

                # Brief pause between batches
                time.sleep(2)
                
                # Log stats every 10 batches
                if self.total_processed % (self.batch_size * 10) < self.batch_size:
                    self._log_statistics()
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Scheduler stopped by user")
            self._log_statistics()
            
        except Exception as e:
            logger.critical(f"❌ Scheduler crashed: {e}", exc_info=True)
            self._log_statistics()
            raise


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Historical News Embedding Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Default Behavior (NO --source argument):
  Processes ALL news sources together in a single scheduler.
  This is the backward-compatible mode.
  
  Example:
    python news_historical_embedding_scheduler.py
    → Processes articles from IRNA, ISNA, Tasnim, Donya-e-Eqtesad all together

Optional Source Filtering (WITH --source argument):
  Processes only articles from a specific source.
  Useful for running multiple parallel schedulers.
  
  Examples:
    # Single source
    python news_historical_embedding_scheduler.py --source IRNA
    
    # Multiple parallel instances (maximum throughput)
    python news_historical_embedding_scheduler.py --source IRNA &
    python news_historical_embedding_scheduler.py --source ISNA &
    python news_historical_embedding_scheduler.py --source Tasnim &
    python news_historical_embedding_scheduler.py --source Donya-e-Eqtesad &
    
    # Or use the helper script
    ./run_parallel_schedulers.sh

Custom Batch Size:
    python news_historical_embedding_scheduler.py --batch-size 50
    python news_historical_embedding_scheduler.py --source IRNA --batch-size 30
        """
    )
    
    parser.add_argument(
        '--source',
        type=str,
        default=None,
        help='Optional: Filter by news source (e.g., IRNA, ISNA, Tasnim, Donya-e-Eqtesad). '
             'If not provided, processes ALL sources (default behavior).'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='Number of articles to process per batch (default: 20)'
    )
    
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=30,
        help='Seconds to wait when no pending articles (default: 30)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    
    # Parse arguments
    args = parse_arguments()
    
    # Initialize managers
    try:
        db_manager = DatabaseManager(settings.database)
        logger.info("✅ DatabaseManager initialized")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize DatabaseManager: {e}")
        sys.exit(1)
    
    try:
        vector_manager = VectorDBManager(
            qdrant_config=settings.qdrant,
            embedding_config=settings.embedding,
            logger=logger
        )
        logger.info(f"✅ VectorDBManager ready with {settings.embedding.provider}")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize VectorDBManager: {e}")
        sys.exit(1)
    
    # Validate source if provided
    if args.source:
        # Optional: validate against known sources
        try:
            from sources import IRNA, ISNA, TASNIM, DONYAYE_EQTESAD
            valid_sources = [IRNA, ISNA, TASNIM, DONYAYE_EQTESAD]
            
            if args.source not in valid_sources:
                logger.warning(
                    f"⚠️  Source '{args.source}' not in known sources: {valid_sources}"
                )
                logger.warning("Proceeding anyway, but check for typos!")
        except ImportError:
            # sources.py not available, skip validation
            pass
    else:
        logger.info("ℹ️  No --source provided: will process ALL sources (default mode)")
    
    # Start scheduler
    scheduler = EmbeddingScheduler(
        db_manager=db_manager,
        vector_manager=vector_manager,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        source=args.source  # None = ALL sources (default)
    )
    
    scheduler.run_forever()


if __name__ == "__main__":
    main()