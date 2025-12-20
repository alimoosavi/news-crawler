#!/usr/bin/env python3
"""
Historical Embedding Scheduler with Optional Source Filtering

Processes historical news from PostgreSQL database and generates embeddings.
Uses embedding provider configured in .env (OpenAI or Ollama).

FIXED VERSION:
- Better error handling for Qdrant timeouts
- Enhanced statistics tracking
- Graceful degradation on failures
- Improved logging

DEFAULT BEHAVIOR (no --source):
    Processes ALL news sources together in a single scheduler.
    This is the backward-compatible mode.

OPTIONAL SOURCE FILTERING (with --source):
    Processes only articles from a specific source.
    Useful for running multiple parallel schedulers (one per source).

Key Features:
- Source-specific processing (optional)
- Batch processing with configurable size
- Automatic retry on failures with exponential backoff
- Comprehensive logging and statistics
- Graceful error handling
- Progress tracking

Usage Examples:
    # DEFAULT: Process all sources together (backward compatible)
    python news_historical_embedding_scheduler.py
    
    # OPTIONAL: Process specific source for parallel processing
    python news_historical_embedding_scheduler.py --source IRNA
    
    # OPTIONAL: Custom batch size
    python news_historical_embedding_scheduler.py --source ISNA --batch-size 50
    
    # OPTIONAL: Custom chunk size for Qdrant uploads
    python news_historical_embedding_scheduler.py --chunk-size 50
    
    # PARALLEL: Run multiple instances for maximum throughput
    python news_historical_embedding_scheduler.py --source IRNA &
    python news_historical_embedding_scheduler.py --source ISNA &
    python news_historical_embedding_scheduler.py --source Tasnim &
    python news_historical_embedding_scheduler.py --source Shargh &
    python news_historical_embedding_scheduler.py --source Donya-e-Eqtesad &
"""
import argparse
import logging
import time
import sys
import signal
from typing import Optional
from datetime import datetime

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
    
    Features:
    - Batch processing with configurable size
    - Automatic retry on failures
    - Graceful shutdown on SIGINT/SIGTERM
    - Comprehensive statistics tracking
    - Progress monitoring
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
                   Examples: 'IRNA', 'ISNA', 'Tasnim', 'Shargh', 'Donya-e-Eqtesad'
        """
        self.db_manager = db_manager
        self.vector_manager = vector_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.source = source  # None = ALL sources (default)
        
        # Statistics
        self.total_processed = 0
        self.total_errors = 0
        self.total_batches = 0
        self.successful_batches = 0
        self.failed_batches = 0
        self.start_time = time.time()
        
        # Graceful shutdown flag
        self.shutdown_requested = False
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"\n🛑 Received {signal_name}, shutting down gracefully...")
        self.shutdown_requested = True

    def _fetch_pending_batch(self):
        """
        Fetch pending news batch based on source filter.
        
        If source is None (default): Fetches from ALL sources
        If source is set: Fetches only from that specific source
        """
        try:
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
        except Exception as e:
            logger.error(f"Failed to fetch pending batch: {e}")
            return []

    def _log_statistics(self):
        """Log comprehensive processing statistics"""
        elapsed = time.time() - self.start_time
        rate = self.total_processed / elapsed if elapsed > 0 else 0
        success_rate = (self.successful_batches / self.total_batches * 100) if self.total_batches > 0 else 0
        
        logger.info("=" * 80)
        logger.info("PROCESSING STATISTICS:")
        logger.info(f"  Total Batches Processed: {self.total_batches}")
        logger.info(f"  Successful Batches: {self.successful_batches}")
        logger.info(f"  Failed Batches: {self.failed_batches}")
        logger.info(f"  Batch Success Rate: {success_rate:.1f}%")
        logger.info(f"  Total Articles Processed: {self.total_processed}")
        logger.info(f"  Total Errors: {self.total_errors}")
        logger.info(f"  Article Success Rate: {(self.total_processed / max(self.total_processed + self.total_errors, 1)) * 100:.1f}%")
        logger.info(f"  Elapsed Time: {elapsed:.0f}s ({elapsed/60:.1f} minutes)")
        logger.info(f"  Processing Rate: {rate:.2f} articles/sec")
        
        if rate > 0:
            logger.info(f"  Estimated Time for 1000 articles: {1000/rate/60:.1f} minutes")
        
        logger.info("=" * 80)

    def _log_collection_stats(self):
        """Log Qdrant collection statistics"""
        try:
            stats = self.vector_manager.get_collection_stats()
            if stats:
                logger.info("COLLECTION STATISTICS:")
                logger.info(f"  Total Vectors: {stats.get('points_count', 0):,}")
                logger.info(f"  Indexed Vectors: {stats.get('indexed_vectors_count', 0):,}")
                logger.info(f"  Collection Status: {stats.get('status', 'unknown')}")
        except Exception as e:
            logger.warning(f"Could not fetch collection stats: {e}")

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
        
        # Log collection stats
        self._log_collection_stats()
        
        logger.info("=" * 80)
        
        idle_cycles = 0
        last_stats_log = time.time()
        STATS_LOG_INTERVAL = 300  # Log stats every 5 minutes
        
        try:
            while not self.shutdown_requested:
                # Fetch pending news
                news_batch = self._fetch_pending_batch()
                
                if not news_batch:
                    idle_cycles += 1
                    
                    if idle_cycles == 1:
                        if self.source:
                            logger.info(
                                f"No pending news for source '{self.source}'. "
                                f"Sleeping for {self.poll_interval}s..."
                            )
                        else:
                            logger.info(
                                f"No pending news (all sources). "
                                f"Sleeping for {self.poll_interval}s..."
                            )
                    elif idle_cycles % 10 == 0:
                        logger.info(
                            f"Still idle (cycle {idle_cycles}). "
                            f"Waiting for new articles..."
                        )
                        self._log_statistics()
                    
                    time.sleep(self.poll_interval)
                    continue
                
                # Reset idle counter
                idle_cycles = 0
                self.total_batches += 1
                
                # Log what we're processing
                if self.source:
                    # Source-specific mode
                    logger.info(f"📥 Fetched {len(news_batch)} pending news from {self.source}")
                else:
                    # Show sources in this batch
                    sources_in_batch = set(n.source for n in news_batch)
                    sources_str = ", ".join(sorted(sources_in_batch))
                    logger.info(
                        f"📥 Fetched {len(news_batch)} pending news from sources: {sources_str}"
                    )

                # Persist into Qdrant
                try:
                    inserted = self.vector_manager.persist_news_batch(news_batch)
                    
                    if inserted > 0:
                        logger.info(f"✅ Inserted {inserted} items into Qdrant")
                        
                        # Mark as COMPLETED in database
                        links = [n.link for n in news_batch[:inserted]]
                        updated = self.db_manager.mark_news_completed(links)
                        logger.info(f"✅ Marked {updated} news as COMPLETED in database")
                        
                        self.total_processed += inserted
                        self.successful_batches += 1
                        
                        # If partial success, count the rest as errors
                        if inserted < len(news_batch):
                            errors = len(news_batch) - inserted
                            self.total_errors += errors
                            logger.warning(
                                f"⚠️  Partial success: {inserted}/{len(news_batch)} inserted, "
                                f"{errors} failed"
                            )
                    else:
                        logger.warning(f"⚠️  No items inserted from batch")
                        self.total_errors += len(news_batch)
                        self.failed_batches += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error processing batch: {e}", exc_info=True)
                    self.total_errors += len(news_batch)
                    self.failed_batches += 1
                    time.sleep(5)  # Brief pause before retry
                    continue

                # Brief pause between batches
                time.sleep(2)
                
                # Log stats periodically (every 5 minutes)
                if time.time() - last_stats_log >= STATS_LOG_INTERVAL:
                    self._log_statistics()
                    self._log_collection_stats()
                    last_stats_log = time.time()
                
                # Log stats every 10 batches
                elif self.total_batches % 10 == 0:
                    self._log_statistics()
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Scheduler stopped by user (KeyboardInterrupt)")
            
        except Exception as e:
            logger.critical(f"❌ Scheduler crashed: {e}", exc_info=True)
            
        finally:
            # Always log final statistics
            logger.info("\n" + "=" * 80)
            logger.info("FINAL STATISTICS")
            logger.info("=" * 80)
            self._log_statistics()
            self._log_collection_stats()
            logger.info("Scheduler shutdown complete")


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
    → Processes articles from IRNA, ISNA, Tasnim, Shargh, Donya-e-Eqtesad all together

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
    python news_historical_embedding_scheduler.py --source Shargh &
    python news_historical_embedding_scheduler.py --source Donya-e-Eqtesad &

Custom Settings:
    # Custom batch size (articles fetched per iteration)
    python news_historical_embedding_scheduler.py --batch-size 50
    
    # Custom chunk size (points uploaded to Qdrant per batch)
    python news_historical_embedding_scheduler.py --chunk-size 50
    
    # Custom poll interval (seconds to wait when idle)
    python news_historical_embedding_scheduler.py --poll-interval 60
        """
    )
    
    parser.add_argument(
        '--source',
        type=str,
        default=None,
        help='Optional: Filter by news source (e.g., IRNA, ISNA, Tasnim, Shargh, Donya-e-Eqtesad). '
             'If not provided, processes ALL sources (default behavior).'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='Number of articles to fetch from database per iteration (default: 20)'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=100,
        help='Maximum number of points to upload to Qdrant in single batch (default: 100). '
             'Lower values = more API calls but better for large collections.'
    )
    
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=30,
        help='Seconds to wait when no pending articles (default: 30)'
    )
    
    return parser.parse_args()


def validate_source(source: Optional[str]) -> bool:
    """
    Validate source name if provided.
    
    Returns True if valid or None, False otherwise.
    """
    if source is None:
        return True
    
    # Try to import known sources for validation
    try:
        from sources import IRNA, ISNA, TASNIM, SHARGH, DONYAYE_EQTESAD
        valid_sources = [IRNA, ISNA, TASNIM, SHARGH, DONYAYE_EQTESAD]
        
        if source not in valid_sources:
            logger.warning(
                f"⚠️  Source '{source}' not in known sources: {valid_sources}"
            )
            logger.warning("Proceeding anyway, but check for typos!")
            return True  # Allow anyway
            
        return True
        
    except ImportError:
        # sources.py not available, skip validation
        logger.info("ℹ️  Cannot validate source (sources.py not found)")
        return True


def main():
    """Main entry point"""
    
    # Parse arguments
    args = parse_arguments()
    
    logger.info("=" * 80)
    logger.info("Starting Historical Embedding Scheduler")
    logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    # Initialize database manager
    try:
        db_manager = DatabaseManager(settings.database)
        logger.info("✅ DatabaseManager initialized")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize DatabaseManager: {e}")
        sys.exit(1)
    
    # Initialize vector database manager
    try:
        vector_manager = VectorDBManager(
            qdrant_config=settings.qdrant,
            embedding_config=settings.embedding,
            logger=logger,
            chunk_size=args.chunk_size
        )
        logger.info(
            f"✅ VectorDBManager ready with {settings.embedding.provider} "
            f"(chunk_size={args.chunk_size})"
        )
    except Exception as e:
        logger.critical(f"❌ Failed to initialize VectorDBManager: {e}")
        sys.exit(1)
    
    # Validate source if provided
    if args.source:
        if not validate_source(args.source):
            logger.error(f"Invalid source: {args.source}")
            sys.exit(1)
    else:
        logger.info("ℹ️  No --source provided: will process ALL sources (default mode)")
    
    # Perform health check
    logger.info("Performing health check...")
    if not vector_manager.health_check():
        logger.critical("❌ Health check failed. Please check Qdrant connection.")
        sys.exit(1)
    
    logger.info("✅ Health check passed")
    
    # Start scheduler
    scheduler = EmbeddingScheduler(
        db_manager=db_manager,
        vector_manager=vector_manager,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        source=args.source  # None = ALL sources (default)
    )
    
    # Run the scheduler
    scheduler.run_forever()


if __name__ == "__main__":
    main()