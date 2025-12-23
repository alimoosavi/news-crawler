#!/usr/bin/env python3
"""
Historical Embedding Scheduler with Optional Source Filtering

Processes historical news from PostgreSQL database and generates embeddings.
Uses embedding provider configured in .env (OpenAI, Ollama, or Rayen).

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
- Health checks before starting
- Graceful error handling with detailed diagnostics

Usage Examples:
    # DEFAULT: Process all sources together (backward compatible)
    python news_historical_embedding_scheduler.py
    
    # OPTIONAL: Process specific source for parallel processing
    python news_historical_embedding_scheduler.py --source IRNA
    
    # OPTIONAL: Custom batch size
    python news_historical_embedding_scheduler.py --source ISNA --batch-size 50
    
    # Run health check only
    python news_historical_embedding_scheduler.py --health-check
    
    # PARALLEL: Run multiple instances for maximum throughput
    python news_historical_embedding_scheduler.py --source IRNA &
    python news_historical_embedding_scheduler.py --source ISNA &
    python news_historical_embedding_scheduler.py --source Tasnim &
    python news_historical_embedding_scheduler.py --source Donya-e-Eqtesad &

FIXED: Added comprehensive health checks before processing
FIXED: Better error handling and diagnostics for embedding failures
"""
import argparse
import logging
import time
import sys
from typing import Optional

from database_manager import DatabaseManager
from vector_db_manager import VectorDBManager
from config import settings
from embedding_service import EmbeddingValidationError

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
        source: Optional[str] = None,
        max_consecutive_errors: int = 5
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
            max_consecutive_errors: Stop after this many consecutive errors
        """
        self.db_manager = db_manager
        self.vector_manager = vector_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.source = source  # None = ALL sources (default)
        self.max_consecutive_errors = max_consecutive_errors
        
        # Statistics
        self.total_processed = 0
        self.total_errors = 0
        self.consecutive_errors = 0
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
        success_count = self.total_processed - self.total_errors
        success_rate = success_count / max(self.total_processed, 1) * 100
        
        logger.info("=" * 80)
        logger.info("STATISTICS:")
        logger.info(f"  Total Processed: {self.total_processed}")
        logger.info(f"  Total Errors: {self.total_errors}")
        logger.info(f"  Success Rate: {success_rate:.1f}%")
        logger.info(f"  Elapsed Time: {elapsed:.0f}s")
        logger.info(f"  Processing Rate: {rate:.2f} articles/sec")
        logger.info(f"  Consecutive Errors: {self.consecutive_errors}")
        logger.info("=" * 80)

    def run_health_check(self) -> bool:
        """
        Run health check on all components.
        
        Returns:
            True if all checks pass, False otherwise
        """
        logger.info("=" * 80)
        logger.info("Running Health Check...")
        logger.info("=" * 80)
        
        all_ok = True
        
        # Check vector DB health
        try:
            health = self.vector_manager.health_check()
            
            logger.info(f"Qdrant Connected: {'✅' if health['qdrant_connected'] else '❌'}")
            logger.info(f"Collection Exists: {'✅' if health['collection_exists'] else '❌'}")
            
            if health['collection_exists']:
                logger.info(f"Collection Dimension: {health['collection_dimension']}")
                logger.info(f"Collection Points: {health['collection_points']:,}")
            
            logger.info(f"Embedding Service: {health['embedding_service']}")
            logger.info(f"Expected Dimension: {health['embedding_dimension']}")
            logger.info(f"Test Embedding: {'✅' if health['test_embedding_ok'] else '❌'}")
            
            if 'test_embedding_error' in health:
                logger.error(f"Test Embedding Error: {health['test_embedding_error']}")
                all_ok = False
            
            if not health['qdrant_connected']:
                logger.error("❌ Cannot connect to Qdrant")
                all_ok = False
            
            if not health['test_embedding_ok']:
                logger.error("❌ Embedding service test failed")
                all_ok = False
                
            # Check dimension match
            if health['collection_exists'] and health['collection_dimension'] != health['embedding_dimension']:
                logger.error(
                    f"❌ Dimension mismatch! Collection: {health['collection_dimension']}, "
                    f"Embedding: {health['embedding_dimension']}"
                )
                all_ok = False
                
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            all_ok = False
        
        logger.info("=" * 80)
        logger.info(f"Health Check Result: {'✅ PASS' if all_ok else '❌ FAIL'}")
        logger.info("=" * 80)
        
        return all_ok

    def run_forever(self):
        """Main processing loop"""
        logger.info("=" * 80)
        logger.info("Historical Embedding Scheduler Started")
        logger.info(f"Provider: {settings.embedding.provider}")
        
        if settings.embedding.provider == 'openai':
            logger.info(f"Model: {settings.embedding.openai_model}")
        elif settings.embedding.provider == 'rayen':
            logger.info(f"Model: {settings.embedding.rayen_model}")
            logger.info(f"Base URL: {settings.embedding.rayen_base_url}")
        else:
            logger.info(f"Model: {settings.embedding.ollama_model}")
            
        logger.info(f"Dimension: {self.vector_manager.embedding_dim}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info(f"Poll Interval: {self.poll_interval}s")
        logger.info(f"Max Consecutive Errors: {self.max_consecutive_errors}")
        
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
        
        # Run initial health check
        if not self.run_health_check():
            logger.critical("❌ Health check failed! Fix issues before continuing.")
            logger.info("Run with --health-check to diagnose problems.")
            sys.exit(1)
        
        # Ensure collection exists
        try:
            self.vector_manager.ensure_collection_exists()
        except ValueError as e:
            logger.critical(f"❌ Collection setup failed: {e}")
            sys.exit(1)
        
        idle_cycles = 0
        
        try:
            while True:
                # Check for too many consecutive errors
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.critical(
                        f"❌ Too many consecutive errors ({self.consecutive_errors}). "
                        f"Stopping to prevent further issues."
                    )
                    logger.info("Check the embedding service and Qdrant connection.")
                    self._log_statistics()
                    sys.exit(1)
                
                # Fetch pending news
                news_batch = self._fetch_pending_batch()
                
                if not news_batch:
                    idle_cycles += 1
                    self.consecutive_errors = 0  # Reset on successful fetch (even if empty)
                    
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
                    
                    if inserted > 0:
                        logger.info(f"✅ Inserted {inserted} items into Qdrant")
                        
                        # Mark as COMPLETED
                        links = [n.link for n in news_batch[:inserted]]  # Only mark successfully inserted
                        updated = self.db_manager.mark_news_completed(links)
                        logger.info(f"✅ Marked {updated} news as COMPLETED")
                        
                        self.total_processed += inserted
                        self.consecutive_errors = 0  # Reset on success
                    else:
                        logger.warning("⚠️ No items inserted - check embedding service")
                        self.total_errors += len(news_batch)
                        self.consecutive_errors += 1
                        
                except EmbeddingValidationError as e:
                    logger.error(f"❌ Embedding validation error: {e}")
                    if e.failed_indices:
                        logger.error(f"   Failed indices: {e.failed_indices[:10]}...")
                    self.total_errors += len(news_batch)
                    self.consecutive_errors += 1
                    time.sleep(5)
                    continue
                    
                except Exception as e:
                    logger.error(f"❌ Error processing batch: {e}", exc_info=True)
                    self.total_errors += len(news_batch)
                    self.consecutive_errors += 1
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
    
Health Check:
    python news_historical_embedding_scheduler.py --health-check

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
    
    parser.add_argument(
        '--max-errors',
        type=int,
        default=5,
        help='Stop after this many consecutive errors (default: 5)'
    )
    
    parser.add_argument(
        '--health-check',
        action='store_true',
        help='Run health check only and exit'
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
            from news_publishers import IRNA, ISNA, TASNIM, DONYAYE_EQTESAD, SHARGH
            valid_sources = [IRNA, ISNA, TASNIM, DONYAYE_EQTESAD, SHARGH]
            
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
    
    # Create scheduler
    scheduler = EmbeddingScheduler(
        db_manager=db_manager,
        vector_manager=vector_manager,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        source=args.source,
        max_consecutive_errors=args.max_errors
    )
    
    # Health check only mode
    if args.health_check:
        success = scheduler.run_health_check()
        sys.exit(0 if success else 1)
    
    # Start scheduler
    scheduler.run_forever()


if __name__ == "__main__":
    main()