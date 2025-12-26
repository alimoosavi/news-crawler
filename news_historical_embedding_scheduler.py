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
- Auto-creation of Qdrant collection on startup

Usage Examples:
    # DEFAULT: Process all sources together (backward compatible)
    python news_historical_embedding_scheduler.py
    
    # OPTIONAL: Process specific source for parallel processing
    python news_historical_embedding_scheduler.py --source IRNA
    
    # OPTIONAL: Custom batch size
    python news_historical_embedding_scheduler.py --source ISNA --batch-size 50
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
        self.db_manager = db_manager
        self.vector_manager = vector_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.source = source  # None = ALL sources (default)
        
        # Statistics
        self.total_processed = 0
        self.total_errors = 0
        self.start_time = time.time()
        self.max_consecutive_errors = 5

    def _fetch_pending_batch(self):
        """Fetch pending news batch based on source filter."""
        if self.source:
            return self.db_manager.get_pending_news_batch_by_source(
                source=self.source,
                limit=self.batch_size
            )
        else:
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
        elif settings.embedding.provider == 'rayen':
            logger.info(f"Model: {settings.embedding.rayen_model}")
        else:
            logger.info(f"Model: {settings.embedding.ollama_model}")
            
        logger.info(f"Dimension: {settings.embedding.embedding_dim}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info(f"Poll Interval: {self.poll_interval}s")
        logger.info(f"Max Consecutive Errors: {self.max_consecutive_errors}")
        
        # Log source filter mode
        if self.source:
            logger.info(f"Mode: SOURCE-SPECIFIC")
            logger.info(f"Source Filter: {self.source}")
            try:
                pending_count = self.db_manager.get_pending_count_by_source(self.source)
                logger.info(f"Pending Articles for {self.source}: {pending_count:,}")
            except Exception as e:
                logger.warning(f"Could not get pending count: {e}")
        else:
            logger.info(f"Mode: ALL SOURCES (default)")
            logger.info(f"Source Filter: NONE (processing all sources)")
            try:
                total_pending = self.db_manager.get_total_pending_count()
                logger.info(f"Total Pending Articles (all sources): {total_pending:,}")
            except Exception as e:
                logger.warning(f"Could not get pending count: {e}")
            
        logger.info("=" * 80)
        
        # ---------------------------------------------------------
        # 🛠️ CRITICAL FIX: Ensure collection exists before starting
        # ---------------------------------------------------------
        logger.info("🔹 Verifying Qdrant Collection...")
        try:
            self.vector_manager.ensure_collection_exists()
        except Exception as e:
            logger.critical(f"❌ Failed to create/verify collection: {e}")
            sys.exit(1)

        # ---------------------------------------------------------
        # 🏥 Health Check (Fixed to handle boolean)
        # ---------------------------------------------------------
        logger.info("=" * 80)
        logger.info("Running Health Check...")
        logger.info("=" * 80)
        
        is_healthy = False
        try:
            if hasattr(self.vector_manager, 'health_check'):
                is_healthy = self.vector_manager.health_check()
            else:
                logger.warning("⚠️ VectorManager has no health_check method. Skipping.")
                is_healthy = True # Assume ok if method missing
        except Exception as e:
            logger.error(f"❌ Health check raised exception: {e}")
            is_healthy = False

        if not is_healthy:
            logger.critical("❌ Health check failed! Fix issues before continuing.")
            logger.info("Run with --health-check to diagnose problems.")
            sys.exit(1)
        
        logger.info("✅ System is HEALTHY. Starting processing loop...")
        logger.info("=" * 80)
        
        idle_cycles = 0
        consecutive_errors = 0
        
        try:
            while True:
                # Break loop if too many errors
                if consecutive_errors >= self.max_consecutive_errors:
                    logger.critical(f"❌ Stopped due to {consecutive_errors} consecutive errors.")
                    break

                # Fetch pending news
                try:
                    news_batch = self._fetch_pending_batch()
                except Exception as e:
                    logger.error(f"❌ Database error fetching batch: {e}")
                    consecutive_errors += 1
                    time.sleep(5)
                    continue
                
                if not news_batch:
                    idle_cycles += 1
                    consecutive_errors = 0 # Reset on valid check
                    
                    if idle_cycles == 1:
                        msg = f"No pending news for source '{self.source}'." if self.source else "No pending news."
                        logger.info(f"{msg} Sleeping for {self.poll_interval}s...")
                    elif idle_cycles % 10 == 0:
                        logger.info(f"Still idle (cycle {idle_cycles}). Waiting...")
                    
                    time.sleep(self.poll_interval)
                    continue
                
                # Reset idle counter
                idle_cycles = 0
                
                # Log batch info
                if self.source:
                    logger.info(f"📥 Fetched {len(news_batch)} pending news from {self.source}")
                else:
                    sources_in_batch = set(n.source for n in news_batch)
                    logger.info(f"📥 Fetched {len(news_batch)} pending news from: {', '.join(sorted(sources_in_batch))}")

                # Persist into Qdrant
                try:
                    inserted = self.vector_manager.persist_news_batch(news_batch)
                    
                    if inserted > 0:
                        logger.info(f"✅ Inserted {inserted} items into Qdrant")
                        links = [n.link for n in news_batch]
                        updated = self.db_manager.mark_news_completed(links)
                        logger.info(f"✅ Marked {updated} news as COMPLETED")
                        
                        self.total_processed += inserted
                        consecutive_errors = 0 # Reset success
                    else:
                        logger.warning("⚠️  No items inserted (validation failed or empty batch)")
                        self.total_errors += len(news_batch)
                        consecutive_errors += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error processing batch: {e}")
                    self.total_errors += len(news_batch)
                    consecutive_errors += 1
                    time.sleep(5)
                    continue

                # Brief pause between batches
                time.sleep(0.5)
                
                # Log stats every 10 batches
                if self.total_processed > 0 and self.total_processed % (self.batch_size * 10) == 0:
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
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--source',
        type=str,
        default=None,
        help='Filter by news source (e.g., IRNA, Tasnim). Default: ALL sources.'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=20,
        help='Articles per batch (default: 20)'
    )
    
    parser.add_argument(
        '--poll-interval',
        type=int,
        default=30,
        help='Wait time when idle (default: 30s)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
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
    
    # Start scheduler
    scheduler = EmbeddingScheduler(
        db_manager=db_manager,
        vector_manager=vector_manager,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        source=args.source
    )
    
    scheduler.run_forever()


if __name__ == "__main__":
    main()