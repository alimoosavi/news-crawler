#!/usr/bin/env python3
"""
Unified Historical Page Content Scheduler

Single entry point for historical page collection with:
- Async collectors for 5-10x performance improvement
- Built-in parallel execution support
- Automatic process management
- Comprehensive monitoring and statistics

USAGE:
    # Single instance mode
    python news_historical_page_scheduler.py --source ISNA
    
    # Parallel mode (recommended for speed)
    python news_historical_page_scheduler.py --source ISNA --parallel 3
    
    # All sources in parallel
    python news_historical_page_scheduler.py --source ALL --parallel 3
    
    # Custom configuration
    python news_historical_page_scheduler.py --source ISNA --parallel 3 --max-concurrent 10 --batch-size 50

FEATURES:
    ✓ Async collectors (5-10x faster than sync)
    ✓ Parallel execution (linear scaling)
    ✓ Automatic process management
    ✓ Progress tracking with ETA
    ✓ Retry tracking and failure handling
    ✓ Comprehensive statistics
    ✓ Process monitoring and control

PERFORMANCE:
    Single instance (concurrent=5):  ~5-10 pages/sec  (3-6 hours for 117K)
    3 instances (concurrent=5):      ~15-30 pages/sec (1-2 hours for 117K)
    5 instances (concurrent=10):     ~40-50 pages/sec (40-60 min for 117K)
"""
import argparse
import logging
import time
import sys
import os
import signal
import subprocess
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path

from collectors.irna.pages_collector import IRNAPageCollector
from collectors.tasnim.pages_collector import TasnimPageCollector
from collectors.donyaye_eghtesad.pages_collector import DonyaEqtesadPageCollector
from collectors.isna.pages_collector import ISNAPageCollector
from config import settings
from database_manager import DatabaseManager
from news_publishers import IRNA, TASNIM, ISNA, DONYAYE_EQTESAD
from schema import NewsLinkData, NewsData

logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("HistoricalPageScheduler")

# Map each publisher to its page collector class
HISTORICAL_PAGE_COLLECTORS = {
    IRNA: IRNAPageCollector,
    TASNIM: TasnimPageCollector,
    DONYAYE_EQTESAD: DonyaEqtesadPageCollector,
    ISNA: ISNAPageCollector
}

ALL_SOURCES = list(HISTORICAL_PAGE_COLLECTORS.keys())


class ProcessManager:
    """Manages multiple scheduler processes for parallel execution"""
    
    def __init__(self, log_dir: str = "logs"):
        self.processes = []
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
    
    def start_instance(
        self,
        source: str,
        instance_num: int,
        batch_size: int,
        max_concurrent: int,
        max_retries: int,
        poll_interval: int
    ) -> subprocess.Popen:
        """Start a single scheduler instance as a subprocess"""
        log_file = self.log_dir / f"{source}_instance_{instance_num}.log"
        
        cmd = [
            sys.executable,
            __file__,  # This script
            "--source", source,
            "--batch-size", str(batch_size),
            "--max-concurrent", str(max_concurrent),
            "--max-retries", str(max_retries),
            "--poll-interval", str(poll_interval),
            "--worker-mode",  # Special flag to indicate worker mode
        ]
        
        logger.info(f"   Starting instance {instance_num} (Log: {log_file})...")
        
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                start_new_session=True
            )
        
        logger.info(f"   ✓ Instance {instance_num} started (PID: {process.pid})")
        return process
    
    def start_parallel(
        self,
        sources: List[str],
        num_instances: int,
        batch_size: int,
        max_concurrent: int,
        max_retries: int,
        poll_interval: int
    ):
        """Start multiple scheduler instances for given sources"""
        logger.info("=" * 80)
        logger.info("🚀 PARALLEL EXECUTION MODE")
        logger.info("=" * 80)
        
        for source in sources:
            logger.info(f"\n📰 Starting {num_instances} instances for {source}")
            logger.info(f"   Batch Size: {batch_size}")
            logger.info(f"   Max Concurrent: {max_concurrent}")
            logger.info("")
            
            for i in range(1, num_instances + 1):
                process = self.start_instance(
                    source=source,
                    instance_num=i,
                    batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    max_retries=max_retries,
                    poll_interval=poll_interval
                )
                
                self.processes.append({
                    'source': source,
                    'instance': i,
                    'pid': process.pid,
                    'process': process
                })
                
                # Brief delay to avoid startup conflicts
                time.sleep(1)
        
        logger.info("\n" + "=" * 80)
        logger.info(f"✓ Started {len(self.processes)} total instances")
        logger.info("=" * 80)
    
    def show_status(self):
        """Show status of all running processes"""
        logger.info("\n📊 PROCESS STATUS")
        logger.info("-" * 80)
        
        alive_count = 0
        for proc_info in self.processes:
            process = proc_info['process']
            status = "RUNNING" if process.poll() is None else "STOPPED"
            if status == "RUNNING":
                alive_count += 1
            
            logger.info(
                f"  [{proc_info['source']}] Instance {proc_info['instance']}: "
                f"{status} (PID: {proc_info['pid']})"
            )
        
        logger.info("-" * 80)
        logger.info(f"Total: {alive_count}/{len(self.processes)} running")
        logger.info("")
    
    def wait_for_completion(self):
        """Wait for all processes to complete"""
        logger.info("\n⏳ Waiting for all instances to complete...")
        logger.info("   Press Ctrl+C to stop all instances\n")
        
        try:
            while True:
                # Check if any processes are still running
                running = [p for p in self.processes if p['process'].poll() is None]
                
                if not running:
                    logger.info("\n✓ All instances completed")
                    break
                
                # Show periodic status
                time.sleep(30)
                self.show_status()
        
        except KeyboardInterrupt:
            logger.info("\n\n🛑 Stopping all instances...")
            self.stop_all()
    
    def stop_all(self):
        """Stop all running processes"""
        for proc_info in self.processes:
            process = proc_info['process']
            if process.poll() is None:
                try:
                    os.kill(process.pid, signal.SIGTERM)
                    logger.info(f"   ✓ Stopped {proc_info['source']} instance {proc_info['instance']}")
                except ProcessLookupError:
                    pass
        
        logger.info("\n✓ All processes stopped")
    
    def show_logs_info(self, sources: List[str]):
        """Show information about log files"""
        logger.info("\n📚 LOG FILES")
        logger.info("-" * 80)
        
        for source in sources:
            log_files = list(self.log_dir.glob(f"{source}_instance_*.log"))
            if log_files:
                logger.info(f"\n  {source}:")
                for log_file in sorted(log_files):
                    logger.info(f"    {log_file}")
        
        logger.info("\n  View logs with:")
        logger.info(f"    tail -f {self.log_dir}/*.log")
        logger.info("")
    
    def estimate_completion(self, sources: List[str], num_instances: int, max_concurrent: int):
        """Estimate completion time"""
        RATE_PER_INSTANCE = 5 if max_concurrent <= 5 else 10  # Conservative estimate
        TOTAL_RATE = RATE_PER_INSTANCE * num_instances * len(sources)
        
        # Get total pending from database
        try:
            db_manager = DatabaseManager(db_config=settings.database)
            total_pending = sum(
                db_manager.get_pending_count_by_source(source)
                for source in sources
            )
        except Exception:
            total_pending = 117000 * len(sources)  # Fallback estimate
        
        ETA_SECONDS = total_pending / TOTAL_RATE if TOTAL_RATE > 0 else 0
        ETA_HOURS = ETA_SECONDS / 3600
        
        logger.info("\n📈 ESTIMATED COMPLETION TIME")
        logger.info("-" * 80)
        logger.info(f"  Total pending: ~{total_pending:,} articles")
        logger.info(f"  Processing rate: ~{TOTAL_RATE} pages/sec")
        logger.info(f"  Instances per source: {num_instances}")
        logger.info(f"  Total instances: {num_instances * len(sources)}")
        logger.info(f"  ETA: ~{ETA_HOURS:.1f} hours")
        logger.info("")


class HistoricalPageScheduler:
    """
    Single-instance scheduler for crawling historical news page content.
    
    This runs as a worker when called with --worker-mode flag,
    or as the main process in single-instance mode.
    """
    
    def __init__(
        self, 
        db_manager: DatabaseManager, 
        source: str,
        batch_size: int = 20,
        max_retries: int = 3,
        poll_interval: int = 30,
        max_concurrent: int = 5
    ):
        """Initialize the scheduler"""
        self.db_manager = db_manager
        self.db_manager.max_retries = max_retries
        self.source = source
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.poll_interval = poll_interval
        self.max_concurrent = max_concurrent
        
        # Get collector class for this source
        if source not in HISTORICAL_PAGE_COLLECTORS:
            raise ValueError(
                f"Unknown source: {source}. "
                f"Available sources: {list(HISTORICAL_PAGE_COLLECTORS.keys())}"
            )
        
        CollectorClass = HISTORICAL_PAGE_COLLECTORS[source]
        
        # Initialize collector with max_concurrent parameter
        try:
            self.collector = CollectorClass(max_concurrent=max_concurrent)
        except TypeError:
            # Fallback for sync collectors that don't have max_concurrent
            self.collector = CollectorClass()
            logger.warning(
                f"Collector for {source} doesn't support max_concurrent parameter. "
                "Using default synchronous mode."
            )
        
        # Statistics
        self.total_processed = 0
        self.total_failed = 0
        self.total_retries = 0
        self.start_time = time.time()
        self.batch_count = 0
        self.batch_times = []
    
    def process_batch(self) -> bool:
        """Process a single batch of pending links"""
        batch_start = time.time()
        
        # Fetch pending links
        pending_links = self.db_manager.get_pending_links_by_source(
            source=self.source,
            limit=self.batch_size,
            exclude_max_retries=True
        )
        
        if not pending_links:
            return False
        
        logger.info(f"📰 Fetched {len(pending_links)} pending links for {self.source}")
        
        # Count retry links
        retry_links = [
            link for link in pending_links 
            if hasattr(link, 'tried_count') and link.tried_count > 0
        ]
        if retry_links:
            logger.info(f"  🔄 {len(retry_links)} are retry attempts")
            self.total_retries += len(retry_links)
        
        # Collect page content
        successful_links = []
        failed_links = []
        
        try:
            results: Dict[str, NewsData] = self.collector.crawl_batch(pending_links)
            
            if results:
                logger.info(f"✅ Successfully crawled {len(results)} pages")
                successful_links = list(results.keys())
                self.total_processed += len(results)
                
                # Insert news data
                news_items = list(results.values())
                self.db_manager.insert_news_batch(news_items)
                
                # Mark as completed
                self.db_manager.mark_links_completed(successful_links)
            
            # Identify failed links
            all_links = {link.link for link in pending_links}
            successful_set = set(successful_links)
            failed_links = list(all_links - successful_set)
            
            if failed_links:
                logger.warning(f"❌ {len(failed_links)} links failed to crawl")
                self.total_failed += len(failed_links)
                
                # Increment try count
                self.db_manager.increment_link_try_count(failed_links)
                
                # Check for exceeded retries
                exceeded = self.db_manager.get_links_exceeding_retries(source=self.source)
                if exceeded:
                    logger.warning(
                        f"⛔️ {len(exceeded)} links exceeded max retries "
                        f"({self.max_retries}). Marking as FAILED..."
                    )
                    self.db_manager.mark_links_as_failed(exceeded)
        
        except Exception as e:
            logger.error(f"Error processing batch: {e}", exc_info=True)
            all_links = [link.link for link in pending_links]
            self.db_manager.increment_link_try_count(all_links)
            return False
        
        # Track batch timing
        batch_duration = time.time() - batch_start
        self.batch_times.append(batch_duration)
        if len(self.batch_times) > 10:
            self.batch_times.pop(0)
        
        self.batch_count += 1
        
        # Log batch performance
        batch_rate = len(pending_links) / batch_duration if batch_duration > 0 else 0
        logger.info(
            f"⚡ Batch completed in {batch_duration:.1f}s "
            f"({batch_rate:.1f} pages/sec)"
        )
        
        return True
    
    def log_statistics(self):
        """Log comprehensive processing statistics"""
        elapsed = time.time() - self.start_time
        rate = self.total_processed / elapsed if elapsed > 0 else 0
        
        # Get pending count
        pending_count = self.db_manager.get_pending_count_by_source(self.source)
        
        # Calculate ETA
        eta_seconds = pending_count / rate if rate > 0 else 0
        eta_hours = eta_seconds / 3600
        
        # Calculate average batch time
        avg_batch_time = (
            sum(self.batch_times) / len(self.batch_times) 
            if self.batch_times else 0
        )
        
        logger.info("=" * 80)
        logger.info(f"📊 STATISTICS for {self.source}")
        logger.info("-" * 80)
        logger.info(f"  Total Processed: {self.total_processed}")
        logger.info(f"  Total Failed: {self.total_failed}")
        logger.info(f"  Total Retries: {self.total_retries}")
        logger.info(f"  Pending: {pending_count}")
        
        total_attempts = self.total_processed + self.total_failed
        if total_attempts > 0:
            success_rate = (self.total_processed / total_attempts) * 100
            logger.info(f"  Success Rate: {success_rate:.1f}%")
        
        logger.info(f"  Elapsed Time: {elapsed/3600:.1f}h")
        logger.info(f"  Processing Rate: {rate:.2f} pages/sec")
        logger.info(f"  Avg Batch Time: {avg_batch_time:.1f}s")
        
        if pending_count > 0 and rate > 0:
            logger.info(f"  📈 ETA for remaining: {eta_hours:.1f} hours")
        
        # Get retry statistics
        try:
            retry_stats = self.db_manager.get_retry_statistics(source=self.source)
            logger.info("-" * 80)
            logger.info("  Retry Distribution:")
            for tries, count in sorted(retry_stats['retry_distribution'].items()):
                label = f"{tries}+ attempts" if tries >= self.max_retries else f"{tries} attempts"
                logger.info(f"    {label}: {count} links")
            
            if retry_stats['near_max_retries'] > 0:
                logger.info(
                    f"  ⚠️  Near Max Retries ({self.max_retries-1}+): "
                    f"{retry_stats['near_max_retries']}"
                )
            
            failed_count = self.db_manager.get_failed_links_count_by_source(self.source)
            if failed_count > 0:
                logger.warning(f"  💀 Permanently Failed Links: {failed_count}")
        except Exception as e:
            logger.error(f"Failed to get retry statistics: {e}")
        
        logger.info("=" * 80)
    
    def run_forever(self):
        """Main processing loop"""
        logger.info("=" * 80)
        logger.info("🚀 Historical Page Scheduler Started")
        logger.info(f"Source: {self.source}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info(f"Max Retries: {self.max_retries}")
        logger.info(f"Max Concurrent: {self.max_concurrent}")
        logger.info(f"Poll Interval: {self.poll_interval}s")
        logger.info("=" * 80)
        
        # Run initial cleanup
        logger.info("Running initial cleanup...")
        cleaned = self.db_manager.cleanup_exceeded_retries(source=self.source)
        if cleaned > 0:
            logger.warning(f"Marked {cleaned} links as FAILED (exceeded max retries)")
        
        # Log initial pending count
        initial_pending = self.db_manager.get_pending_count_by_source(self.source)
        logger.info(f"📊 Initial pending links: {initial_pending}")
        
        idle_cycles = 0
        
        try:
            while True:
                had_work = self.process_batch()
                
                if not had_work:
                    idle_cycles += 1
                    
                    if idle_cycles == 1:
                        logger.info(
                            f"😴 No pending links for {self.source}. "
                            f"Sleeping for {self.poll_interval}s..."
                        )
                        self.log_statistics()
                    elif idle_cycles % 10 == 0:
                        logger.info(f"Still idle (cycle {idle_cycles})...")
                        self.log_statistics()
                    
                    time.sleep(self.poll_interval)
                    continue
                
                idle_cycles = 0
                time.sleep(2)
                
                # Log stats every 10 batches
                if self.batch_count % 10 == 0:
                    self.log_statistics()
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Scheduler stopped by user")
            self.log_statistics()
        
        except Exception as e:
            logger.critical(f"💥 Scheduler crashed: {e}", exc_info=True)
            self.log_statistics()
            raise


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Unified Historical News Page Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Single instance mode
    python news_historical_page_scheduler.py --source ISNA
    
    # Parallel mode - 3 instances (recommended)
    python news_historical_page_scheduler.py --source ISNA --parallel 3
    
    # All sources in parallel
    python news_historical_page_scheduler.py --source ALL --parallel 3
    
    # High performance mode
    python news_historical_page_scheduler.py --source ISNA --parallel 5 --max-concurrent 10
    
    # Custom configuration
    python news_historical_page_scheduler.py --source ISNA --parallel 3 --batch-size 50 --max-concurrent 5

Performance Guide:
    Single instance (concurrent=5):  ~5-10 pages/sec  (3-6 hours for 117K)
    3 instances (concurrent=5):      ~15-30 pages/sec (1-2 hours for 117K)
    5 instances (concurrent=10):     ~40-50 pages/sec (40-60 min for 117K)
        """
    )
    
    parser.add_argument(
        '--source',
        type=str,
        required=True,
        choices=['IRNA', 'ISNA', 'Tasnim', 'Donya-e-Eqtesad', 'ALL'],
        help='News source to process (or ALL for all sources)'
    )
    
    parser.add_argument(
        '--parallel',
        type=int,
        default=None,
        help='Number of parallel instances per source (enables parallel mode)'
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
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum concurrent page fetches (default: 5)'
    )
    
    parser.add_argument(
        '--worker-mode',
        action='store_true',
        help=argparse.SUPPRESS  # Hidden flag for worker processes
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Initialize database manager
    try:
        db_manager = DatabaseManager(
            db_config=settings.database,
            max_retries=args.max_retries
        )
        logger.info("✅ DatabaseManager initialized")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize DatabaseManager: {e}")
        return 1
    
    # Determine sources
    if args.source == 'ALL':
        sources = ALL_SOURCES
    else:
        sources = [args.source]
    
    # PARALLEL MODE
    if args.parallel and not args.worker_mode:
        process_manager = ProcessManager()
        
        process_manager.start_parallel(
            sources=sources,
            num_instances=args.parallel,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
            max_retries=args.max_retries,
            poll_interval=args.poll_interval
        )
        
        process_manager.show_status()
        process_manager.estimate_completion(sources, args.parallel, args.max_concurrent)
        process_manager.show_logs_info(sources)
        process_manager.wait_for_completion()
        
        return 0
    
    # SINGLE INSTANCE MODE (or worker mode)
    if len(sources) > 1:
        logger.error("Cannot run multiple sources in single-instance mode. Use --parallel flag.")
        return 1
    
    scheduler = HistoricalPageScheduler(
        db_manager=db_manager,
        source=sources[0],
        batch_size=args.batch_size,
        max_retries=args.max_retries,
        poll_interval=args.poll_interval,
        max_concurrent=args.max_concurrent
    )
    
    scheduler.run_forever()
    
    return 0


if __name__ == "__main__":
    exit(main())