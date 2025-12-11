"""
Historical Links Scheduler

Orchestrates historical news link collection across multiple sources.
Supports CLI arguments for flexible source selection and date ranges.

Features:
- Multi-source support (IRNA, ISNA, Tasnim, Donya-e-Eqtesad, Shargh)
- CLI argument parsing for source filtering
- Configurable date ranges
- Prometheus metrics integration
- Parallel or sequential execution modes

Usage:
    # Collect from all sources
    python historical_links_scheduler.py

    # Collect from specific sources
    python historical_links_scheduler.py --sources IRNA Shargh

    # Custom date range
    python historical_links_scheduler.py --start-date 2024-01-01 --end-date 2024-12-31

    # Specific source with custom dates
    python historical_links_scheduler.py --sources Shargh --start-date 2024-11-01 --end-date 2024-11-30
"""

import argparse
import logging
import sys
from datetime import date as dt_date, datetime
from typing import List, Optional

from prometheus_client import Summary

from collectors.irna.historical_links_collector import IRNAHistoricalLinksCollector
from collectors.tasnim.historical_links_collector import TasnimHistoricalLinksCollector
from collectors.donyaye_eghtesad.historical_links_collector import DonyaEqtesadHistoricalLinksCollector
from collectors.isna.historical_links_collector import ISNAHistoricalLinksCollector
from collectors.shargh.historical_links_collector import SharghHistoricalLinksCollector
from config import settings
from database_manager import DatabaseManager
from news_publishers import IRNA, TASNIM, ISNA, DONYAYE_EQTESAD, SHARGH

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("HistoricalLinksScheduler")

# -------------------------------
# Prometheus Metrics
# -------------------------------
CRAWL_DURATION_SECONDS = Summary(
    'historical_crawler_duration_seconds',
    'Duration of historical crawl for a source',
    ['source']
)

CRAWL_LINKS_COLLECTED = Summary(
    'historical_crawler_links_collected',
    'Number of links collected during historical crawl',
    ['source']
)

# -------------------------------
# Collector Registry
# -------------------------------
HISTORICAL_RANGE_LINKS_COLLECTOR = {
    IRNA: IRNAHistoricalLinksCollector,
    TASNIM: TasnimHistoricalLinksCollector,
    DONYAYE_EQTESAD: DonyaEqtesadHistoricalLinksCollector,
    ISNA: ISNAHistoricalLinksCollector,
    SHARGH: SharghHistoricalLinksCollector,
}

# Valid source names for CLI
VALID_SOURCES = list(HISTORICAL_RANGE_LINKS_COLLECTOR.keys())


def get_db_manager():
    """Initialize database manager with settings"""
    return DatabaseManager(db_config=settings.database)


def parse_date(date_str: str) -> dt_date:
    """
    Parse date string in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to parse
        
    Returns:
        Parsed date object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format '{date_str}'. Expected YYYY-MM-DD") from e


def validate_sources(sources: List[str]) -> List[str]:
    """
    Validate that all requested sources are available.
    
    Args:
        sources: List of source names to validate
        
    Returns:
        List of validated sources
        
    Raises:
        ValueError: If any source is invalid
    """
    invalid_sources = [s for s in sources if s not in VALID_SOURCES]
    
    if invalid_sources:
        raise ValueError(
            f"Invalid source(s): {', '.join(invalid_sources)}. "
            f"Valid sources: {', '.join(VALID_SOURCES)}"
        )
    
    return sources


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Historical news links crawler for multiple sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect from all sources (default date range)
  python historical_links_scheduler.py

  # Collect from specific sources
  python historical_links_scheduler.py --sources IRNA Shargh

  # Custom date range for all sources
  python historical_links_scheduler.py --start-date 2024-01-01 --end-date 2024-12-31

  # Specific source with custom dates
  python historical_links_scheduler.py --sources Shargh --start-date 2024-11-01 --end-date 2024-11-30

  # Multiple sources with date range
  python historical_links_scheduler.py --sources IRNA ISNA Shargh --start-date 2024-01-01 --end-date 2024-03-31
        """
    )
    
    parser.add_argument(
        '--sources',
        nargs='+',
        choices=VALID_SOURCES,
        default=None,
        metavar='SOURCE',
        help=f'News sources to crawl. Choices: {", ".join(VALID_SOURCES)}. Default: all sources'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        metavar='YYYY-MM-DD',
        help='Start date for historical crawl (format: YYYY-MM-DD). Default: 2024-11-29'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        metavar='YYYY-MM-DD',
        help='End date for historical crawl (format: YYYY-MM-DD). Default: today'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of days to process per batch (default: 10)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers per source (default: 4)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print configuration without executing crawl'
    )
    
    return parser.parse_args()


def collect_source(
    source: str,
    collector_class,
    db_manager: DatabaseManager,
    start_date: dt_date,
    end_date: dt_date,
    batch_size: int = 10,
    workers: int = 4
) -> None:
    """
    Collect historical links for a single source.
    
    Args:
        source: Source name
        collector_class: Collector class to instantiate
        db_manager: Database manager instance
        start_date: Start date for collection
        end_date: End date for collection
        batch_size: Batch size for processing
        workers: Number of parallel workers
    """
    print("\n" + "-" * 80)
    print(f"🔎 Starting historical crawl for: {source}")
    print(f"   Date range: {start_date} → {end_date}")
    print(f"   Configuration: batch_size={batch_size}, workers={workers}")
    print("-" * 80)
    
    try:
        # Track duration with Prometheus metric
        with CRAWL_DURATION_SECONDS.labels(source=source).time():
            collector = collector_class(
                db_manager=db_manager,
                batch_size=batch_size,
                workers=workers
            )
            collector.collect_range(start_date=start_date, end_date=end_date)
        
        print(f"✅ Successfully completed crawl for {source}")
        
    except Exception as e:
        logger.error(f"❌ Failed to crawl {source}: {e}", exc_info=True)
        print(f"❌ Error crawling {source}: {e}")


def main():
    """Main execution function"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Initialize database manager
    database_manager = get_db_manager()
    
    # Determine sources to crawl
    if args.sources:
        sources_to_crawl = args.sources
        logger.info(f"Crawling selected sources: {', '.join(sources_to_crawl)}")
    else:
        sources_to_crawl = VALID_SOURCES
        logger.info(f"Crawling all sources: {', '.join(sources_to_crawl)}")
    
    # Determine date range
    if args.start_date:
        try:
            start_date_greg = parse_date(args.start_date)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)
    else:
        start_date_greg = dt_date(2024, 11, 29)  # Default start date
        logger.info(f"Using default start date: {start_date_greg}")
    
    if args.end_date:
        try:
            end_date_greg = parse_date(args.end_date)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)
    else:
        end_date_greg = dt_date.today()  # Default to today
        logger.info(f"Using default end date (today): {end_date_greg}")
    
    # Validate date range
    if start_date_greg > end_date_greg:
        logger.error("Start date cannot be after end date")
        sys.exit(1)
    
    # Calculate total days
    total_days = (end_date_greg - start_date_greg).days + 1
    
    # Print configuration
    print("\n" + "=" * 80)
    print("HISTORICAL LINKS CRAWLER")
    print("=" * 80)
    print(f"Sources:     {', '.join(sources_to_crawl)}")
    print(f"Date range:  {start_date_greg} → {end_date_greg} ({total_days} days)")
    print(f"Batch size:  {args.batch_size} days")
    print(f"Workers:     {args.workers} parallel workers per source")
    print("=" * 80)
    
    if args.dry_run:
        print("\n🔍 DRY RUN MODE - No actual crawling will be performed")
        print("\nConfiguration:")
        for source in sources_to_crawl:
            collector_class = HISTORICAL_RANGE_LINKS_COLLECTOR[source]
            print(f"  - {source}: {collector_class.__name__}")
        print("\n✅ Dry run complete")
        return
    
    # Execute crawls
    try:
        successful_sources = []
        failed_sources = []
        
        for source in sources_to_crawl:
            collector_class = HISTORICAL_RANGE_LINKS_COLLECTOR[source]
            
            try:
                collect_source(
                    source=source,
                    collector_class=collector_class,
                    db_manager=database_manager,
                    start_date=start_date_greg,
                    end_date=end_date_greg,
                    batch_size=args.batch_size,
                    workers=args.workers
                )
                successful_sources.append(source)
                
            except Exception as e:
                logger.error(f"Error processing {source}: {e}")
                failed_sources.append(source)
        
        # Print summary
        print("\n" + "=" * 80)
        print("CRAWL SUMMARY")
        print("=" * 80)
        print(f"✅ Successful: {len(successful_sources)}/{len(sources_to_crawl)}")
        if successful_sources:
            for source in successful_sources:
                print(f"   ✓ {source}")
        
        if failed_sources:
            print(f"\n❌ Failed: {len(failed_sources)}/{len(sources_to_crawl)}")
            for source in failed_sources:
                print(f"   ✗ {source}")
        
        print("=" * 80)
        
        if failed_sources:
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Crawl interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"❌ Unexpected error in main execution: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()