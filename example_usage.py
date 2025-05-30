from crawlers.isna.links_crawler import ISNALinksCrawler
from config import settings
from utils.shamsi_date import ShamsiDate
from datetime import date, timedelta
import logging

# Setup logging using config
logging.basicConfig(
    level=getattr(logging, settings.app.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def crawl_batch_concurrent():
    """Example of concurrent batch crawling for recent dates"""
    crawler = ISNALinksCrawler()
    
    try:
        # Get current Shamsi date and last 8 days
        current_year, current_month, current_day = ShamsiDate.current_shamsi_date()
        print(f"Current Shamsi date: {current_year}/{current_month}/{current_day}")
        
        # Get last 8 days including today
        dates_to_crawl = ShamsiDate.get_last_n_days(8)
        
        print("=== Concurrent Batch Crawling ===")
        print(f"Crawling {len(dates_to_crawl)} recent dates concurrently...")
        print("Dates to crawl:")
        for i, (year, month, day) in enumerate(dates_to_crawl):
            print(f"  {i+1}. {year}/{month}/{day}")
        
        # Run batch crawl with 3 concurrent workers
        batch_result = crawler.crawl_dates_batch(dates_to_crawl, max_workers=3)
        
        # Display results
        print(f"\n=== Batch Crawling Results ===")
        summary = batch_result['summary']
        print(f"Total dates processed: {summary['total_dates']}")
        print(f"Successful: {summary['successful_dates']}")
        print(f"Failed: {summary['failed_dates']}")
        print(f"Total links crawled: {summary['total_links']}")
        
        print(f"\n=== Detailed Results ===")
        for date_tuple, result in batch_result['results'].items():
            year, month, day = date_tuple
            if result['success']:
                print(f"✓ {year}/{month}/{day}: {result['links_count']} links")
            else:
                print(f"✗ {year}/{month}/{day}: {result['error']}")
        
        return summary['total_links']
        
    finally:
        crawler.cleanup()

def crawl_last_n_days_concurrent(days=10, max_workers=3):
    """Crawl last N days using concurrent processing with dynamic dates"""
    crawler = ISNALinksCrawler()
    
    try:
        # Get current Shamsi date
        current_year, current_month, current_day = ShamsiDate.current_shamsi_date()
        print(f"Current Shamsi date: {current_year}/{current_month}/{current_day}")
        
        # Generate date list for last N days
        dates_to_crawl = ShamsiDate.get_last_n_days(days)
        
        print(f"=== Concurrent Crawling for Last {days} Days ===")
        print(f"Using {max_workers} concurrent workers...")
        print("Date range:")
        if dates_to_crawl:
            first_date = dates_to_crawl[-1]  # Oldest date
            last_date = dates_to_crawl[0]    # Most recent date
            print(f"  From: {first_date[0]}/{first_date[1]}/{first_date[2]}")
            print(f"  To: {last_date[0]}/{last_date[1]}/{last_date[2]}")
        
        # Run batch crawl
        batch_result = crawler.crawl_dates_batch(dates_to_crawl, max_workers=max_workers)
        
        # Display results
        summary = batch_result['summary']
        print(f"\n=== Summary ===")
        print(f"Total dates: {summary['total_dates']}")
        print(f"Successful: {summary['successful_dates']}")
        print(f"Failed: {summary['failed_dates']}")
        print(f"Total links: {summary['total_links']}")
        
        return summary['total_links']
        
    finally:
        crawler.cleanup()

def crawl_custom_date_range(start_days_ago=10, end_days_ago=0, max_workers=3):
    """Crawl a custom range of days ago"""
    crawler = ISNALinksCrawler()
    
    try:
        current_year, current_month, current_day = ShamsiDate.current_shamsi_date()
        print(f"Current Shamsi date: {current_year}/{current_month}/{current_day}")
        
        # Generate date range
        dates_to_crawl = []
        for days_ago in range(end_days_ago, start_days_ago + 1):
            year, month, day = ShamsiDate.subtract_days(current_year, current_month, current_day, days_ago)
            dates_to_crawl.append((year, month, day))
        
        # Reverse to have most recent first
        dates_to_crawl.reverse()
        
        print(f"=== Crawling Custom Date Range ===")
        print(f"From {start_days_ago} days ago to {end_days_ago} days ago")
        print(f"Using {max_workers} concurrent workers...")
        print(f"Dates to crawl: {len(dates_to_crawl)} dates")
        
        # Run batch crawl
        batch_result = crawler.crawl_dates_batch(dates_to_crawl, max_workers=max_workers)
        
        # Display results
        summary = batch_result['summary']
        print(f"\n=== Summary ===")
        print(f"Total dates: {summary['total_dates']}")
        print(f"Successful: {summary['successful_dates']}")
        print(f"Failed: {summary['failed_dates']}")
        print(f"Total links: {summary['total_links']}")
        
        return summary['total_links']
        
    finally:
        crawler.cleanup()

def show_database_stats():
    """Show database statistics"""
    crawler = ISNALinksCrawler()
    
    try:
        # Get all unprocessed links
        all_unprocessed = crawler.get_unprocessed_links()
        
        print("=== Database Statistics ===")
        print(f"Total unprocessed ISNA links: {len(all_unprocessed)}")
        
        # Group by date
        date_counts = {}
        for link in all_unprocessed:
            link_date = str(link['date'])
            date_counts[link_date] = date_counts.get(link_date, 0) + 1
        
        print("\nLinks by date:")
        for date_str, count in sorted(date_counts.items(), reverse=True):
            print(f"  {date_str}: {count} links")
        
        # Show recent links
        print(f"\nRecent links (first 5):")
        for i, link in enumerate(all_unprocessed[:5], 1):
            print(f"{i}. {link['title'][:50]}...")
            print(f"   URL: {link['link']}")
            print(f"   Date: {link['date']}")
            print()
            
    finally:
        crawler.cleanup()

def main():
    """Main function demonstrating concurrent crawling scenarios with dynamic dates"""
    
    # Show current Shamsi date
    current_year, current_month, current_day = ShamsiDate.current_shamsi_date()
    print(f"🗓️  Current Shamsi Date: {current_year}/{current_month}/{current_day}")
    print(f"📅 Current Shamsi Month: {ShamsiDate.MONTH_NAMES[current_month-1]}")
    print()
    
    # Scenario 1: Concurrent batch crawling for recent dates
    print("SCENARIO 1: Concurrent batch crawling (last 8 days)")
    print("=" * 60)
    total_links = crawl_batch_concurrent()
    
    print(f"\n{'='*60}")
    print("SCENARIO 2: Concurrent crawling for last 10 days")
    print("=" * 60)
    total_links_concurrent = crawl_last_n_days_concurrent(days=10, max_workers=3)
    
    print(f"\n{'='*60}")
    print("SCENARIO 3: Custom date range (5-15 days ago)")
    print("=" * 60)
    custom_links = crawl_custom_date_range(start_days_ago=15, end_days_ago=5, max_workers=3)
    
    print(f"\n{'='*60}")
    print("SCENARIO 4: Show database statistics")
    print("=" * 60)
    show_database_stats()

if __name__ == "__main__":
    main() 