from crawlers.isna.page_crawler import ISNAPageCrawler
from crawlers.isna.links_crawler import ISNALinksCrawler
from database_manager import DatabaseManager
from utils.shamsi_date import ShamsiDate
from config import settings
import logging
import time

# Setup logging using config
logging.basicConfig(
    level=getattr(logging, settings.app.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def complete_news_pipeline():
    """Complete pipeline: crawl links then process them into articles"""
    
    print("🚀 ISNA News Crawling and Processing Pipeline")
    print("=" * 60)
    
    # Step 1: Crawl links for recent dates
    print("\n📡 Step 1: Crawling news links...")
    links_crawler = ISNALinksCrawler()
    
    try:
        # Get last 5 days of links
        dates_to_crawl = ShamsiDate.get_last_n_days(5)
        
        print(f"📅 Crawling links for {len(dates_to_crawl)} recent dates:")
        for i, (year, month, day) in enumerate(dates_to_crawl, 1):
            print(f"   {i}. {year}/{month}/{day}")
        
        # Crawl links with 2 concurrent workers
        batch_result = links_crawler.crawl_dates_batch(dates_to_crawl, max_workers=2)
        
        links_summary = batch_result['summary']
        print(f"\n✅ Links crawling completed:")
        print(f"   📊 Total links crawled: {links_summary['total_links']}")
        print(f"   ✅ Successful dates: {links_summary['successful_dates']}")
        print(f"   ❌ Failed dates: {links_summary['failed_dates']}")
        
        if links_summary['total_links'] == 0:
            print("⚠️  No new links found. Proceeding with existing unprocessed links...")
        
    except Exception as e:
        print(f"❌ Error in links crawling: {str(e)}")
        return
    finally:
        links_crawler.cleanup()
    
    # Step 2: Process unprocessed links into articles
    print(f"\n📰 Step 2: Processing links into articles...")
    page_crawler = ISNAPageCrawler()
    
    try:
        # Process up to 30 unprocessed links with 3 concurrent workers
        processing_result = page_crawler.crawl_unprocessed_links(
            source='ISNA', 
            limit=30, 
            max_workers=3
        )
        
        processing_summary = processing_result['summary']
        print(f"\n✅ Article processing completed:")
        print(f"   📊 Total links processed: {processing_summary['total_processed']}")
        print(f"   ✅ Successful extractions: {processing_summary['successful']}")
        print(f"   ❌ Failed extractions: {processing_summary['failed']}")
        
        if processing_summary['total_processed'] > 0:
            success_rate = (processing_summary['successful'] / processing_summary['total_processed']) * 100
            print(f"   📈 Success rate: {success_rate:.1f}%")
        
        # Show sample successful articles
        successful_results = [r for r in processing_result['results'].values() if r['success']]
        if successful_results:
            print(f"\n📄 Sample processed articles:")
            for i, result in enumerate(successful_results[:3], 1):
                print(f"   {i}. {result['title']}")
        
    except Exception as e:
        print(f"❌ Error in article processing: {str(e)}")
    finally:
        page_crawler.cleanup()

def process_unprocessed_links_only():
    """Process only existing unprocessed links without crawling new ones"""
    
    print("📰 Processing Existing Unprocessed Links")
    print("=" * 50)
    
    page_crawler = ISNAPageCrawler()
    
    try:
        # Check how many unprocessed links we have
        db_manager = DatabaseManager()
        db_manager.connect()
        unprocessed_count = len(db_manager.get_unprocessed_links(source='ISNA'))
        db_manager.close()
        
        print(f"📊 Found {unprocessed_count} unprocessed ISNA links")
        
        if unprocessed_count == 0:
            print("ℹ️  No unprocessed links found. Run link crawling first.")
            return
        
        # Process up to 50 unprocessed links with 4 concurrent workers
        result = page_crawler.crawl_unprocessed_links(
            source='ISNA', 
            limit=min(50, unprocessed_count), 
            max_workers=4
        )
        
        summary = result['summary']
        print(f"\n✅ Processing completed:")
        print(f"   📊 Total links: {summary['total_links']}")
        print(f"   ✅ Successful: {summary['successful']}")
        print(f"   ❌ Failed: {summary['failed']}")
        
        if summary['total_processed'] > 0:
            success_rate = (summary['successful'] / summary['total_processed']) * 100
            print(f"   📈 Success rate: {success_rate:.1f}%")
        
        # Show some successful results
        successful_results = [r for r in result['results'].values() if r['success']]
        if successful_results:
            print(f"\n📄 Sample successful articles:")
            for i, result in enumerate(successful_results[:5], 1):
                print(f"   {i}. {result['title']}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        page_crawler.cleanup()

def process_recent_links_by_date():
    """Process links from recent days only"""
    
    print("📅 Processing Recent Links by Date Range")
    print("=" * 50)
    
    page_crawler = ISNAPageCrawler()
    
    try:
        # Process links from last 7 days with 3 concurrent workers
        result = page_crawler.crawl_batch_by_date_range(
            source='ISNA', 
            days_back=7, 
            max_workers=3
        )
        
        if result:
            summary = result['summary']
            print(f"✅ Recent links processing completed:")
            print(f"   📅 Date range: {summary.get('date_range', 'N/A')}")
            print(f"   📊 Total links: {summary['total_links']}")
            print(f"   ✅ Successful: {summary['successful']}")
            print(f"   ❌ Failed: {summary['failed']}")
            
            if summary['total_processed'] > 0:
                success_rate = (summary['successful'] / summary['total_processed']) * 100
                print(f"   📈 Success rate: {success_rate:.1f}%")
        else:
            print("ℹ️  No recent unprocessed links found.")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        page_crawler.cleanup()

def show_comprehensive_statistics():
    """Show comprehensive database statistics"""
    
    print("📊 Comprehensive Database Statistics")
    print("=" * 50)
    
    db_manager = DatabaseManager()
    
    try:
        db_manager.connect()
        
        # Get general statistics
        stats = db_manager.get_news_statistics()
        print(f"📈 Database Overview:")
        print(f"   🔗 Total links: {stats['total_links']}")
        print(f"   ✅ Processed links: {stats['processed_links']}")
        print(f"   ⏳ Unprocessed links: {stats['unprocessed_links']}")
        print(f"   📰 Total articles: {stats['total_articles']}")
        print(f"   📡 Sources: {stats['sources_count']}")
        
        # Calculate processing rate
        if stats['total_links'] > 0:
            processing_rate = (stats['processed_links'] / stats['total_links']) * 100
            print(f"   📊 Processing rate: {processing_rate:.1f}%")
        
        # Get recent articles
        recent_articles = db_manager.get_news_articles(source='ISNA', limit=5)
        if recent_articles:
            print(f"\n📄 Recent articles:")
            for i, article in enumerate(recent_articles, 1):
                title = article['title'][:60] + '...' if article['title'] and len(article['title']) > 60 else article['title'] or 'No title'
                published = article['published_date'] or 'Unknown date'
                tags_count = len(article['tags']) if article['tags'] else 0
                
                print(f"   {i}. {title}")
                print(f"      📅 Published: {published}")
                print(f"      🏷️  Tags: {tags_count} tags")
                print()
        
        # Get unprocessed links sample
        unprocessed_links = db_manager.get_unprocessed_links(source='ISNA', limit=3)
        if unprocessed_links:
            print(f"⏳ Sample unprocessed links:")
            for i, link in enumerate(unprocessed_links, 1):
                title = link['title'][:50] + '...' if link['title'] else 'No title'
                print(f"   {i}. {title}")
                print(f"      🔗 {link['link']}")
                print(f"      📅 Date: {link['date']}")
                print()
        
    except Exception as e:
        print(f"❌ Error fetching statistics: {str(e)}")
    finally:
        db_manager.close()

def quick_test_single_article():
    """Quick test to process just one article"""
    
    print("🧪 Quick Test: Processing Single Article")
    print("=" * 45)
    
    page_crawler = ISNAPageCrawler()
    
    try:
        # Get one unprocessed link
        db_manager = DatabaseManager()
        db_manager.connect()
        unprocessed_links = db_manager.get_unprocessed_links(source='ISNA', limit=1)
        db_manager.close()
        
        if not unprocessed_links:
            print("ℹ️  No unprocessed links available for testing.")
            return
        
        link_data = unprocessed_links[0]
        print(f"🔗 Testing with link: {link_data['link']}")
        print(f"📰 Title: {link_data['title'] or 'No title'}")
        
        # Process the single link
        result = page_crawler.crawl_single_page(link_data)
        
        if result['success']:
            print(f"✅ Successfully processed!")
            print(f"   📰 Extracted title: {result['title']}")
            print(f"   🆔 News ID: {result['news_id']}")
        else:
            print(f"❌ Processing failed: {result['error']}")
        
    except Exception as e:
        print(f"❌ Error in test: {str(e)}")
    finally:
        page_crawler.cleanup()

def benchmark_processing_speed():
    """Benchmark the processing speed with different worker counts"""
    
    print("⚡ Processing Speed Benchmark")
    print("=" * 40)
    
    # Test with different worker counts
    worker_counts = [1, 2, 3, 4]
    
    for workers in worker_counts:
        print(f"\n🔧 Testing with {workers} worker(s)...")
        
        page_crawler = ISNAPageCrawler()
        start_time = time.time()
        
        try:
            result = page_crawler.crawl_unprocessed_links(
                source='ISNA', 
                limit=10,  # Process 10 links for benchmark
                max_workers=workers
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            summary = result['summary']
            if summary['total_processed'] > 0:
                speed = summary['total_processed'] / duration
                print(f"   ⏱️  Duration: {duration:.2f} seconds")
                print(f"   📊 Processed: {summary['total_processed']} links")
                print(f"   ⚡ Speed: {speed:.2f} links/second")
                print(f"   ✅ Success rate: {(summary['successful']/summary['total_processed']*100):.1f}%")
            else:
                print(f"   ℹ️  No links to process")
                break
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
        finally:
            page_crawler.cleanup()
            
        # Small delay between tests
        time.sleep(2)

def main():
    """Main function with menu-driven interface"""
    
    print("🗞️  ISNA News Crawler - Page Processing Examples")
    print("=" * 60)
    
    # Show current Shamsi date
    current_year, current_month, current_day = ShamsiDate.current_shamsi_date()
    print(f"📅 Current Shamsi Date: {current_year}/{current_month}/{current_day}")
    print(f"🌙 Current Month: {ShamsiDate.MONTH_NAMES[current_month-1]}")
    print()
    
    while True:
        print("Choose an option:")
        print("1. 🚀 Complete Pipeline (Crawl links + Process articles)")
        print("2. 📰 Process Existing Unprocessed Links")
        print("3. 📅 Process Recent Links by Date Range")
        print("4. 📊 Show Database Statistics")
        print("5. 🧪 Quick Test (Single Article)")
        print("6. ⚡ Benchmark Processing Speed")
        print("7. 🚪 Exit")
        
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == '1':
            complete_news_pipeline()
        elif choice == '2':
            process_unprocessed_links_only()
        elif choice == '3':
            process_recent_links_by_date()
        elif choice == '4':
            show_comprehensive_statistics()
        elif choice == '5':
            quick_test_single_article()
        elif choice == '6':
            benchmark_processing_speed()
        elif choice == '7':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")
        
        print("\n" + "="*60)

if __name__ == "__main__":
    main() 