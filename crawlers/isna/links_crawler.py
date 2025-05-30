from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, date, timedelta
import logging
from config import settings
from database_manager import DatabaseManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading

class ISNALinksCrawler:
    def __init__(self, base_url="https://www.isna.ir", headless=True, db_manager=None):
        self.base_url = base_url
        self.headless = headless
        self.driver = None
        self.logger = logging.getLogger(__name__)
        self.selenium_config = settings.selenium
        
        # Initialize database manager
        self.db_manager = db_manager or DatabaseManager()
        self._setup_database()
        
        # Track seen URLs to detect duplicates
        self.seen_urls = set()
        
        # Thread-local storage for drivers
        self._local = threading.local()
        
        # Lock for database operations
        self._db_lock = Lock()
        
    def _setup_database(self):
        """Setup database connection and create tables if they don't exist"""
        try:
            if not self.db_manager.connection:
                self.db_manager.connect()
            self.db_manager.create_all_tables()  # Updated to create all tables
            self.logger.info("Database setup completed")
        except Exception as e:
            self.logger.error(f"Error setting up database: {str(e)}")
            raise
    
    def _get_thread_driver(self):
        """Get or create a driver for the current thread"""
        if not hasattr(self._local, 'driver') or self._local.driver is None:
            self._local.driver = self._create_driver()
        return self._local.driver
    
    def _create_driver(self):
        """Create a new Chrome driver instance"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Connect to remote Chrome instance using config
        driver = webdriver.Remote(
            command_executor=self.selenium_config.hub_url,
            options=chrome_options
        )
        return driver
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Connect to remote Chrome instance using config
        self.driver = webdriver.Remote(
            command_executor=self.selenium_config.hub_url,
            options=chrome_options
        )
        
    def close_driver(self):
        """Close the Chrome driver"""
        if self.driver:
            self.driver.quit()
    
    def _close_thread_driver(self):
        """Close the driver for the current thread"""
        if hasattr(self._local, 'driver') and self._local.driver:
            self._local.driver.quit()
            self._local.driver = None
            
    def close_database(self):
        """Close database connection"""
        if self.db_manager:
            self.db_manager.close()
            
    def cleanup(self):
        """Cleanup all resources"""
        self.close_driver()
        self._close_thread_driver()
        self.close_database()
            
    def build_archive_url(self, year, month, day, page=1):
        """Build ISNA archive URL with Shamsi date parameters"""
        return f"{self.base_url}/page/archive.xhtml?mn={month}&wide=0&dy={day}&ms=0&pi={page}&yr={year}"
    
    def extract_news_links_css(self, html_content):
        """Extract news links from HTML content using CSS selectors"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Target links within article titles (h3 and h4 tags)
        news_links = []
        
        # Extract from h3 and h4 title links
        title_links = soup.select('.items h3 a, .items h4 a')
        
        for link in title_links:
            href = link.get('href')
            title = link.get_text(strip=True)
            
            if href:
                # Convert relative URLs to absolute if needed
                full_url = self.base_url + href if href.startswith('/') else href
                news_links.append({
                    'url': full_url,
                    'title': title
                })
        
        return news_links
    
    def crawl_page(self, year, month, day, page=1, use_thread_driver=False):
        """Crawl a single page and extract news links"""
        url = self.build_archive_url(year, month, day, page)
        
        # Use thread-local driver for concurrent operations
        driver = self._get_thread_driver() if use_thread_driver else self.driver
        
        try:
            driver.get(url)
            
            # Wait for content to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "items"))
            )
            
            # Get page source and extract links
            html_content = driver.page_source
            news_links = self.extract_news_links_css(html_content)
            
            self.logger.info(f"Extracted {len(news_links)} links from page {page}")
            return news_links
            
        except Exception as e:
            self.logger.error(f"Error crawling page {page}: {str(e)}")
            return []
    
    def _check_for_duplicates(self, links, seen_urls_set):
        """Check if links are duplicates of previously seen URLs"""
        new_links = []
        duplicate_count = 0
        
        for link in links:
            url = link['url']
            if url not in seen_urls_set:
                seen_urls_set.add(url)
                new_links.append(link)
            else:
                duplicate_count += 1
        
        return new_links, duplicate_count
    
    def crawl_date_pages(self, year, month, day, use_thread_driver=False):
        """Crawl all pages for a specific date until no more items or duplicates found"""
        all_links = []
        page = 1
        consecutive_empty_pages = 0
        max_consecutive_empty = 3  # Stop after 3 consecutive empty pages
        
        # Use separate seen URLs set for each date
        seen_urls_set = set()
        
        # Setup driver if not using thread driver
        if not use_thread_driver and not self.driver:
            self.setup_driver()
        
        try:
            while True:
                self.logger.info(f"Crawling page {page} for date {year}/{month}/{day}")
                
                links = self.crawl_page(year, month, day, page, use_thread_driver)
                
                # Check if page is empty
                if not links:
                    consecutive_empty_pages += 1
                    self.logger.info(f"Empty page {page}, consecutive empty pages: {consecutive_empty_pages}")
                    
                    if consecutive_empty_pages >= max_consecutive_empty:
                        self.logger.info(f"Stopping after {max_consecutive_empty} consecutive empty pages")
                        break
                else:
                    consecutive_empty_pages = 0  # Reset counter
                    
                    # Check for duplicates
                    new_links, duplicate_count = self._check_for_duplicates(links, seen_urls_set)
                    
                    if new_links:
                        all_links.extend(new_links)
                        self.logger.info(f"Added {len(new_links)} new links from page {page}")
                    
                    # If all links are duplicates, we might be seeing repeated content
                    if duplicate_count == len(links) and duplicate_count > 0:
                        self.logger.info(f"All {duplicate_count} links on page {page} are duplicates, stopping")
                        break
                
                page += 1
                
                # Add delay between requests
                time.sleep(2)
                
        except Exception as e:
            self.logger.error(f"Error during crawling: {str(e)}")
        
        self.logger.info(f"Completed crawling for {year}/{month}/{day}. Total unique links: {len(all_links)}")
        return all_links
    
    def store_links(self, links, crawl_date):
        """Store extracted links in database (thread-safe)"""
        if not links:
            self.logger.warning("No links to store")
            return 0
            
        # Prepare data for database insertion
        links_data = []
        for link_info in links:
            links_data.append((
                'ISNA',  # source
                link_info['url'],  # link
                crawl_date,  # date
                link_info.get('title', ''),  # title
                False  # has_processed
            ))
        
        # Store in database with thread safety
        with self._db_lock:
            self.db_manager.insert_news_links_batch(links_data)
            
        self.logger.info(f"Successfully stored {len(links_data)} links")
        return len(links_data)
    
    def crawl_and_store(self, year, month, day, use_thread_driver=False):
        """Complete workflow: crawl links and store them in database"""
        try:
            self.logger.info(f"Starting crawl and store for date {year}/{month}/{day}")
            
            # Crawl links (automatically stops when appropriate)
            links = self.crawl_date_pages(year, month, day, use_thread_driver)
            
            if not links:
                self.logger.warning(f"No links found for date {year}/{month}/{day}")
                return 0
            
            # Store links
            crawl_date = date(year, month, day)
            stored_count = self.store_links(links, crawl_date)
            
            self.logger.info(f"Crawl and store completed. Stored {stored_count} links for date {year}/{month}/{day}")
            return stored_count
            
        except Exception as e:
            self.logger.error(f"Error during crawl and store: {str(e)}")
            raise
    
    def _crawl_single_date_worker(self, date_tuple):
        """Worker function for crawling a single date (used in concurrent processing)"""
        year, month, day = date_tuple
        thread_id = threading.current_thread().ident
        
        try:
            self.logger.info(f"Thread {thread_id}: Starting crawl for {year}/{month}/{day}")
            
            # Use thread-local driver
            links_count = self.crawl_and_store(year, month, day, use_thread_driver=True)
            
            self.logger.info(f"Thread {thread_id}: Completed crawl for {year}/{month}/{day} - {links_count} links")
            
            return {
                'date': (year, month, day),
                'links_count': links_count,
                'success': True,
                'error': None
            }
            
        except Exception as e:
            self.logger.error(f"Thread {thread_id}: Error crawling {year}/{month}/{day}: {str(e)}")
            return {
                'date': (year, month, day),
                'links_count': 0,
                'success': False,
                'error': str(e)
            }
        finally:
            # Clean up thread-local driver
            self._close_thread_driver()
    
    def crawl_dates_batch(self, dates_list, max_workers=3):
        """
        Crawl multiple dates concurrently
        
        Args:
            dates_list: List of tuples (year, month, day)
            max_workers: Maximum number of concurrent threads
            
        Returns:
            Dictionary with results for each date
        """
        self.logger.info(f"Starting batch crawl for {len(dates_list)} dates with {max_workers} workers")
        
        results = {}
        total_links = 0
        successful_dates = 0
        failed_dates = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_date = {
                executor.submit(self._crawl_single_date_worker, date_tuple): date_tuple 
                for date_tuple in dates_list
            }
            
            # Process completed tasks
            for future in as_completed(future_to_date):
                date_tuple = future_to_date[future]
                
                try:
                    result = future.result()
                    results[date_tuple] = result
                    
                    if result['success']:
                        total_links += result['links_count']
                        successful_dates += 1
                        self.logger.info(f"✓ {date_tuple}: {result['links_count']} links")
                    else:
                        failed_dates += 1
                        self.logger.error(f"✗ {date_tuple}: {result['error']}")
                        
                except Exception as e:
                    failed_dates += 1
                    error_msg = f"Future execution error: {str(e)}"
                    results[date_tuple] = {
                        'date': date_tuple,
                        'links_count': 0,
                        'success': False,
                        'error': error_msg
                    }
                    self.logger.error(f"✗ {date_tuple}: {error_msg}")
        
        # Log summary
        self.logger.info(f"Batch crawl completed:")
        self.logger.info(f"  Total dates: {len(dates_list)}")
        self.logger.info(f"  Successful: {successful_dates}")
        self.logger.info(f"  Failed: {failed_dates}")
        self.logger.info(f"  Total links: {total_links}")
        
        return {
            'results': results,
            'summary': {
                'total_dates': len(dates_list),
                'successful_dates': successful_dates,
                'failed_dates': failed_dates,
                'total_links': total_links
            }
        }
    
    def crawl_date_range(self, start_date, end_date):
        """Crawl multiple dates in a range"""
        total_links = 0
        current_date = start_date
        
        while current_date <= end_date:
            try:
                # Convert Gregorian date to Shamsi (simplified - you might want to use a proper library)
                # For now, assuming the dates are already in Shamsi format
                links_count = self.crawl_and_store(
                    current_date.year, 
                    current_date.month, 
                    current_date.day
                )
                total_links += links_count
                
                self.logger.info(f"Crawled {links_count} links for {current_date}")
                
            except Exception as e:
                self.logger.error(f"Error crawling date {current_date}: {str(e)}")
            
            current_date += timedelta(days=1)
        
        return total_links
    
    def get_unprocessed_links(self, limit=None):
        """Get unprocessed ISNA links from database"""
        return self.db_manager.get_unprocessed_links(source='ISNA', limit=limit)
    
    def mark_link_processed(self, link_id):
        """Mark a link as processed"""
        return self.db_manager.mark_link_processed(link_id)

def extract_news_links_css(html_content, base_url=""):
    """Legacy function for backward compatibility"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Target links within article titles (h3 and h4 tags)
    news_links = []
    
    # Extract from h3 and h4 title links
    title_links = soup.select('.items h3 a, .items h4 a')
    
    for link in title_links:
        href = link.get('href')
        title = link.get_text(strip=True)
        
        if href:
            # Convert relative URLs to absolute if needed
            full_url = base_url + href if href.startswith('/') else href
            news_links.append({
                'url': full_url,
                'title': title
            })
    
    return news_links