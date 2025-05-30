from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
from config import settings
from database_manager import DatabaseManager
from .page_parser import extract_news_article
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading
import psycopg2
from psycopg2.extras import RealDictCursor

class ISNAPageCrawler:
    def __init__(self, db_manager=None, headless=True):
        self.headless = headless
        self.logger = logging.getLogger(__name__)
        self.selenium_config = settings.selenium
        
        # Initialize database manager
        self.db_manager = db_manager or DatabaseManager()
        self._setup_database()
        
        # Thread-local storage for drivers
        self._local = threading.local()
        
        # Lock for database operations
        self._db_lock = Lock()
        
    def _setup_database(self):
        """Setup database connection and create tables if they don't exist"""
        try:
            if not self.db_manager.connection:
                self.db_manager.connect()
            self.db_manager.create_tables_if_not_exist()
            self.logger.info("Database setup completed for page crawler")
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
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Connect to remote Chrome instance using config
        driver = webdriver.Remote(
            command_executor=self.selenium_config.hub_url,
            options=chrome_options
        )
        
        # Execute script to remove webdriver property
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    
    def _close_thread_driver(self):
        """Close the driver for the current thread"""
        if hasattr(self._local, 'driver') and self._local.driver:
            try:
                self._local.driver.quit()
                self._local.driver = None
            except Exception as e:
                self.logger.warning(f"Error closing thread driver: {str(e)}")
    
    def crawl_single_page(self, link_data):
        """
        Crawl a single news page and extract article data
        
        Args:
            link_data: Dictionary containing link information from database
            
        Returns:
            Dictionary with crawling results
        """
        link_id = link_data['id']
        url = link_data['link']
        
        result = {
            'link_id': link_id,
            'news_id': None,
            'success': False,
            'error': None,
            'title': None,
            'published_date': None
        }
        
        driver = None
        
        try:
            # Get thread-local driver
            driver = self._get_thread_driver()
            
            self.logger.info(f"Crawling page: {url}")
            
            # Navigate to the page
            driver.get(url)
            
            # Wait for page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Get page source
            html_content = driver.page_source
            
            # Extract article data using the parser
            article_data = extract_news_article(html_content)
            
            # Validate extracted data
            if not article_data.get('title'):
                raise Exception("No title found in article")
            
            # Prepare data for database insertion
            source = 'ISNA'
            published_date = article_data.get('published_date')  # Now properly extracted Shamsi date
            title = article_data.get('title')
            summary = article_data.get('summary')
            content = article_data.get('content')
            tags = article_data.get('tags')  # List of tags
            
            # Insert into database with thread safety
            with self._db_lock:
                try:
                    # Insert news article
                    news_id = self.db_manager.insert_news_article(
                        source=source,
                        published_date=published_date,  # Shamsi datetime string
                        title=title,
                        summary=summary,
                        content=content,
                        tags=tags,
                        link_id=link_id
                    )
                    
                    # Mark link as processed
                    self.db_manager.mark_link_processed(link_id)
                    
                    result.update({
                        'news_id': news_id,
                        'success': True,
                        'title': title,
                        'published_date': published_date
                    })
                    
                    self.logger.info(f"Successfully processed link {link_id}: {title[:50]}...")
                    
                except Exception as db_error:
                    raise Exception(f"Database error: {str(db_error)}")
            
        except Exception as e:
            error_msg = f"Error crawling {url}: {str(e)}"
            result['error'] = error_msg
            self.logger.error(error_msg)
            
            # Mark link as processed even if failed to avoid infinite retries
            try:
                with self._db_lock:
                    self.db_manager.mark_link_processed(link_id)
            except Exception as mark_error:
                self.logger.error(f"Failed to mark link {link_id} as processed: {str(mark_error)}")
        
        return result
    
    def crawl_unprocessed_links(self, source='ISNA', limit=50, max_workers=3):
        """
        Crawl multiple unprocessed links concurrently
        
        Args:
            source: Source name to filter links
            limit: Maximum number of links to process
            max_workers: Number of concurrent workers
            
        Returns:
            Dictionary with results and summary
        """
        self.logger.info(f"Starting concurrent crawling of unprocessed {source} links")
        
        # Get unprocessed links
        unprocessed_links = self.db_manager.get_unprocessed_links(source=source, limit=limit)
        
        if not unprocessed_links:
            self.logger.info("No unprocessed links found")
            return {
                'results': {},
                'summary': {
                    'total_processed': 0,
                    'successful': 0,
                    'failed': 0
                }
            }
        
        self.logger.info(f"Found {len(unprocessed_links)} unprocessed links")
        
        # Process links concurrently
        results = {}
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_link = {
                executor.submit(self.crawl_single_page, link_data): link_data['id']
                for link_data in unprocessed_links
            }
            
            # Process completed tasks
            for future in as_completed(future_to_link):
                link_id = future_to_link[future]
                
                try:
                    result = future.result()
                    results[link_id] = result
                    
                    if result['success']:
                        successful += 1
                        self.logger.info(f"✓ Link {link_id}: {result['title']}")
                        if result['published_date']:
                            self.logger.debug(f"  📅 Published: {result['published_date']}")
                    else:
                        failed += 1
                        self.logger.error(f"✗ Link {link_id}: {result['error']}")
                        
                except Exception as e:
                    failed += 1
                    error_msg = f"Future execution error: {str(e)}"
                    results[link_id] = {
                        'link_id': link_id,
                        'news_id': None,
                        'success': False,
                        'error': error_msg,
                        'title': 'Future error',
                        'published_date': None
                    }
                    self.logger.error(f"✗ Link {link_id}: {error_msg}")
        
        total_processed = successful + failed
        self.logger.info(f"Concurrent crawling completed:")
        self.logger.info(f"  Total processed: {total_processed}")
        self.logger.info(f"  Successful: {successful}")
        self.logger.info(f"  Failed: {failed}")
        
        return {
            'results': results,
            'summary': {
                'total_processed': total_processed,
                'successful': successful,
                'failed': failed
            }
        }
    
    def crawl_batch_by_date_range(self, source='ISNA', days_back=7, max_workers=3):
        """
        Crawl unprocessed links from a specific date range
        
        Args:
            source: Source name to filter links
            days_back: Number of days back to include
            max_workers: Number of concurrent workers
            
        Returns:
            Dictionary with results and summary
        """
        from datetime import datetime, timedelta
        
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        self.logger.info(f"Crawling {source} links from {start_date} to {end_date}")
        
        # Get all unprocessed links
        all_unprocessed = self.db_manager.get_unprocessed_links(source=source)
        
        # Filter by date range (assuming date field in links)
        date_filtered_links = []
        for link in all_unprocessed:
            link_date = link.get('date')
            if link_date and start_date <= link_date <= end_date:
                date_filtered_links.append(link)
        
        if not date_filtered_links:
            self.logger.info(f"No unprocessed links found in date range {start_date} to {end_date}")
            return {
                'results': {},
                'summary': {
                    'total_links': 0,
                    'successful': 0,
                    'failed': 0,
                    'total_processed': 0,
                    'date_range': f"{start_date} to {end_date}"
                }
            }
        
        self.logger.info(f"Found {len(date_filtered_links)} unprocessed links in date range")
        
        # Process the links
        results = {}
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_link = {
                executor.submit(self.crawl_single_page, link_data): link_data['id']
                for link_data in date_filtered_links
            }
            
            # Process completed tasks
            for future in as_completed(future_to_link):
                link_id = future_to_link[future]
                
                try:
                    result = future.result()
                    results[link_id] = result
                    
                    if result['success']:
                        successful += 1
                        self.logger.info(f"✓ Link {link_id}: {result['title']}")
                    else:
                        failed += 1
                        self.logger.error(f"✗ Link {link_id}: {result['error']}")
                        
                except Exception as e:
                    failed += 1
                    error_msg = f"Future execution error: {str(e)}"
                    results[link_id] = {
                        'link_id': link_id,
                        'news_id': None,
                        'success': False,
                        'error': error_msg,
                        'title': 'Future error'
                    }
                    self.logger.error(f"✗ Link {link_id}: {error_msg}")
        
        total_processed = successful + failed
        self.logger.info(f"Date range crawling completed:")
        self.logger.info(f"  Date range: {start_date} to {end_date}")
        self.logger.info(f"  Total links: {len(date_filtered_links)}")
        self.logger.info(f"  Successful: {successful}")
        self.logger.info(f"  Failed: {failed}")
        
        return {
            'results': results,
            'summary': {
                'total_links': len(date_filtered_links),
                'successful': successful,
                'failed': failed,
                'total_processed': total_processed,
                'date_range': f"{start_date} to {end_date}"
            }
        }
    
    def cleanup(self):
        """Cleanup all resources"""
        self._close_thread_driver()
        if self.db_manager:
            self.db_manager.close()
