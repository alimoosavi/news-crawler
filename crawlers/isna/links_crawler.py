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
import re

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
            self.db_manager.create_tables_if_not_exist()
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
        
        # Laptop-optimized Chrome options
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # Save bandwidth
        chrome_options.add_argument("--disable-javascript")  # For link extraction only
        
        # User agent
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            driver.implicitly_wait(10)
            return driver
        except Exception as e:
            self.logger.error(f"Error creating Chrome driver: {str(e)}")
            raise
    
    def crawl_shamsi_date(self, shamsi_year, shamsi_month, shamsi_day):
        """
        Crawl ISNA links for a specific Shamsi date with smart pagination
        
        Args:
            shamsi_year: Shamsi year (e.g., 1404)
            shamsi_month: Shamsi month (1-12)
            shamsi_day: Shamsi day (1-31)
        
        Returns:
            Number of links found and stored
        """
        target_date_str = f"{shamsi_year:04d}/{shamsi_month:02d}/{shamsi_day:02d}"
        self.logger.info(f"🗓️ Crawling ISNA links for Shamsi date: {target_date_str}")
        
        driver = self._get_thread_driver()
        total_links = 0
        page_number = 1
        consecutive_empty_pages = 0
        max_empty_pages = 3
        
        try:
            while True:
                self.logger.info(f"📄 Processing page {page_number} for date {target_date_str}")
                
                # Construct archive URL for the page
                archive_url = self._build_archive_url(shamsi_year, shamsi_month, shamsi_day, page_number)
                
                try:
                    # Load the page
                    driver.get(archive_url)
                    time.sleep(2)  # Wait for page to load
                    
                    # Extract links from current page
                    page_links, boundary_reached = self._extract_links_from_page(driver.page_source, shamsi_year, shamsi_month, shamsi_day)
                    
                    if not page_links:
                        consecutive_empty_pages += 1
                        self.logger.warning(f"No links found on page {page_number}")
                        
                        if consecutive_empty_pages >= max_empty_pages:
                            self.logger.info(f"Stopping after {max_empty_pages} consecutive empty pages")
                            break
                        
                        page_number += 1
                        continue
                    
                    # Reset empty page counter
                    consecutive_empty_pages = 0
                    
                    # Check if we've reached the boundary (previous day's content)
                    if boundary_reached:
                        self.logger.info(f"🛑 Reached date boundary at page {page_number}. "
                                       f"Found content from previous days.")
                        break
                    
                    # Store the links
                    stored_count = self._store_links_batch(page_links)
                    total_links += stored_count
                    
                    self.logger.info(f"✅ Page {page_number}: Found {len(page_links)} links for target date, "
                                   f"stored {stored_count} new links")
                    
                    page_number += 1
                    
                    # Safety limit to prevent infinite loops
                    if page_number > 200:
                        self.logger.warning(f"Reached maximum page limit (200) for date {target_date_str}")
                        break
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {page_number}: {str(e)}")
                    consecutive_empty_pages += 1
                    
                    if consecutive_empty_pages >= max_empty_pages:
                        break
                    
                    page_number += 1
                    continue
        
        except Exception as e:
            self.logger.error(f"Error in crawl_shamsi_date: {str(e)}")
        
        finally:
            self.logger.info(f"🏁 Finished crawling {target_date_str}. Total links found: {total_links}")
        
        return total_links
    
    def _build_archive_url(self, year, month, day, page=1):
        """Build ISNA archive URL for specific Shamsi date and page"""
        # ISNA archive URL format: https://www.isna.ir/archive?service_id=-1&day=YYYY-MM-DD&page=N
        shamsi_date_str = f"{year:04d}-{month:02d}-{day:02d}"
        
        if page == 1:
            return f"{self.base_url}/archive?service_id=-1&day={shamsi_date_str}"
        else:
            return f"{self.base_url}/archive?service_id=-1&day={shamsi_date_str}&page={page}"
    
    def _extract_links_from_page(self, page_source, target_shamsi_year, target_shamsi_month, target_shamsi_day):
        """
        Extract links from ISNA page with Shamsi date filtering
        
        Returns:
            tuple: (links_for_target_date, has_previous_day_content)
        """
        try:
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # First find the items container
            items_div = soup.find('div', class_='items')
            if not items_div:
                self.logger.warning("No items div found on page")
                return [], False
            
            # Find all li elements within the items div (class can vary)
            news_items = items_div.find_all('li')
            
            links_for_target_date = []
            has_previous_day_content = False
            
            for item in news_items:
                try:
                    # Extract link from the first anchor tag
                    link_tag = item.find('a', href=True)
                    if not link_tag:
                        continue
                    
                    link = link_tag['href']
                    if not link.startswith('http'):
                        link = self.base_url + link
                    
                    # Extract title - look for h3 first, then h4
                    title = "No Title"
                    title_tag = item.find('h3')
                    if title_tag:
                        title_link = title_tag.find('a')
                        if title_link:
                            title = self._clean_text(title_link.get_text())
                    else:
                        # Try h4 if h3 not found
                        title_tag = item.find('h4')
                        if title_tag:
                            title_link = title_tag.find('a')
                            if title_link:
                                title = self._clean_text(title_link.get_text())
                    
                    # Extract summary from p tag
                    summary_tag = item.find('p')
                    summary = self._clean_text(summary_tag.get_text()) if summary_tag else ""
                    
                    # Extract Shamsi date from time element
                    time_tag = item.find('time')
                    if not time_tag:
                        continue
                    
                    # Get the title attribute which contains the full date
                    # "یکشنبه ۱۱ خرداد ۱۴۰۴ - ۲۳:۳۵"
                    date_text = time_tag.get('title', '')
                    if not date_text:
                        # Fallback to text content
                        date_text = self._clean_text(time_tag.get_text())
                    
                    shamsi_components = self._parse_isna_full_date(date_text)
                    
                    if not shamsi_components:
                        continue
                    
                    year, month, day, hour, minute = shamsi_components
                    
                    # Check if this is the target date
                    if year == target_shamsi_year and month == target_shamsi_month and day == target_shamsi_day:
                        # Convert to Gregorian for database compatibility
                        gregorian_date = self._shamsi_to_gregorian_simple(year, month, day)
                        
                        # Create timezone-aware datetime
                        published_datetime = None
                        if gregorian_date:
                            try:
                                import pytz
                                tehran_tz = pytz.timezone('Asia/Tehran')
                                dt = datetime.combine(gregorian_date, datetime.min.time().replace(hour=hour, minute=minute))
                                published_datetime = tehran_tz.localize(dt)
                            except Exception:
                                pass
                        
                        link_data = {
                            'source': 'ISNA',
                            'link': link,
                            'title': title,
                            'summary': summary,
                            'date': gregorian_date,
                            'published_datetime': published_datetime,
                            'shamsi_year': year,
                            'shamsi_month': month,
                            'shamsi_day': day,
                            'shamsi_date_string': f"{year:04d}/{month:02d}/{day:02d}"
                        }
                        
                        links_for_target_date.append(link_data)
                        
                    elif year < target_shamsi_year or \
                         (year == target_shamsi_year and month < target_shamsi_month) or \
                         (year == target_shamsi_year and month == target_shamsi_month and day < target_shamsi_day):
                        # Found content from previous days - boundary reached
                        has_previous_day_content = True
                        self.logger.info(f"Found boundary: {year}/{month:02d}/{day:02d} is before target {target_shamsi_year}/{target_shamsi_month:02d}/{target_shamsi_day:02d}")
                        break
                    
                except Exception as e:
                    self.logger.warning(f"Error processing news item: {str(e)}")
                    continue
            
            return links_for_target_date, has_previous_day_content
            
        except Exception as e:
            self.logger.error(f"Error extracting links from page: {str(e)}")
            return [], False
    
    def _parse_isna_full_date(self, date_string):
        """
        Parse ISNA full date format like "یکشنبه ۱۱ خرداد ۱۴۰۴ - ۲۳:۳۵"
        Returns: (year, month, day, hour, minute) or None
        """
        try:
            # Convert Persian digits to English
            from utils.shamsi_converter import ShamsiDateConverter
            converter = ShamsiDateConverter()
            normalized = converter.persian_to_english_digits(date_string.strip())
            
            # Pattern for "weekday day month year - hour:minute"
            # Example: "یکشنبه 11 خرداد 1404 - 23:35"
            import re
            pattern = r'(\w+)\s+(\d+)\s+(\w+)\s+(\d+)\s*-\s*(\d+):(\d+)'
            match = re.search(pattern, normalized)
            
            if not match:
                # Try simpler pattern without weekday
                pattern = r'(\d+)\s+(\w+)\s+(\d+)\s*-\s*(\d+):(\d+)'
                match = re.search(pattern, normalized)
                if not match:
                    return None
                day, month_name, year, hour, minute = match.groups()
            else:
                weekday, day, month_name, year, hour, minute = match.groups()
            
            # Convert month name to number using utils
            month = self._get_shamsi_month_number(month_name)
            if month is None:
                return None
            
            # Convert to integers
            year_int = int(year)
            day_int = int(day)
            hour_int = int(hour)
            minute_int = int(minute)
            
            return (year_int, month, day_int, hour_int, minute_int)
            
        except Exception as e:
            self.logger.warning(f"Error parsing ISNA full date '{date_string}': {e}")
            return None
    
    def _get_shamsi_month_number(self, month_name):
        """
        Get Shamsi month number from month name using utils
        """
        try:
            from utils.shamsi_converter import ShamsiDateConverter
            converter = ShamsiDateConverter()
            
            # Check if month name matches any of the Shamsi months
            for shamsi_month, month_num in converter.SHAMSI_MONTHS.items():
                if month_name in shamsi_month or shamsi_month.startswith(month_name):
                    return month_num
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error getting month number for '{month_name}': {e}")
            return None
    
    def _store_links_batch(self, links_data):
        """Store a batch of links in the database"""
        if not links_data:
            return 0
        
        try:
            with self._db_lock:
                # Filter out duplicates based on URL
                new_links = []
                for link_data in links_data:
                    if link_data['link'] not in self.seen_urls:
                        new_links.append(link_data)
                        self.seen_urls.add(link_data['link'])
                
                if new_links:
                    # Use bulk insert with Shamsi date support
                    self.db_manager.bulk_insert_news_links(new_links)
                    return len(new_links)
                
                return 0
                
        except Exception as e:
            self.logger.error(f"Error storing links batch: {str(e)}")
            return 0
    
    def _shamsi_to_gregorian_simple(self, shamsi_year, shamsi_month, shamsi_day):
        """Simple Shamsi to Gregorian conversion"""
        try:
            from utils.shamsi_converter import ShamsiDateConverter
            converter = ShamsiDateConverter()
            return converter.shamsi_to_gregorian(shamsi_year, shamsi_month, shamsi_day)
        except Exception:
            return None
    
    def _clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = ' '.join(text.split())
        
        # Remove common unwanted characters
        text = text.replace('\u200c', ' ')  # Zero-width non-joiner
        text = text.replace('\u200d', '')   # Zero-width joiner
        
        return text.strip()
    
    def crawl_date_range(self, start_shamsi_date, end_shamsi_date):
        """
        Crawl multiple Shamsi dates in a range
        
        Args:
            start_shamsi_date: Tuple (year, month, day)
            end_shamsi_date: Tuple (year, month, day)
        """
        total_links = 0
        
        start_year, start_month, start_day = start_shamsi_date
        end_year, end_month, end_day = end_shamsi_date
        
        current_year, current_month, current_day = start_year, start_month, start_day
        
        while True:
            try:
                links_count = self.crawl_shamsi_date(current_year, current_month, current_day)
                total_links += links_count
                
                self.logger.info(f"Crawled {links_count} links for {current_year}/{current_month:02d}/{current_day:02d}")
                
                # Move to next day
                current_day += 1
                
                # Handle month/year overflow (simplified)
                if current_day > 31:  # Simplified - should use proper Shamsi calendar logic
                    current_day = 1
                    current_month += 1
                    
                    if current_month > 12:
                        current_month = 1
                        current_year += 1
                
                # Check if we've reached the end date
                if (current_year > end_year or 
                    (current_year == end_year and current_month > end_month) or
                    (current_year == end_year and current_month == end_month and current_day > end_day)):
                    break
                
            except Exception as e:
                self.logger.error(f"Error crawling date {current_year}/{current_month:02d}/{current_day:02d}: {str(e)}")
                # Continue to next date
                current_day += 1
        
        return total_links
    
    def get_unprocessed_links(self, limit=None):
        """Get unprocessed ISNA links from database"""
        return self.db_manager.get_unprocessed_links(source='ISNA', limit=limit)
    
    def mark_link_processed(self, link_id):
        """Mark a link as processed"""
        return self.db_manager.mark_link_processed(link_id)
    
    def close(self):
        """Clean up resources"""
        if hasattr(self._local, 'driver') and self._local.driver:
            self._local.driver.quit()