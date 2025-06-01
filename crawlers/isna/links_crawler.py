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
                    page_links = self._extract_links_from_page(driver.page_source, target_date_str)
                    
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
                    boundary_reached = self._check_date_boundary(page_links, target_date_str)
                    
                    # Filter links for target date only
                    target_date_links = [
                        link for link in page_links 
                        if link.get('shamsi_date_string') == target_date_str
                    ]
                    
                    if target_date_links:
                        # Store the links
                        stored_count = self._store_links_batch(target_date_links)
                        total_links += stored_count
                        
                        self.logger.info(f"✅ Page {page_number}: Found {len(target_date_links)} links for target date, "
                                       f"stored {stored_count} new links")
                    else:
                        self.logger.info(f"📄 Page {page_number}: No links for target date {target_date_str}")
                    
                    # If we've reached the boundary, stop crawling
                    if boundary_reached:
                        self.logger.info(f"🛑 Reached date boundary at page {page_number}. "
                                       f"Found content from previous days.")
                        break
                    
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
    
    def _extract_links_from_page(self, html_content, target_date_str):
        """
        Extract news links from ISNA archive page with Shamsi date parsing
        
        Args:
            html_content: HTML content of the page
            target_date_str: Target date string in format "YYYY/MM/DD"
        
        Returns:
            List of dictionaries containing link data with Shamsi dates
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links_data = []
            
            # Find all news items (li elements with class "text")
            news_items = soup.find_all('li', class_='text')
            
            for item in news_items:
                try:
                    link_data = self._extract_single_link_data(item)
                    if link_data:
                        links_data.append(link_data)
                        
                except Exception as e:
                    self.logger.debug(f"Error extracting single link: {str(e)}")
                    continue
            
            self.logger.debug(f"Extracted {len(links_data)} links from page")
            return links_data
            
        except Exception as e:
            self.logger.error(f"Error extracting links from page: {str(e)}")
            return []
    
    def _extract_single_link_data(self, item_element):
        """
        Extract data from a single news item element
        
        Expected HTML structure:
        <li class="text">
            <figure><a href="/news/..."><img src="..." alt="..."></a></figure>
            <div class="desc">
                <h3><a href="/news/..." target="_blank">Title</a></h3>
                <p>Summary text</p>
                <time><a href="/news/..." target="_blank" title="...">
                    <span>۱۰</span> خرداد ۰۴ - ۲۱:۱۶
                </a></time>
            </div>
        </li>
        """
        try:
            # Extract link URL
            link_element = item_element.find('h3').find('a') if item_element.find('h3') else None
            if not link_element:
                return None
            
            href = link_element.get('href')
            if not href:
                return None
            
            # Convert relative URL to absolute
            full_url = self.base_url + href if href.startswith('/') else href
            
            # Extract title
            title = self._clean_text(link_element.get_text())
            
            # Extract summary
            summary_element = item_element.find('p')
            summary = self._clean_text(summary_element.get_text()) if summary_element else ""
            
            # Extract published date from time element
            time_element = item_element.find('time')
            if not time_element:
                return None
            
            # Get the date text from time element
            date_text = self._clean_text(time_element.get_text())
            
            # Parse Shamsi date components
            shamsi_components = self._parse_shamsi_date_from_text(date_text)
            if not shamsi_components:
                self.logger.debug(f"Could not parse Shamsi date from: {date_text}")
                return None
            
            # Format Shamsi date string
            shamsi_date_string = f"{shamsi_components['year']:04d}/{shamsi_components['month']:02d}/{shamsi_components['day']:02d}"
            
            # Try to convert to Gregorian for compatibility
            gregorian_date = self._shamsi_to_gregorian_simple(
                shamsi_components['year'],
                shamsi_components['month'], 
                shamsi_components['day']
            )
            
            return {
                'source': 'ISNA',
                'link': full_url,
                'title': title,
                'summary': summary,
                'date': gregorian_date,  # Gregorian date for compatibility
                'shamsi_year': shamsi_components['year'],
                'shamsi_month': shamsi_components['month'],
                'shamsi_day': shamsi_components['day'],
                'shamsi_date_string': shamsi_date_string,
                'shamsi_month_name': shamsi_components.get('month_name', ''),
                'published_datetime': None  # Will be extracted during content crawling
            }
            
        except Exception as e:
            self.logger.debug(f"Error extracting single link data: {str(e)}")
            return None
    
    def _parse_shamsi_date_from_text(self, date_text):
        """
        Parse Shamsi date from text like "۱۰ خرداد ۰۴ - ۲۱:۱۶"
        
        Returns:
            Dict with year, month, day, month_name
        """
        try:
            from utils.shamsi_converter import ShamsiDateConverter
            converter = ShamsiDateConverter()
            
            # Parse the date string
            parsed = converter.parse_shamsi_date_string(date_text)
            return parsed
            
        except Exception as e:
            self.logger.debug(f"Error parsing Shamsi date '{date_text}': {str(e)}")
            return None
    
    def _check_date_boundary(self, page_links, target_date_str):
        """
        Check if we've reached the boundary (previous day's content)
        
        Args:
            page_links: List of links from current page
            target_date_str: Target date string "YYYY/MM/DD"
        
        Returns:
            True if boundary reached, False otherwise
        """
        if not page_links:
            return False
        
        # Check if any link has a date earlier than target date
        target_parts = target_date_str.split('/')
        target_year, target_month, target_day = int(target_parts[0]), int(target_parts[1]), int(target_parts[2])
        
        for link in page_links:
            link_date_str = link.get('shamsi_date_string', '')
            if not link_date_str:
                continue
            
            try:
                link_parts = link_date_str.split('/')
                link_year, link_month, link_day = int(link_parts[0]), int(link_parts[1]), int(link_parts[2])
                
                # Compare dates
                if (link_year < target_year or 
                    (link_year == target_year and link_month < target_month) or
                    (link_year == target_year and link_month == target_month and link_day < target_day)):
                    
                    self.logger.info(f"Found boundary: link date {link_date_str} is before target {target_date_str}")
                    return True
                    
            except (ValueError, IndexError):
                continue
        
        return False
    
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