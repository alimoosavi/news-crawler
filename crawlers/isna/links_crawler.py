import logging
import re
from dataclasses import dataclass
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from database_manager import DatabaseManager


@dataclass
class NewsItem:
    """Typed data class for news items"""
    title: str
    link: str
    published_datetime: str


class ISNALinksCrawler:
    def __init__(self, db_manager: DatabaseManager, base_url="https://www.isna.ir", headless=True):
        self.base_url = base_url
        self.headless = headless
        self.logger = logging.getLogger(__name__)
        self._db_manager = db_manager
        self._driver = None

        self._db_manager.create_tables_if_not_exist()

    def crawl_archive_page(self, year: int, month: int, day: int, page_index: int = 1) -> list[NewsItem]:
        """
        Crawl archive page for specific date and page index

        Args:
            year: Year (e.g., 1404 for Persian calendar)
            month: Month (1-12)
            day: Day (1-31)
            page_index: Page index (default: 1)

        Returns:
            List of NewsItem objects extracted from the page
        """
        # Construct the archive URL
        params = {
            'mn': month,
            'wide': 0,
            'dy': day,
            'ms': 0,
            'pi': page_index,
            'yr': year
        }

        archive_url = f"{self.base_url}/page/archive.xhtml?{urlencode(params)}"
        self.logger.info(f"Crawling archive page: {archive_url}")

        try:
            # Initialize driver if not already done
            if not self._driver:
                self._driver = self._create_driver()

            # Navigate to the archive page
            self._driver.get(archive_url)

            # Wait for page to load - wait for the items container
            wait = WebDriverWait(self._driver, 10)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "items")))

            # Get page source after it's loaded
            html_content = self._driver.page_source

            # Extract news items using the existing method
            news_items = self.extract_news_items(html_content, self.base_url)

            self.logger.info(f"Successfully extracted {len(news_items)} news items from archive page")
            return news_items

        except TimeoutException:
            self.logger.error(f"Timeout waiting for page to load: {archive_url}")
            return []
        except WebDriverException as e:
            self.logger.error(f"WebDriver error while crawling {archive_url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while crawling {archive_url}: {e}")
            return []

    @classmethod
    def extract_news_items(cls, html_content: str, base_url: str = "https://www.isna.ir") -> list[NewsItem]:
        """
        Extract news items from HTML content containing div.items structure

        Args:
            html_content: HTML string containing the news items
            base_url: Base URL to prepend to relative links

        Returns:
            List of NewsItem dataclass instances
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        news_items = []

        # Find the main container
        items_div = soup.find('div', class_='items')
        if not items_div:
            return news_items

        # Extract each news item
        for li in items_div.find_all('li'):
            try:
                # Extract title from image alt attribute
                img_tag = li.find('img')
                title = img_tag.get('alt', '').strip() if img_tag else ""

                # Extract link from href attribute
                link_tag = li.find('a')
                relative_link = link_tag.get('href', '') if link_tag else ""
                link = f"{base_url}{relative_link}" if relative_link and not relative_link.startswith(
                    'http') else relative_link

                # Extract published_datetime from title attribute of <time> tag
                time_tag = li.find('time')
                published_datetime = ""
                if time_tag:
                    time_link = time_tag.find('a')
                    if time_link:
                        published_datetime = time_link.get('title', '').strip()

                # Only add if we have essential data
                if title and link:
                    news_item = NewsItem(
                        title=cls.clean_text(title),
                        link=link,
                        published_datetime=published_datetime
                    )
                    news_items.append(news_item)

            except Exception as e:
                # Log error but continue processing other items
                print(f"Error processing news item: {e}")
                continue

        return news_items

    @classmethod
    def clean_text(cls, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        # Remove extra whitespace and normalize
        return re.sub(r'\s+', ' ', text).strip()

    def _create_driver(self):
        """Create Chrome driver"""
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        return webdriver.Chrome(options=options)

    def close_driver(self):
        """Close the webdriver instance"""
        if self._driver:
            try:
                self._driver.quit()
                self._driver = None
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {e}")

    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_driver()
