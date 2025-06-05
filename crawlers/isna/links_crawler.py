import logging
import re
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode
import pytz
import jdatetime

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
    source: str
    title: str
    link: str
    published_datetime: datetime
    published_year: int
    published_month: int
    published_day: int
    published_hour: int
    published_minute: int


class ISNALinksCrawler:
    SOURCE_NAME = 'ISNA'

    def __init__(self, db_manager: DatabaseManager, base_url="https://www.isna.ir", headless=True):
        self.base_url = base_url
        self.headless = headless
        self.logger = logging.getLogger(__name__)
        self._db_manager = db_manager
        self._driver = None
        self.tehran_tz = pytz.timezone('Asia/Tehran')

        self._db_manager.create_tables_if_not_exist()

    def crawl_archive(self, year: int, month: int, day: int):
        page_index = 1
        while page_index < 50:
            self.logger.info(
                f"Crawling ISNA archive for {year}/{month:02d}/{day:02d}, page {page_index}")
            news_items = self.crawl_archive_page(year, month, day, page_index)
            self._db_manager.bulk_insert_news_links(news_items)
            page_index += 1

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
            if not self._driver:
                self._driver = self._create_driver()

            self._driver.get(archive_url)
            wait = WebDriverWait(self._driver, 10)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "items")))
            html_content = self._driver.page_source
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
    def parse_persian_datetime(cls, persian_str):
        """Parse Persian datetime string into components"""
        persian_str = persian_str.replace("'", "").strip()

        pattern = r'(?P<weekday>\S+)\s+(?P<day>\d{1,2})\s+(?P<month>\S+)\s+(?P<year>\d{4})\s*-\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})'
        match = re.search(pattern, persian_str)

        if not match:
            raise ValueError("Input string format is not valid.")

        parts = match.groupdict()
        persian_months = {
            'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4,
            'مرداد': 5, 'شهریور': 6, 'مهر': 7, 'آبان': 8,
            'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
        }
        shamsi_year = int(parts['year'])
        shamsi_month = persian_months.get(parts['month'])
        shamsi_day = int(parts['day'])
        hour = int(parts['hour'])
        minute = int(parts['minute'])

        if shamsi_month is None:
            raise ValueError(f"Invalid month name: {parts['month']}")

        shamsi_datetime = jdatetime.datetime(shamsi_year, shamsi_month, shamsi_day, hour, minute)

        return {
            "datetime": shamsi_datetime,
            "shamsi_year": shamsi_year,
            "shamsi_month": shamsi_month,
            "shamsi_day": shamsi_day,
            "hour": hour,
            "minute": minute
        }

    @classmethod
    def parse_persian_date(cls, date_string: str) -> tuple[datetime, int, int, int, int, int]:
        """
        Parse Persian date string (e.g., 'سه‌شنبه ۱۳ خرداد ۱۴۰۴ - ۲۰:۵۵') into components

        Args:
            date_string: Persian date string

        Returns:
            Tuple of (datetime_obj, year, month, day, hour, minute)
        """
        try:
            parsed = cls.parse_persian_datetime(date_string)
            shamsi_dt = parsed['datetime']
            gregorian_dt = shamsi_dt.togregorian()
            dt = datetime(
                gregorian_dt.year,
                gregorian_dt.month,
                gregorian_dt.day,
                gregorian_dt.hour,
                gregorian_dt.minute
            )
            dt = pytz.timezone('Asia/Tehran').localize(dt)

            return (
                dt,
                parsed['shamsi_year'],
                parsed['shamsi_month'],
                parsed['shamsi_day'],
                parsed['hour'],
                parsed['minute']
            )

        except Exception as e:
            logging.getLogger(__name__).error(f"Error parsing Persian date '{date_string}': {str(e)}")
            return None, 0, 0, 0, 0, 0

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
        items_div = soup.find('div', class_='items')
        if not items_div:
            return news_items

        for li in items_div.find_all('li'):
            try:
                img_tag = li.find('img')
                title = img_tag.get('alt', '').strip() if img_tag else ""
                link_tag = li.find('a')
                relative_link = link_tag.get('href', '') if link_tag else ""
                link = f"{base_url}{relative_link}" if relative_link and not relative_link.startswith(
                    'http') else relative_link
                time_tag = li.find('time')
                published_datetime_str = ""
                if time_tag:
                    time_link = time_tag.find('a')
                    if time_link:
                        published_datetime_str = time_link.get('title', '').strip()

                if title and link and published_datetime_str:
                    dt, year, month, day, hour, minute = cls.parse_persian_date(published_datetime_str)
                    if dt is not None:
                        news_item = NewsItem(
                            source=cls.SOURCE_NAME,
                            title=cls.clean_text(title),
                            link=link,
                            published_datetime=dt,
                            published_year=year,
                            published_month=month,
                            published_day=day,
                            published_hour=hour,
                            published_minute=minute
                        )
                        news_items.append(news_item)

            except Exception as e:
                logging.getLogger(__name__).error(f"Error processing news item: {e}")
                continue

        return news_items

    @classmethod
    def clean_text(cls, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
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
