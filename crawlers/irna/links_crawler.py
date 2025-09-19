import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlencode

import jdatetime
import pytz
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options

from broker_manager import BrokerManager
from config import settings
from schema import NewsLinkData


class IRNALinksCrawler:
    SOURCE_NAME = 'IRNA'
    BASE_URL = "https://www.irna.ir"
    ARCHIVE_ENDPOINT = "/archive"

    def __init__(self, broker_manager: BrokerManager, save_html: bool = False, html_save_dir: str = "html_pages"):
        self.logger = logging.getLogger(__name__)
        self._broker_manager = broker_manager
        self.tehran_tz = pytz.timezone('Asia/Tehran')
        self.save_html = save_html
        self.html_save_dir = Path(html_save_dir)

        # Log initialization parameters
        self.logger.info(f"Initializing IRNALinksCrawler with save_html={save_html}, html_save_dir={html_save_dir}")

        # Initialize Selenium WebDriver
        self.driver = self._create_webdriver()

        # Create HTML save directory if saving is enabled
        if self.save_html:
            self.html_save_dir.mkdir(exist_ok=True)
            self.logger.info(f"Created HTML save directory: {self.html_save_dir.absolute()}")

    def _create_webdriver(self) -> webdriver.Remote:
        """Create a Selenium Remote WebDriver connected to docker-compose service."""
        self.logger.debug("Creating Selenium WebDriver with headless mode")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        try:
            driver = webdriver.Remote(
                command_executor=settings.selenium.hub_url,
                options=chrome_options
            )
            driver.set_page_load_timeout(30)
            self.logger.info(f"Successfully connected to Selenium WebDriver at {settings.selenium.hub_url}")
            return driver
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Selenium WebDriver: {e}", exc_info=True)
            raise

    def crawl_archive_page(self, year: int, month: int, day: int, page_index: int) -> List[NewsLinkData]:
        """Crawl a specific paginated IRNA archive page using Selenium WebDriver."""
        params = {
            "wide": 0,
            "ty": 1,
            "ms": 0,
            "yr": year,
            "mn": month,
            "dy": day,
            "pi": page_index
        }
        archive_url = f"{self.BASE_URL}{self.ARCHIVE_ENDPOINT}?{urlencode(params)}"
        self.logger.info(f"Starting crawl of archive page: {archive_url}")

        try:
            self.logger.debug(f"Navigating to {archive_url}")
            self.driver.get(archive_url)

            # Pause the execution for 7 seconds to allow the page to load
            self.logger.debug("Waiting for 7 seconds to allow page to load")
            time.sleep(7)

            html_content = self.driver.page_source
            self.logger.debug(f"Retrieved HTML content (length: {len(html_content)} characters)")

            # Optional: save HTML for debugging
            if self.save_html:
                file_path = self.html_save_dir / f"archive_{year}_{month}_{day}_p{page_index}.html"
                self.logger.info(f"Saving HTML to {file_path}")
                file_path.write_text(html_content, encoding="utf-8")
                self.logger.debug(f"HTML saved successfully to {file_path}")

            self.logger.debug("Extracting news items from HTML content")
            news_items = self.extract_news_items(html_content)
            self.logger.info(f"Crawled {len(news_items)} news items from {archive_url}")
            return news_items

        except TimeoutException:
            self.logger.error(f"Timeout occurred while loading {archive_url}", exc_info=True)
            return []
        except WebDriverException as e:
            self.logger.error(f"Selenium WebDriver error while crawling {archive_url}: {e}", exc_info=True)
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while processing {archive_url}: {e}", exc_info=True)
            return []

    def parse_persian_datetime(self, persian_date_str: str, persian_time_str: str) -> Optional[datetime]:
        """Convert Persian date & time strings into timezone-aware datetime (Tehran)."""
        self.logger.debug(f"Parsing Persian datetime: {persian_date_str} {persian_time_str}")
        try:
            date_obj = jdatetime.datetime.strptime(persian_date_str, "%Y/%m/%d")
            time_obj = datetime.strptime(persian_time_str, "%H:%M")
            combined = jdatetime.datetime(
                date_obj.year,
                date_obj.month,
                date_obj.day,
                time_obj.hour,
                time_obj.minute
            ).togregorian()
            localized_datetime = self.tehran_tz.localize(combined)
            self.logger.debug(f"Successfully parsed datetime: {localized_datetime}")
            return localized_datetime
        except Exception as e:
            self.logger.error(f"Failed to parse Persian datetime: {persian_date_str} {persian_time_str}: {e}", exc_info=True)
            return None

    def parse_persian_date(self, persian_date_str: str) -> Optional[datetime]:
        """Convert Persian date string into timezone-aware datetime (Tehran)."""
        self.logger.debug(f"Parsing Persian date: {persian_date_str}")
        try:
            date_obj = jdatetime.datetime.strptime(persian_date_str, "%Y/%m/%d")
            localized_date = self.tehran_tz.localize(date_obj.togregorian())
            self.logger.debug(f"Successfully parsed date: {localized_date}")
            return localized_date
        except Exception as e:
            self.logger.error(f"Failed to parse Persian date: {persian_date_str}: {e}", exc_info=True)
            return None

    def extract_news_items(self, html_content: str) -> List[NewsLinkData]:
        """Extract news items from archive HTML page."""
        self.logger.debug("Starting extraction of news items from HTML")
        soup = BeautifulSoup(html_content, "html.parser")
        news_items = []

        # Select all news items with class 'news'
        news_elements = soup.select("li.news")
        self.logger.info(f"Found {len(news_elements)} news elements in HTML")

        for index, item in enumerate(news_elements, 1):
            try:
                self.logger.debug(f"Processing news item {index}")
                # Extract the link from the <a> tag in the <h3> element
                link_tag = item.select_one("div.desc h3 a")
                if not link_tag or "href" not in link_tag.attrs:
                    self.logger.warning(f"No valid link found in news item {index}")
                    continue

                news_url = link_tag["href"]
                if not news_url.startswith("http"):
                    news_url = f"{self.BASE_URL}{news_url}"
                self.logger.debug(f"Extracted news URL: {news_url}")

                # Extract the publication date and time from the <time> tag
                time_tag = item.select_one("time a")
                persian_datetime_str = time_tag.get_text(strip=True) if time_tag else None
                self.logger.debug(f"Extracted Persian datetime: {persian_datetime_str}")

                published_at = None
                if persian_datetime_str:
                    try:
                        # Parse Persian datetime (format: YYYY-MM-DD HH:MM)
                        date_time_obj = jdatetime.datetime.strptime(persian_datetime_str, "%Y-%m-%d %H:%M")
                        published_at = self.tehran_tz.localize(date_time_obj.togregorian())
                        self.logger.debug(f"Parsed published datetime: {published_at}")
                    except Exception as e:
                        self.logger.error(f"Failed to parse Persian datetime {persian_datetime_str} for item {index}: {e}", exc_info=True)

                # Create NewsLinkData object
                news_item = NewsLinkData(
                    source=self.SOURCE_NAME,
                    link=news_url,
                    published_datetime=published_at
                )
                news_items.append(news_item)
                self.logger.debug(f"Added news item: {news_item.link}, published at {news_item.published_datetime}")

            except Exception as e:
                self.logger.error(f"Error extracting news item {index}: {e}", exc_info=True)

        self.logger.info(f"Successfully extracted {len(news_items)} news items")
        return news_items

    def crawl_recent_links(self, last_seen_link: Optional[str] = None) -> Optional[str]:
        """Crawl recent news links until reaching the last seen link."""
        today = jdatetime.date.today()
        self.logger.info(f"Starting crawl of recent links for {today.year}-{today.month}-{today.day}, last_seen_link={last_seen_link}")
        page_index = 1
        latest_link = None
        stop_crawling = False

        while not stop_crawling:
            self.logger.debug(f"Crawling page {page_index}")
            news_items = self.crawl_archive_page(today.year, today.month, today.day, page_index)
            self.logger.info(f"Retrieved {len(news_items)} items from page {page_index}")

            if not news_items:
                self.logger.info("No more news items found, stopping crawl")
                break

            for item in news_items:
                if item.link == last_seen_link:
                    self.logger.info(f"Reached last seen link: {last_seen_link}, stopping crawl")
                    stop_crawling = True
                    break

                # Send a batch of one item to the broker
                self.logger.debug(f"Sending news item to broker: {item.link}")
                self._broker_manager.produce_links([item])

                if latest_link is None:
                    latest_link = item.link
                    self.logger.debug(f"Set latest link: {latest_link}")

            page_index += 1

        self.logger.info(f"Crawl completed, latest link: {latest_link}")
        return latest_link

    def __enter__(self):
        self.logger.debug("Entering IRNALinksCrawler context")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup WebDriver on exit."""
        if hasattr(self, 'driver') and self.driver:
            self.logger.debug("Closing Selenium WebDriver")
            self.driver.quit()
            self.logger.info("Selenium WebDriver closed successfully")