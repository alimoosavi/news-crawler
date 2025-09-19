import logging
import time  # Import the time module
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

        # Initialize Selenium WebDriver
        self.driver = self._create_webdriver()

        # Create HTML save directory if saving is enabled
        if self.save_html:
            self.html_save_dir.mkdir(exist_ok=True)
            self.logger.info(f"HTML pages will be saved to: {self.html_save_dir.absolute()}")

    def _create_webdriver(self) -> webdriver.Remote:
        """Create a Selenium Remote WebDriver connected to docker-compose service."""
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
            self.logger.info("Connected to Selenium WebDriver successfully.")
            return driver
        except WebDriverException as e:
            self.logger.error(f"Error initializing Selenium WebDriver: {e}")
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
        self.logger.debug(f"Crawling archive page: {archive_url}")

        try:
            self.driver.get(archive_url)

            # Pause the execution for 7 seconds to allow the page to load
            self.logger.info("Waiting for 7 seconds for the page to load...")
            time.sleep(7)

            html_content = self.driver.page_source

            # Optional: save HTML for debugging
            if self.save_html:
                file_path = self.html_save_dir / f"archive_{year}_{month}_{day}_p{page_index}.html"
                file_path.write_text(html_content, encoding="utf-8")

            return self.extract_news_items(html_content)

        except TimeoutException:
            self.logger.error(f"Timeout while loading {archive_url}")
            return []
        except WebDriverException as e:
            self.logger.error(f"Selenium WebDriver error while crawling {archive_url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while processing {archive_url}: {e}")
            return []

    def parse_persian_datetime(self, persian_date_str: str, persian_time_str: str) -> Optional[datetime]:
        """Convert Persian date & time strings into timezone-aware datetime (Tehran)."""
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
            return self.tehran_tz.localize(combined)
        except Exception as e:
            self.logger.error(f"Error parsing Persian datetime: {persian_date_str} {persian_time_str} -> {e}")
            return None

    def parse_persian_date(self, persian_date_str: str) -> Optional[datetime]:
        """Convert Persian date string into timezone-aware datetime (Tehran)."""
        try:
            date_obj = jdatetime.datetime.strptime(persian_date_str, "%Y/%m/%d")
            return self.tehran_tz.localize(date_obj.togregorian())
        except Exception as e:
            self.logger.error(f"Error parsing Persian date: {persian_date_str} -> {e}")
            return None

    def extract_news_items(self, html_content: str) -> List[NewsLinkData]:
        """Extract news items from archive HTML page."""
        soup = BeautifulSoup(html_content, "html.parser")
        news_items = []

        # Select all news items with class 'news'
        for item in soup.select("li.news"):
            try:
                # Extract the link from the <a> tag in the <h3> element
                link_tag = item.select_one("div.desc h3 a")
                if not link_tag or "href" not in link_tag.attrs:
                    self.logger.warning("No valid link found in news item")
                    continue

                news_url = link_tag["href"]
                if not news_url.startswith("http"):
                    news_url = f"{self.BASE_URL}{news_url}"

                # Extract the publication date and time from the <time> tag
                time_tag = item.select_one("time a")
                persian_datetime_str = time_tag.get_text(strip=True) if time_tag else None

                published_at = None
                if persian_datetime_str:
                    try:
                        # Parse Persian datetime (format: YYYY-MM-DD HH:MM)
                        date_time_obj = jdatetime.datetime.strptime(persian_datetime_str, "%Y-%m-%d %H:%M")
                        published_at = self.tehran_tz.localize(date_time_obj.togregorian())
                    except Exception as e:
                        self.logger.error(f"Error parsing Persian datetime: {persian_datetime_str} -> {e}")

                # Create NewsLinkData object
                news_item = NewsLinkData(
                    source=self.SOURCE_NAME,
                    link=news_url,
                    published_datetime=published_at
                )
                news_items.append(news_item)
            except Exception as e:
                self.logger.error(f"Error extracting news item: {e}")

        self.logger.info(f"Extracted {len(news_items)} news items from the page")
        return news_items

    def crawl_recent_links(self, last_seen_link: Optional[str] = None) -> Optional[str]:
        """Crawl recent news links until reaching the last seen link."""
        today = jdatetime.date.today()
        page_index = 1
        latest_link = None
        stop_crawling = False

        while not stop_crawling:
            news_items = self.crawl_archive_page(today.year, today.month, today.day, page_index)
            if not news_items:
                break

            for item in news_items:
                if item.link == last_seen_link:
                    stop_crawling = True
                    break

                # Send a batch of one item to the broker
                self._broker_manager.produce_links([item])

                if latest_link is None:
                    latest_link = item.link

            page_index += 1

        return latest_link

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup WebDriver on exit."""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            self.logger.info("Selenium WebDriver closed.")
