import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlencode

import jdatetime
import pytz
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from broker_manager import BrokerManager
from schema import NewsLinkData


class IRNALinksCrawler:
    SOURCE_NAME = 'IRNA'
    BASE_URL = "https://www.irna.ir"
    ARCHIVE_ENDPOINT = "/archive"

    def __init__(self, broker_manager: BrokerManager, save_html: bool = False, html_save_dir: str = "html_pages"):
        self.logger = logging.getLogger(__name__)
        self._broker_manager = broker_manager
        self.tehran_tz = pytz.timezone('Asia/Tehran')
        self.session = self._create_session()
        self.save_html = save_html
        self.html_save_dir = Path(html_save_dir)

        # Create HTML save directory if saving is enabled
        if self.save_html:
            self.html_save_dir.mkdir(exist_ok=True)
            self.logger.info(f"HTML pages will be saved to: {self.html_save_dir.absolute()}")

    def _create_session(self) -> requests.Session:
        """Create a robust requests session with retry strategy and headers."""
        session = requests.Session()

        # Retry strategy for resilience
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Add realistic headers to avoid blocking
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        return session

    def crawl_archive(self, year: int, month: int, day: int):
        """Crawl paginated news links for a specific Shamsi date."""
        page_index = 1
        found_fresh_link = True
        consecutive_empty_pages = 0
        max_empty_pages = 3  # Stop after 3 consecutive empty pages

        while found_fresh_link and consecutive_empty_pages < max_empty_pages:
            self.logger.info(
                f"Crawling IRNA archive for {year}/{month:02d}/{day:02d}, page {page_index}"
            )

            news_items = self.crawl_archive_page(year, month, day, page_index)

            if not news_items:
                consecutive_empty_pages += 1
                self.logger.info(
                    f"No news items found on page {page_index} for {year}/{month:02d}/{day:02d} "
                    f"(consecutive empty: {consecutive_empty_pages})"
                )

                if consecutive_empty_pages >= max_empty_pages:
                    self.logger.info(f"Stopping after {max_empty_pages} consecutive empty pages")
                    break

                page_index += 1
                continue

            # Reset consecutive empty counter
            consecutive_empty_pages = 0

            # Batch produce to Kafka for better throughput
            self._broker_manager.produce_links(news_items)
            self.logger.info(f"Produced {len(news_items)} links to Kafka")

            page_index += 1

            # Add small delay to be respectful to the server
            time.sleep(0.5)

    def crawl_archive_page(self, year: int, month: int, day: int, page_index: int) -> List[NewsLinkData]:
        """Crawl a specific paginated IRNA archive page with enhanced error handling."""
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
            response = self.session.get(
                archive_url,
                timeout=(10, 30),  # (connect_timeout, read_timeout)
                allow_redirects=True
            )
            response.raise_for_status()

            # Check if we got valid content
            if len(response.content) < 100:
                self.logger.warning(f"Suspiciously small response from {archive_url}")
                return []

            return self.extract_news_items(response.text)

        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout while crawling {archive_url}: {e}")
            return []
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                self.logger.warning(f"Rate limited on {archive_url}, waiting...")
                time.sleep(5)  # Wait before retrying
            else:
                self.logger.error(f"HTTP error while crawling {archive_url}: {e}")
            return []
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error while crawling {archive_url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while processing {archive_url}: {e}")
            return []

    @classmethod
    def parse_persian_datetime(cls, persian_str: str) -> dict:
        """Parse Persian datetime string with better error handling."""
        persian_str = persian_str.replace("'", "").strip()
        pattern = r'.*?:\s*(?P<weekday>\S+)\s+(?P<day>\d{1,2})\s+(?P<month>\S+)\s+(?P<year>\d{4})\s*-\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})'
        match = re.search(pattern, persian_str)

        if not match:
            raise ValueError(f"Input string format is not valid for IRNA: {persian_str}")

        parts = match.groupdict()
        persian_months = {
            'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4,
            'مرداد': 5, 'شهریور': 6, 'مهر': 7, 'آبان': 8,
            'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
        }

        shamsi_month = persian_months.get(parts['month'])
        if shamsi_month is None:
            raise ValueError(f"Invalid month name: {parts['month']}")

        shamsi_year, shamsi_day = int(parts['year']), int(parts['day'])
        hour, minute = int(parts['hour']), int(parts['minute'])

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
    def parse_persian_date(cls, date_string: str) -> Optional[datetime]:
        """Parse Persian date string to datetime with timezone."""
        try:
            parsed = cls.parse_persian_datetime(date_string)
            shamsi_dt = parsed['datetime']
            gregorian_dt = shamsi_dt.togregorian()
            dt = datetime(
                gregorian_dt.year, gregorian_dt.month, gregorian_dt.day,
                gregorian_dt.hour, gregorian_dt.minute
            )
            return pytz.timezone('Asia/Tehran').localize(dt)
        except Exception as e:
            logging.getLogger(__name__).error(f"Error parsing IRNA Persian date '{date_string}': {str(e)}")
            return None

    @classmethod
    def extract_news_items(cls, html_content: str) -> List[NewsLinkData]:
        """Extract news items from IRNA's archive HTML content with improved parsing."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            logging.getLogger(__name__).error(f"Error parsing HTML content: {e}")
            return []

        items_divs = soup.find_all('div', class_='item-archive')
        news_items: List[NewsLinkData] = []

        for div in items_divs:
            try:
                # More robust link extraction
                h3_tag = div.find('h3')
                if not h3_tag:
                    continue

                link_tag = h3_tag.find('a')
                if not link_tag:
                    continue

                title = link_tag.get_text().strip()
                if not title:
                    continue

                relative_link = link_tag.get('href', '')
                if not relative_link:
                    continue

                # Construct full URL
                link = f"{cls.BASE_URL}{relative_link}" if relative_link and not relative_link.startswith(
                    'http') else relative_link

                # More robust date extraction
                date_div = div.find('div', class_='item-date-line')
                if not date_div:
                    continue

                time_tag = date_div.find('span', class_='date')
                if not time_tag:
                    continue

                published_datetime_str = time_tag.get_text().strip()
                if not published_datetime_str:
                    continue

                dt = cls.parse_persian_date(published_datetime_str)
                if dt is not None:
                    news_items.append(
                        NewsLinkData(
                            source=cls.SOURCE_NAME,
                            link=link,
                            published_datetime=dt
                        )
                    )

            except Exception as e:
                logging.getLogger(__name__).error(f"Error processing news item: {e}")
                continue

        # Sort by publication date (newest first)
        news_items.sort(key=lambda item: item.published_datetime, reverse=True)
        return news_items

    def crawl_recent_links(self, last_link: Optional[str] = None) -> Optional[str]:
        """
        Crawl recent news links for today only, iterating over page indexes.
        Stops when reaching the given `last_link` or after max pages.
        Returns the newest link found.
        """
        self.logger.info("Starting crawl_recent_links for today")
        today = datetime.now(self.tehran_tz)
        shamsi_date = jdatetime.datetime.fromgregorian(datetime=today)
        max_pages = 30

        newest_link: Optional[str] = None
        stop_crawling = False
        total_items_processed = 0
        consecutive_empty_pages = 0
        max_empty_pages = 3

        for page_index in range(1, max_pages + 1):
            if stop_crawling:
                break

            self.logger.debug(f"Crawling page {page_index} for {today.date()}")
            news_items = self.crawl_archive_page(
                shamsi_date.year, shamsi_date.month, shamsi_date.day, page_index
            )

            if not news_items:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_empty_pages:
                    self.logger.info(f"No more content after {max_empty_pages} empty pages, stopping.")
                    break
                continue

            consecutive_empty_pages = 0  # Reset counter
            fresh_items = []

            for item in news_items:
                if last_link and item.link == last_link:
                    self.logger.info(f"Reached last_link: {last_link}")
                    stop_crawling = True
                    break
                fresh_items.append(item)

            if fresh_items:
                self._broker_manager.produce_links(fresh_items)
                total_items_processed += len(fresh_items)
                self.logger.info(f"Produced {len(fresh_items)} new links to Kafka")

                # Track newest link (first item in sorted list)
                if not newest_link:
                    newest_link = fresh_items[0].link

            # Small delay between pages
            time.sleep(0.5)

        self.logger.info(f"Crawl completed. Total items processed: {total_items_processed}")
        return newest_link

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleanup session on exit."""
        if hasattr(self, 'session'):
            self.session.close()
