import logging
import re
from datetime import datetime

import jdatetime
import pytz
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from urllib.parse import urlencode

from database_manager import DatabaseManager
from schema import NewsLinkData


class IRNALinksCrawler:
    SOURCE_NAME = 'IRNA'
    BASE_URL = "https://www.irna.ir"
    ARCHIVE_ENDPOINT = "/page/archive.xhtml"

    def __init__(self, db_manager: DatabaseManager, headless=True):
        self.headless = headless
        self.logger = logging.getLogger(__name__)
        self._db_manager = db_manager
        self._driver = None
        self.tehran_tz = pytz.timezone('Asia/Tehran')
        self.session = requests.Session()

        self._db_manager.create_tables_if_not_exist()

    def crawl_archive(self, year: int, month: int, day: int):
        """
        Crawls paginated news links for a specific Shamsi date.
        """
        page_index = 1
        found_fresh_link = True

        while found_fresh_link:
            self.logger.info(
                f"Crawling IRNA archive for {year}/{month:02d}/{day:02d}, page {page_index}"
            )

            # Pass page_index to the crawling method
            news_items = self.crawl_archive_page(year, month, day, page_index)

            if not news_items:
                self.logger.info(f"No news items found on page {page_index} for {year}/{month:02d}/{day:02d}")
                found_fresh_link = False
                continue

            self._db_manager.bulk_insert_news_links(news_items)
            self.logger.info(f"Successfully extracted {len(news_items)} news items from archive page")

            # The logic to find a "fresh" link or to stop is now handled by the outer loop
            # and the absence of news items on a page
            page_index += 1

    def crawl_archive_page(self, year: int, month: int, day: int, page_index: int) -> list[NewsLinkData]:
        """
        Crawl a specific paginated IRNA archive page using the given Shamsi date and page index.

        This method uses a requests session as the new URL structure is static,
        eliminating the need for Selenium.
        """
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
        self.logger.info(f"Crawling archive page: {archive_url}")

        try:
            response = self.session.get(archive_url, timeout=15)
            response.raise_for_status()  # Raise an HTTPError for bad responses

            news_items = self.extract_news_items(response.text)
            return news_items

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error while crawling {archive_url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while processing {archive_url}: {e}")
            return []

    def crawl_recent_links(self, target_link: str) -> str:
        """
        Crawl recent IRNA links by iterating back from today's date.
        This method is adapted for the new paginated archive structure.
        """
        today_shamsi = jdatetime.date.today()
        current_date = today_shamsi

        while True:
            self.logger.info(f"Checking archive for {current_date}")

            # Start with page 1 for the current date
            page_index = 1
            last_link_on_page = None
            found_on_this_date = False

            while True:
                news_items = self.crawl_archive_page(
                    current_date.year,
                    current_date.month,
                    current_date.day,
                    page_index
                )

                if not news_items:
                    self.logger.info(
                        f"No news items found on page {page_index} for {current_date}. Moving to next day.")
                    break  # Break inner loop, move to next day

                # Check for the target link within the found items
                target_index = next((i for i, item in enumerate(news_items) if item.link == target_link), None)

                if target_index is not None:
                    fresh_items = news_items[:target_index]
                    if fresh_items:
                        self._db_manager.bulk_insert_news_links(fresh_items)
                    self.logger.info(f"Target link found on page {page_index} for {current_date}. Stopping crawl.")
                    # Return the newest link found
                    return news_items[0].link

                # No target link found on this page, but there are items.
                # Store the last link to check if it's new later.
                last_link_on_page = news_items[0].link
                self._db_manager.bulk_insert_news_links(news_items)
                found_on_this_date = True
                page_index += 1

            # If we've reached the point where no links are found for a full day,
            # and no links were found today, we can stop the search.
            if not found_on_this_date:
                self.logger.info("No news links found for the entire day. Stopping.")
                break

            # Move to the previous day
            current_date -= jdatetime.timedelta(days=1)

        self.logger.warning("Target link not found after checking previous days.")
        return None

    @classmethod
    def parse_persian_datetime(cls, persian_str):
        # ... (Method remains the same as in the original code)
        persian_str = persian_str.replace("'", "").strip()
        pattern = r'.*?:\s*(?P<weekday>\S+)\s+(?P<day>\d{1,2})\s+(?P<month>\S+)\s+(?P<year>\d{4})\s*-\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})'
        match = re.search(pattern, persian_str)
        if not match:
            raise ValueError("Input string format is not valid for IRNA.")
        parts = match.groupdict()
        persian_months = {'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4,
                          'مرداد': 5, 'شهریور': 6, 'مهر': 7, 'آبان': 8,
                          'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12}
        shamsi_year, shamsi_month, shamsi_day = int(parts['year']), persian_months.get(parts['month']), int(
            parts['day'])
        hour, minute = int(parts['hour']), int(parts['minute'])
        if shamsi_month is None:
            raise ValueError(f"Invalid month name: {parts['month']}")
        shamsi_datetime = jdatetime.datetime(shamsi_year, shamsi_month, shamsi_day, hour, minute)
        return {"datetime": shamsi_datetime, "shamsi_year": shamsi_year, "shamsi_month": shamsi_month,
                "shamsi_day": shamsi_day, "hour": hour, "minute": minute}

    @classmethod
    def parse_persian_date(cls, date_string: str) -> datetime | None:
        # ... (Method remains the same as in the original code)
        try:
            parsed = cls.parse_persian_datetime(date_string)
            shamsi_dt = parsed['datetime']
            gregorian_dt = shamsi_dt.togregorian()
            dt = datetime(gregorian_dt.year, gregorian_dt.month, gregorian_dt.day, gregorian_dt.hour,
                          gregorian_dt.minute)
            return pytz.timezone('Asia/Tehran').localize(dt)
        except Exception as e:
            logging.getLogger(__name__).error(f"Error parsing IRNA Persian date '{date_string}': {str(e)}")
            return None

    @classmethod
    def extract_news_items(cls, html_content: str) -> list[NewsLinkData]:
        """
        Extract news items from IRNA's paginated archive HTML content.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        news_items = []
        # The news items are inside a div with class 'item-archive'
        items_divs = soup.find_all('div', class_='item-archive')

        for div in items_divs:
            try:
                link_tag = div.find('h3').find('a')
                if not link_tag:
                    continue

                title = link_tag.get_text().strip()
                relative_link = link_tag.get('href', '')
                link = f"{cls.BASE_URL}{relative_link}" if relative_link and not relative_link.startswith(
                    'http') else relative_link

                time_tag = div.find('div', class_='item-date-line').find('span', class_='date')
                published_datetime_str = time_tag.get_text().strip() if time_tag else ""

                if title and link and published_datetime_str:
                    dt = cls.parse_persian_date(published_datetime_str)
                    if dt is not None:
                        news_item = NewsLinkData(
                            source=cls.SOURCE_NAME,
                            link=link,
                            published_datetime=dt
                        )
                        news_items.append(news_item)

            except Exception as e:
                logging.getLogger(__name__).error(f"Error processing news item: {e}")
                continue

        # Sort by datetime in descending order
        news_items.sort(key=lambda item: item.published_datetime, reverse=True)
        return news_items

    def _create_driver(self):
        # ... (Method remains the same, but it's now unused in the new logic)
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        return webdriver.Chrome(options=options)

    def close_driver(self):
        # ... (Method remains the same)
        if self._driver:
            try:
                self._driver.quit()
                self._driver = None
                self.logger.info("WebDriver closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {e}")

    def __del__(self):
        self.close_driver()