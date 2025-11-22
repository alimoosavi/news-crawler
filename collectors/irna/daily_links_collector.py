import logging
from typing import List, Optional

import jdatetime
from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from schema import NewsLinkData

# --- CONSTANTS ---
IRNA_BASE_URL = "https://www.irna.ir"
IRNA_ARCHIVE_URL_BASE = f"{IRNA_BASE_URL}/archive?ms=0&dy={{day}}&mn={{month}}&yr={{year}}"
IRNA_SOURCE_NAME = "IRNA"

# Persian to Latin numeral translation table
PERSIAN_TO_LATIN = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")


class IRNADailyLinkCollector:
    """
    Collects all news links for a specific Shamsi day from IRNA's archive.
    Stops crawling when links from the previous day are encountered.
    """

    # CSS selector for individual news items
    NEWS_ITEM_SELECTOR = "li.news"

    # Selector for the datetime element within a news item
    DATETIME_SELECTOR = "div.desc time a"

    def __init__(self, year: int, month: int, day: int, fetch_timeout: int = 30):
        """
        Initializes the collector with the target Shamsi date.
        """
        self._year = year
        self._month = month
        self._day = day
        self.fetch_timeout = fetch_timeout
        self.logger = logging.getLogger(self.__class__.__name__)

        try:
            self._target_date_shamsi = jdatetime.date(year, month, day)
            self._stop_date_shamsi = self._target_date_shamsi - jdatetime.timedelta(days=1)
        except ValueError as e:
            self.logger.error(
                f"Invalid Shamsi date: {year}-{month}-{day}. Error: {e}"
            )
            raise

    def _get_archive_url(self, page_index: int) -> str:
        """
        Constructs the archive URL for a given page index and date.
        """
        base_url = IRNA_ARCHIVE_URL_BASE.format(
            day=self._day, month=self._month, year=self._year
        )
        return base_url if page_index == 1 else f"{base_url}&pi={page_index}"

    def _fetch_html(self, url: str) -> Optional[str]:
        """
        Fetches the HTML content using Playwright.
        """
        self.logger.debug(f"Loading archive page with Playwright: {url}")

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=self.fetch_timeout * 1000)
                    page.wait_for_selector(self.NEWS_ITEM_SELECTOR, timeout=5000)
                    html_content = page.content()
                    self.logger.info(f"Successfully fetched rendered HTML from {url}")
                    return html_content
                finally:
                    browser.close()

        except PlaywrightTimeoutError:
            self.logger.warning(
                f"Timeout while fetching {url}. Likely end of available pages."
            )
            return None
        except Exception as e:
            self.logger.error(f"Playwright error while fetching {url}: {e}", exc_info=True)
            return None

    def _parse_news_item(self, news_item_tag: Tag) -> Optional[NewsLinkData]:
        """
        Extracts link and datetime from a single news item tag.
        """
        try:
            link_tag = news_item_tag.select_one(self.DATETIME_SELECTOR)
            if not link_tag or not link_tag.get("href"):
                self.logger.debug("Skipping news item: missing link.")
                return None

            relative_link = link_tag["href"]
            full_link = f"{IRNA_BASE_URL}{relative_link}"

            datetime_str = link_tag.get_text(strip=True)
            latin_datetime_str = datetime_str.translate(PERSIAN_TO_LATIN)

            shamsi_dt = jdatetime.datetime.strptime(latin_datetime_str, "%Y-%m-%d %H:%M")
            news_date_shamsi = shamsi_dt.date()

            if news_date_shamsi > self._target_date_shamsi:
                self.logger.warning(f"Skipping future date news: {news_date_shamsi}")
                return None

            published_datetime = shamsi_dt.togregorian()

            return NewsLinkData(
                source=IRNA_SOURCE_NAME,
                link=full_link,
                published_datetime=published_datetime,
            )

        except Exception as e:
            self.logger.error(f"Error parsing news item: {e}", exc_info=True)
            return None

    def collect_links(self) -> List[NewsLinkData]:
        """
        Main method to crawl all daily news links from IRNA.
        """
        self.logger.info(f"Starting link collection for: {self._target_date_shamsi}")
        all_links: List[NewsLinkData] = []
        page_index = 1

        while True:
            url = self._get_archive_url(page_index)
            self.logger.info(f"Crawling page {page_index}: {url}")

            html = self._fetch_html(url)
            if not html:
                self.logger.info(f"No HTML fetched for page {page_index}. Ending crawl.")
                break

            soup = BeautifulSoup(html, "html.parser")
            news_items = soup.select(self.NEWS_ITEM_SELECTOR)

            if not news_items:
                self.logger.info(f"No news items found on page {page_index}. Ending crawl.")
                break

            found_previous_day_news = False
            page_links_count = 0

            for item in news_items:
                link_data = self._parse_news_item(item)
                if not link_data:
                    continue

                link_date_shamsi = jdatetime.date.fromgregorian(
                    date=link_data.published_datetime.date()
                )

                if link_date_shamsi == self._target_date_shamsi:
                    all_links.append(link_data)
                    page_links_count += 1
                elif link_date_shamsi == self._stop_date_shamsi:
                    self.logger.info(f"Reached previous day ({self._stop_date_shamsi}). Stopping crawl.")
                    found_previous_day_news = True
                    break
                else:
                    self.logger.debug(f"Skipping old date news: {link_date_shamsi}")

            if found_previous_day_news or page_links_count == 0:
                break

            self.logger.info(f"Collected {page_links_count} links from page {page_index}. Next page...")
            page_index += 1

        self.logger.info(f"Finished collection. Total links found: {len(all_links)}")
        return all_links
