import logging
from typing import List, Optional

import jdatetime
from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from schema import NewsLinkData

# --- CONSTANTS ---
ISNA_BASE_URL = "https://www.isna.ir"
ISNA_ARCHIVE_URL_BASE = (
    f"{ISNA_BASE_URL}/page/archive.xhtml?ms=0&dy={{day}}&mn={{month}}&yr={{year}}"
)
ISNA_SOURCE_NAME = "ISNA"

PERSIAN_TO_LATIN = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")


class ISNADailyLinkCollector:
    """
    Collects all ISNA news links for a specific Shamsi day.
    Stops crawling when it encounters links from the previous day.
    """

    NEWS_ITEM_SELECTOR = "div.items ul li"
    TIME_ANCHOR_SELECTOR = "div.desc time a"

    def __init__(self, year: int, month: int, day: int, fetch_timeout: int = 30):
        self._year = year
        self._month = month
        self._day = day
        self.fetch_timeout = fetch_timeout
        self.logger = logging.getLogger(self.__class__.__name__)

        try:
            self._target_date_shamsi = jdatetime.date(year, month, day)
            self._stop_date_shamsi = self._target_date_shamsi - jdatetime.timedelta(days=1)
        except ValueError as e:
            self.logger.error(f"Invalid Shamsi date: {year}-{month}-{day}. Error: {e}")
            raise

    def _get_archive_url(self, page_index: int) -> str:
        """
        Constructs archive URL for a given date and page index.
        """
        base_url = ISNA_ARCHIVE_URL_BASE.format(
            day=self._day, month=self._month, year=self._year
        )
        return base_url if page_index == 1 else f"{base_url}&pi={page_index}"

    def _fetch_html(self, url: str) -> Optional[str]:
        """
        Fetches rendered HTML using Playwright.
        """
        self.logger.debug(f"Loading ISNA archive page: {url}")

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=self.fetch_timeout * 1000)
                    page.wait_for_selector(self.NEWS_ITEM_SELECTOR, timeout=5000)
                    html = page.content()
                    self.logger.info(f"Fetched rendered HTML from {url}")
                    return html
                finally:
                    browser.close()
        except PlaywrightTimeoutError:
            self.logger.warning(f"Timeout while fetching {url}. Possibly end of archive.")
            return None
        except Exception as e:
            self.logger.error(f"Playwright error fetching {url}: {e}", exc_info=True)
            return None

    def _parse_news_item(self, item_tag: Tag) -> Optional[NewsLinkData]:
        """
        Extracts link and datetime from a news list item.
        """
        try:
            time_anchor = item_tag.select_one(self.TIME_ANCHOR_SELECTOR)
            if not time_anchor or not time_anchor.get("href"):
                return None

            relative_link = time_anchor["href"]
            full_link = f"{ISNA_BASE_URL}{relative_link}"

            datetime_title = time_anchor.get("title", "").strip()
            if not datetime_title:
                self.logger.debug("Missing datetime title; skipping item.")
                return None

            # Example: "چهارشنبه ۱۲ شهریور ۱۴۰۴ - ۱۸:۱۷"
            # We extract the numeric parts and translate Persian digits
            latin_text = datetime_title.translate(PERSIAN_TO_LATIN)

            # Parse the Shamsi date from text
            # We expect parts like: '۱۲ شهریور ۱۴۰۴ - ۱۸:۱۷'
            try:
                parts = latin_text.split("-")
                date_part, time_part = parts[0].strip(), parts[1].strip()
                day, month_name, year = date_part.split()[-3:]
                day = int(day)
                year = int("13" + year) if len(year) == 2 else int(year)

                # Map Persian month names to month numbers
                month_map = {
                    "فروردین": 1, "اردیبهشت": 2, "خرداد": 3, "تیر": 4, "مرداد": 5,
                    "شهریور": 6, "مهر": 7, "آبان": 8, "آذر": 9, "دی": 10, "بهمن": 11, "اسفند": 12
                }
                month = month_map.get(month_name, None)
                if not month:
                    raise ValueError(f"Unknown month name: {month_name}")

                shamsi_dt = jdatetime.datetime.strptime(f"{year}-{month}-{day} {time_part}", "%Y-%m-%d %H:%M")
                news_date_shamsi = shamsi_dt.date()
                published_datetime = shamsi_dt.togregorian()

            except Exception as e:
                self.logger.warning(f"Failed to parse datetime from '{datetime_title}': {e}")
                return None

            if news_date_shamsi > self._target_date_shamsi:
                self.logger.debug(f"Skipping future news: {news_date_shamsi}")
                return None

            return NewsLinkData(
                source=ISNA_SOURCE_NAME,
                link=full_link,
                published_datetime=published_datetime,
            )

        except Exception as e:
            self.logger.error(f"Error parsing ISNA news item: {e}", exc_info=True)
            return None

    def collect_links(self) -> List[NewsLinkData]:
        """
        Crawl through all ISNA archive pages for the given date.
        """
        self.logger.info(f"Starting ISNA link collection for {self._target_date_shamsi}")
        all_links: List[NewsLinkData] = []
        page_index = 1

        while True:
            url = self._get_archive_url(page_index)
            self.logger.info(f"Fetching page {page_index}: {url}")
            html = self._fetch_html(url)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            news_items = soup.select(self.NEWS_ITEM_SELECTOR)
            if not news_items:
                self.logger.info(f"No news items found on page {page_index}. Stopping.")
                break

            found_previous_day = False
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
                    found_previous_day = True
                    break

            if found_previous_day or page_links_count == 0:
                break

            self.logger.info(f"Collected {page_links_count} links from page {page_index}. Continuing...")
            page_index += 1

        self.logger.info(f"Finished ISNA collection. Total links: {len(all_links)}")
        return all_links
