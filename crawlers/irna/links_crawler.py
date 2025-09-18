import logging
import re
from datetime import datetime
from urllib.parse import urlencode

import jdatetime
import pytz
import requests
from bs4 import BeautifulSoup

from broker_manager import BrokerManager
from schema import NewsLinkData


class IRNALinksCrawler:
    SOURCE_NAME = 'IRNA'
    BASE_URL = "https://www.irna.ir"
    ARCHIVE_ENDPOINT = "/archive"

    def __init__(self, broker_manager: BrokerManager):
        self.logger = logging.getLogger(__name__)
        self._broker_manager = broker_manager
        self.tehran_tz = pytz.timezone('Asia/Tehran')
        self.session = requests.Session()

    def crawl_archive(self, year: int, month: int, day: int):
        """Crawl paginated news links for a specific Shamsi date."""
        page_index = 1
        found_fresh_link = True

        while found_fresh_link:
            self.logger.info(
                f"Crawling IRNA archive for {year}/{month:02d}/{day:02d}, page {page_index}"
            )

            news_items = self.crawl_archive_page(year, month, day, page_index)

            if not news_items:
                self.logger.info(f"No news items found on page {page_index} for {year}/{month:02d}/{day:02d}")
                found_fresh_link = False
                continue

            # ✅ Instead of saving to DB → produce to Kafka
            self._broker_manager.produce_links(news_items)
            self.logger.info(f"Produced {len(news_items)} links to Kafka")

            page_index += 1

    def crawl_archive_page(self, year: int, month: int, day: int, page_index: int) -> list[NewsLinkData]:
        """Crawl a specific paginated IRNA archive page."""
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
            response.raise_for_status()
            return self.extract_news_items(response.text)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error while crawling {archive_url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while processing {archive_url}: {e}")
            return []

    @classmethod
    def parse_persian_datetime(cls, persian_str):
        persian_str = persian_str.replace("'", "").strip()
        pattern = r'.*?:\s*(?P<weekday>\S+)\s+(?P<day>\d{1,2})\s+(?P<month>\S+)\s+(?P<year>\d{4})\s*-\s*(?P<hour>\d{1,2}):(?P<minute>\d{2})'
        match = re.search(pattern, persian_str)
        if not match:
            raise ValueError("Input string format is not valid for IRNA.")

        parts = match.groupdict()
        persian_months = {
            'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4,
            'مرداد': 5, 'شهریور': 6, 'مهر': 7, 'آبان': 8,
            'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
        }
        shamsi_year, shamsi_month, shamsi_day = (
            int(parts['year']),
            persian_months.get(parts['month']),
            int(parts['day'])
        )
        hour, minute = int(parts['hour']), int(parts['minute'])

        if shamsi_month is None:
            raise ValueError(f"Invalid month name: {parts['month']}")

        shamsi_datetime = jdatetime.datetime(shamsi_year, shamsi_month, shamsi_day, hour, minute)
        return {"datetime": shamsi_datetime, "shamsi_year": shamsi_year, "shamsi_month": shamsi_month,
                "shamsi_day": shamsi_day, "hour": hour, "minute": minute}

    @classmethod
    def parse_persian_date(cls, date_string: str) -> datetime | None:
        try:
            parsed = cls.parse_persian_datetime(date_string)
            shamsi_dt = parsed['datetime']
            gregorian_dt = shamsi_dt.togregorian()
            dt = datetime(gregorian_dt.year, gregorian_dt.month, gregorian_dt.day,
                          gregorian_dt.hour, gregorian_dt.minute)
            return pytz.timezone('Asia/Tehran').localize(dt)
        except Exception as e:
            logging.getLogger(__name__).error(f"Error parsing IRNA Persian date '{date_string}': {str(e)}")
            return None

    @classmethod
    def extract_news_items(cls, html_content: str) -> list[NewsLinkData]:
        """Extract news items from IRNA's archive HTML content."""
        soup = BeautifulSoup(html_content, 'html.parser')
        items_divs = soup.find_all('div', class_='item-archive')
        news_items: list[NewsLinkData] = []

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

        news_items.sort(key=lambda item: item.published_datetime, reverse=True)
        return news_items

    def crawl_recent_links(self, last_link: str | None) -> str | None:
        """
        Crawl recent news links starting from today.
        Stops when reaching the given `last_link` (URL string).
        Returns the newest link found.
        """
        self.logger.info("Starting crawl_recent_links")
        today = datetime.now(self.tehran_tz)
        max_pages_per_day = 30

        newest_link: str | None = None
        stop_crawling = False

        # Crawl today and previous 6 days (adjust range as needed)
        for day_offset in range(0, 7):
            crawl_date = today - jdatetime.timedelta(days=day_offset)
            shamsi_date = jdatetime.datetime.fromgregorian(datetime=crawl_date)

            for page_index in range(1, max_pages_per_day + 1):
                self.logger.info(f"Crawling page {page_index} for {crawl_date.date()}")
                news_items = self.crawl_archive_page(
                    shamsi_date.year, shamsi_date.month, shamsi_date.day, page_index
                )

                if not news_items:
                    self.logger.info(f"No news items on page {page_index}, stopping day crawl")
                    break

                fresh_items = []
                for item in news_items:
                    if last_link and item.link == last_link:
                        stop_crawling = True
                        break
                    fresh_items.append(item)

                if fresh_items:
                    self._broker_manager.produce_links(fresh_items)
                    self.logger.info(f"Produced {len(fresh_items)} new links to Kafka")

                    # Track newest link (first item in sorted list)
                    if not newest_link:
                        newest_link = fresh_items[0].link

                if stop_crawling:
                    self.logger.info("Reached last_link, stopping crawl")
                    return newest_link

        return newest_link
