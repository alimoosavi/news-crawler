import logging
from datetime import timezone
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as dateutil_parse

# Assuming these are imported from your schema file
from schema import NewsLinkData, LinksCollectingMetrics
from broker_manager import BrokerManager
from news_publishers import IRNA

class IRNAFreshLinksCollector:
    """
    Refactored IRNA Links collector to read recent links directly from the RSS feed.
    No longer requires Selenium or archive page crawling.
    """
    RSS_URL = "https://www.irna.ir/rss"

    def __init__(self, broker_manager: BrokerManager):
        self.logger = logging.getLogger(__name__)
        self._broker_manager = broker_manager
        self.logger.info("IRNAFreshLinksCollector initialized for RSS fetching.")

    # Removed: _create_webdriver, crawl_archive_page, parse_shamsi_to_utc, extract_news_items (Archive logic)

    def _fetch_rss_feed(self) -> Optional[str]:
        """Fetches the IRNA RSS feed content."""
        self.logger.info(f"Fetching RSS feed from: {self.RSS_URL}")
        try:
            # Add a timeout to prevent hanging
            response = requests.get(self.RSS_URL, timeout=15)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            self.logger.info("Successfully fetched RSS feed.")
            return response.text
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching RSS feed from {self.RSS_URL}: {e}", exc_info=True)
            return None

    def _parse_rss_feed(self, rss_content: str) -> List[NewsLinkData]:
        """
        Parses the RSS XML content, extracts news links and published datetimes,
        and sorts the result by published_datetime descending.
        """
        self.logger.debug("Starting RSS content parsing.")
        try:
            # Use BeautifulSoup with 'xml' parser
            soup = BeautifulSoup(rss_content, "xml")
            news_items = []

            # Find all <item> tags
            rss_items = soup.find_all("item")
            self.logger.info(f"Found {len(rss_items)} items in the RSS feed.")

            for index, item in enumerate(rss_items, 1):
                try:
                    # Extract the link
                    link_tag = item.find("link")
                    news_url = link_tag.text.strip() if link_tag else None

                    # Extract the publication date string
                    pub_date_tag = item.find("pubDate")
                    pub_date_str = pub_date_tag.text.strip() if pub_date_tag else None

                    if not news_url or not pub_date_str:
                        self.logger.warning(
                            f"Missing link or pubDate in RSS item {index}. Skipping."
                        )
                        continue

                    # Parse the pubDate string into a timezone-aware datetime object (it's in GMT/UTC)
                    published_dt_gmt = dateutil_parse(pub_date_str)

                    # Ensure it's explicitly UTC
                    # Use .astimezone(timezone.utc) to ensure it is UTC
                    published_datetime_utc = published_dt_gmt.astimezone(timezone.utc)

                    news_item = NewsLinkData(
                        source=IRNA,
                        link=news_url,
                        published_datetime=published_datetime_utc
                    )
                    news_items.append(news_item)
                    self.logger.debug(
                        f"Parsed news item: {news_item.link}, published at {news_item.published_datetime}"
                    )

                except Exception as e:
                    self.logger.error(f"Error parsing RSS item {index}: {e}", exc_info=True)

            # --- Sorting Logic Added Here ---
            # Sort the extracted news items by published_datetime in descending order (newest first)
            news_items.sort(key=lambda item: item.published_datetime, reverse=True)
            self.logger.info("Sorted news links by published_datetime descending.")
            # ---------------------------------

            self.logger.info(f"Successfully extracted and sorted {len(news_items)} valid news links from RSS.")
            return news_items

        except Exception as e:
            self.logger.error(f"General error during RSS parsing: {e}", exc_info=True)
            return []

    def crawl_recent_links(self, last_seen_link: Optional[str] = None) -> LinksCollectingMetrics:
        """
        Fetches the RSS feed, parses the links, and sends new links to the broker
        until last_seen_link is reached.
        """
        self.logger.info(
            f"Starting RSS feed crawl. Last_seen_link: {last_seen_link if last_seen_link else 'None'}"
        )

        rss_content = self._fetch_rss_feed()
        if not rss_content:
            return LinksCollectingMetrics(latest_link=None, links_scraped_count=0)

        # Get all links from the RSS feed (usually the most recent ones)
        all_rss_links = self._parse_rss_feed(rss_content)
        total_links_scraped = 0
        latest_link = None
        batch_to_send: List[NewsLinkData] = []

        # The RSS feed is usually ordered from newest to oldest (as seen in the provided XML)
        for item in all_rss_links:
            # 1. Capture the very first link encountered (the newest one in the feed)
            if latest_link is None:
                latest_link = item.link
                self.logger.debug(f"Set overall newest link: {latest_link}")

            # 2. Check if we reached the last link we successfully processed previously
            if last_seen_link and item.link == last_seen_link:
                self.logger.info(f"Reached last seen link: {last_seen_link}. Stopping link collection.")
                break

            # 3. This is a new link.
            batch_to_send.append(item)

        if batch_to_send:
            self.logger.info(f"Sending batch of {len(batch_to_send)} new links to broker")
            self._broker_manager.produce_links(batch_to_send)
            total_links_scraped += len(batch_to_send)
        else:
            self.logger.info("No new links found or last_seen_link matches the newest link.")

        self.logger.info(
            f"RSS crawl completed. Total new links: {total_links_scraped}, Latest link: {latest_link}"
        )

        # Return the metrics object
        return LinksCollectingMetrics(
            latest_link=latest_link,
            links_scraped_count=total_links_scraped
        )

    def __enter__(self):
        self.logger.debug("Entering IRNAFreshLinksCollector context (No Selenium cleanup needed).")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """No WebDriver cleanup needed for RSS-based crawler."""
        self.logger.debug("Exiting IRNAFreshLinksCollector context.")
        pass
