import logging
from datetime import datetime, date
from typing import Optional, List, Dict

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from broker_manager import BrokerManager
from schema import NewsLinkData, LinksCollectingMetrics
from news_publishers import DONYAYE_EQTESAD


class DonyaEqtesadDailyLinksCollector:
    """
    A links collector for the Donya-e-Eqtesad news website that fetches links from sitemaps.
    Compatible with both fresh daily crawling and historical range crawling.
    """

    BASE_URL = "https://donya-e-eqtesad.com"
    SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap.xml"

    def __init__(self, broker_manager: Optional[BrokerManager]):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._broker_manager = broker_manager
        self.logger.info("DonyaEqtesadDailyLinksCollector initialized.")

    # --------------------------
    # Internal XML utilities
    # --------------------------
    def _fetch_xml(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch an XML file from a given URL and return a BeautifulSoup object."""
        self.logger.debug(f"Fetching XML from {url}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, "xml")
        except RequestException as e:
            self.logger.error(f"Failed to fetch XML from {url}: {e}", exc_info=True)
            return None

    # --------------------------
    # Sitemap navigation
    # --------------------------
    def _get_last_sitemap_url(self) -> Optional[str]:
        """
        Old backward-compatible API:
        Fetches the sitemap index and returns the URL of the **latest daily sitemap**.
        """
        soup = self._fetch_xml(self.SITEMAP_INDEX_URL)
        if not soup:
            return None

        sitemaps = soup.find_all("sitemap")
        if not sitemaps:
            return None

        last_sitemap = sitemaps[-1]
        loc_tag = last_sitemap.find("loc")
        return loc_tag.text.strip() if loc_tag else None

    def _get_daily_sitemap_url(self, g_date: date) -> Optional[str]:
        """
        NEW API: Fetch sitemap index, find the sitemap whose <lastmod> matches g_date.
        """
        soup = self._fetch_xml(self.SITEMAP_INDEX_URL)
        if not soup:
            return None

        for sm in soup.find_all("sitemap"):
            loc_tag = sm.find("loc")
            lastmod_tag = sm.find("lastmod")
            if not loc_tag or not lastmod_tag:
                continue

            try:
                lastmod_dt = datetime.fromisoformat(lastmod_tag.text.strip()).date()
                if lastmod_dt == g_date:
                    return loc_tag.text.strip()
            except Exception:
                continue

        self.logger.warning(f"No sitemap found for {g_date}")
        return None

    # --------------------------
    # Extracting news links
    # --------------------------
    def _get_news_links_from_sitemap(self, sitemap_url: str) -> List[NewsLinkData]:
        """
        Extracts all news links and their publication dates from a daily sitemap.
        """
        soup = self._fetch_xml(sitemap_url)
        if not soup:
            return []

        news_links = []
        for url_tag in soup.find_all("url"):
            try:
                link_tag = url_tag.find("loc")
                lastmod_tag = url_tag.find("lastmod")

                if link_tag and lastmod_tag:
                    link = link_tag.text.strip()
                    published_datetime = datetime.fromisoformat(lastmod_tag.text.strip())

                    news_item = NewsLinkData(
                        source=DONYAYE_EQTESAD,
                        link=link,
                        published_datetime=published_datetime
                    )
                    news_links.append(news_item)
            except Exception as e:
                self.logger.error(f"Error parsing a URL entry: {e}", exc_info=True)
                continue

        news_links.sort(key=lambda item: item.published_datetime, reverse=True)
        return news_links

    # --------------------------
    # Fresh (recent) crawl
    # --------------------------
    def crawl_recent_links(self, last_seen_link: Optional[str] = None) -> LinksCollectingMetrics:
        """
        Backward-compatible fresh crawl: only processes the last sitemap.
        """
        last_sitemap_url = self._get_last_sitemap_url()
        if not last_sitemap_url:
            return LinksCollectingMetrics()

        all_news_links = self._get_news_links_from_sitemap(last_sitemap_url)
        new_links_to_produce = []
        latest_link_url = None

        for link_item in all_news_links:
            if latest_link_url is None:
                latest_link_url = link_item.link
            if link_item.link == last_seen_link:
                break
            new_links_to_produce.append(link_item)

        if new_links_to_produce and self._broker_manager:
            self._broker_manager.produce_links(new_links_to_produce)

        return LinksCollectingMetrics(
            latest_link=latest_link_url,
            links_scraped_count=len(new_links_to_produce)
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
