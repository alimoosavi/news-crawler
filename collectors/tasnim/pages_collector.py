import logging
from typing import List, Optional, Dict

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from schema import NewsData, NewsLinkData
from news_publishers import TASNIM


class TasnimPageCollector:
    """
    A page collector for the Tasnim News website using requests and BeautifulSoup.
    Structured to match DonyaEqtesadPageCollector interface but adapted for Tasnim layout.
    """

    def __init__(self, fetch_timeout: int = 15):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fetch_timeout = fetch_timeout
        self.logger.info("TasnimPageCollector initialized.")

    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetch the HTML content of a page."""
        try:
            self.logger.debug(f"Fetching page: {url}")
            response = requests.get(url, timeout=self.fetch_timeout)
            response.raise_for_status()
            self.logger.info(f"Successfully fetched HTML from {url}")
            return response.text
        except RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}", exc_info=True)
            return None

    def crawl_batch(self, batch: List[NewsLinkData]) -> Dict[str, NewsData]:
        """
        Crawl a batch of links and return a dictionary mapping link -> NewsData.
        """
        results: Dict[str, NewsData] = {}

        for link_data in batch:
            if link_data.source != TASNIM:
                continue

            html = self._fetch_html(link_data.link)
            if not html:
                continue

            news_data = self.extract_news(html, link_data)
            if news_data:
                results[link_data.link] = news_data

        return results

    def extract_news(self, html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse HTML into a NewsData object for Tasnim layout."""
        try:
            soup = BeautifulSoup(html, "html.parser")

            # --- Title ---
            title_tag = soup.select_one("h1.title")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"

            # --- Summary ---
            summary_tag = soup.select_one("h3.lead")
            summary = summary_tag.get_text(strip=True) if summary_tag else None

            # --- Content ---
            content = ""
            story_div = soup.select_one("div.story")
            if story_div:
                paragraphs = story_div.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            # --- Keywords ---
            keywords = [
                tag.get_text(strip=True)
                for tag in soup.select("ul.smart-keyword li.skeyword-item a")
            ]

            # --- Images ---
            images = []
            img_tag = soup.select_one("article.single-news figure img")
            if img_tag and img_tag.get("src"):
                images.append(img_tag["src"])

            return NewsData(
                source=link_data.source,
                title=title,
                content=content,
                link=link_data.link,
                keywords=keywords if keywords else None,
                published_datetime=link_data.published_datetime,
                published_timestamp=int(link_data.published_datetime.timestamp()),
                images=images if images else None,
                summary=summary,
            )
        except Exception as e:
            self.logger.error(f"Error parsing HTML for {link_data.link}: {e}", exc_info=True)
            return None
