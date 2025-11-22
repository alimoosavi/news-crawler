import logging
from typing import List, Optional, Dict

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from schema import NewsData, NewsLinkData
from news_publishers import DONYAYE_EQTESAD


class DonyaEqtesadPageCollector:
    """
    A page collector for the Donya-e-Eqtesad news website using requests and BeautifulSoup.
    Refactored to be a utility class for the scheduler.
    """

    def __init__(self, fetch_timeout: int = 15):
        """Initializes the collector as a utility class."""
        self.logger = logging.getLogger(self.__class__.__name__)
        # No broker_manager, group_id, or batch_size needed here
        self.fetch_timeout = fetch_timeout
        self.logger.info("DonyaEqtesadPageCollector initialized as a utility class.")

    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetch the HTML content of a page using a simple HTTP request."""
        try:
            self.logger.debug(f"Fetching page: {url}")
            response = requests.get(url, timeout=self.fetch_timeout)
            response.raise_for_status()
            self.logger.info(f"Successfully fetched HTML from {url}")
            return response.text
        except RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}", exc_info=True)
            return None

    # --- UNIFIED PUBLIC METHOD ---
    def crawl_batch(self, batch: List[NewsLinkData]) -> Dict[str, NewsData]:
        """
        Crawl a batch of links and return a dictionary mapping link to NewsData.
        """
        results: Dict[str, NewsData] = {}

        for link_data in batch:
            if link_data.source != DONYAYE_EQTESAD:
                continue

            html = self._fetch_html(link_data.link)
            if not html:
                continue

            news_data = self.extract_news(html, link_data)
            if news_data:
                results[link_data.link] = news_data

        return results

    # The extract_news method's logic remains UNCHANGED as requested:
    def extract_news(self, html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse HTML into a NewsData object."""
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Extract title from h1
            title_tag = soup.select_one("h1.title")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"

            # Extract summary from div.lead.justify
            summary_tag = soup.select_one("div.lead.justify")
            summary = ""
            if summary_tag:
                # Remove nested strong tags (source name like "اکوایران :") from summary
                for strong_tag in summary_tag.find_all("strong"):
                    strong_tag.decompose()
                summary = summary_tag.get_text(strip=True)

            # Extract main body text from div#echo-detail
            content = ""
            content_div = soup.select_one("div#echo-detail")
            if content_div:
                # Find all <p> tags and join their text content
                paragraphs = content_div.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            # Extract keywords from div.article-tag a tags
            keywords = [
                tag.get_text(strip=True)
                for tag in soup.select("div.article-tag a.tags-detail")
            ]

            # Extract images from div.contain-img img and other potential image sources
            images = []
            main_img_tag = soup.select_one("div.contain-img img")
            if main_img_tag and main_img_tag.get("src"):
                images.append(main_img_tag["src"])

            # Create and return the NewsData object
            return NewsData(
                source=link_data.source,
                title=title,
                content=content,
                link=link_data.link,
                keywords=keywords if keywords else None,
                published_datetime=link_data.published_datetime,
                published_timestamp=int(link_data.published_datetime.timestamp()),
                images=images if images else None,
                summary=summary if summary else None,
            )
        except Exception as e:
            self.logger.error(f"Error parsing HTML for {link_data.link}: {e}", exc_info=True)
            return None

    # Removed: process_batch, run, __enter__, __exit__
