import logging
import time
import re
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from dataclasses import dataclass

from database_manager import DatabaseManager


@dataclass
class NewsItem:
    """Typed data class for news items"""
    title: str
    link: str
    summary: str
    published_datetime: str


class ISNALinksCrawler:
    def __init__(self, db_manager: DatabaseManager, base_url="https://www.isna.ir", headless=True):
        self.base_url = base_url
        self.headless = headless
        self.logger = logging.getLogger(__name__)
        self._db_manager = db_manager
        self._driver = None

        # Setup database
        if not self._db_manager.connection:
            self._db_manager.connect()
        self._db_manager.create_tables_if_not_exist()

    @classmethod
    def extract_news_items(cls, html_content: str, base_url: str = "https://www.isna.ir") -> list[NewsItem]:
        """
        Extract news items from HTML content containing div.items structure

        Args:
            html_content: HTML string containing the news items
            base_url: Base URL to prepend to relative links

        Returns:
            List of NewsItem dataclass instances
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        news_items = []

        # Find the main container
        items_div = soup.find('div', class_='items')
        if not items_div:
            return news_items

        # Extract each news item
        for li in items_div.find_all('li'):
            try:
                # Extract title from image alt attribute
                img_tag = li.find('img')
                title = img_tag.get('alt', '').strip() if img_tag else ""

                # Extract link from href attribute
                link_tag = li.find('a')
                relative_link = link_tag.get('href', '') if link_tag else ""
                link = f"{base_url}{relative_link}" if relative_link and not relative_link.startswith(
                    'http') else relative_link

                # Extract summary from <p> inside <div class="desc">
                desc_div = li.find('div', class_='desc')
                summary = ""
                if desc_div:
                    p_tag = desc_div.find('p')
                    summary = p_tag.get_text(strip=True) if p_tag else ""

                # Extract published_datetime from title attribute of <time> tag
                time_tag = li.find('time')
                published_datetime = ""
                if time_tag:
                    time_link = time_tag.find('a')
                    if time_link:
                        published_datetime = time_link.get('title', '').strip()

                # Only add if we have essential data
                if title and link:
                    news_item = NewsItem(
                        title=cls.clean_text(title),
                        link=link,
                        summary=cls.clean_text(summary),
                        published_datetime=published_datetime
                    )
                    news_items.append(news_item)

            except Exception as e:
                # Log error but continue processing other items
                print(f"Error processing news item: {e}")
                continue

        return news_items

    @classmethod
    def clean_text(cls, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        # Remove extra whitespace and normalize
        return re.sub(r'\s+', ' ', text).strip()

    def _create_driver(self):
        """Create Chrome driver"""
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        return webdriver.Chrome(options=options)
