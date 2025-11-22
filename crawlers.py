import asyncio
from typing import List, Optional, Dict

import aiohttp
from bs4 import BeautifulSoup


class BaseCrawler:
    """Base class for asynchronous news collectors."""

    def __init__(self, logger, news_links: List[str]):
        self.logger = logger
        self._news_links = news_links
        self._news = {}
        self._lock = asyncio.Lock()

    @staticmethod
    async def fetch(news_link: str) -> Optional[str]:
        """Fetch the HTML content asynchronously with retries."""
        if not news_link.startswith(("http://", "https://")):
            news_link = "https://" + news_link  # Ensure protocol

        headers = {"User-Agent": "Mozilla/5.0"}
        for _ in range(3):  # Retry up to 3 times
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(news_link, headers=headers, timeout=10) as response:
                        if response.status == 200:
                            return await response.text()
                        else:
                            print(f"Error {response.status}: {news_link}")
                            return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Retrying due to error: {e}")
                await asyncio.sleep(1)  # Wait before retrying
        return None

    async def fetch_batch_concurrently(self, batch: List[str]):
        """Fetch all news links in a batch asynchronously."""
        tasks = [self.fetch(link) for link in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        result_dict = {}
        for link, html_content in zip(batch, results):
            if isinstance(html_content, Exception):
                self.logger.error(f"Error fetching {link}: {html_content}")
                result_dict[link] = None
            else:
                try:
                    result_dict[link] = self.process_news_content_page(html_content) if html_content else None
                except Exception as e:
                    self.logger.error(f"Error processing {link}: {e}")
                    result_dict[link] = None

        async with self._lock:
            self._news.update(result_dict)

    def process_news_content_page(self, html_content: str) -> Optional[Dict[str, str]]:
        """Process and extract news content. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement this method.")

    async def run(self):
        """Fetch all news links in parallel."""
        batch_size = 10  # Adjust batch size as needed
        batches = [self._news_links[i: i + batch_size] for i in range(0, len(self._news_links), batch_size)]

        await asyncio.gather(*[self.fetch_batch_concurrently(batch) for batch in batches])

    def get_news(self) -> Dict[str, Optional[Dict[str, str]]]:
        """Return a copy of the fetched news safely."""
        return self._news.copy()


class IRNACrawler(BaseCrawler):
    """Crawler for IRNA news website."""

    def __init__(self, logger, news_links: List[str]):
        super().__init__(logger, news_links)

    def process_news_content_page(self, html_content: str) -> Optional[Dict[str, str]]:
        """Parse and extract title & body from IRNA news page."""
        soup = BeautifulSoup(html_content, "html.parser")
        title_tag = soup.find("h1", class_="title")
        body_tag = soup.find("div", class_="item-body")

        if not title_tag or not body_tag:
            return None

        title = title_tag.a.get_text(strip=True) if title_tag.a else "No Title"
        body = "\n".join(
            [p.get_text(strip=True) for p in body_tag.find_all("p") if p.get_text(strip=True)]
        )

        return {"title": title, "body": body}


class ISNACrawler(BaseCrawler):
    """Crawler for IRNA news website."""

    def __init__(self, logger, news_links: List[str]):
        super().__init__(logger, news_links)

    def process_news_content_page(self, html_content: str) -> Optional[Dict[str, str]]:
        """Parse and extract title & body from IRNA news page."""
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract title
        title_tag = soup.find("h1", class_="first-title", itemprop="headline")
        title = title_tag.get_text(strip=True) if title_tag else "No Title"

        # Extract body from <div itemprop="articleBody" class="item-text">
        body_tag = soup.find("div", itemprop="articleBody", class_="item-text")
        if not body_tag:
            return None  # If no body tag is found, return None

        body = "\n".join(
            [p.get_text(strip=True) for p in body_tag.find_all("p") if p.get_text(strip=True)]
        )
        return {"title": title, "body": body}
