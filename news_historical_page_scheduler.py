import asyncio
import logging
import time
from typing import Dict, List, Optional
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import aiohttp
from bs4 import BeautifulSoup
from prometheus_client import Summary
from playwright.async_api import async_playwright, Browser, BrowserContext

from config import settings
from database_manager import DatabaseManager
from news_publishers import IRNA, TASNIM, DONYAYE_EQTESAD, ISNA
from schema import NewsLinkData, NewsData

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("OptimizedNewsScheduler")

# -------------------------------
# Prometheus Metrics
# -------------------------------
CRAWL_DURATION_SECONDS = Summary(
    "crawler_scrape_duration_seconds",
    "Duration of a full crawl cycle for a source",
    ["source"],
)


@dataclass
class BrowserPool:
    """Manages reusable browser instances for Playwright"""
    browser: Browser
    contexts: List[BrowserContext]
    semaphore: asyncio.Semaphore
    
    @classmethod
    async def create(cls, max_contexts: int = 5):
        """Create browser pool with multiple contexts"""
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
            ]
        )
        
        # Pre-create contexts for reuse
        contexts = []
        for _ in range(max_contexts):
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            contexts.append(context)
        
        return cls(
            browser=browser,
            contexts=contexts,
            semaphore=asyncio.Semaphore(max_contexts)
        )
    
    async def close(self):
        """Cleanup browser resources"""
        for context in self.contexts:
            await context.close()
        await self.browser.close()


class OptimizedNewsScheduler:
    """
    High-performance async scheduler with:
    - Async HTTP requests (aiohttp) - 10x faster than requests
    - Browser pooling (Playwright) - reuse browsers instead of recreating
    - Concurrent fetching and parsing pipeline
    - Batch database operations
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        batch_size: int = 50,  # Increased from 10
        poll_interval: int = 60,
        max_concurrent_fetches: int = 20,  # Concurrent HTTP requests
        max_browser_contexts: int = 5,  # Concurrent Playwright contexts
    ):
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.max_concurrent_fetches = max_concurrent_fetches
        self.max_browser_contexts = max_browser_contexts
        
        # Browser pool (lazy init)
        self.browser_pool: Optional[BrowserPool] = None
        
        # HTTP session pool
        self.http_session: Optional[aiohttp.ClientSession] = None

    # ---------------------------------------------
    # Async HTTP Fetcher (for requests-based sources)
    # ---------------------------------------------
    async def fetch_html_http(self, url: str, timeout: int = 15) -> Optional[str]:
        """Fetch HTML using aiohttp (10x faster than requests)"""
        if not self.http_session:
            self.http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                }
            )
        
        try:
            async with self.http_session.get(url) as response:
                response.raise_for_status()
                html = await response.text()
                logger.debug(f"Fetched HTML from {url} ({len(html)} bytes)")
                return html
        except Exception as e:
            logger.error(f"HTTP fetch error for {url}: {e}")
            return None

    # ---------------------------------------------
    # Async Playwright Fetcher (with browser pooling)
    # ---------------------------------------------
    async def fetch_html_playwright(
        self, 
        url: str, 
        selector: str = "body",
        timeout: int = 15000
    ) -> Optional[str]:
        """Fetch HTML using Playwright with browser pooling (reuses browsers)"""
        if not self.browser_pool:
            self.browser_pool = await BrowserPool.create(self.max_browser_contexts)
        
        async with self.browser_pool.semaphore:
            # Get available context (round-robin)
            context = self.browser_pool.contexts[
                hash(url) % len(self.browser_pool.contexts)
            ]
            
            page = None
            try:
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                await page.wait_for_selector(selector, timeout=5000)
                html = await page.content()
                logger.debug(f"Fetched HTML via Playwright from {url}")
                return html
            except Exception as e:
                logger.error(f"Playwright fetch error for {url}: {e}")
                return None
            finally:
                if page:
                    await page.close()

    # ---------------------------------------------
    # Source-specific fetchers
    # ---------------------------------------------
    async def fetch_irna(self, link_data: NewsLinkData) -> Optional[NewsData]:
        """IRNA - requires Playwright"""
        html = await self.fetch_html_playwright(
            link_data.link,
            selector="div.item-body"
        )
        if not html:
            return None
        
        # Parse in executor to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._parse_irna,
            html,
            link_data
        )
    
    async def fetch_isna(self, link_data: NewsLinkData) -> Optional[NewsData]:
        """ISNA - requires Playwright"""
        html = await self.fetch_html_playwright(
            link_data.link,
            selector="div.item-body.content-full-news"
        )
        if not html:
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._parse_isna,
            html,
            link_data
        )
    
    async def fetch_tasnim(self, link_data: NewsLinkData) -> Optional[NewsData]:
        """Tasnim - lightweight HTTP"""
        html = await self.fetch_html_http(link_data.link)
        if not html:
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._parse_tasnim,
            html,
            link_data
        )
    
    async def fetch_donyaye_eqtesad(self, link_data: NewsLinkData) -> Optional[NewsData]:
        """DonyaEqtesad - lightweight HTTP"""
        html = await self.fetch_html_http(link_data.link)
        if not html:
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._parse_donyaye_eqtesad,
            html,
            link_data
        )

    # ---------------------------------------------
    # Parsers (run in thread pool to avoid blocking)
    # ---------------------------------------------
    @staticmethod
    def _parse_irna(html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse IRNA HTML"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            title_tag = soup.select_one("h1.title a[itemprop='headline']")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            
            body_tag = soup.select_one("div.item-body")
            content = ""
            if body_tag:
                paragraphs = body_tag.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            
            summary_tag = soup.select_one("p.summary.introtext[itemprop='description']")
            summary = summary_tag.get_text(strip=True) if summary_tag else None
            
            keywords = [tag.get_text(strip=True) for tag in soup.select("section.tags li a")]
            
            images = []
            main_img = soup.select_one("figure.item-img img")
            if main_img and main_img.get("src"):
                images.append(main_img["src"])
            
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
            logger.error(f"Error parsing IRNA: {e}")
            return None

    @staticmethod
    def _parse_isna(html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse ISNA HTML"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            title_tag = soup.select_one("h1.first-title[itemprop='headline']")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            
            summary_tag = soup.select_one("p.summary[itemprop='description']")
            summary = summary_tag.get_text(strip=True) if summary_tag else None
            
            content_tag = soup.select_one("div.item-text[itemprop='articleBody']")
            content = ""
            if content_tag:
                paragraphs = content_tag.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            
            keywords = [tag.get_text(strip=True) for tag in soup.select("section.box.trending-tags ul li a")]
            
            images = []
            main_img = soup.select_one("figure.item-img img")
            if main_img and main_img.get("src"):
                images.append(main_img["src"])
            
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
            logger.error(f"Error parsing ISNA: {e}")
            return None

    @staticmethod
    def _parse_tasnim(html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse Tasnim HTML"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            title_tag = soup.select_one("h1.title")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            
            summary_tag = soup.select_one("h3.lead")
            summary = summary_tag.get_text(strip=True) if summary_tag else None
            
            content = ""
            story_div = soup.select_one("div.story")
            if story_div:
                paragraphs = story_div.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            
            keywords = [tag.get_text(strip=True) for tag in soup.select("ul.smart-keyword li.skeyword-item a")]
            
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
            logger.error(f"Error parsing Tasnim: {e}")
            return None

    @staticmethod
    def _parse_donyaye_eqtesad(html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse DonyaEqtesad HTML"""
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            title_tag = soup.select_one("h1.title")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            
            summary_tag = soup.select_one("div.lead.justify")
            summary = ""
            if summary_tag:
                for strong_tag in summary_tag.find_all("strong"):
                    strong_tag.decompose()
                summary = summary_tag.get_text(strip=True)
            
            content = ""
            content_div = soup.select_one("div#echo-detail")
            if content_div:
                paragraphs = content_div.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            
            keywords = [tag.get_text(strip=True) for tag in soup.select("div.article-tag a.tags-detail")]
            
            images = []
            main_img = soup.select_one("div.contain-img img")
            if main_img and main_img.get("src"):
                images.append(main_img["src"])
            
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
            logger.error(f"Error parsing DonyaEqtesad: {e}")
            return None

    # ---------------------------------------------
    # Source router
    # ---------------------------------------------
    async def fetch_single(self, link_data: NewsLinkData) -> Optional[NewsData]:
        """Route to appropriate fetcher based on source"""
        try:
            if link_data.source == IRNA:
                return await self.fetch_irna(link_data)
            elif link_data.source == ISNA:
                return await self.fetch_isna(link_data)
            elif link_data.source == TASNIM:
                return await self.fetch_tasnim(link_data)
            elif link_data.source == DONYAYE_EQTESAD:
                return await self.fetch_donyaye_eqtesad(link_data)
            else:
                logger.warning(f"Unknown source: {link_data.source}")
                return None
        except Exception as e:
            logger.error(f"Error fetching {link_data.link}: {e}")
            return None

    # ---------------------------------------------
    # Concurrent batch processing
    # ---------------------------------------------
    async def process_batch_concurrent(
        self,
        links: List[NewsLinkData]
    ) -> List[NewsData]:
        """Process batch with controlled concurrency"""
        semaphore = asyncio.Semaphore(self.max_concurrent_fetches)
        
        async def bounded_fetch(link_data: NewsLinkData):
            async with semaphore:
                return await self.fetch_single(link_data)
        
        # Fetch all concurrently with semaphore control
        tasks = [bounded_fetch(link) for link in links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        news_items = []
        for result in results:
            if isinstance(result, NewsData):
                news_items.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
        
        return news_items

    # ---------------------------------------------
    # Process single source
    # ---------------------------------------------
    async def process_source(self, source: str) -> int:
        """Process all pending links for a source"""
        # Fetch links from DB (sync operation)
        loop = asyncio.get_event_loop()
        links = await loop.run_in_executor(
            None,
            self.db_manager.get_pending_links_by_source,
            source,
            self.batch_size
        )
        
        if not links:
            logger.info(f"No pending links for {source}")
            return 0
        
        logger.info(f"Processing {len(links)} links for {source}")
        
        # Crawl concurrently
        start_time = time.time()
        news_items = await self.process_batch_concurrent(links)
        duration = time.time() - start_time
        
        logger.info(
            f"Fetched {len(news_items)}/{len(links)} articles for {source} "
            f"in {duration:.2f}s ({len(news_items)/duration:.1f} items/s)"
        )
        
        if news_items:
            # Save to DB (sync operation)
            processed_links = [item.link for item in news_items]
            await loop.run_in_executor(
                None,
                self.db_manager.insert_news_batch,
                news_items
            )
            await loop.run_in_executor(
                None,
                self.db_manager.mark_links_completed,
                processed_links
            )
            
            logger.info(f"Saved {len(news_items)} articles for {source}")
        
        return len(news_items)

    # ---------------------------------------------
    # Run cycle for all sources
    # ---------------------------------------------
    async def run_cycle(self) -> int:
        """Process all sources concurrently"""
        logger.info("Starting async crawler cycle...")
        start_time = time.time()
        
        sources = [IRNA, ISNA, TASNIM, DONYAYE_EQTESAD]
        
        # Process all sources concurrently
        tasks = [self.process_source(source) for source in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_crawled = sum(r for r in results if isinstance(r, int))
        duration = time.time() - start_time
        
        logger.info(
            f"Cycle complete: {total_crawled} articles in {duration:.2f}s "
            f"({total_crawled/duration:.1f} items/s)"
        )
        
        return total_crawled

    # ---------------------------------------------
    # Continuous scheduler
    # ---------------------------------------------
    async def run_forever(self):
        """Run scheduler continuously"""
        logger.info("Async scheduler started...")
        
        try:
            while True:
                crawled = await self.run_cycle()
                
                if crawled == 0:
                    logger.info(f"No links found. Sleeping {self.poll_interval}s...")
                    await asyncio.sleep(self.poll_interval)
                else:
                    # Small pause between cycles
                    await asyncio.sleep(1)
        finally:
            await self.cleanup()

    # ---------------------------------------------
    # Cleanup
    # ---------------------------------------------
    async def cleanup(self):
        """Cleanup resources"""
        if self.browser_pool:
            await self.browser_pool.close()
        if self.http_session:
            await self.http_session.close()
        logger.info("Cleanup complete")


# ---------------------------------------------
# Entry point
# ---------------------------------------------
def main():
    db_manager = DatabaseManager(db_config=settings.database)
    
    scheduler = OptimizedNewsScheduler(
        db_manager=db_manager,
        batch_size=50,  # Larger batches
        poll_interval=30,
        max_concurrent_fetches=20,  # 20 concurrent HTTP requests
        max_browser_contexts=5,  # 5 concurrent Playwright contexts
    )
    
    # Run async scheduler
    asyncio.run(scheduler.run_forever())


if __name__ == "__main__":
    main()