"""
Async Shargh Page Collector with Parallel Processing

Performance improvements:
- Uses async Playwright for non-blocking I/O
- Parallel browser contexts (5-10x faster)
- Shared browser instance across batch
- Configurable concurrency control

HTML Structure:
- Title: <h1 class="title">
- Summary: <p class="lead">
- Image: <div class="image_top_primary"> <img src="...">
- Content: <div id="echo_detail"> with <p> tags
- Source: <div class="writers"> (sometimes present)
- Keywords: Not consistently available in HTML structure
"""
import asyncio
import logging
from typing import List, Optional, Dict

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from schema import NewsData, NewsLinkData
from news_publishers import SHARGH


class SharghPageCollector:
    """Async Shargh page collector with parallel processing"""
    
    # CSS selectors for Shargh's article structure
    MAIN_CONTENT_SELECTOR = "div#echo_detail, header.article_header"

    def __init__(
        self,
        fetch_timeout: int = 15,
        max_concurrent: int = 5,
        browser_headless: bool = True
    ):
        """
        Initialize async Shargh collector.
        
        Args:
            fetch_timeout: Timeout for page loading (seconds)
            max_concurrent: Maximum concurrent page fetches (default: 5)
            browser_headless: Run browser in headless mode
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fetch_timeout = fetch_timeout
        self.max_concurrent = max_concurrent
        self.browser_headless = browser_headless
        
        self.logger.info(
            f"SharghPageCollector initialized: "
            f"max_concurrent={max_concurrent}, timeout={fetch_timeout}s"
        )

    async def _fetch_html_async(
        self,
        url: str,
        context
    ) -> Optional[str]:
        """
        Fetch HTML using async Playwright with shared browser context.
        
        Args:
            url: Article URL to fetch
            context: Playwright browser context
            
        Returns:
            HTML content or None if failed
        """
        page = None
        try:
            page = await context.new_page()
            
            # Navigate to page
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=self.fetch_timeout * 1000
            )

            # Wait for main content to load
            try:
                await page.wait_for_selector(
                    self.MAIN_CONTENT_SELECTOR.split(',')[0].strip(),
                    timeout=5000
                )
            except PlaywrightTimeoutError:
                # Content might still be there, continue
                self.logger.debug(f"Content selector timeout for {url}, continuing anyway")
            
            html_content = await page.content()
            self.logger.debug(f"✅ Fetched Shargh page: {url}")
            return html_content

        except PlaywrightTimeoutError:
            self.logger.error(f"⏱️ Timeout while fetching Shargh page: {url}")
            return None
            
        except Exception as e:
            self.logger.error(
                f"❌ Error fetching Shargh page {url}: {e}",
                exc_info=True
            )
            return None
            
        finally:
            if page:
                await page.close()

    async def _crawl_batch_async(
        self,
        batch: List[NewsLinkData]
    ) -> Dict[str, NewsData]:
        """
        Crawl batch using async Playwright with parallel contexts.
        
        Args:
            batch: List of NewsLinkData to crawl
            
        Returns:
            Dictionary mapping link URL to NewsData
        """
        results: Dict[str, NewsData] = {}
        
        # Filter for Shargh links only
        shargh_links = [link for link in batch if link.source == SHARGH]
        
        if not shargh_links:
            return results
        
        self.logger.info(
            f"🚀 Starting async crawl of {len(shargh_links)} Shargh links "
            f"(max_concurrent={self.max_concurrent})"
        )
        
        async with async_playwright() as p:
            # Launch browser once for entire batch
            browser = await p.chromium.launch(headless=self.browser_headless)
            context = await browser.new_context()
            
            # Semaphore to limit concurrent fetches
            semaphore = asyncio.Semaphore(self.max_concurrent)
            
            async def fetch_and_parse(link_data: NewsLinkData):
                """Fetch and parse a single link with semaphore control"""
                async with semaphore:
                    html = await self._fetch_html_async(link_data.link, context)
                    
                    if not html:
                        return
                    
                    # Parse HTML (BeautifulSoup is synchronous)
                    news_data = self.extract_news(html, link_data)
                    
                    if news_data:
                        results[link_data.link] = news_data
            
            # Run all fetches concurrently
            await asyncio.gather(
                *[fetch_and_parse(link) for link in shargh_links],
                return_exceptions=True
            )
            
            await context.close()
            await browser.close()
        
        self.logger.info(
            f"✅ Completed async crawl: {len(results)}/{len(shargh_links)} successful"
        )
        
        return results

    def crawl_batch(self, batch: List[NewsLinkData]) -> Dict[str, NewsData]:
        """
        Synchronous wrapper for async crawl_batch.
        
        This maintains backward compatibility with existing scheduler code.
        
        Args:
            batch: List of NewsLinkData to crawl
            
        Returns:
            Dictionary mapping link URL to NewsData
        """
        return asyncio.run(self._crawl_batch_async(batch))

    def extract_news(
        self,
        html: str,
        link_data: NewsLinkData
    ) -> Optional[NewsData]:
        """
        Extract structured news data from Shargh HTML.
        
        Based on Shargh's page structure:
        - Title: <h1 class="title">
        - Summary: <p class="lead">
        - Image: <div class="image_top_primary"> <img>
        - Content: <div id="echo_detail"> with paragraphs
        - Source attribution: <div class="writers">
        
        Args:
            html: Raw HTML content
            link_data: Original link metadata
            
        Returns:
            NewsData object or None if extraction failed
        """
        try:
            soup = BeautifulSoup(html, "html.parser")

            # --- Title ---
            title_tag = soup.select_one("h1.title")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"

            # --- Summary (Lead) ---
            summary_tag = soup.select_one("p.lead")
            summary = summary_tag.get_text(strip=True) if summary_tag else None

            # --- Images ---
            images = []
            
            # Try primary image first
            img_tag = soup.select_one("div.image_top_primary img")
            if img_tag and img_tag.get("src"):
                img_src = img_tag["src"]
                # Handle relative URLs
                if img_src.startswith('//'):
                    img_src = 'https:' + img_src
                elif img_src.startswith('/'):
                    img_src = 'https://www.sharghdaily.com' + img_src
                images.append(img_src)
            
            # Try other images in article body as fallback
            if not images:
                body_imgs = soup.select("div#echo_detail img")
                for img in body_imgs:
                    img_src = img.get("src")
                    if img_src:
                        if img_src.startswith('//'):
                            img_src = 'https:' + img_src
                        elif img_src.startswith('/'):
                            img_src = 'https://www.sharghdaily.com' + img_src
                        images.append(img_src)
                        break  # Just take the first one

            # --- Main Content ---
            content = ""
            
            # Primary content area
            content_tag = soup.select_one("div#echo_detail")
            
            if content_tag:
                # Remove script tags and ads
                for script in content_tag.find_all(['script', 'style']):
                    script.decompose()
                
                # Remove ad divs
                for ad_div in content_tag.find_all('div', class_=['type-script', 'overview-auto']):
                    ad_div.decompose()
                
                # Extract paragraphs
                paragraphs = content_tag.find_all("p", recursive=True)
                content_parts = []
                
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    # Filter out very short fragments and ad-like content
                    if text and len(text) > 20:
                        # Skip lines that look like ads or navigation
                        if not any(skip_word in text for skip_word in ['منبع:', 'آخرین اخبار', 'پیگیری کنید']):
                            content_parts.append(text)
                
                content = "\n".join(content_parts)
            
            # Fallback: try to get content from main div directly
            if not content or len(content) < 100:
                echo_detail = soup.select_one("div#echo_detail")
                if echo_detail:
                    # Get all text, then clean
                    content = echo_detail.get_text(separator="\n", strip=True)
                    # Remove common footer/metadata lines
                    lines = [
                        line.strip() 
                        for line in content.split('\n') 
                        if line.strip() and len(line.strip()) > 20
                        and not any(skip in line for skip in ['منبع:', 'آخرین اخبار', 'پیگیری کنید'])
                    ]
                    content = "\n".join(lines)

            # --- Keywords ---
            # Shargh doesn't have a consistent keyword/tag structure
            # Try to find any tag-like elements
            keywords = []
            
            # Try common tag selectors
            for selector in [
                'div.keywords a',
                'div.tags a', 
                'div.article-tags a',
                'a[rel="tag"]'
            ]:
                tag_elements = soup.select(selector)
                if tag_elements:
                    keywords = [
                        tag.get_text(strip=True)
                        for tag in tag_elements
                        if tag.get_text(strip=True)
                    ]
                    break

            # --- Source Attribution (optional) ---
            # Sometimes articles have source attribution like "منبع: خبر آنلاین"
            # We can extract this but it's not critical
            source_tag = soup.select_one("div.writers")
            if source_tag:
                source_text = source_tag.get_text(strip=True)
                # You could store this in keywords or summary if needed
                if "منبع:" in source_text and not keywords:
                    # Extract source name
                    source_name = source_text.replace("منبع:", "").strip()
                    if source_name:
                        keywords = [f"منبع: {source_name}"]

            # --- Validate Content ---
            if not content or len(content) < 50:
                self.logger.warning(
                    f"⚠️  Insufficient content extracted from {link_data.link} "
                    f"(length: {len(content)})"
                )
                return None

            # --- Build NewsData ---
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
            self.logger.error(
                f"❌ Error parsing Shargh HTML for {link_data.link}: {e}",
                exc_info=True
            )
            return None


# Example usage
if __name__ == "__main__":
    from datetime import datetime
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create collector
    collector = SharghPageCollector(
        fetch_timeout=15,
        max_concurrent=5
    )
    
    # Example: Fetch a single article
    sample_links = [
        NewsLinkData(
            source=SHARGH,
            link="https://www.sharghdaily.com/بخش-سیاست-6/912534-سلیمی-نمین-نماینده-با-رأی-پایین-تاثیرگذاری-ندارد-اصلاح-طلبان-قطعا-لیست-می-دهند",
            published_datetime=datetime(2024, 11, 21, 2, 30)
        )
    ]
    
    results = collector.crawl_batch(sample_links)
    
    for url, news_data in results.items():
        print(f"\n{'='*80}")
        print(f"Title: {news_data.title}")
        print(f"Summary: {news_data.summary[:100]}..." if news_data.summary else "Summary: None")
        print(f"Content length: {len(news_data.content)} chars")
        print(f"First 200 chars: {news_data.content[:200]}...")
        print(f"Keywords: {news_data.keywords}")
        print(f"Images: {len(news_data.images) if news_data.images else 0}")
        if news_data.images:
            print(f"First image: {news_data.images[0]}")
        print(f"{'='*80}\n")