import logging
from typing import List, Optional, Dict

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from schema import NewsData, NewsLinkData
from news_publishers import ISNA


class ISNAPageCollector:
    MAIN_CONTENT_SELECTOR = "div.item-body.content-full-news"

    def __init__(self, fetch_timeout: int = 15):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.fetch_timeout = fetch_timeout
        self.logger.info("ISNAPageCollector initialized as a utility class.")

    # --- HTML Fetcher (Same as IRNA) ---
    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetches the HTML content using Playwright (JS-rendered)."""
        self.logger.debug(f"Loading ISNA page with Playwright: {url}")
        page = None

        with sync_playwright() as p:
            browser = None
            try:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=self.fetch_timeout * 1000)

                # Wait until main content appears
                page.wait_for_selector(self.MAIN_CONTENT_SELECTOR, timeout=5000)
                html_content = page.content()
                self.logger.info(f"Successfully fetched ISNA HTML from {url}")
                return html_content

            except PlaywrightTimeoutError:
                self.logger.error(f"Timeout while fetching ISNA page: {url}")
                return None
            except Exception as e:
                self.logger.error(f"Playwright error while fetching ISNA page {url}: {e}", exc_info=True)
                return None
            finally:
                if page:
                    page.close()
                if browser:
                    browser.close()

    # --- Batch Processor ---
    def crawl_batch(self, batch: List[NewsLinkData]) -> Dict[str, NewsData]:
        """Crawl a batch of ISNA links and return {link: NewsData}."""
        results: Dict[str, NewsData] = {}

        for link_data in batch:
            if link_data.source != ISNA:
                continue

            html = self._fetch_html(link_data.link)
            if not html:
                continue

            news_data = self.extract_news(html, link_data)
            if news_data:
                results[link_data.link] = news_data

        return results

    # --- HTML Parser ---
    def extract_news(self, html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Extract structured news data from ISNA HTML."""
        try:
            soup = BeautifulSoup(html, "html.parser")

            # --- Title ---
            title_tag = soup.select_one("h1.first-title[itemprop='headline']")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"

            # --- Summary ---
            summary_tag = soup.select_one("p.summary[itemprop='description']")
            summary = summary_tag.get_text(strip=True) if summary_tag else None

            # --- Images ---
            images = []
            main_img_tag = soup.select_one("figure.item-img img")
            if main_img_tag and main_img_tag.get("src"):
                images.append(main_img_tag["src"])

            # --- Main Content ---
            content_tag = soup.select_one("div.item-text[itemprop='articleBody']")
            content = ""
            if content_tag:
                paragraphs = content_tag.find_all("p")
                content = "\n".join(
                    p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
                )

            # --- Keywords ---
            keywords = [
                tag.get_text(strip=True)
                for tag in soup.select("section.box.trending-tags ul li a")
            ]

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
            self.logger.error(f"Error parsing ISNA HTML for {link_data.link}: {e}", exc_info=True)
            return None
