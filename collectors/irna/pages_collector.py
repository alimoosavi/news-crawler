import logging
from typing import List, Optional, Dict

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from schema import NewsData, NewsLinkData
from news_publishers import IRNA


class IRNAPageCollector:
    MAIN_CONTENT_SELECTOR = "div.item-body"

    def __init__(self, fetch_timeout: int = 15):
        self.logger = logging.getLogger(self.__class__.__name__)
        # No broker_manager, group_id, or batch_size needed here
        self.fetch_timeout = fetch_timeout
        self.logger.info("IRNAPageCollector initialized as a utility class.")

    def _fetch_html(self, url: str) -> Optional[str]:
        """
        Fetches the HTML content using Playwright, allowing JavaScript to execute.
        A new Playwright instance is launched for robustness in this utility model.
        """
        self.logger.debug(f"Loading page with Playwright: {url}")

        page = None
        # We start and stop Playwright's execution context for each call to keep the class clean
        # The scheduler handles threading, which makes starting/stopping safer here.
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()

                # Navigate and wait for content (converted to milliseconds)
                page.goto(url, wait_until="domcontentloaded", timeout=self.fetch_timeout * 1000)

                # Wait for the article body to appear
                page.wait_for_selector(
                    self.MAIN_CONTENT_SELECTOR,
                    timeout=5000
                )

                html_content = page.content()
                self.logger.info(f"Successfully fetched rendered HTML from {url} (final URL: {page.url})")
                return html_content

            except PlaywrightTimeoutError:
                self.logger.error(f"Playwright Timeout during navigation or waiting for selector on {url}")
                return None
            except Exception as e:
                self.logger.error(f"Playwright error while fetching {url}: {e}", exc_info=True)
                return None
            finally:
                if page:
                    page.close()
                if browser:
                    browser.close()

    # --- UNIFIED PUBLIC METHOD ---
    def crawl_batch(self, batch: List[NewsLinkData]) -> Dict[str, NewsData]:
        """
        Crawl a batch of links and return a dictionary mapping link to NewsData.
        """
        results: Dict[str, NewsData] = {}

        for link_data in batch:
            # Note: Source check is technically redundant here since the scheduler dispatches correctly,
            # but it's kept as a safety guard.
            if link_data.source != IRNA:
                continue

            html = self._fetch_html(link_data.link)
            if not html:
                continue

            news_data = self.extract_news(html, link_data)
            if news_data:
                results[link_data.link] = news_data

        return results

    # The extract_news method is renamed and its logic remains UNCHANGED as requested:
    def extract_news(self, html: str, link_data: NewsLinkData) -> Optional[NewsData]:
        """Parse HTML into a NewsData object."""
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Extract title from h1 > a
            title_tag = soup.select_one("h1.title a[itemprop='headline']")
            title = title_tag.get_text(strip=True) if title_tag else "Untitled"

            # Extract main body text
            body_tag = soup.select_one("div.item-body")
            content = ""
            if body_tag:
                paragraphs = body_tag.find_all("p")
                content = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            # Extract summary
            summary_tag = soup.select_one("p.summary.introtext[itemprop='description']")
            summary = summary_tag.get_text(strip=True) if summary_tag else None

            # Extract all images from figure.item-img and inside body
            images = []
            main_img_tag = soup.select_one("figure.item-img img")
            if main_img_tag and main_img_tag.get("src"):
                images.append(main_img_tag["src"])

            # Also add any images inside the body
            body_imgs = body_tag.find_all("img") if body_tag else []
            for img in body_imgs:
                src = img.get("src")
                if src and src not in images:
                    images.append(src)

            # --- New Logic for Keywords ---
            keywords = [
                tag.get_text(strip=True)
                for tag in soup.select("section.tags li a")
            ]

            # --- Updated NewsData Creation ---
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

    # Removed: process_batch, run, cleanup, __enter__, __exit__
