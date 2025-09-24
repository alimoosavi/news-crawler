import logging
import time
from typing import List, Optional

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options

from broker_manager import BrokerManager
from config import settings
from schema import NewsData, NewsLinkData


class IRNAPageCrawler:
    SOURCE_NAME = "IRNA"

    def __init__(self, broker_manager: BrokerManager, batch_size: int = 5, fetch_timeout: int = 15):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._broker_manager = broker_manager
        self.batch_size = batch_size
        self.fetch_timeout = fetch_timeout

        # Initialize Selenium WebDriver
        self.driver = self._create_webdriver()
        self.logger.info("IRNAPageCrawler initialized.")

    def _create_webdriver(self) -> webdriver.Remote:
        """Create a Selenium Remote WebDriver connected to docker-compose service."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        try:
            driver = webdriver.Remote(
                command_executor=settings.selenium.hub_url,
                options=chrome_options
            )
            driver.set_page_load_timeout(self.fetch_timeout)
            self.logger.info(f"Connected to Selenium WebDriver at {settings.selenium.hub_url}")
            return driver
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Selenium WebDriver: {e}", exc_info=True)
            raise

    def fetch_page(self, url: str) -> Optional[str]:
        """Load page with Selenium and return HTML."""
        try:
            self.logger.debug(f"Loading page: {url}")
            self.driver.get(url)
            time.sleep(5)  # allow dynamic content to load
            return self.driver.page_source
        except TimeoutException:
            self.logger.error(f"Timeout while loading {url}", exc_info=True)
            return None
        except WebDriverException as e:
            self.logger.error(f"WebDriver error while fetching {url}: {e}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {url}: {e}", exc_info=True)
            return None

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
                images=images if images else None,
                summary=summary,
            )
        except Exception as e:
            self.logger.error(f"Error parsing HTML for {link_data.link}: {e}", exc_info=True)
            return None

    def process_batch(self, batch: List[NewsLinkData]):
        """Fetch and parse all links in a batch, then produce results."""
        results = []
        for link_data in batch:
            if link_data.source != self.SOURCE_NAME:
                continue  # skip other sources

            html = self.fetch_page(link_data.link)
            if not html:
                continue

            news_data = self.extract_news(html, link_data)
            if news_data:
                results.append(news_data)

        if results:
            self._broker_manager.produce_content(results)
            self.logger.info(f"Produced {len(results)} IRNA news contents")

    def run(self):
        """Continuously consume from Kafka, crawl, and produce results."""
        topic = self._broker_manager.config.news_links_topic
        group_id = "page_crawlers"  # all workers in same consumer group

        self.logger.info(f"IRNA Page Crawler listening on '{topic}' with group '{group_id}'...")

        try:
            for batch in self._broker_manager.consume_batch(
                    topic,
                    NewsLinkData,
                    batch_size=self.batch_size,
                    group_id=group_id
            ):
                if not batch:
                    time.sleep(1)
                    continue

                self.logger.info(f"Received {len(batch)} links")
                self.process_batch(batch)

                # commit after successful processing
                self._broker_manager.commit_offsets()

        except KeyboardInterrupt:
            self.logger.info("IRNA Page Crawler stopped manually")
        except Exception as e:
            self.logger.exception(f"Unexpected error in crawler: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Ensure WebDriver is closed on exit."""
        if hasattr(self, "driver") and self.driver:
            self.logger.debug("Closing Selenium WebDriver")
            self.driver.quit()
            self.logger.info("Selenium WebDriver closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()