import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from config import settings
from collectors.isna.page_parser import extract_news_article
from database_manager import DatabaseManager


class ISNAPageCrawler:
    def __init__(self, db_manager: DatabaseManager = None, headless: bool = True):
        self.logger = logging.getLogger(__name__)
        self.db_manager = db_manager or DatabaseManager(
            host=settings.db.host,
            port=settings.db.port,
            db_name=settings.db.name,
            user=settings.db.user,
            password=settings.db.password,
            min_conn=settings.db.min_conn,
            max_conn=settings.db.max_conn
        )
        self.headless = headless
        self.selenium_config = settings.selenium

        self._local = threading.local()  # Thread-local storage for Selenium drivers
        self._db_lock = Lock()  # Optional, in case DB access needs sync

    def _create_driver(self):
        """Create a new Selenium WebDriver instance (Chrome)"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        return webdriver.Chrome(options=chrome_options)

    def _get_driver(self):
        """Thread-safe method to get or create a driver for the current thread"""
        if not hasattr(self._local, 'driver') or self._local.driver is None:
            self._local.driver = self._create_driver()
        return self._local.driver

    def _close_driver(self):
        """Close the driver associated with the current thread"""
        driver = getattr(self._local, 'driver', None)
        if driver:
            driver.quit()
            self._local.driver = None

    def _process_link(self, link_record: dict):
        """Fetch and parse article for one link"""
        link_id = link_record['news_link_id']
        link_url = link_record['link']
        source = link_record['source']

        try:
            driver = self._get_driver()
            driver.get(link_url)
            time.sleep(2)  # Consider WebDriverWait instead if dynamic loading occurs

            article_data = extract_news_article(driver.page_source)

            if not article_data or not article_data["title"]:
                self.logger.warning(f"No article extracted from {link_url}")
                return

            self.db_manager.insert_news_article(
                source=source,
                title=article_data["title"],
                summary=article_data["summary"],
                content=article_data["content"],
                tags=article_data["tags"],
                link_id=link_id,
                published_datetime=link_record["published_datetime"]
            )

            self.db_manager.mark_link_processed(link_id)
            self.logger.info(f"Processed article for link ID {link_id}")

        except Exception as e:
            self.logger.error(f"Error processing link {link_id}: {str(e)}")

    def crawl_unprocessed_links(self, source: str = "ISNA", max_links: int = 20, workers: int = 4):
        """Main crawler logic: fetch unprocessed links and process concurrently"""
        links_to_process = self.db_manager.get_unprocessed_links(source=source, limit=max_links)

        if not links_to_process:
            self.logger.info("No unprocessed links found.")
            return

        self.logger.info(f"Starting to process {len(links_to_process)} links...")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(self._process_link, link) for link in links_to_process]
            for future in as_completed(futures):
                future.result()

        self.logger.info("Finished crawling session.")

    def shutdown(self):
        """Gracefully shut down drivers across threads"""
        self._close_driver()
