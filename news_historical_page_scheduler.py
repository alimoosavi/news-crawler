import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

from prometheus_client import Summary

from collectors.irna.pages_collector import IRNAPageCollector
from collectors.tasnim.pages_collector import TasnimPageCollector
from collectors.donyaye_eghtesad.pages_collector import DonyaEqtesadPageCollector
from collectors.isna.pages_collector import ISNAPageCollector
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
logger = logging.getLogger("NewsScheduler")

# -------------------------------
# Prometheus Metrics
# -------------------------------
CRAWL_DURATION_SECONDS = Summary(
    "crawler_scrape_duration_seconds",
    "Duration of a full crawl cycle for a source",
    ["source"],
)


class NewsScheduler:
    """
    Fully concurrent scheduler:
    - Fetches pending links from DB
    - Processes multiple sources in parallel
    - Crawls links concurrently per source
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        batch_size: int = 10,
        poll_interval: int = 60,
        max_workers_per_source: int = 5,
    ):
        self.db_manager = db_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.max_workers_per_source = max_workers_per_source

        # Map publisher → collector
        self.collectors: Dict[str, object] = {
            IRNA: IRNAPageCollector(),
            ISNA: ISNAPageCollector(),
            # TASNIM: TasnimPageCollector(),
            DONYAYE_EQTESAD: DonyaEqtesadPageCollector(),

        }

    # ---------------------------------------------
    # Crawl a batch of links concurrently per source
    # ---------------------------------------------
    def crawl_batch_concurrent(
        self, collector, link_batch: List[NewsLinkData]
    ) -> Dict[str, NewsData]:
        results: Dict[str, NewsData] = {}

        def fetch_single(link_data: NewsLinkData) -> Tuple[str, NewsData]:
            try:
                html = collector._fetch_html(link_data.link)
                if not html:
                    return link_data.link, None
                return link_data.link, collector.extract_news(html, link_data)
            except Exception as e:
                logger.error(f"Error crawling {link_data.link}: {e}", exc_info=True)
                return link_data.link, None

        with ThreadPoolExecutor(max_workers=self.max_workers_per_source) as executor:
            future_to_link = {executor.submit(fetch_single, link): link for link in link_batch}
            for future in as_completed(future_to_link):
                link, news_data = future.result()
                if news_data:
                    results[link] = news_data

        return results

    # ---------------------------------------------
    # Process a single source concurrently
    # ---------------------------------------------
    def process_source(self, source: str, collector) -> int:
        links = self.db_manager.get_pending_links_by_source(
            source=source, limit=self.batch_size
        )
        if not links:
            logger.info(f"No pending links for {source}")
            return 0

        link_batch = [
            NewsLinkData(source=l.source, link=l.link, published_datetime=l.published_datetime)
            for l in links
        ]

        # Crawl batch concurrently
        with CRAWL_DURATION_SECONDS.labels(source=source).time():
            results = self.crawl_batch_concurrent(collector, link_batch)

        logger.info(f"Crawled {len(results)} news items for {source}")

        if results:
            # Persist news data
            inserted_count = self.db_manager.insert_news_batch(list(results.values()))
            logger.info(f"Persisted {inserted_count} news items for {source}")

            # Mark links as completed
            processed_links = [l.link for l in link_batch]
            self.db_manager.mark_links_completed(processed_links)
            logger.info(f"Marked {len(processed_links)} links as completed for {source}")

        return len(results)

    # ---------------------------------------------
    # Run one cycle: multiple sources concurrently
    # ---------------------------------------------
    def run_cycle(self) -> int:
        total_crawled = 0
        logger.info("Starting concurrent scheduler cycle...")

        with ThreadPoolExecutor(max_workers=len(self.collectors)) as executor:
            future_to_source = {
                executor.submit(self.process_source, source, collector): source
                for source, collector in self.collectors.items()
            }
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    count = future.result()
                    total_crawled += count
                except Exception as e:
                    logger.error(f"Error processing source {source}: {e}", exc_info=True)

        logger.info(f"Cycle complete. Total crawled: {total_crawled}")
        return total_crawled

    # ---------------------------------------------
    # Run scheduler continuously
    # ---------------------------------------------
    def run_forever(self):
        logger.info("Scheduler started in continuous mode...")
        while True:
            crawled = self.run_cycle()
            if crawled == 0:
                logger.info(f"No new links found. Sleeping for {self.poll_interval}s...")
                time.sleep(self.poll_interval)


# ---------------------------------------------
# Entry point
# ---------------------------------------------
def main():
    db_manager = DatabaseManager(db_config=settings.database)
    scheduler = NewsScheduler(
        db_manager=db_manager,
        batch_size=20,
        poll_interval=30,
        max_workers_per_source=5
    )
    scheduler.run_forever()


if __name__ == "__main__":
    main()
