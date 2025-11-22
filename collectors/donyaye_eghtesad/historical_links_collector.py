import logging
import multiprocessing as mp
from datetime import date as dt_date, timedelta
from typing import List, Tuple

from database_manager import DatabaseManager
from schema import NewsLinkData
from .daily_links_collector import DonyaEqtesadDailyLinksCollector


class DonyaEqtesadHistoricalLinksCollector:
    """
    Manages the historical crawl across a range of Gregorian dates for Donya-e-Eqtesad.
    Uses the daily collector to fetch the sitemap URL for each day in the range.
    """

    def __init__(self, db_manager: DatabaseManager, batch_size: int = 10, workers: int = 4):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.batch_size = batch_size
        self.workers = workers
        self.db_manager = db_manager

    @staticmethod
    def _crawl_single_day(g_date: dt_date) -> Tuple[str, List[NewsLinkData]]:
        try:
            collector = DonyaEqtesadDailyLinksCollector(broker_manager=None)
            daily_sitemap_url = collector._get_daily_sitemap_url(g_date)
            if not daily_sitemap_url:
                return str(g_date), []

            links = collector._get_news_links_from_sitemap(daily_sitemap_url)
            return str(g_date), links
        except Exception as e:
            logging.error(f"Error crawling Donya-e-Eqtesad {g_date}: {e}", exc_info=True)
            return str(g_date), []

    def collect_range(self, start_date: dt_date, end_date: dt_date):
        if start_date > end_date:
            self.logger.error("Start date cannot be after end date.")
            return

        self.logger.info(f"Donya-e-Eqtesad Historical Crawl {start_date} → {end_date}")

        current_date = start_date
        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=self.batch_size - 1), end_date)
            batch_dates = [
                current_date + timedelta(days=i)
                for i in range((batch_end - current_date).days + 1)
            ]

            with mp.Pool(processes=self.workers) as pool:
                results = pool.map(self._crawl_single_day, batch_dates)

            for date_str, links in results:
                if links:
                    self.db_manager.insert_new_links(links)
                self.logger.info(f"Day {date_str}: {len(links)} links persisted.")

            current_date = batch_end + timedelta(days=1)

        self.logger.info("Donya-e-Eqtesad historical crawl completed.")
