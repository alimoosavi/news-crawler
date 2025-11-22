import logging
from datetime import date as dt_date, timedelta
from typing import List, Tuple
import jdatetime
import multiprocessing as mp

from schema import NewsLinkData
from .daily_links_collector import IRNADailyLinkCollector
from database_manager import DatabaseManager


class IRNAHistoricalLinksCollector:
    """
    Manages the historical crawl across a range of Gregorian dates (miladi).
    Converts each date internally to Shamsi for IRNA's daily collector.
    """

    def __init__(self, db_manager: DatabaseManager, batch_size: int = 10, workers: int = 4):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.batch_size = batch_size
        self.workers = workers
        self.db_manager = db_manager

    @staticmethod
    def _crawl_single_day(g_date: dt_date) -> Tuple[str, List[NewsLinkData]]:
        """
        Crawl a single Gregorian date.
        Internally converts to Jalali for IRNADailyLinkCollector.
        """
        try:
            shamsi_date = jdatetime.date.fromgregorian(date=g_date)
            collector = IRNADailyLinkCollector(
                year=shamsi_date.year,
                month=shamsi_date.month,
                day=shamsi_date.day
            )
            links = collector.collect_links()
            return str(g_date), links  # return Gregorian date for consistency
        except Exception as e:
            logging.error(f"Error crawling {g_date}: {e}")
            return str(g_date), []

    def collect_range(self, start_date: dt_date, end_date: dt_date):
        """
        Collects and persists all news links between start and end Gregorian dates (inclusive).
        Uses multiprocessing to crawl batches of days concurrently.
        """
        if start_date > end_date:
            self.logger.error("Start date cannot be after end date.")
            return

        self.logger.info(f"Starting IRNA historical crawl from {start_date} to {end_date} (Gregorian).")

        current_date = start_date
        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=self.batch_size - 1), end_date)

            self.logger.info(f"\n--- Processing batch {current_date} → {batch_end} ---")

            # Prepare batch dates
            batch_dates = [
                current_date + timedelta(days=i)
                for i in range((batch_end - current_date).days + 1)
            ]

            # Run multiprocessing pool
            with mp.Pool(processes=self.workers) as pool:
                results = pool.map(self._crawl_single_day, batch_dates)

            # Persist batch results immediately
            for date_str, links in results:
                if links:
                    self.db_manager.insert_new_links(links)
                self.logger.info(f"Day {date_str} finished. Collected {len(links)} links.")

            # Move to next batch
            current_date = batch_end + timedelta(days=1)

        self.logger.info("\nIRNA historical range crawl completed.")
