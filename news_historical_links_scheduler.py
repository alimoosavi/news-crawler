import logging
from datetime import date as dt_date, timedelta
from prometheus_client import Summary

from collectors.irna.historical_links_collector import IRNAHistoricalLinksCollector
from collectors.tasnim.historical_links_collector import TasnimHistoricalLinksCollector
from collectors.donyaye_eghtesad.historical_links_collector import DonyaEqtesadHistoricalLinksCollector
from collectors.isna.historical_links_collector import ISNAHistoricalLinksCollector
from config import settings
from database_manager import DatabaseManager
from news_publishers import IRNA, TASNIM, ISNA, DONYAYE_EQTESAD

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=settings.app.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("LinksScheduler")

# -------------------------------
# Prometheus Metrics
# -------------------------------
CRAWL_DURATION_SECONDS = Summary(
    'crawler_scrape_duration_seconds',
    'Duration of a full crawl cycle for a source',
    ['source']
)

# Map each publisher to its historical range collector class
HISTORICAL_RANGE_LINKS_COLLECTOR = {
    IRNA: IRNAHistoricalLinksCollector,
    TASNIM: TasnimHistoricalLinksCollector,
    DONYAYE_EQTESAD: DonyaEqtesadHistoricalLinksCollector,
    ISNA: ISNAHistoricalLinksCollector
}


def get_db_manager():
    return DatabaseManager(db_config=settings.database)


def main():
    database_manager = get_db_manager()

    today_greg = dt_date.today()
    start_date_greg = today_greg - timedelta(days=60)
    end_date_greg = today_greg

    print("\n" + "=" * 80)
    print("HISTORICAL LINKS CRAWLER DEMO")
    print(f"Target Gregorian Range: {start_date_greg} to {end_date_greg}")
    print("=" * 80)

    try:
        for source, CollectorClass in HISTORICAL_RANGE_LINKS_COLLECTOR.items():
            print("\n" + "-" * 60)
            print(f"🔎 Starting crawl for source: {source}")
            print("-" * 60)

            # Track duration with Prometheus metric
            with CRAWL_DURATION_SECONDS.labels(source=source).time():
                collector = CollectorClass(db_manager=database_manager)
                collector.collect_range(start_date=start_date_greg, end_date=end_date_greg)

            print(f"✅ Finished crawl for {source}")

        print("\n" + "#" * 50)
        print("SUMMARY: Historical crawl executed for all sources, results persisted directly in DB.")
        print("#" * 50)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main execution: {e}")


if __name__ == "__main__":
    main()
