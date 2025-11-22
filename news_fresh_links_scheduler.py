import logging
import time
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.background import BackgroundScheduler
from prometheus_client import Gauge, Counter, Summary, start_http_server, Info

from broker_manager import BrokerManager
from cache_manager import CacheManager
from config import settings
from collectors.donyaye_eghtesad.daily_links_collector import DonyaEqtesadDailyLinksCollector
from collectors.irna.fresh_links_collector import IRNAFreshLinksCollector
from schema import LinksCollectingMetrics
from news_publishers import IRNA, DONYAYE_EQTESAD

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
# General info about the service
SERVICE_INFO = Info(
    'news_links_crawler_info',
    'General information about the news links crawler service'
)

# Gauges
LAST_CRAWL_TIMESTAMP = Gauge(
    'crawler_last_crawl_timestamp_seconds',
    'Timestamp of the last successful crawl for a source',
    ['source']
)
CRAWL_ACTIVE = Gauge(
    'crawler_active',
    'Indicates if a crawl job is currently running (1 for active, 0 for inactive)',
    ['source']
)

# Counters
LINKS_SCRAPED = Counter(
    'crawler_links_scraped_total',
    'Total number of links scraped from a source',
    ['source']
)
LINKS_PRODUCED = Counter(
    'crawler_links_produced_total',
    'Total number of unique links produced to the Kafka topic',
    ['source']
)
CRAWL_ERRORS = Counter(
    'crawler_errors_total',
    'Total number of errors encountered during crawling',
    ['source', 'error_type']
)

# Summaries
CRAWL_DURATION_SECONDS = Summary(
    'crawler_scrape_duration_seconds',
    'Duration of a full crawl cycle for a source',
    ['source']
)

# -------------------------------
# News sources and collectors
# -------------------------------
NEWS_SOURCES = [
    IRNA,
    DONYAYE_EQTESAD
]

LINK_CRAWLERS = {
    IRNA: IRNAFreshLinksCollector,
    DONYAYE_EQTESAD: DonyaEqtesadDailyLinksCollector,
}


# -------------------------------
# Cache Manager
# -------------------------------
def get_cache_manager() -> CacheManager:
    return CacheManager()


# -------------------------------
# Crawl links for a single source
# -------------------------------
def crawl_links_for_source(source: str, broker_manager: BrokerManager, cache_manager: CacheManager):
    """
    Run the link crawler for a single source, pushing results to Kafka.
    """
    CRAWL_ACTIVE.labels(source=source).set(1)

    with CRAWL_DURATION_SECONDS.labels(source=source).time():
        try:
            logger.info(f"[{source}] Starting link crawl")
            last_link = cache_manager.get_last_link(source)

            # Inject broker manager into crawler
            crawler = LINK_CRAWLERS[source](broker_manager)

            # EXPECT LinksCrawlingMetrics object
            metrics: LinksCollectingMetrics = crawler.crawl_recent_links(last_link)

            # Update metrics based on the crawler's output
            if metrics.links_scraped_count > 0:
                num_scraped = metrics.links_scraped_count

                LINKS_SCRAPED.labels(source=source).inc(num_scraped)

                # The crawler's internal logic should handle Kafka production
                # and you would ideally increment LINKS_PRODUCED there.
                # For this example, we assume all scraped links are produced.
                LINKS_PRODUCED.labels(source=source).inc(num_scraped)

                fresh_last_link = metrics.latest_link
                if fresh_last_link:
                    logger.info(f"[{source}] Last crawled link: {fresh_last_link}")
                    cache_manager.update_last_link(source, fresh_last_link)
                    LAST_CRAWL_TIMESTAMP.labels(source=source).set(time.time())

            logger.info(
                f"[{source}] Link crawl finished. Scraped: {metrics.links_scraped_count}, New Last Link: {metrics.latest_link}")

        except Exception as e:
            logger.exception(f"[{source}] Link crawl failed: {e}")
            CRAWL_ERRORS.labels(source=source, error_type="crawl_error").inc()
        finally:
            CRAWL_ACTIVE.labels(source=source).set(0)


# -------------------------------
# Schedule crawling
# -------------------------------
def schedule_news_links(broker_manager: BrokerManager):
    """
    Schedule crawling for all configured news sources using a thread pool.
    """
    cache_manager = get_cache_manager()
    with ThreadPoolExecutor(max_workers=len(NEWS_SOURCES)) as executor:
        for source in NEWS_SOURCES:
            executor.submit(crawl_links_for_source, source, broker_manager, cache_manager)


# -------------------------------
# Main
# -------------------------------
def main():
    # Start Prometheus metrics server
    try:
        start_http_server(settings.prom.port)
        logger.info(f"Prometheus metrics server started on port {settings.prom.port}")
    except OSError as e:
        logger.warning(f"Failed to start Prometheus server, port {settings.prom.port} might be in use: {e}")

    # Set service info
    SERVICE_INFO.info({
        'version': '1.0.0',
        'pipeline_stage': 'news_links_crawler',
        'sources': ','.join(NEWS_SOURCES)
    })

    # Initialize broker manager and create topics if missing
    with BrokerManager(settings.redpanda, logger) as broker_manager:
        broker_manager.create_topics()

        # Start scheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(schedule_news_links, 'interval', seconds=15, args=[broker_manager])
        scheduler.start()
        logger.info("Scheduler started for news links")

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            logger.info("Scheduler shut down gracefully")


if __name__ == "__main__":
    main()
