import threading
import time
import signal
import logging
from typing import List
from collectors.page_crawler.dispatcher import NewsLinkDispatcher
from collectors.page_crawler.worker import NewsWorker
from database_manager import DatabaseManager
from config import settings
from news_sources import NewsSourceInterface

logger = logging.getLogger(__name__)


class PageCrawlerOrchestrator:
    """
    Generic orchestrator that works with any news source
    """

    def __init__(self, news_source: NewsSourceInterface, db_manager: DatabaseManager):
        self.news_source = news_source  # Injected dependency
        self.db_manager = db_manager
        self.dispatcher = None
        self.workers: List[NewsWorker] = []
        self.worker_threads: List[threading.Thread] = []

        # Laptop-optimized configuration
        self.max_workers = min(settings.crawler.max_workers, 4)
        self.running = False

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(f"Orchestrator initialized for {self.news_source.source_name} "
                    f"with {self.max_workers} workers")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}. Shutting down...")
        self.stop()

    def start(self):
        """Start the system"""
        if self.running:
            logger.warning("Orchestrator is already running")
            return

        logger.info(f"🚀 Starting {self.news_source.source_name} crawler")
        self.running = True

        try:

            # Start dispatcher
            self._start_dispatcher()

            # Start workers
            self._start_workers()

            # Start monitoring
            self._start_monitoring()

            logger.info(f"✅ {self.news_source.source_name} crawler started successfully")

        except Exception as e:
            logger.error(f"Failed to start orchestrator: {str(e)}")
            self.stop()
            raise

    def stop(self):
        """Stop the system"""
        if not self.running:
            return

        logger.info(f"🛑 Stopping {self.news_source.source_name} crawler...")
        self.running = False

        # Stop workers
        self._stop_workers()

        # Stop dispatcher
        self._stop_dispatcher()

        # Cleanup database
        self._cleanup_database()

        logger.info(f"✅ {self.news_source.source_name} crawler stopped")

    def _start_dispatcher(self):
        """Start the dispatcher with news source injection"""
        try:
            self.dispatcher = NewsLinkDispatcher(self.news_source, self.db_manager)
            self.dispatcher.start()
            logger.info(f"📡 Dispatcher started for {self.news_source.source_name}")
        except Exception as e:
            logger.error(f"Failed to start dispatcher: {str(e)}")
            raise

    def _start_workers(self):
        """Start worker threads with news source injection"""
        try:
            for worker_id in range(self.max_workers):
                # Create worker with injected news source
                worker = NewsWorker(worker_id, self.dispatcher, self.news_source)
                self.workers.append(worker)

                # Create worker thread
                worker_thread = threading.Thread(
                    target=worker.start,
                    name=f"Worker-{worker_id}-{self.news_source.source_name}",
                    daemon=True
                )
                self.worker_threads.append(worker_thread)
                worker_thread.start()

                logger.info(f"👷 Worker {worker_id} started for {self.news_source.source_name}")

                # Small delay to avoid resource spikes
                time.sleep(1)

        except Exception as e:
            logger.error(f"Failed to start workers: {str(e)}")
            raise

    def _stop_workers(self):
        """Stop all workers"""
        logger.info("Stopping workers...")

        for worker in self.workers:
            worker.stop()

        for thread in self.worker_threads:
            if thread.is_alive():
                thread.join(timeout=10)

        self.workers.clear()
        self.worker_threads.clear()
        logger.info("All workers stopped")

    def _stop_dispatcher(self):
        """Stop the dispatcher"""
        if self.dispatcher:
            self.dispatcher.stop()
            self.dispatcher = None
            logger.info("📡 Dispatcher stopped")

    def _cleanup_database(self):
        """Cleanup database"""
        if self.db_manager:
            self.db_manager.close()
            logger.info("📊 Database closed")

    def _start_monitoring(self):
        """Start monitoring"""
        monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            name=f"Monitor-{self.news_source.source_name}",
            daemon=True
        )
        monitoring_thread.start()
        logger.info(f"📈 Monitoring started for {self.news_source.source_name}")

    def _monitoring_loop(self):
        """Monitoring loop"""
        while self.running:
            try:
                time.sleep(120)  # Every 2 minutes

                if not self.running:
                    break

                # Get and log stats
                stats = self.get_system_stats()
                self._log_stats(stats)

            except Exception as e:
                logger.error(f"Error in monitoring: {str(e)}")

    def _log_stats(self, stats: dict):
        """Log statistics"""
        try:
            db_stats = self.db_manager.get_processing_statistics()

            logger.info(f"📊 {self.news_source.source_name} System Status:")
            logger.info(f"   Workers: {stats['system']['active_workers']}/{stats['system']['workers_count']}")
            logger.info(f"   Database: {db_stats['unprocessed_links']} unprocessed, "
                        f"{db_stats['total_articles']} articles")

            if stats['dispatcher']:
                d_stats = stats['dispatcher']
                logger.info(f"   Progress: {d_stats['total_completed']} completed, "
                            f"{d_stats['total_failed']} failed")

        except Exception as e:
            logger.error(f"Error logging stats: {str(e)}")

    def get_system_stats(self) -> dict:
        """Get system statistics"""
        stats = {
            'system': {
                'running': self.running,
                'workers_count': len(self.workers),
                'active_workers': sum(1 for w in self.workers if w.running),
                'source': self.news_source.source_name
            },
            'dispatcher': {},
            'workers': []
        }

        if self.dispatcher:
            stats['dispatcher'] = self.dispatcher.get_stats()

        for worker in self.workers:
            stats['workers'].append({
                'id': worker.worker_id,
                'running': worker.running,
                'stats': worker.get_stats()
            })

        return stats
