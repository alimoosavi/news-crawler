import threading
import time
import signal
import logging
from typing import List
from crawlers.dispatcher import NewsLinkDispatcher
from crawlers.worker import NewsWorker
from database_manager import DatabaseManager
from config import settings

logger = logging.getLogger(__name__)

class CrawlerOrchestrator:
    """
    Orchestrator optimized for laptop resources
    """
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.dispatcher = None
        self.workers: List[NewsWorker] = []
        self.worker_threads: List[threading.Thread] = []
        
        # Laptop-optimized configuration
        self.max_workers = min(settings.crawler.max_workers, 4)  # Cap at 4 for laptops
        self.running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Orchestrator initialized with {self.max_workers} workers")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}. Shutting down...")
        self.stop()
    
    def start(self):
        """Start the laptop-optimized system"""
        if self.running:
            logger.warning("Orchestrator is already running")
            return
        
        logger.info("🚀 Starting news crawler")
        self.running = True
        
        try:
            # Setup database
            self._setup_database()
            
            # Start dispatcher
            self._start_dispatcher()
            
            # Start workers
            self._start_workers()
            
            # Start lightweight monitoring
            self._start_monitoring()
            
            logger.info("✅ Crawler started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start orchestrator: {str(e)}")
            self.stop()
            raise
    
    def stop(self):
        """Stop the system"""
        if not self.running:
            return
        
        logger.info("🛑 Stopping crawler...")
        self.running = False
        
        # Stop workers
        self._stop_workers()
        
        # Stop dispatcher
        self._stop_dispatcher()
        
        # Cleanup database
        self._cleanup_database()
        
        logger.info("✅ Crawler stopped")
    
    def _setup_database(self):
        """Setup database connection"""
        try:
            if not self.db_manager.connection:
                self.db_manager.connect()
            self.db_manager.create_tables_if_not_exist()
            logger.info("📊 Database setup completed")
        except Exception as e:
            logger.error(f"Database setup failed: {str(e)}")
            raise
    
    def _start_dispatcher(self):
        """Start the dispatcher"""
        try:
            self.dispatcher = NewsLinkDispatcher(self.db_manager)
            self.dispatcher.start()
            logger.info("📡 Dispatcher started")
        except Exception as e:
            logger.error(f"Failed to start dispatcher: {str(e)}")
            raise
    
    def _start_workers(self):
        """Start worker threads"""
        try:
            for worker_id in range(self.max_workers):
                # Create worker
                worker = NewsWorker(worker_id, self.dispatcher)
                self.workers.append(worker)
                
                # Create worker thread
                worker_thread = threading.Thread(
                    target=worker.start,
                    name=f"Worker-{worker_id}",
                    daemon=True
                )
                self.worker_threads.append(worker_thread)
                worker_thread.start()
                
                logger.info(f"👷 Worker {worker_id} started")
                
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
        """Start lightweight monitoring"""
        monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            name="Monitor",
            daemon=True
        )
        monitoring_thread.start()
        logger.info("📈 Monitoring started")
    
    def _monitoring_loop(self):
        """Simple monitoring loop"""
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
        """Log simple statistics"""
        try:
            db_stats = self.db_manager.get_processing_statistics()
            
            logger.info("📊 System Status:")
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
                'active_workers': sum(1 for w in self.workers if w.running)
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