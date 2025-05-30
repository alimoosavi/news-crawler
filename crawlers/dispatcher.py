import threading
import time
import logging
from queue import Queue, Empty
from typing import Dict, List, Optional, Set
from datetime import datetime
from database_manager import DatabaseManager
from config import settings

logger = logging.getLogger(__name__)

class NewsLinkDispatcher:
    """
    Dispatcher optimized for laptop resources
    """
    
    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        
        # Laptop-optimized configuration
        self.bulk_size = min(settings.crawler.bulk_size, 20)  # Cap at 20 for laptops
        self.max_workers = min(settings.crawler.max_workers, 6)  # Cap at 6 workers
        self.sleep_interval = settings.crawler.sleep_interval
        
        # Simple task management
        self.task_queue = Queue(maxsize=self.bulk_size * 2)
        self.result_queue = Queue()
        
        # Lightweight caching
        self.processing_cache: Set[int] = set()
        self.cache_lock = threading.RLock()
        
        # Simple statistics
        self.stats = {
            'total_dispatched': 0,
            'total_completed': 0,
            'total_failed': 0,
            'cache_size': 0,
            'queue_size': 0
        }
        
        # State management
        self.running = False
        self.fetcher_thread = None
        self.result_processor_thread = None
        
        logger.info(f"Dispatcher initialized: bulk_size={self.bulk_size}, max_workers={self.max_workers}")
    
    def start(self):
        """Start the lightweight dispatcher"""
        if self.running:
            logger.warning("Dispatcher is already running")
            return
        
        self.running = True
        
        # Setup database
        if not self.db_manager.connection:
            self.db_manager.connect()
        
        # Start lightweight background threads
        self.fetcher_thread = threading.Thread(target=self._fetch_links_loop, daemon=True)
        self.result_processor_thread = threading.Thread(target=self._process_results_loop, daemon=True)
        
        self.fetcher_thread.start()
        self.result_processor_thread.start()
        
        logger.info("🚀 Dispatcher started")
    
    def stop(self):
        """Stop the dispatcher"""
        logger.info("🛑 Stopping dispatcher...")
        self.running = False
        
        # Wait for threads
        if self.fetcher_thread and self.fetcher_thread.is_alive():
            self.fetcher_thread.join(timeout=5)
        
        if self.result_processor_thread and self.result_processor_thread.is_alive():
            self.result_processor_thread.join(timeout=5)
        
        # Clear cache
        with self.cache_lock:
            self.processing_cache.clear()
        
        logger.info("✅ Dispatcher stopped")
    
    def get_task(self, timeout: float = 2.0) -> Optional[Dict]:
        """Get a task for a worker"""
        try:
            task = self.task_queue.get(timeout=timeout)
            
            # Add to processing cache
            with self.cache_lock:
                self.processing_cache.add(task['id'])
                self.stats['cache_size'] = len(self.processing_cache)
            
            self.stats['total_dispatched'] += 1
            return task
            
        except Empty:
            return None
    
    def submit_result(self, result: Dict):
        """Submit a processing result"""
        self.result_queue.put(result)
    
    def get_stats(self) -> Dict:
        """Get current statistics"""
        with self.cache_lock:
            self.stats['cache_size'] = len(self.processing_cache)
            self.stats['queue_size'] = self.task_queue.qsize()
        return self.stats.copy()
    
    def _fetch_links_loop(self):
        """Background thread to fetch and queue links"""
        logger.info("🔄 Link fetcher thread started")
        
        while self.running:
            try:
                # Check if we need more tasks
                if self.task_queue.qsize() < self.bulk_size // 2:
                    self._fetch_and_queue_links()
                
                # Sleep to avoid overwhelming the system
                time.sleep(self.sleep_interval)
                
            except Exception as e:
                logger.error(f"Error in fetch loop: {str(e)}")
                time.sleep(5)  # Longer sleep on error
        
        logger.info("🏁 Link fetcher thread stopped")
    
    def _fetch_and_queue_links(self):
        """Fetch unprocessed links and add to queue"""
        try:
            # Get excluded IDs from cache
            with self.cache_lock:
                excluded_ids = self.processing_cache.copy()
            
            # Fetch links excluding those being processed
            links = self._get_unprocessed_links_excluding(excluded_ids)
            
            # Queue the links
            queued_count = 0
            for link in links:
                if not self.running:
                    break
                
                try:
                    self.task_queue.put_nowait(link)
                    queued_count += 1
                except:
                    break  # Queue is full
            
            if queued_count > 0:
                logger.debug(f"📋 Queued {queued_count} new tasks")
        
        except Exception as e:
            logger.error(f"Error fetching and queuing links: {str(e)}")
    
    def _get_unprocessed_links_excluding(self, excluded_ids: Set[int]) -> List[Dict]:
        """Get unprocessed links excluding cached ones"""
        try:
            all_links = self.db_manager.get_unprocessed_links(source='ISNA', limit=self.bulk_size)
            
            # Filter out excluded IDs
            filtered_links = [link for link in all_links if link['id'] not in excluded_ids]
            
            return filtered_links[:self.bulk_size]
        
        except Exception as e:
            logger.error(f"Error fetching unprocessed links: {str(e)}")
            return []
    
    def _process_results_loop(self):
        """Background thread to process results"""
        logger.info("🔄 Result processor thread started")
        
        while self.running:
            try:
                result = self.result_queue.get(timeout=1.0)
                self._handle_worker_result(result)
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in result processor: {str(e)}")
        
        # Process remaining results
        while not self.result_queue.empty():
            try:
                result = self.result_queue.get_nowait()
                self._handle_worker_result(result)
            except Empty:
                break
        
        logger.info("🏁 Result processor thread stopped")
    
    def _handle_worker_result(self, result: Dict):
        """Handle a single result from a worker"""
        link_id = result.get('link_id')
        success = result.get('success', False)
        
        try:
            if success:
                # Store the news article
                news_data = result.get('news_data', {})
                
                news_id = self.db_manager.insert_news_article(
                    source='ISNA',
                    published_date=news_data.get('published_date'),
                    title=news_data.get('title'),
                    summary=news_data.get('summary'),
                    content=news_data.get('content'),
                    tags=news_data.get('tags'),
                    link_id=link_id
                )
                
                # Mark as processed
                self.db_manager.mark_link_processed(link_id)
                
                self.stats['total_completed'] += 1
                logger.info(f"✅ Processed link {link_id}: {news_data.get('title', 'No title')[:50]}...")
                
            else:
                error = result.get('error', 'Unknown error')
                self.stats['total_failed'] += 1
                logger.error(f"❌ Failed link {link_id}: {error}")
        
        except Exception as e:
            logger.error(f"Error handling result for link {link_id}: {str(e)}")
            self.stats['total_failed'] += 1
        
        finally:
            # Remove from processing cache
            with self.cache_lock:
                self.processing_cache.discard(link_id)
                self.stats['cache_size'] = len(self.processing_cache) 