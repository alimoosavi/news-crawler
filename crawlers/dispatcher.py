import threading
import time
import logging
from queue import Queue, Empty
from typing import Dict, List, Optional, Set
from datetime import datetime
from database_manager import DatabaseManager
from config import settings
from news_sources import NewsSourceInterface

logger = logging.getLogger(__name__)

class NewsLinkDispatcher:
    """
    Generic dispatcher that works with any news source
    """
    
    def __init__(self, news_source: NewsSourceInterface, db_manager: DatabaseManager = None):
        self.news_source = news_source  # Injected dependency
        self.db_manager = db_manager or DatabaseManager()
        
        # Laptop-optimized configuration
        self.bulk_size = min(settings.crawler.bulk_size, 20)
        self.max_workers = min(settings.crawler.max_workers, 6)
        self.sleep_interval = settings.crawler.sleep_interval
        
        # Task management
        self.task_queue = Queue(maxsize=self.bulk_size * 2)
        self.result_queue = Queue()
        
        # Lightweight caching
        self.processing_cache: Set[int] = set()
        self.cache_lock = threading.RLock()
        
        # Statistics
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
        
        logger.info(f"Dispatcher initialized for {self.news_source.source_name}: "
                   f"bulk_size={self.bulk_size}, max_workers={self.max_workers}")
    
    def start(self):
        """Start the dispatcher"""
        if self.running:
            logger.warning("Dispatcher is already running")
            return
        
        self.running = True
        
        # Setup database
        if not self.db_manager.connection:
            self.db_manager.connect()
        
        # Start background threads
        self.fetcher_thread = threading.Thread(target=self._fetch_links_loop, daemon=True)
        self.result_processor_thread = threading.Thread(target=self._process_results_loop, daemon=True)
        
        self.fetcher_thread.start()
        self.result_processor_thread.start()
        
        logger.info(f"🚀 Dispatcher started for {self.news_source.source_name}")
    
    def stop(self):
        """Stop the dispatcher"""
        logger.info(f"🛑 Stopping dispatcher for {self.news_source.source_name}...")
        self.running = False
        
        # Wait for threads
        if self.fetcher_thread and self.fetcher_thread.is_alive():
            self.fetcher_thread.join(timeout=5)
        
        if self.result_processor_thread and self.result_processor_thread.is_alive():
            self.result_processor_thread.join(timeout=5)
        
        # Clear cache
        with self.cache_lock:
            self.processing_cache.clear()
        
        logger.info(f"✅ Dispatcher stopped for {self.news_source.source_name}")
    
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
        logger.info(f"🔄 Link fetcher thread started for {self.news_source.source_name}")
        
        while self.running:
            try:
                # Check if we need more tasks
                if self.task_queue.qsize() < self.bulk_size // 2:
                    self._fetch_and_queue_links()
                
                # Sleep to avoid overwhelming the system
                time.sleep(self.sleep_interval)
                
            except Exception as e:
                logger.error(f"Error in fetch loop: {str(e)}")
                time.sleep(5)
        
        logger.info(f"🏁 Link fetcher thread stopped for {self.news_source.source_name}")
    
    def _fetch_and_queue_links(self):
        """Fetch unprocessed links and add to queue"""
        try:
            # Get excluded IDs from cache
            with self.cache_lock:
                excluded_ids = self.processing_cache.copy()
            
            # Fetch links for this specific news source
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
                logger.debug(f"📋 Queued {queued_count} new tasks for {self.news_source.source_name}")
        
        except Exception as e:
            logger.error(f"Error fetching and queuing links: {str(e)}")
    
    def _get_unprocessed_links_excluding(self, excluded_ids: Set[int]) -> List[Dict]:
        """Get unprocessed links for this news source, excluding cached ones"""
        try:
            # Get links specifically for this news source
            all_links = self.db_manager.get_unprocessed_links(
                source=self.news_source.source_name, 
                limit=self.bulk_size
            )
            
            # Filter out excluded IDs
            filtered_links = [link for link in all_links if link['id'] not in excluded_ids]
            
            return filtered_links[:self.bulk_size]
        
        except Exception as e:
            logger.error(f"Error fetching unprocessed links: {str(e)}")
            return []
    
    def _process_results_loop(self):
        """Background thread to process results"""
        logger.info(f"🔄 Result processor thread started for {self.news_source.source_name}")
        
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
        
        logger.info(f"🏁 Result processor thread stopped for {self.news_source.source_name}")
    
    def _handle_worker_result(self, result: Dict):
        """Handle a single result from a worker with Shamsi date support"""
        link_id = result.get('link_id')
        success = result.get('success', False)
        
        try:
            if success:
                # Store the news article with Shamsi date support
                news_data = result.get('news_data', {})
                
                news_id = self.db_manager.insert_news_article(
                    source=self.news_source.source_name,
                    published_date=news_data.get('published_date'),
                    title=news_data.get('title'),
                    summary=news_data.get('summary'),
                    content=news_data.get('content'),
                    tags=news_data.get('tags'),
                    link_id=link_id,
                    published_datetime=news_data.get('published_datetime'),
                    shamsi_year=news_data.get('shamsi_year'),
                    shamsi_month=news_data.get('shamsi_month'),
                    shamsi_day=news_data.get('shamsi_day'),
                    author=news_data.get('author')
                )
                
                # Mark as processed
                self.db_manager.mark_link_processed(link_id)
                
                self.stats['total_completed'] += 1
                
                # Enhanced logging with Shamsi date
                title_preview = news_data.get('title', 'No title')[:50]
                shamsi_info = ""
                if news_data.get('shamsi_year'):
                    shamsi_date = f"{news_data['shamsi_year']}/{news_data['shamsi_month']:02d}/{news_data['shamsi_day']:02d}"
                    shamsi_month_name = news_data.get('shamsi_month_name', '')
                    shamsi_info = f" [{shamsi_date} - {shamsi_month_name}]"
                
                logger.info(f"✅ Processed {self.news_source.source_name} link {link_id}: "
                           f"{title_preview}...{shamsi_info}")
                
            else:
                error = result.get('error', 'Unknown error')
                self.stats['total_failed'] += 1
                logger.error(f"❌ Failed {self.news_source.source_name} link {link_id}: {error}")
        
        except Exception as e:
            logger.error(f"Error handling result for link {link_id}: {str(e)}")
            self.stats['total_failed'] += 1
        
        finally:
            # Remove from processing cache
            with self.cache_lock:
                self.processing_cache.discard(link_id)
                self.stats['cache_size'] = len(self.processing_cache) 