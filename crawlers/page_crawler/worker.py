import threading
import time
import logging
from typing import Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from config import settings
from news_sources import NewsSourceInterface

logger = logging.getLogger(__name__)

class NewsWorker:
    """
    Generic news worker that can work with any news source
    """
    
    def __init__(self, worker_id: int, dispatcher, news_source: NewsSourceInterface):
        self.worker_id = worker_id
        self.dispatcher = dispatcher
        self.news_source = news_source  # Injected dependency
        self.selenium_config = settings.selenium
        
        # Worker state
        self.running = False
        self.driver = None
        self.stats = {
            'tasks_processed': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None
        }
        
        logger.info(f"Worker {self.worker_id} initialized for {self.news_source.source_name}")
    
    def start(self):
        """Start the worker"""
        if self.running:
            logger.warning(f"Worker {self.worker_id} is already running")
            return
        
        self.running = True
        self.stats['start_time'] = time.time()
        
        # Initialize Selenium driver
        self._setup_driver()
        
        # Start processing loop
        self._process_loop()
    
    def stop(self):
        """Stop the worker"""
        logger.info(f"🛑 Stopping worker {self.worker_id}...")
        self.running = False
        
        # Cleanup driver
        self._cleanup_driver()
        
        logger.info(f"✅ Worker {self.worker_id} stopped")
    
    def get_stats(self) -> Dict:
        """Get worker statistics"""
        stats = self.stats.copy()
        if stats['start_time']:
            stats['uptime'] = time.time() - stats['start_time']
        return stats
    
    def _setup_driver(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Connect to remote Chrome instance
            self.driver = webdriver.Remote(
                command_executor=self.selenium_config.hub_url,
                options=chrome_options
            )
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info(f"🌐 Worker {self.worker_id} driver initialized")
            
        except Exception as e:
            logger.error(f"Failed to setup driver for worker {self.worker_id}: {str(e)}")
            raise
    
    def _cleanup_driver(self):
        """Cleanup Selenium WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                logger.debug(f"Driver cleaned up for worker {self.worker_id}")
            except Exception as e:
                logger.warning(f"Error cleaning up driver for worker {self.worker_id}: {str(e)}")
    
    def _process_loop(self):
        """Main processing loop for the worker"""
        logger.info(f"🔄 Worker {self.worker_id} started processing")
        
        while self.running:
            try:
                # Get task from dispatcher
                task = self.dispatcher.get_task(timeout=2.0)
                
                if task is None:
                    # No task available, continue loop
                    continue
                
                # Process the task
                result = self._process_task(task)
                
                # Submit result to dispatcher
                self.dispatcher.submit_result(result)
                
                # Update stats
                self.stats['tasks_processed'] += 1
                if result['success']:
                    self.stats['successful'] += 1
                else:
                    self.stats['failed'] += 1
                
            except Exception as e:
                logger.error(f"Error in worker {self.worker_id} processing loop: {str(e)}")
                time.sleep(1)  # Brief pause on error
        
        logger.info(f"🏁 Worker {self.worker_id} processing loop ended")
    
    def _process_task(self, task: Dict) -> Dict:
        """
        Process a single crawling task with Shamsi date support
        """
        link_id = task['id']
        url = task['link']
        
        result = {
            'link_id': link_id,
            'success': False,
            'error': None,
            'news_data': {}
        }
        
        try:
            logger.info(f"🕷️ Worker {self.worker_id} crawling: {url}")
            
            # Validate link belongs to this news source
            if not self.news_source.validate_link(url):
                raise Exception(f"Link does not belong to {self.news_source.source_name}")
            
            # Navigate to the page
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Get page source
            html_content = self.driver.page_source
            
            # Extract article data using the injected news source
            article_data = self.news_source.extract_news_content(html_content, url)
            
            # Validate extracted data
            if not article_data or not article_data.get('title'):
                raise Exception("No valid content extracted from article")
            
            # Prepare result with Shamsi date support
            result.update({
                'success': True,
                'news_data': article_data
            })
            
            # Log with Shamsi date if available
            shamsi_info = ""
            if article_data.get('shamsi_year'):
                shamsi_date = f"{article_data['shamsi_year']}/{article_data['shamsi_month']:02d}/{article_data['shamsi_day']:02d}"
                shamsi_info = f" ({shamsi_date})"
            
            logger.debug(f"✅ Worker {self.worker_id} successfully processed link {link_id}{shamsi_info}")
            
        except Exception as e:
            error_msg = f"Error processing task: {str(e)}"
            result['error'] = error_msg
            logger.error(f"❌ Worker {self.worker_id} failed to process link {link_id}: {error_msg}")
        
        return result 