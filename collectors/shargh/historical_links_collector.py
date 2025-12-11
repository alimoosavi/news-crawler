"""
Shargh Historical Links Collector

This collector fetches news links from Shargh's date-based sitemaps.
The sitemap structure:
1. Main sitemap (sitemap.xml) contains daily sitemap links
2. Each daily sitemap contains news article URLs with metadata

Key differences from Donya-e-Eqtesad:
- Uses date-encoded sitemap URLs (base64 encoded JSON)
- Requires parsing XML sitemaps (not direct JSON)
- Has multiple language sitemaps (we focus on Persian content)
"""

import base64
import json
import logging
import multiprocessing as mp
from datetime import date as dt_date, timedelta
from typing import List, Tuple, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from database_manager import DatabaseManager
from schema import NewsLinkData
from news_publishers import SHARGH


class SharghHistoricalLinksCollector:
    """
    Manages historical crawl across a range of Gregorian dates for Shargh.
    Fetches daily sitemaps and extracts news links from XML.
    """

    BASE_SITEMAP_URL = "https://www.sharghdaily.com/sitemap.xml"
    DAILY_SITEMAP_TEMPLATE = "https://www.sharghdaily.com/sitemap/{encoded_params}.xml"

    def __init__(
        self,
        db_manager: DatabaseManager,
        batch_size: int = 10,
        workers: int = 4,
        request_timeout: int = 30
    ):
        """
        Initialize Shargh historical collector.
        
        Args:
            db_manager: Database manager for persisting links
            batch_size: Number of days to process in parallel per batch
            workers: Number of parallel worker processes
            request_timeout: HTTP request timeout in seconds
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.batch_size = batch_size
        self.workers = workers
        self.request_timeout = request_timeout
        self.db_manager = db_manager

    @staticmethod
    def _encode_date_params(date: dt_date) -> str:
        """
        Encode date parameters to match Shargh's sitemap URL format.
        
        Format: base64(json({"model": "newsstudioDateRange", "date": "YYYY-MM-DD"}))
        
        Args:
            date: Date to encode
            
        Returns:
            Base64-encoded parameter string
        """
        params = {
            "model": "newsstudioDateRange",
            "date": date.strftime("%Y-%m-%d")
        }
        json_str = json.dumps(params, separators=(',', ':'))
        encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
        return encoded

    @staticmethod
    def _get_daily_sitemap_url(date: dt_date) -> str:
        """
        Construct daily sitemap URL for given date.
        
        Args:
            date: Date to get sitemap for
            
        Returns:
            Full sitemap URL
        """
        encoded_params = SharghDailyHistoricalLinksCollector._encode_date_params(date)
        return f"https://www.sharghdaily.com/sitemap/{encoded_params}.xml"

    @staticmethod
    def _parse_sitemap_xml(xml_content: str) -> List[NewsLinkData]:
        """
        Parse Shargh sitemap XML and extract news links.
        
        Expected XML structure:
        <urlset>
            <url>
                <loc>https://www.sharghdaily.com/...</loc>
                <lastmod>2024-01-01T12:00:00+00:00</lastmod>
                <image:image>...</image:image>
            </url>
        </urlset>
        
        Args:
            xml_content: Raw XML content from sitemap
            
        Returns:
            List of NewsLinkData objects
        """
        logger = logging.getLogger("SharghHistoricalLinksCollector")
        news_links = []
        
        try:
            soup = BeautifulSoup(xml_content, 'xml')
            
            # Find all <url> elements in the sitemap
            url_entries = soup.find_all('url')
            
            for url_entry in url_entries:
                try:
                    # Extract link
                    loc_tag = url_entry.find('loc')
                    if not loc_tag or not loc_tag.text:
                        continue
                    
                    link = loc_tag.text.strip()
                    
                    # Skip non-article links (menu, static pages, etc.)
                    # Shargh articles typically have numeric IDs
                    if not any(char.isdigit() for char in link):
                        continue
                    
                    # Extract publication datetime
                    lastmod_tag = url_entry.find('lastmod')
                    if lastmod_tag and lastmod_tag.text:
                        try:
                            published_datetime = date_parser.parse(lastmod_tag.text)
                        except Exception as e:
                            logger.warning(f"Could not parse date '{lastmod_tag.text}': {e}")
                            continue
                    else:
                        continue
                    
                    # Create NewsLinkData object
                    news_link = NewsLinkData(
                        source=SHARGH,
                        link=link,
                        published_datetime=published_datetime
                    )
                    news_links.append(news_link)
                    
                except Exception as e:
                    logger.warning(f"Error parsing URL entry: {e}")
                    continue
            
            logger.debug(f"Parsed {len(news_links)} news links from sitemap")
            
        except Exception as e:
            logger.error(f"Error parsing sitemap XML: {e}", exc_info=True)
        
        return news_links

    @staticmethod
    def _fetch_sitemap_content(url: str, timeout: int = 30) -> Optional[str]:
        """
        Fetch sitemap XML content from URL.
        
        Args:
            url: Sitemap URL
            timeout: Request timeout in seconds
            
        Returns:
            XML content as string, or None if fetch failed
        """
        logger = logging.getLogger("SharghHistoricalLinksCollector")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; NewsLensBot/1.0)',
                'Accept': 'application/xml, text/xml, */*'
            }
            
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            return response.text
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout fetching sitemap: {url}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching sitemap {url}: {e}")
            return None

    @staticmethod
    def _crawl_single_day(date: dt_date, timeout: int = 30) -> Tuple[str, List[NewsLinkData]]:
        """
        Crawl a single day's sitemap and extract news links.
        
        This is a static method designed to be called by multiprocessing.Pool.
        
        Args:
            date: Date to crawl
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (date_string, list of NewsLinkData)
        """
        logger = logging.getLogger("SharghHistoricalLinksCollector")
        
        try:
            # Get sitemap URL for the date
            sitemap_url = SharghHistoricalLinksCollector._get_daily_sitemap_url(date)
            logger.info(f"📅 Fetching Shargh sitemap for {date}: {sitemap_url}")
            
            # Fetch sitemap content
            xml_content = SharghHistoricalLinksCollector._fetch_sitemap_content(
                sitemap_url,
                timeout
            )
            
            if not xml_content:
                return str(date), []
            
            # Parse XML and extract links
            news_links = SharghHistoricalLinksCollector._parse_sitemap_xml(xml_content)
            
            logger.info(f"✅ Day {date}: Found {len(news_links)} news links")
            return str(date), news_links
            
        except Exception as e:
            logger.error(f"❌ Error crawling Shargh {date}: {e}", exc_info=True)
            return str(date), []

    def collect_range(self, start_date: dt_date, end_date: dt_date):
        """
        Collect news links across a date range using parallel processing.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
        """
        if start_date > end_date:
            self.logger.error("❌ Start date cannot be after end date.")
            return

        self.logger.info(
            f"🚀 Shargh Historical Crawl: {start_date} → {end_date} "
            f"(batch_size={self.batch_size}, workers={self.workers})"
        )

        total_links = 0
        current_date = start_date
        
        while current_date <= end_date:
            # Calculate batch dates
            batch_end = min(
                current_date + timedelta(days=self.batch_size - 1),
                end_date
            )
            batch_dates = [
                current_date + timedelta(days=i)
                for i in range((batch_end - current_date).days + 1)
            ]

            self.logger.info(
                f"📦 Processing batch: {current_date} → {batch_end} "
                f"({len(batch_dates)} days)"
            )

            # Parallel crawl batch
            with mp.Pool(processes=self.workers) as pool:
                # Pass timeout to each worker
                crawl_func = lambda date: self._crawl_single_day(
                    date,
                    self.request_timeout
                )
                results = pool.map(crawl_func, batch_dates)

            # Persist results
            batch_total = 0
            for date_str, links in results:
                if links:
                    self.db_manager.insert_new_links(links)
                    batch_total += len(links)
                    self.logger.info(
                        f"✅ Day {date_str}: {len(links)} links persisted"
                    )
                else:
                    self.logger.warning(f"⚠️  Day {date_str}: No links found")

            total_links += batch_total
            self.logger.info(
                f"📊 Batch complete: {batch_total} links | "
                f"Total so far: {total_links}"
            )

            # Move to next batch
            current_date = batch_end + timedelta(days=1)

        self.logger.info(
            f"🎉 Shargh historical crawl completed! "
            f"Total links collected: {total_links}"
        )


# Example usage
if __name__ == "__main__":
    from datetime import date
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Create collector
    collector = SharghHistoricalLinksCollector(
        db_manager=db_manager,
        batch_size=10,  # Process 10 days at a time
        workers=4       # 4 parallel workers
    )
    
    # Collect links for a date range
    collector.collect_range(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31)
    )