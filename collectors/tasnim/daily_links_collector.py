import logging
from typing import List, Optional
from xml.etree import ElementTree as ET
import requests
from datetime import datetime, timezone

from schema import NewsLinkData
from news_publishers import TASNIM

class TasnimDailyLinkCollector:
    """
    Collects all news links for a specific Shamsi day from Tasnim's sitemap.
    """

    BASE_URL = "https://www.tasnimnews.com/fa/{year}/{month}/{day}/fa_{year}_{month}_{day}_sitemap.xml"

    def __init__(self, year: int, month: int, day: int):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.year = year
        self.month = month
        self.day = day

    def collect_links(self) -> List[NewsLinkData]:
        links: List[NewsLinkData] = []
        url = self.BASE_URL.format(year=self.year, month=self.month, day=self.day)

        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                self.logger.warning(f"Failed to fetch sitemap: {url} (status {resp.status_code})")
                return []

            # Parse XML
            root = ET.fromstring(resp.content)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            for url_tag in root.findall("ns:url", ns):
                loc = url_tag.find("ns:loc", ns)
                lastmod = url_tag.find("ns:lastmod", ns)

                link = loc.text if loc is not None else None
                lastmod_str = lastmod.text if lastmod is not None else None

                published_dt: Optional[datetime] = None
                if lastmod_str:
                    try:
                        # Example format: 2024-08-30T20:27:00Z
                        published_dt = datetime.strptime(lastmod_str, "%Y-%m-%dT%H:%M:%SZ")
                        published_dt = published_dt.replace(tzinfo=timezone.utc)
                    except ValueError:
                        self.logger.warning(f"Could not parse lastmod: {lastmod_str}")

                if link:
                    links.append(
                        NewsLinkData(
                            link=link,
                            published_datetime=published_dt,  # <-- now datetime instead of str
                            source=TASNIM,
                        )
                    )

            self.logger.info(f"Tasnim {self.year}-{self.month}-{self.day}: Collected {len(links)} links")
            return links

        except Exception as e:
            self.logger.error(f"Error fetching Tasnim sitemap {url}: {e}")
            return []
