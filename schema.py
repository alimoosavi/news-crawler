from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NewsLinkData:
    """Typed data class for news links, aligned with the news_links table schema"""
    source: str
    link: str
    news_link_id: Optional[int] = None
    published_datetime: Optional[datetime] = None
    published_year: Optional[int] = None
    published_month: Optional[int] = None
    published_day: Optional[int] = None
    has_processed: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def date_string(self) -> Optional[str]:
        """Derive date_string from published_year, published_month, published_day, and published_datetime"""
        if self.published_year and self.published_month and self.published_day and self.published_datetime:
            return f"{self.published_year:04d}/{self.published_month:02d}/{self.published_day:02d} {self.published_datetime.hour:02d}:{self.published_datetime.minute:02d}"
        return None
