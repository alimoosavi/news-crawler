from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NewsLinkData:
    """Typed data class for news links, aligned with the news_links table schema"""
    source: str
    link: str
    published_datetime: datetime


@dataclass
class NewsData:
    """Typed data class for news data, aligned with the news_links table schema"""
    source: str
    title: str
    content: str
    link: str
    published_datetime: datetime
    images: Optional[list[str]]
    summary: Optional[str] = None