from datetime import datetime
from typing import Optional

from sqlalchemy import String, DateTime, Integer, JSON, Enum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import enum


# --- SQLAlchemy Base ---

class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""
    pass


# --- Status Enum ---

class StatusEnum(str, enum.Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"  # NEW: For links that exceeded max retries


# --- Matches NewsLinkData ---

class NewsLink(Base):
    """
    Stores news link metadata.
    Mirrors NewsLinkData.
    
    NEW FIELDS:
    - tried_count: Number of times the link was attempted to be crawled
    - last_tried_at: Last time the link was attempted
    """
    __tablename__ = "news_links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    source: Mapped[str] = mapped_column(String(100), nullable=False)
    link: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    published_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Status field - FIXED: Added values_callable
    status: Mapped[StatusEnum] = mapped_column(
        Enum(StatusEnum, name="news_link_status", values_callable=lambda x: [e.value for e in x]),
        nullable=False,
        default=StatusEnum.PENDING,
    )
    
    # NEW: Retry tracking fields
    tried_count: Mapped[int] = mapped_column(
        Integer, 
        nullable=False, 
        default=0,
        comment="Number of times the link was attempted to be crawled"
    )
    
    last_tried_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), 
        nullable=True,
        comment="Last time the link was attempted"
    )

    def __repr__(self) -> str:
        return (
            f"NewsLink(id={self.id!r}, link={self.link[:50]!r}, "
            f"source={self.source!r}, status={self.status!r}, "
            f"tried_count={self.tried_count!r})"
        )
    
    def can_retry(self, max_retries: int = 3) -> bool:
        """
        Check if this link can be retried.
        
        Args:
            max_retries: Maximum number of retry attempts allowed
            
        Returns:
            True if the link hasn't exceeded max retries, False otherwise
        """
        return self.tried_count < max_retries


# --- Matches NewsData ---

class NewsContent(Base):
    """
    Stores full news data (title, body, keywords, etc.).
    Mirrors NewsData.
    """
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    source: Mapped[str] = mapped_column(String(100), nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    content: Mapped[str] = mapped_column(String, nullable=False)
    link: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)

    keywords: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True)
    published_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    images: Mapped[Optional[list[str]]] = mapped_column(JSON, nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Status field - FIXED: Added values_callable
    status: Mapped[StatusEnum] = mapped_column(
        Enum(StatusEnum, name="news_content_status", values_callable=lambda x: [e.value for e in x]),
        nullable=False,
        default=StatusEnum.PENDING,
    )

    def __repr__(self) -> str:
        return (
            f"NewsContent(id={self.id!r}, title={self.title[:30]!r}, "
            f"source={self.source!r}, status={self.status!r})"
        )