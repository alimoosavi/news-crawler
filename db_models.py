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


# --- Matches NewsLinkData ---

class NewsLink(Base):
    """
    Stores news link metadata.
    Mirrors NewsLinkData.
    """
    __tablename__ = "news_links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    source: Mapped[str] = mapped_column(String(100), nullable=False)
    link: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    published_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # New field
    status: Mapped[StatusEnum] = mapped_column(
        Enum(StatusEnum, name="news_link_status"),
        nullable=False,
        default=StatusEnum.PENDING,
    )

    def __repr__(self) -> str:
        return (
            f"NewsLink(id={self.id!r}, link={self.link[:50]!r}, "
            f"source={self.source!r}, status={self.status!r})"
        )


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

    # New field
    status: Mapped[StatusEnum] = mapped_column(
        Enum(StatusEnum, name="news_content_status"),
        nullable=False,
        default=StatusEnum.PENDING,
    )

    def __repr__(self) -> str:
        return (
            f"NewsContent(id={self.id!r}, title={self.title[:30]!r}, "
            f"source={self.source!r}, status={self.status!r})"
        )
