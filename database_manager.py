import logging
from datetime import timezone, datetime
from typing import List, Optional
import time

from sqlalchemy import create_engine, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from config import DatabaseConfig
from db_models import Base, NewsLink, StatusEnum, NewsContent
from schema import NewsLinkData, NewsData

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Optimized database manager with proper enum handling.
    
    IMPORTANT: All StatusEnum values use .value to get lowercase strings
    that match PostgreSQL enum values.
    """

    def __init__(self, db_config: DatabaseConfig, max_retries: int = 3):
        self.db_config = db_config
        self.max_retries = max_retries
        
        self.db_url = (
            f"postgresql+psycopg://{db_config.user}:{db_config.password}"
            f"@{db_config.host}:{db_config.port}/{db_config.db}"
        )

        self.engine = create_engine(
            self.db_url,
            pool_size=20,
            max_overflow=40,
            pool_pre_ping=True,
            pool_recycle=3600,
        )

    def initialize_database(self):
        """Creates the tables if they don't exist."""
        try:
            logger.info("Initializing database: creating tables...")
            Base.metadata.create_all(self.engine)
            logger.info("Database initialization complete.")
        except Exception as e:
            logger.error(f"Could not connect to database or create tables: {e}")
            raise

    def insert_news_batch_optimized(self, news_items: List[NewsData]) -> int:
        """Optimized bulk insert using INSERT ... ON CONFLICT."""
        if not news_items:
            return 0

        start_time = time.time()
        
        with Session(self.engine) as session:
            news_records = [
                {
                    'source': item.source,
                    'title': item.title,
                    'content': item.content,
                    'link': item.link,
                    'keywords': item.keywords,
                    'published_datetime': item.published_datetime.replace(tzinfo=timezone.utc),
                    'published_timestamp': item.published_timestamp,
                    'images': item.images,
                    'summary': item.summary,
                    'status': StatusEnum.PENDING.value,  # FIXED: Use .value
                }
                for item in news_items
            ]
            
            stmt = insert(NewsContent).values(news_records)
            stmt = stmt.on_conflict_do_nothing(index_elements=['link'])
            
            result = session.execute(stmt)
            session.commit()
            
            inserted_count = result.rowcount
            duration = time.time() - start_time
            
            logger.info(
                f"Bulk inserted {inserted_count}/{len(news_items)} news items "
                f"in {duration:.2f}s ({inserted_count/duration:.0f} items/sec)"
            )
            
            return inserted_count

    def increment_link_try_count(self, links: List[str]) -> int:
        """Increment the tried_count for a list of links."""
        if not links:
            return 0
        
        with Session(self.engine) as session:
            stmt = (
                update(NewsLink)
                .where(NewsLink.link.in_(links))
                .values(
                    tried_count=NewsLink.tried_count + 1,
                    last_tried_at=datetime.now(timezone.utc)
                )
            )
            
            result = session.execute(stmt)
            session.commit()
            
            updated_count = result.rowcount
            logger.info(f"Incremented try count for {updated_count} links")
            
            return updated_count
    
    def mark_links_as_failed(self, links: List[str]) -> int:
        """Mark links as FAILED when they exceed max retry attempts."""
        if not links:
            return 0
        
        with Session(self.engine) as session:
            stmt = (
                update(NewsLink)
                .where(NewsLink.link.in_(links))
                .values(status=StatusEnum.FAILED.value)  # FIXED: Added .value
            )
            
            result = session.execute(stmt)
            session.commit()
            
            updated_count = result.rowcount
            logger.warning(f"Marked {updated_count} links as FAILED (exceeded max retries)")
            
            return updated_count
    
    def get_pending_links_by_source(
        self, 
        source: str, 
        limit: int = 50,
        exclude_max_retries: bool = True
    ) -> List[NewsLinkData]:
        """Fetch pending links efficiently."""
        with Session(self.engine) as session:
            stmt = (
                select(NewsLink)
                .where(NewsLink.source == source)
                .where(NewsLink.status == StatusEnum.PENDING.value)  # FIXED: Added .value
            )
            
            if exclude_max_retries:
                stmt = stmt.where(NewsLink.tried_count < self.max_retries)
            
            stmt = (
                stmt.order_by(NewsLink.published_datetime.asc())
                .limit(limit)
            )
            
            orm_links = session.scalars(stmt).all()
            
            return [
                NewsLinkData(
                    source=link.source,
                    link=link.link,
                    published_datetime=link.published_datetime,
                )
                for link in orm_links
            ]
    
    def get_failed_links_count_by_source(self, source: str) -> int:
        """Get count of failed links for a specific source."""
        with Session(self.engine) as session:
            from sqlalchemy import func
            stmt = (
                select(func.count())
                .select_from(NewsLink)
                .where(NewsLink.source == source)
                .where(NewsLink.status == StatusEnum.FAILED.value)  # FIXED: Added .value
            )
            count = session.scalar(stmt)
            return count or 0
    
    def get_links_exceeding_retries(self, source: Optional[str] = None) -> List[str]:
        """Get links that have exceeded max retries but are still marked as PENDING."""
        with Session(self.engine) as session:
            stmt = (
                select(NewsLink.link)
                .where(NewsLink.status == StatusEnum.PENDING.value)  # FIXED: Added .value
                .where(NewsLink.tried_count >= self.max_retries)
            )
            
            if source:
                stmt = stmt.where(NewsLink.source == source)
            
            links = session.scalars(stmt).all()
            return list(links)
    
    def cleanup_exceeded_retries(self, source: Optional[str] = None) -> int:
        """Mark all links that exceeded max retries as FAILED."""
        exceeded_links = self.get_links_exceeding_retries(source)
        
        if exceeded_links:
            logger.info(
                f"Found {len(exceeded_links)} links exceeding max retries "
                f"({self.max_retries}). Marking as FAILED..."
            )
            return self.mark_links_as_failed(exceeded_links)
        
        return 0

    def mark_links_completed_optimized(self, links: List[str]) -> int:
        """Mark multiple links as completed in a single UPDATE query."""
        if not links:
            return 0

        start_time = time.time()
        
        with Session(self.engine) as session:
            stmt = (
                update(NewsLink)
                .where(NewsLink.link.in_(links))
                .values(status=StatusEnum.COMPLETED.value)  # FIXED: Added .value
            )
            
            result = session.execute(stmt)
            session.commit()
            
            updated_count = result.rowcount
            duration = time.time() - start_time
            
            logger.info(
                f"Marked {updated_count} links as completed in {duration:.3f}s"
            )
            
            return updated_count

    def filter_unprocessed_links(self, links: List[str]) -> List[str]:
        """Filter out links that are already processed."""
        if not links:
            return []
        
        with Session(self.engine) as session:
            stmt = (
                select(NewsContent.link)
                .where(NewsContent.link.in_(links))
            )
            
            existing_links = set(session.scalars(stmt).all())
            unprocessed = [link for link in links if link not in existing_links]
            
            logger.info(
                f"Filtered: {len(unprocessed)}/{len(links)} links are unprocessed"
            )
            
            return unprocessed

    def insert_new_links(self, links: List[NewsLinkData]) -> int:
        """Insert links with ON CONFLICT handling"""
        if not links:
            return 0

        with Session(self.engine) as session:
            link_records = [
                {
                    'source': link_data.source,
                    'link': link_data.link,
                    'published_datetime': link_data.published_datetime.replace(tzinfo=timezone.utc),
                    'status': StatusEnum.PENDING.value,  # FIXED: Added .value
                    'tried_count': 0,
                }
                for link_data in links
            ]
            
            stmt = insert(NewsLink).values(link_records)
            stmt = stmt.on_conflict_do_nothing(index_elements=['link'])
            
            result = session.execute(stmt)
            session.commit()
            
            inserted_count = result.rowcount
            logger.info(f"Inserted {inserted_count} new links.")
            return inserted_count

    def insert_news_batch(self, news_items: List[NewsData]) -> int:
        """Alias to optimized method"""
        return self.insert_news_batch_optimized(news_items)
    
    def mark_links_completed(self, links: List[str]) -> int:
        """Alias to optimized method"""
        return self.mark_links_completed_optimized(links)

    def get_pending_news_batch(self, limit: int = 50) -> List[NewsData]:
        """Fetch pending news from ALL SOURCES."""
        with Session(self.engine) as session:
            stmt = (
                select(NewsContent)
                .where(NewsContent.status == StatusEnum.PENDING.value)  # FIXED: Added .value
                .order_by(NewsContent.published_datetime.asc())
                .limit(limit)
            )
            orm_news = session.scalars(stmt).all()

            return [
                NewsData(
                    source=n.source,
                    title=n.title,
                    content=n.content,
                    link=n.link,
                    keywords=n.keywords,
                    published_datetime=n.published_datetime,
                    published_timestamp=n.published_timestamp,
                    images=n.images,
                    summary=n.summary,
                )
                for n in orm_news
            ]

    def get_pending_news_batch_by_source(self, source: str, limit: int = 50) -> List[NewsData]:
        """Fetch pending news for a SPECIFIC SOURCE."""
        with Session(self.engine) as session:
            stmt = (
                select(NewsContent)
                .where(NewsContent.source == source)
                .where(NewsContent.status == StatusEnum.PENDING.value)  # FIXED: Added .value
                .order_by(NewsContent.published_datetime.asc())
                .limit(limit)
            )
            orm_news = session.scalars(stmt).all()

            news_list = [
                NewsData(
                    source=n.source,
                    title=n.title,
                    content=n.content,
                    link=n.link,
                    keywords=n.keywords,
                    published_datetime=n.published_datetime,
                    published_timestamp=n.published_timestamp,
                    images=n.images,
                    summary=n.summary,
                )
                for n in orm_news
            ]
            
            if news_list:
                logger.info(
                    f"Fetched {len(news_list)} pending news items for source '{source}'"
                )
            
            return news_list

    def get_pending_count_by_source(self, source: str) -> int:
        """Get count of pending news items for a specific source."""
        with Session(self.engine) as session:
            from sqlalchemy import func
            stmt = (
                select(func.count())
                .select_from(NewsContent)
                .where(NewsContent.source == source)
                .where(NewsContent.status == StatusEnum.PENDING.value)  # FIXED: Added .value
            )
            count = session.scalar(stmt)
            return count or 0

    def get_total_pending_count(self) -> int:
        """Get total count of pending news items across ALL sources."""
        with Session(self.engine) as session:
            from sqlalchemy import func
            stmt = (
                select(func.count())
                .select_from(NewsContent)
                .where(NewsContent.status == StatusEnum.PENDING.value)  # FIXED: Added .value
            )
            count = session.scalar(stmt)
            return count or 0

    def mark_news_completed(self, links: List[str]) -> int:
        """Mark news as completed"""
        if not links:
            return 0

        with Session(self.engine) as session:
            stmt = (
                update(NewsContent)
                .where(NewsContent.link.in_(links))
                .values(status=StatusEnum.COMPLETED.value)  # FIXED: Added .value
            )
            result = session.execute(stmt)
            session.commit()
            return result.rowcount
    
    def get_retry_statistics(self, source: Optional[str] = None) -> dict:
        """Get retry statistics for monitoring."""
        with Session(self.engine) as session:
            from sqlalchemy import func
            
            stmt = (
                select(
                    NewsLink.tried_count,
                    func.count().label('count')
                )
                .select_from(NewsLink)
                .where(NewsLink.status == StatusEnum.PENDING.value)  # FIXED: Added .value
            )
            
            if source:
                stmt = stmt.where(NewsLink.source == source)
            
            stmt = stmt.group_by(NewsLink.tried_count).order_by(NewsLink.tried_count)
            
            results = session.execute(stmt).all()
            
            retry_distribution = {row.tried_count: row.count for row in results}
            
            near_max_stmt = (
                select(func.count())
                .select_from(NewsLink)
                .where(NewsLink.status == StatusEnum.PENDING.value)  # FIXED: Added .value
                .where(NewsLink.tried_count >= self.max_retries - 1)
            )
            
            if source:
                near_max_stmt = near_max_stmt.where(NewsLink.source == source)
            
            near_max_count = session.scalar(near_max_stmt) or 0
            
            return {
                'retry_distribution': retry_distribution,
                'near_max_retries': near_max_count,
                'max_retries': self.max_retries,
                'source': source or 'ALL'
            }