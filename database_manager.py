import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, Date, DateTime, ForeignKey, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, text
from sqlalchemy import Index
import pytz
from typing import List, Optional
from datetime import date

logger = logging.getLogger(__name__)

# SQLAlchemy setup
Base = declarative_base()

class NewsLink(Base):
    __tablename__ = 'news_links'

    news_link_id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    link = Column(Text, unique=True, nullable=False)
    news_type = Column(String(50))
    published_datetime = Column(DateTime(timezone=True))
    published_year = Column(Integer)
    published_month = Column(Integer)
    published_day = Column(Integer)
    date_string = Column(String(20))
    has_processed = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    __table_args__ = (
        Index('idx_news_links_source', 'source'),
        Index('idx_news_links_type', 'news_type'),
        Index('idx_news_links_processed', 'has_processed', postgresql_where=(has_processed == False)),
        Index('idx_news_links_persian', 'published_year', 'published_month', 'published_day',
              postgresql_using='btree', postgresql_ops={'published_year': 'DESC', 'published_month': 'DESC', 'published_day': 'DESC'}),
        Index('idx_news_links_date_string', 'date_string'),
    )

class News(Base):
    __tablename__ = 'news'

    news_id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    published_date = Column(Date)
    published_datetime = Column(DateTime(timezone=True))
    published_year = Column(Integer)
    published_month = Column(Integer)
    published_day = Column(Integer)
    date_string = Column(String(20))
    month_name = Column(String(20))
    title = Column(Text, nullable=False)
    summary = Column(Text)
    content = Column(Text)
    tags = Column(ARRAY(Text))
    author = Column(String(255))
    link_id = Column(Integer, ForeignKey('news_links.news_link_id'))
    has_processed = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    __table_args__ = (
        Index('idx_news_source', 'source'),
        Index('idx_news_published_date', 'published_date', postgresql_ops={'published_date': 'DESC'}),
        Index('idx_news_published_datetime', 'published_datetime', postgresql_ops={'published_datetime': 'DESC'}),
        Index('idx_news_persian', 'published_year', 'published_month', 'published_day',
              postgresql_using='btree', postgresql_ops={'published_year': 'DESC', 'published_month': 'DESC', 'published_day': 'DESC'}),
        Index('idx_news_date_string', 'date_string'),
        Index('idx_news_month_name', 'month_name'),
        Index('idx_news_link_id', 'link_id'),
        Index('idx_news_processed', 'has_processed', postgresql_where=(has_processed == False)),
        Index('idx_news_title', 'title', postgresql_using='gin', postgresql_ops={'title': 'gin_trgm_ops'}),
    )

class DatabaseManager:
    """
    Database manager using SQLAlchemy with Persian date support
    """

    def __init__(self,
                 host: str,
                 port: int,
                 db_name: str,
                 user: str,
                 password: str,
                 min_conn: int,
                 max_conn: int):
        self.logger = logging.getLogger(__name__)
        self.tehran_tz = pytz.timezone('Asia/Tehran')
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(
            connection_string,
            pool_size=max_conn,
            max_overflow=max_conn - min_conn,
            pool_timeout=10
        )
        self.Session = sessionmaker(bind=self.engine)

    def __del__(self):
        """Dispose of the engine when object is destroyed"""
        self.engine.dispose()
        self.logger.info("Database engine disposed.")

    @contextmanager
    def get_session(self, commit=True):
        """Context manager for SQLAlchemy sessions"""
        session = self.Session()
        try:
            yield session
            if commit:
                session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error in session: {str(e)}")
            raise
        finally:
            session.close()

    def enable_pg_trgm_extension(self):
        """Enable the pg_trgm extension in the database"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
                conn.commit()
            self.logger.info("pg_trgm extension enabled successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error enabling pg_trgm extension: {str(e)}")
            return False

    def create_tables_if_not_exist(self):
        """Create all required tables if they don't exist"""
        try:
            # Ensure pg_trgm extension is enabled before creating tables
            if not self.enable_pg_trgm_extension():
                raise Exception("Failed to enable pg_trgm extension")
            Base.metadata.create_all(self.engine)
            self.logger.info("All tables verified/created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating tables: {str(e)}")
            return False

    def insert_news_link(self, source: str, link: str, published_datetime=None,
                         year: Optional[int]=None, month: Optional[int]=None,
                         day: Optional[int]=None, news_type: Optional[str]=None) -> int:
        """Insert a single news link with Persian date support and news_type"""
        date_string = None
        if year and month and day:
            date_string = f"{year:04d}/{month:02d}/{day:02d}"

        news_link = NewsLink(
            source=source,
            link=link,
            news_type=news_type,
            published_datetime=published_datetime,
            published_year=year,
            published_month=month,
            published_day=day,
            date_string=date_string
        )

        try:
            with self.get_session() as session:
                session.merge(news_link)
                session.flush()
                link_id = news_link.news_link_id
                self.logger.debug(f"Inserted/updated news link with ID: {link_id}")
                return link_id
        except Exception as e:
            self.logger.error(f"Error inserting news link: {str(e)}")
            raise

    def insert_news_article(self, source: str, published_date: Optional[date], title: str,
                           summary: Optional[str], content: Optional[str], tags: Optional[List[str]],
                           link_id: int, published_datetime=None, year: Optional[int]=None,
                           month: Optional[int]=None, day: Optional[int]=None,
                           author: Optional[str]=None) -> int:
        """Insert a single news article with Persian date support"""
        date_string = None
        month_name = None
        if year and month and day:
            date_string = f"{year:04d}/{month:02d}/{day:02d}"
            month_name = self._get_month_name(month)

        news_article = News(
            source=source,
            published_date=published_date,
            published_datetime=published_datetime,
            published_year=year,
            published_month=month,
            published_day=day,
            date_string=date_string,
            month_name=month_name,
            title=title,
            summary=summary,
            content=content,
            tags=tags,
            author=author,
            link_id=link_id,
            has_processed=False
        )

        try:
            with self.get_session() as session:
                session.add(news_article)
                session.flush()
                news_id = news_article.news_id
                self.logger.debug(f"Inserted news article with ID: {news_id}")
                return news_id
        except Exception as e:
            self.logger.error(f"Error inserting news article: {str(e)}")
            raise

    def get_unprocessed_links(self, source: Optional[str]=None, limit: Optional[int]=None) -> List[dict]:
        """Get unprocessed links with laptop-friendly limits"""
        if limit is None:
            limit = 50

        try:
            with self.get_session(commit=False) as session:
                query = session.query(NewsLink).filter(NewsLink.has_processed == False)
                if source:
                    query = query.filter(NewsLink.source == source)
                query = query.order_by(NewsLink.published_datetime.desc().nullslast(), NewsLink.created_at.asc())
                query = query.limit(limit)
                return [row.__dict__ for row in query.all()]
        except Exception as e:
            self.logger.error(f"Error fetching unprocessed links: {str(e)}")
            raise

    def mark_link_processed(self, link_id: int) -> None:
        """Mark a news link as processed"""
        try:
            with self.get_session() as session:
                news_link = session.query(NewsLink).filter(NewsLink.news_link_id == link_id).first()
                if news_link:
                    news_link.has_processed = True
                    news_link.updated_at = func.current_timestamp()
                    self.logger.debug(f"Marked link {link_id} as processed")
                else:
                    self.logger.warning(f"No link found with ID: {link_id}")
        except Exception as e:
            self.logger.error(f"Error marking link as processed: {str(e)}")
            raise

    def get_processing_statistics(self) -> dict:
        """Get processing statistics for monitoring"""
        try:
            with self.get_session(commit=False) as session:
                link_stats = session.query(
                    func.count().label('total_links'),
                    func.count(func.nullif(NewsLink.has_processed, False)).label('processed_links'),
                    func.count(func.nullif(NewsLink.has_processed, True)).label('unprocessed_links'),
                    func.count(func.distinct(NewsLink.source)).label('sources_count')
                ).one()

                news_stats = session.query(
                    func.count().label('total_articles')
                ).select_from(News).one()

                return {
                    'total_links': link_stats.total_links,
                    'processed_links': link_stats.processed_links,
                    'unprocessed_links': link_stats.unprocessed_links,
                    'sources_count': link_stats.sources_count,
                    'total_articles': news_stats.total_articles
                }
        except Exception as e:
            self.logger.error(f"Error getting processing statistics: {str(e)}")
            return {
                'total_links': 0,
                'processed_links': 0,
                'unprocessed_links': 0,
                'sources_count': 0,
                'total_articles': 0
            }

    def bulk_insert_news_links(self, links_data: List[dict]) -> None:
        """Bulk insert news links with Persian date support and news_type"""
        if not links_data:
            return

        news_links = []
        for item in links_data:
            date_string = None
            if item.get('published_year') and item.get('published_month') and item.get('published_day'):
                date_string = f"{item['published_year']:04d}/{item['published_month']:02d}/{item['published_day']:02d}"
            news_links.append(
                NewsLink(
                    source=item.get('source'),
                    link=item.get('link'),
                    news_type=item.get('news_type'),
                    published_datetime=item.get('published_datetime'),
                    published_year=item.get('published_year'),
                    published_month=item.get('published_month'),
                    published_day=item.get('published_day'),
                    date_string=date_string
                )
            )

        try:
            with self.get_session() as session:
                for news_link in news_links:
                    session.merge(news_link)
                self.logger.info(f"Bulk inserted {len(links_data)} links with Persian dates")
        except Exception as e:
            self.logger.error(f"Error in bulk insert with Persian dates: {str(e)}")
            raise

    @classmethod
    def _get_month_name(cls, month_number: Optional[int]) -> str:
        """Get Persian month name from number"""
        month_names = {
            1: 'فروردین', 2: 'اردیبهشت', 3: 'خرداد', 4: 'تیر',
            5: 'مرداد', 6: 'شهریور', 7: 'مهر', 8: 'آبان',
            9: 'آذر', 10: 'دی', 11: 'بهمن', 12: 'اسفند'
        }
        return month_names.get(month_number, 'نامشخص')