import logging
from contextlib import contextmanager
from datetime import datetime
from typing import List, Optional

import pytz
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    ARRAY,
    Index,
    func,
    text as sa_text,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from pydantic import BaseModel

logger = logging.getLogger(__name__)
Base = declarative_base()


# Pydantic model for bulk_insert input
class NewsLinkData(BaseModel):
    source: str
    link: str
    published_datetime: Optional[datetime] = None


class NewsLink(Base):
    __tablename__ = 'news_links'

    news_link_id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    link = Column(Text, unique=True, nullable=False)
    published_datetime = Column(DateTime(timezone=True))
    has_processed = Column(Boolean, default=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.current_timestamp()
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    __table_args__ = (
        Index('idx_news_links_source', 'source'),
        Index(
            'idx_news_links_processed',
            'has_processed',
            postgresql_where=(has_processed == False),
        ),
        Index(
            'idx_news_links_published_datetime',
            'published_datetime',
            postgresql_ops={'published_datetime': 'DESC'},
        ),
    )


class News(Base):
    __tablename__ = 'news'

    news_id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    published_datetime = Column(DateTime(timezone=True))
    title = Column(Text, nullable=False)
    summary = Column(Text)
    content = Column(Text)
    tags = Column(ARRAY(Text))
    author = Column(String(255))
    link_id = Column(Integer, ForeignKey('news_links.news_link_id'))
    has_processed = Column(Boolean, default=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.current_timestamp()
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
    )

    __table_args__ = (
        Index('idx_news_source', 'source'),
        Index(
            'idx_news_published_datetime',
            'published_datetime',
            postgresql_ops={'published_datetime': 'DESC'},
        ),
        Index('idx_news_link_id', 'link_id'),
        Index(
            'idx_news_processed',
            'has_processed',
            postgresql_where=(has_processed == False),
        ),
        # Fixed GIN trigram index
        Index(
            'idx_news_title',
            sa_text("title gin_trgm_ops"),
            postgresql_using='gin',
        ),
    )


class DatabaseManager:
    def __init__(
        self,
        host: str,
        port: int,
        db_name: str,
        user: str,
        password: str,
        min_conn: int,
        max_conn: int,
    ):
        self.logger = logging.getLogger(__name__)
        self.tehran_tz = pytz.timezone('Asia/Tehran')
        conn_str = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
        )
        self.engine = create_engine(
            conn_str,
            pool_size=max_conn,
            max_overflow=max_conn - min_conn,
            pool_timeout=10,
        )
        self.Session = sessionmaker(bind=self.engine)

    def __del__(self):
        try:
            self.engine.dispose()
            self.logger.info("Database engine disposed.")
        except Exception:
            pass  # engine might already be closed

    @contextmanager
    def get_session(self, commit: bool = True):
        session = self.Session()
        try:
            yield session
            if commit:
                session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error in session: {e}")
            raise
        finally:
            session.close()

    def enable_pg_trgm_extension(self) -> bool:
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    sa_text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
            self.logger.info("pg_trgm extension enabled successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error enabling pg_trgm extension: {e}")
            return False

    def create_tables_if_not_exist(self) -> bool:
        try:
            if not self.enable_pg_trgm_extension():
                raise RuntimeError("Failed to enable pg_trgm extension")
            Base.metadata.create_all(self.engine)
            self.logger.info("All tables verified/created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            return False

    def insert_news_article(
        self,
        source: str,
        title: str,
        summary: Optional[str],
        content: Optional[str],
        tags: Optional[List[str]],
        link_id: int,
        published_datetime: Optional[datetime] = None,
        author: Optional[str] = None,
    ) -> int:
        article = News(
            source=source,
            published_datetime=published_datetime,
            title=title,
            summary=summary,
            content=content,
            tags=tags,
            author=author,
            link_id=link_id,
            has_processed=False,
        )
        try:
            with self.get_session() as session:
                session.add(article)
                session.flush()
                return article.news_id
        except Exception as e:
            self.logger.error(f"Error inserting news article: {e}")
            raise

    def get_unprocessed_links(
        self, source: Optional[str] = None, limit: int = 50
    ) -> List[dict]:
        try:
            with self.get_session(commit=False) as session:
                q = session.query(NewsLink).filter(
                    NewsLink.has_processed == False)
                if source:
                    q = q.filter(NewsLink.source == source)
                q = q.order_by(
                    NewsLink.published_datetime.desc().nullslast(),
                    NewsLink.created_at.asc(),
                ).limit(limit)

                results = q.all()
                return [
                    {
                        "news_link_id": r.news_link_id,
                        "source": r.source,
                        "link": r.link,
                        "published_datetime": r.published_datetime,
                        "has_processed": r.has_processed,
                        "created_at": r.created_at,
                        "updated_at": r.updated_at,
                    }
                    for r in results
                ]
        except Exception as e:
            self.logger.error(f"Error fetching unprocessed links: {e}")
            raise

    def bulk_insert_news_links(self, links: List[NewsLinkData]) -> int:
        if not links:
            return 0

        try:
            with self.get_session() as session:
                stmt = insert(NewsLink).values(
                    [
                        {
                            "source": l.source,
                            "link": l.link,
                            "published_datetime": l.published_datetime,
                            "has_processed": False,
                        }
                        for l in links
                    ]
                ).on_conflict_do_nothing(index_elements=["link"])
                res = session.execute(stmt)
                return res.rowcount or 0
        except Exception as e:
            self.logger.error(f"Error bulk inserting news links: {e}")
            raise

    def mark_link_processed(self, link_id: int) -> None:
        try:
            with self.get_session() as session:
                nl = session.query(NewsLink).filter(
                    NewsLink.news_link_id == link_id
                ).first()
                if nl:
                    nl.has_processed = True
                    nl.updated_at = datetime.now(pytz.utc)
                else:
                    self.logger.warning(f"No link found for ID {link_id}")
        except Exception as e:
            self.logger.error(
                f"Error marking link {link_id} as processed: {e}")
            raise
