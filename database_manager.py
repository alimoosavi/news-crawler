import logging
from datetime import timezone
from typing import List

from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config import DatabaseConfig
from db_models import Base, NewsLink, StatusEnum, NewsContent
from schema import NewsLinkData, NewsData

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DatabaseManager:
    """
    Handles all interactions with the PostgreSQL database,
    converts ORM entities into schema dataclasses for use in the app.
    """

    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.db_url = (
            f"postgresql+psycopg://{db_config.user}:{db_config.password}"
            f"@{db_config.host}:{db_config.port}/{db_config.db}"
        )

        self.engine = create_engine(
            self.db_url,
            pool_size=5,
            max_overflow=10,
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

    # ----------------------------
    # LINKS
    # ----------------------------
    def insert_new_links(self, links: List[NewsLinkData]) -> int:
        if not links:
            return 0

        inserted_count = 0
        with Session(self.engine) as session:
            for link_data in links:
                new_link = NewsLink(
                    source=link_data.source,
                    link=link_data.link,
                    published_datetime=link_data.published_datetime.replace(
                        tzinfo=timezone.utc
                    ),
                    status=StatusEnum.PENDING,
                )

                try:
                    session.add(new_link)
                    session.flush()
                    inserted_count += 1
                except Exception as e:
                    if "duplicate key value violates unique constraint" in str(e):
                        logger.debug(f"Link already exists, skipping: {new_link.link}")
                        session.rollback()
                    else:
                        logger.error(f"Error inserting link: {new_link.link}, {e}")
                        session.rollback()
                        raise

            session.commit()

        logger.info(f"Inserted {inserted_count} new links.")
        return inserted_count

    def get_pending_links_by_source(self, source: str, limit: int = 50) -> List[NewsLinkData]:
        """Fetch pending links for a given source as schema objects."""
        with Session(self.engine) as session:
            stmt = (
                select(NewsLink)
                .where(NewsLink.source == source)
                .where(NewsLink.status == StatusEnum.PENDING)
                .order_by(NewsLink.published_datetime.asc())
                .limit(limit)
            )
            orm_links = session.scalars(stmt).all()

            # Convert ORM → schema
            return [
                NewsLinkData(
                    source=link.source,
                    link=link.link,
                    published_datetime=link.published_datetime,
                )
                for link in orm_links
            ]

    def mark_links_completed(self, links: List[str]) -> int:
        """Mark given links as completed in the `news_links` table."""
        if not links:
            return 0

        with Session(self.engine) as session:
            updated = (
                session.query(NewsLink)
                .filter(NewsLink.link.in_(links))
                .update(
                    {NewsLink.status: StatusEnum.COMPLETED},
                    synchronize_session=False,
                )
            )
            session.commit()
            return updated

    # ----------------------------
    # NEWS CONTENT
    # ----------------------------
    def insert_news_batch(self, news_items: List[NewsData]) -> int:
        """Insert a batch of fully crawled news into the `news` table."""
        if not news_items:
            return 0

        inserted_count = 0
        with Session(self.engine) as session:
            for item in news_items:
                new_news = NewsContent(
                    source=item.source,
                    title=item.title,
                    content=item.content,
                    link=item.link,
                    keywords=item.keywords,
                    published_datetime=item.published_datetime.replace(tzinfo=timezone.utc),
                    published_timestamp=item.published_timestamp,
                    images=item.images,
                    summary=item.summary,
                    status=StatusEnum.PENDING,
                )
                try:
                    session.add(new_news)
                    session.flush()
                    inserted_count += 1

                    # Mark original link as completed
                    session.query(NewsLink).filter_by(link=item.link).update(
                        {"status": StatusEnum.COMPLETED}
                    )

                except IntegrityError:
                    session.rollback()
                    logger.debug(f"Duplicate news skipped: {item.link}")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error inserting news: {item.link}, {e}", exc_info=True)
                    raise

            session.commit()

        logger.info(f"Inserted {inserted_count} new news contents.")
        return inserted_count

    def get_pending_news_batch(self, limit: int = 50) -> List[NewsData]:
        """
        Fetch a batch of pending news as schema objects (not ORM).
        """
        with Session(self.engine) as session:
            stmt = (
                select(NewsContent)
                .where(NewsContent.status == StatusEnum.PENDING)
                .order_by(NewsContent.published_datetime.asc())
                .limit(limit)
            )
            orm_news = session.scalars(stmt).all()

            # Convert ORM → schema
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

    def mark_news_completed(self, links: List[str]) -> int:
        """Mark given news as COMPLETED in the `news` table."""
        if not links:
            return 0

        with Session(self.engine) as session:
            updated = (
                session.query(NewsContent)
                .filter(NewsContent.link.in_(links))
                .update(
                    {NewsContent.status: StatusEnum.COMPLETED},
                    synchronize_session=False,
                )
            )
            session.commit()
            return updated
