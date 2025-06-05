import logging
import pytz
from contextlib import contextmanager
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Database manager optimized for laptop resources with Persian date support
    """

    def __init__(self,
                 host: str,
                 port: int,
                 db_name: str,
                 user: str,
                 password: str,
                 min_conn: int,
                 max_conn: int):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.logger = logging.getLogger(__name__)
        self.connection_pool = pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=host,
            port=port,
            database=db_name,
            user=user,
            password=password,
            connect_timeout=10
        )
        self.tehran_tz = pytz.timezone('Asia/Tehran')

    def __del__(self):
        """Ensure pool is closed when object is destroyed"""
        self.close_pool()

    @contextmanager
    def get_connection(self):
        """Get connection from pool"""
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)

    @contextmanager
    def get_cursor(self, commit=True):
        """Context manager for database operations with automatic commit/rollback"""
        with self.get_connection() as conn:
            cursor = None
            try:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                yield cursor
                if commit:
                    conn.commit()
            except Exception as e:
                if cursor:
                    conn.rollback()
                raise e
            finally:
                if cursor:
                    cursor.close()

    def close_pool(self):
        """Close the connection pool safely"""
        try:
            self.connection_pool.closeall()
            self.logger.info("Database connection pool closed.")
        except Exception as e:
            self.logger.error(f"Error closing connection pool: {str(e)}")

    def table_exists(self, table_name):
        """Check if a table exists in the database"""
        check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(check_query, (table_name,))
                return cursor.fetchone()['exists']
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            return False

    def create_news_links_table(self):
        """Create the news_links table with Persian date support"""
        if self.table_exists('news_links'):
            self.logger.info("news_links table already exists")
            return True

        create_table_query = """
        CREATE TABLE news_links (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            link TEXT UNIQUE NOT NULL,
            published_datetime TIMESTAMPTZ,
            published_year INTEGER,
            published_month INTEGER,
            published_day INTEGER,
            date_string VARCHAR(20),
            has_processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_news_links_source ON news_links (source);
        CREATE INDEX idx_news_links_processed ON news_links (has_processed) WHERE has_processed = FALSE;
        CREATE INDEX idx_news_links_persian ON news_links (published_year DESC, published_month DESC, published_day DESC);
        CREATE INDEX idx_news_links_date_string ON news_links (date_string);
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(create_table_query)
            self.logger.info("news_links table created successfully with Persian date support")
            return True
        except Exception as e:
            self.logger.error(f"Error creating news_links table: {str(e)}")
            return False

    def create_news_table(self):
        """Create the news table with Persian date support"""
        if self.table_exists('news'):
            self.logger.info("news table already exists")
            return True

        create_table_query = """
        CREATE TABLE news (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            published_date DATE,
            published_datetime TIMESTAMPTZ,
            published_year INTEGER,
            published_month INTEGER,
            published_day INTEGER,
            date_string VARCHAR(20),
            month_name VARCHAR(20),
            title TEXT NOT NULL,
            summary TEXT,
            content TEXT,
            tags TEXT[],
            author VARCHAR(255),
            link_id INTEGER REFERENCES news_links(id),
            has_processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_news_source ON news (source);
        CREATE INDEX idx_news_published_date ON news (published_date DESC);
        CREATE INDEX idx_news_published_datetime ON news (published_datetime DESC);
        CREATE INDEX idx_news_persian ON news (published_year DESC, published_month DESC, published_day DESC);
        CREATE INDEX idx_news_date_string ON news (date_string);
        CREATE INDEX idx_news_month_name ON news (month_name);
        CREATE INDEX idx_news_link_id ON news (link_id);
        CREATE INDEX idx_news_processed ON news (has_processed) WHERE has_processed = FALSE;
        CREATE INDEX idx_news_title ON news USING gin(to_tsvector('english', title));
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(create_table_query)
            self.logger.info("news table created successfully with Persian date support")
            return True
        except Exception as e:
            self.logger.error(f"Error creating news table: {str(e)}")
            return False

    def create_tables_if_not_exist(self):
        """Create all required tables if they don't exist"""
        try:
            success = True
            success &= self.create_news_links_table()
            success &= self.create_news_table()
            if success:
                self.logger.info("All tables verified/created successfully")
            else:
                self.logger.error("Some tables failed to create")
            return success
        except Exception as e:
            self.logger.error(f"Error creating tables: {str(e)}")
            return False

    def insert_news_link(self, source, link, published_datetime=None,
                         year=None, month=None, day=None):
        """Insert a single news link with Persian date support"""
        date_string = None
        if year and month and day:
            date_string = f"{year:04d}/{month:02d}/{day:02d}"

        insert_query = """
        INSERT INTO news_links (source, link, published_datetime, 
                               published_year, published_month, published_day, date_string)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (link) DO UPDATE SET
            published_datetime = EXCLUDED.published_datetime,
            published_year = EXCLUDED.published_year,
            published_month = EXCLUDED.published_month,
            published_day = EXCLUDED.published_day,
            date_string = EXCLUDED.date_string,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, (
                    source, link, published_datetime,
                    year, month, day, date_string
                ))
                link_id = cursor.fetchone()['id']
                self.logger.debug(f"Inserted/updated news link with ID: {link_id}")
                return link_id
        except Exception as e:
            self.logger.error(f"Error inserting news link: {str(e)}")
            raise

    def insert_news_article(self, source, published_date, title, summary, content, tags, link_id,
                            published_datetime=None, year=None, month=None,
                            day=None, author=None):
        """Insert a single news article with Persian date support"""
        date_string = None
        month_name = None
        if year and month and day:
            date_string = f"{year:04d}/{month:02d}/{day:02d}"
            month_name = self._get_month_name(month)

        insert_query = """
        INSERT INTO news (source, published_date, published_datetime, title, summary, content, 
                         tags, link_id, has_processed, published_year, published_month, published_day,
                         date_string, month_name, author)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, (
                    source, published_date, published_datetime, title, summary, content,
                    tags, link_id, False, year, month, day,
                    date_string, month_name, author
                ))
                news_id = cursor.fetchone()['id']
                self.logger.debug(f"Inserted news article with ID: {news_id}")
                return news_id
        except Exception as e:
            self.logger.error(f"Error inserting news article: {str(e)}")
            raise

    def get_unprocessed_links(self, source=None, limit=None):
        """Get unprocessed links with laptop-friendly limits"""
        query = "SELECT * FROM news_links WHERE has_processed = FALSE"
        params = []
        if source:
            query += " AND source = %s"
            params.append(source)
        query += " ORDER BY published_datetime DESC NULLS LAST, created_at ASC"
        if limit is None:
            limit = 50
        query += " LIMIT %s"
        params.append(limit)

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching unprocessed links: {str(e)}")
            raise

    def mark_link_processed(self, link_id):
        """Mark a news link as processed"""
        update_query = """
        UPDATE news_links 
        SET has_processed = TRUE, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s;
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(update_query, (link_id,))
                self.logger.debug(f"Marked link {link_id} as processed")
        except Exception as e:
            self.logger.error(f"Error marking link as processed: {str(e)}")
            raise

    def get_processing_statistics(self):
        """Get processing statistics for monitoring"""
        try:
            stats_query = """
            SELECT 
                COUNT(*) as total_links,
                COUNT(CASE WHEN has_processed = TRUE THEN 1 END) as processed_links,
                COUNT(CASE WHEN has_processed = FALSE THEN 1 END) as unprocessed_links,
                COUNT(DISTINCT source) as sources_count
            FROM news_links;
            """
            news_stats_query = """
            SELECT COUNT(*) as total_articles FROM news;
            """

            with self.get_cursor(commit=False) as cursor:
                cursor.execute(stats_query)
                link_stats = dict(cursor.fetchone())
                cursor.execute(news_stats_query)
                news_stats = dict(cursor.fetchone())
                return {
                    **link_stats,
                    **news_stats
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

    @classmethod
    def _get_month_name(cls, month_number):
        """Get Persian month name from number"""
        month_names = {
            1: 'فروردین', 2: 'اردیبهشت', 3: 'خرداد', 4: 'تیر',
            5: 'مرداد', 6: 'شهریور', 7: 'مهر', 8: 'آبان',
            9: 'آذر', 10: 'دی', 11: 'بهمن', 12: 'اسفند'
        }
        return month_names.get(month_number, 'نامشخص')

    def bulk_insert_news_links(self, links_data):
        """Bulk insert news links with Persian date support"""
        if not links_data:
            return

        insert_query = """
        INSERT INTO news_links (
            source, link, published_datetime,
            published_year, published_month, published_day, date_string
        ) VALUES %s
        ON CONFLICT (link) DO UPDATE SET
            published_datetime = EXCLUDED.published_datetime,
            published_year = EXCLUDED.published_year,
            published_month = EXCLUDED.published_month,
            published_day = EXCLUDED.published_day,
            date_string = EXCLUDED.date_string,
            updated_at = CURRENT_TIMESTAMP;
        """

        try:
            values = []
            for item in links_data:
                date_string = None
                if item.published_year and item.published_month and item.published_day:
                    date_string = f"{item.published_year:04d}/{item.published_month:02d}/{item.published_day:02d}"
                values.append((
                    item.source,
                    item.link,
                    item.published_datetime,
                    item.published_year,
                    item.published_month,
                    item.published_day,
                    date_string
                ))

            with self.get_cursor() as cursor:
                execute_values(cursor, insert_query, values, template=None, page_size=100)
            self.logger.info(f"Bulk inserted {len(links_data)} links with Persian dates")
        except Exception as e:
            self.logger.error(f"Error in bulk insert with Persian dates: {str(e)}")
            raise
