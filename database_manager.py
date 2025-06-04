import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import logging
from config import settings
import threading
from contextlib import contextmanager
import pytz

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Database manager optimized for laptop resources with Shamsi date support
    """

    def __init__(self, host: str, port: int, db_name: str, user: str, password: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self._connection_lock = threading.Lock()
        self.tehran_tz = pytz.timezone('Asia/Tehran')

    def connect(self):
        """Establish single database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.user,
                password=self.password,
                # Laptop-optimized connection settings
                connect_timeout=10,
                application_name='news_crawler_laptop'
            )
            self.connection.autocommit = False
            self.logger.info(f"Database connection established to {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise

    @contextmanager
    def get_cursor(self, commit=True):
        """Context manager for database operations with automatic commit/rollback"""
        with self._connection_lock:
            cursor = None
            try:
                cursor = self.connection.cursor(cursor_factory=RealDictCursor)
                yield cursor
                if commit:
                    self.connection.commit()
            except Exception as e:
                if cursor:
                    self.connection.rollback()
                raise e
            finally:
                if cursor:
                    cursor.close()

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

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
            with self.get_cursor() as cursor:
                cursor.execute(check_query, (table_name,))
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            return False

    def create_news_links_table(self):
        """Create the news_links table with Shamsi date support"""
        if self.table_exists('news_links'):
            self.logger.info("news_links table already exists")
            return True

        create_table_query = """
        CREATE TABLE news_links (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            link TEXT UNIQUE NOT NULL,
            date DATE,  -- Gregorian date for compatibility
            published_datetime TIMESTAMPTZ,  -- Zone-aware datetime
            
            -- Shamsi date fields
            shamsi_year INTEGER,
            shamsi_month INTEGER,
            shamsi_day INTEGER,
            shamsi_date_string VARCHAR(20),  -- e.g., "1403/03/05"
            
            has_processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Indexes for performance
        CREATE INDEX idx_news_links_source ON news_links (source);
        CREATE INDEX idx_news_links_processed ON news_links (has_processed) WHERE has_processed = FALSE;
        CREATE INDEX idx_news_links_date ON news_links (date DESC);
        CREATE INDEX idx_news_links_shamsi ON news_links (shamsi_year DESC, shamsi_month DESC, shamsi_day DESC);
        CREATE INDEX idx_news_links_shamsi_date ON news_links (shamsi_date_string);
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(create_table_query)
            self.logger.info("news_links table created successfully with Shamsi date support")
            return True
        except Exception as e:
            self.logger.error(f"Error creating news_links table: {str(e)}")
            return False

    def create_news_table(self):
        """Create the news table with Shamsi date support"""
        if self.table_exists('news'):
            self.logger.info("news table already exists")
            return True

        create_table_query = """
        CREATE TABLE news (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            published_date DATE,  -- Gregorian date
            published_datetime TIMESTAMPTZ,  -- Zone-aware datetime
            
            -- Shamsi date fields
            shamsi_year INTEGER,
            shamsi_month INTEGER,
            shamsi_day INTEGER,
            shamsi_date_string VARCHAR(20),  -- e.g., "1403/03/05"
            shamsi_month_name VARCHAR(20),   -- e.g., "خرداد"
            
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
        
        -- Indexes
        CREATE INDEX idx_news_source ON news (source);
        CREATE INDEX idx_news_published_date ON news (published_date DESC);
        CREATE INDEX idx_news_published_datetime ON news (published_datetime DESC);
        CREATE INDEX idx_news_shamsi ON news (shamsi_year DESC, shamsi_month DESC, shamsi_day DESC);
        CREATE INDEX idx_news_shamsi_date ON news (shamsi_date_string);
        CREATE INDEX idx_news_shamsi_month ON news (shamsi_month_name);
        CREATE INDEX idx_news_link_id ON news (link_id);
        CREATE INDEX idx_news_processed ON news (has_processed) WHERE has_processed = FALSE;
        CREATE INDEX idx_news_title ON news USING gin(to_tsvector('english', title));
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(create_table_query)
            self.logger.info("news table created successfully with Shamsi date support")
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

    def insert_news_link(self, source, link, date=None, published_datetime=None,
                         shamsi_year=None, shamsi_month=None, shamsi_day=None):
        """Insert a single news link with Shamsi date support"""

        # Generate Shamsi date string if components are provided
        shamsi_date_string = None
        if shamsi_year and shamsi_month and shamsi_day:
            shamsi_date_string = f"{shamsi_year:04d}/{shamsi_month:02d}/{shamsi_day:02d}"

        insert_query = """
        INSERT INTO news_links (source, link, date, published_datetime, 
                               shamsi_year, shamsi_month, shamsi_day, shamsi_date_string)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (link) DO UPDATE SET
            date = EXCLUDED.date,
            published_datetime = EXCLUDED.published_datetime,
            shamsi_year = EXCLUDED.shamsi_year,
            shamsi_month = EXCLUDED.shamsi_month,
            shamsi_day = EXCLUDED.shamsi_day,
            shamsi_date_string = EXCLUDED.shamsi_date_string,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, (
                    source, link, date, published_datetime,
                    shamsi_year, shamsi_month, shamsi_day, shamsi_date_string
                ))
                link_id = cursor.fetchone()['id']
                self.logger.debug(f"Inserted/updated news link with ID: {link_id}")
                return link_id
        except Exception as e:
            self.logger.error(f"Error inserting news link: {str(e)}")
            raise

    def insert_news_article(self, source, published_date, title, summary, content, tags, link_id,
                            published_datetime=None, shamsi_year=None, shamsi_month=None,
                            shamsi_day=None, author=None):
        """Insert a single news article with Shamsi date support"""

        # Generate Shamsi date string and month name
        shamsi_date_string = None
        shamsi_month_name = None

        if shamsi_year and shamsi_month and shamsi_day:
            shamsi_date_string = f"{shamsi_year:04d}/{shamsi_month:02d}/{shamsi_day:02d}"
            shamsi_month_name = self._get_shamsi_month_name(shamsi_month)

        insert_query = """
        INSERT INTO news (source, published_date, published_datetime, title, summary, content, 
                         tags, link_id, has_processed, shamsi_year, shamsi_month, shamsi_day,
                         shamsi_date_string, shamsi_month_name, author)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, (
                    source, published_date, published_datetime, title, summary, content,
                    tags, link_id, False, shamsi_year, shamsi_month, shamsi_day,
                    shamsi_date_string, shamsi_month_name, author
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

        # Default limit for laptop performance
        if limit is None:
            limit = 50  # Smaller default for laptops

        query += " LIMIT %s"
        params.append(limit)

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching unprocessed links: {str(e)}")
            raise

    def get_unprocessed_news(self, source=None, limit=None):
        """Get unprocessed news articles for further processing"""
        query = "SELECT * FROM news WHERE has_processed = FALSE"
        params = []

        if source:
            query += " AND source = %s"
            params.append(source)

        query += " ORDER BY published_datetime DESC NULLS LAST, created_at ASC"

        # Default limit for laptop performance
        if limit is None:
            limit = 50

        query += " LIMIT %s"
        params.append(limit)

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching unprocessed news: {str(e)}")
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

    def mark_news_processed(self, news_id):
        """Mark a news article as processed"""
        update_query = """
        UPDATE news 
        SET has_processed = TRUE, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s;
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(update_query, (news_id,))
                self.logger.debug(f"Marked news {news_id} as processed")
        except Exception as e:
            self.logger.error(f"Error marking news as processed: {str(e)}")
            raise

    def bulk_mark_news_processed(self, news_ids):
        """Bulk mark multiple news articles as processed"""
        if not news_ids:
            return

        update_query = """
        UPDATE news 
        SET has_processed = TRUE, updated_at = CURRENT_TIMESTAMP
        WHERE id = ANY(%s);
        """

        try:
            with self.get_cursor() as cursor:
                cursor.execute(update_query, (news_ids,))
                self.logger.info(f"Bulk marked {len(news_ids)} news articles as processed")
        except Exception as e:
            self.logger.error(f"Error bulk marking news as processed: {str(e)}")
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
                # Get link stats
                cursor.execute(stats_query)
                link_stats = dict(cursor.fetchone())

                # Get news stats
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

    def bulk_insert_links(self, links_data):
        """Bulk insert news links efficiently with datetime support"""
        if not links_data:
            return

        insert_query = """
        INSERT INTO news_links (source, link, date, published_datetime)
        VALUES %s
        ON CONFLICT (link) DO UPDATE SET
            date = EXCLUDED.date,
            published_datetime = EXCLUDED.published_datetime,
            updated_at = CURRENT_TIMESTAMP;
        """

        try:
            from psycopg2.extras import execute_values

            # Prepare data tuples with datetime support
            values = []
            for item in links_data:
                values.append((
                    item['source'],
                    item['link'],
                    item.get('date'),
                    item.get('published_datetime')
                ))

            with self.get_cursor() as cursor:
                execute_values(cursor, insert_query, values, template=None, page_size=100)

            self.logger.info(f"Bulk inserted {len(links_data)} links")

        except Exception as e:
            self.logger.error(f"Error in bulk insert: {str(e)}")
            raise

    def get_news_by_date_range(self, start_date, end_date, source=None, processed_only=True, use_datetime=False):
        """Get news articles within a date range"""
        if use_datetime:
            date_field = "n.published_datetime"
        else:
            date_field = "n.published_date"

        query = f"""
        SELECT n.*, nl.link, nl.date as link_date, nl.published_datetime as link_datetime
        FROM news n
        JOIN news_links nl ON n.link_id = nl.id
        WHERE {date_field} BETWEEN %s AND %s
        """
        params = [start_date, end_date]

        if source:
            query += " AND n.source = %s"
            params.append(source)

        if processed_only:
            query += " AND n.has_processed = TRUE"

        query += f" ORDER BY {date_field} DESC"

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching news by date range: {str(e)}")
            raise

    def get_news_by_shamsi_date_range(self, start_shamsi_date, end_shamsi_date, source=None, processed_only=True):
        """Get news articles within a Shamsi date range"""
        query = """
        SELECT n.*, nl.link, nl.shamsi_date_string as link_shamsi_date
        FROM news n
        JOIN news_links nl ON n.link_id = nl.id
        WHERE n.shamsi_date_string BETWEEN %s AND %s
        """
        params = [start_shamsi_date, end_shamsi_date]

        if source:
            query += " AND n.source = %s"
            params.append(source)

        if processed_only:
            query += " AND n.has_processed = TRUE"

        query += " ORDER BY n.shamsi_year DESC, n.shamsi_month DESC, n.shamsi_day DESC"

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching news by Shamsi date range: {str(e)}")
            raise

    def get_news_by_shamsi_month(self, shamsi_year, shamsi_month, source=None):
        """Get news articles for a specific Shamsi month"""
        query = """
        SELECT n.*, nl.link
        FROM news n
        JOIN news_links nl ON n.link_id = nl.id
        WHERE n.shamsi_year = %s AND n.shamsi_month = %s
        """
        params = [shamsi_year, shamsi_month]

        if source:
            query += " AND n.source = %s"
            params.append(source)

        query += " ORDER BY n.shamsi_day DESC"

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching news by Shamsi month: {str(e)}")
            raise

    def get_shamsi_date_statistics(self, source=None):
        """Get statistics grouped by Shamsi dates"""
        query = """
        SELECT 
            shamsi_year,
            shamsi_month,
            shamsi_month_name,
            COUNT(*) as article_count,
            COUNT(CASE WHEN has_processed = TRUE THEN 1 END) as processed_count
        FROM news
        """
        params = []

        if source:
            query += " WHERE source = %s"
            params.append(source)

        query += """
        GROUP BY shamsi_year, shamsi_month, shamsi_month_name
        ORDER BY shamsi_year DESC, shamsi_month DESC
        """

        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error fetching Shamsi date statistics: {str(e)}")
            raise

    def _get_shamsi_month_name(self, month_number):
        """Get Shamsi month name from number"""
        month_names = {
            1: 'فروردین', 2: 'اردیبهشت', 3: 'خرداد', 4: 'تیر',
            5: 'مرداد', 6: 'شهریور', 7: 'مهر', 8: 'آبان',
            9: 'آذر', 10: 'دی', 11: 'بهمن', 12: 'اسفند'
        }
        return month_names.get(month_number, 'نامشخص')

    def bulk_insert_news_links(self, links_data):
        """Bulk insert news links with Shamsi date support"""
        if not links_data:
            return

        insert_query = """
        INSERT INTO news_links (
            source, link, date, published_datetime,
            shamsi_year, shamsi_month, shamsi_day, shamsi_date_string
        ) VALUES %s
        ON CONFLICT (link) DO UPDATE SET
            date = EXCLUDED.date,
            published_datetime = EXCLUDED.published_datetime,
            shamsi_year = EXCLUDED.shamsi_year,
            shamsi_month = EXCLUDED.shamsi_month,
            shamsi_day = EXCLUDED.shamsi_day,
            shamsi_date_string = EXCLUDED.shamsi_date_string,
            updated_at = CURRENT_TIMESTAMP;
        """

        try:
            from psycopg2.extras import execute_values

            # Prepare data tuples with Shamsi date support
            values = []
            for item in links_data:
                values.append((
                    item['source'],
                    item['link'],
                    item.get('date'),
                    item.get('published_datetime'),
                    item.get('shamsi_year'),
                    item.get('shamsi_month'),
                    item.get('shamsi_day'),
                    item.get('shamsi_date_string')
                ))

            with self.get_cursor() as cursor:
                execute_values(cursor, insert_query, values, template=None, page_size=100)

            self.logger.info(f"Bulk inserted {len(links_data)} links with Shamsi dates")

        except Exception as e:
            self.logger.error(f"Error in bulk insert with Shamsi dates: {str(e)}")
            raise
