import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging
from config import settings
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager optimized for laptop resources
    """
    
    def __init__(self):
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self.db_config = settings.database
        self._connection_lock = threading.Lock()
        
    def connect(self):
        """Establish single database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                # Laptop-optimized connection settings
                connect_timeout=10,
                application_name='news_crawler_laptop'
            )
            self.connection.autocommit = False
            self.logger.info(f"Database connection established to {self.db_config.host}:{self.db_config.port}")
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
        """Create the news_links table if it doesn't exist"""
        if self.table_exists('news_links'):
            self.logger.info("news_links table already exists")
            return True
            
        create_table_query = """
        CREATE TABLE news_links (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            link TEXT NOT NULL UNIQUE,
            date DATE,
            has_processed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Lightweight indexes for laptop
        CREATE INDEX idx_news_links_processed ON news_links (has_processed) WHERE has_processed = FALSE;
        CREATE INDEX idx_news_links_source ON news_links (source);
        """
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute(create_table_query)
            self.logger.info("news_links table created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating news_links table: {str(e)}")
            return False
    
    def create_news_table(self):
        """Create the news table if it doesn't exist"""
        if self.table_exists('news'):
            self.logger.info("news table already exists")
            return True
            
        create_table_query = """
        CREATE TABLE news (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            published_date TIMESTAMP,
            title TEXT NOT NULL,
            summary TEXT,
            content TEXT,
            tags TEXT[],
            link_id INTEGER REFERENCES news_links(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Lightweight indexes
        CREATE INDEX idx_news_source ON news (source);
        CREATE INDEX idx_news_published_date ON news (published_date DESC);
        CREATE INDEX idx_news_link_id ON news (link_id);
        """
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute(create_table_query)
            self.logger.info("news table created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating news table: {str(e)}")
            return False
    
    def create_tables_if_not_exist(self):
        """Create all required tables if they don't exist"""
        try:
            self.create_news_links_table()
            self.create_news_table()
            self.logger.info("All tables verified/created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error creating tables: {str(e)}")
            return False
    
    def insert_news_article(self, source, published_date, title, summary, content, tags, link_id):
        """Insert a single news article"""
        insert_query = """
        INSERT INTO news (source, published_date, title, summary, content, tags, link_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute(insert_query, (source, published_date, title, summary, content, tags, link_id))
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
        
        query += " ORDER BY created_at ASC"
        
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
        """Get basic processing statistics"""
        stats_query = """
        SELECT 
            COUNT(*) as total_links,
            COUNT(*) FILTER (WHERE has_processed = TRUE) as processed_links,
            COUNT(*) FILTER (WHERE has_processed = FALSE) as unprocessed_links,
            COUNT(DISTINCT source) as sources_count,
            (SELECT COUNT(*) FROM news) as total_articles
        FROM news_links;
        """
        
        try:
            with self.get_cursor(commit=False) as cursor:
                cursor.execute(stats_query)
                return dict(cursor.fetchone())
        except Exception as e:
            self.logger.error(f"Error fetching statistics: {str(e)}")
            raise
    
    def bulk_insert_links(self, links_data):
        """Bulk insert news links efficiently"""
        if not links_data:
            return
        
        insert_query = """
        INSERT INTO news_links (source, link, date)
        VALUES %s
        ON CONFLICT (link) DO NOTHING;
        """
        
        try:
            from psycopg2.extras import execute_values
            
            # Prepare data tuples
            values = [(item['source'], item['link'], item['date']) for item in links_data]
            
            with self.get_cursor() as cursor:
                execute_values(cursor, insert_query, values, template=None, page_size=100)
                
            self.logger.info(f"Bulk inserted {len(links_data)} links")
            
        except Exception as e:
            self.logger.error(f"Error in bulk insert: {str(e)}")
            raise 