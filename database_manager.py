import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging
from config import settings

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.logger = logging.getLogger(__name__)
        self.db_config = settings.database
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password
            )
            self.logger.info(f"Database connection established to {self.db_config.host}:{self.db_config.port}")
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise
    
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
            with self.connection.cursor() as cursor:
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
            date DATE NOT NULL,
            has_processed BOOLEAN DEFAULT FALSE,
            title TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX idx_news_links_source ON news_links(source);
        CREATE INDEX idx_news_links_date ON news_links(date);
        CREATE INDEX idx_news_links_processed ON news_links(has_processed);
        CREATE INDEX idx_news_links_source_processed ON news_links(source, has_processed);
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                self.connection.commit()
                self.logger.info("news_links table created successfully with indexes")
                return True
        except Exception as e:
            self.logger.error(f"Error creating news_links table: {str(e)}")
            self.connection.rollback()
            raise
    
    def create_news_table(self):
        """Create the news table based on your definition"""
        if self.table_exists('news'):
            self.logger.info("news table already exists")
            return True
            
        create_table_query = """
        CREATE TABLE news (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            published_date TEXT,
            title TEXT,
            summary TEXT,
            content TEXT,
            tags TEXT[],
            link_id INTEGER REFERENCES news_links(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance and search capabilities
        CREATE INDEX idx_news_source ON news(source);
        CREATE INDEX idx_news_published_date ON news(published_date);
        CREATE INDEX idx_news_link_id ON news(link_id);
        CREATE INDEX idx_news_created_at ON news(created_at);
        
        -- Full-text search indexes (requires PostgreSQL with text search support)
        CREATE INDEX idx_news_title_fts ON news USING gin(to_tsvector('english', COALESCE(title, '')));
        CREATE INDEX idx_news_content_fts ON news USING gin(to_tsvector('english', COALESCE(content, '')));
        CREATE INDEX idx_news_summary_fts ON news USING gin(to_tsvector('english', COALESCE(summary, '')));
        
        -- GIN index for tags array
        CREATE INDEX idx_news_tags ON news USING gin(tags);
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                self.connection.commit()
                self.logger.info("news table created successfully with indexes")
                return True
        except Exception as e:
            self.logger.error(f"Error creating news table: {str(e)}")
            self.connection.rollback()
            raise
    
    def create_tables_if_not_exist(self):
        """Create all required tables if they don't exist"""
        self.logger.info("Checking and creating tables if they don't exist...")
        
        tables_created = []
        
        try:
            # Create news_links table first (referenced by news table)
            if not self.table_exists('news_links'):
                self.create_news_links_table()
                tables_created.append('news_links')
            else:
                self.logger.info("news_links table already exists")
            
            # Create news table
            if not self.table_exists('news'):
                self.create_news_table()
                tables_created.append('news')
            else:
                self.logger.info("news table already exists")
            
            if tables_created:
                self.logger.info(f"Successfully created tables: {', '.join(tables_created)}")
            else:
                self.logger.info("All tables already exist")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error in create_tables_if_not_exist: {str(e)}")
            raise
    
    def create_all_tables(self):
        """Alias for create_tables_if_not_exist for backward compatibility"""
        return self.create_tables_if_not_exist()
    
    def get_table_info(self, table_name):
        """Get detailed information about a table"""
        info_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = %s
        ORDER BY ordinal_position;
        """
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(info_query, (table_name,))
                return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return []
    
    def get_database_schema_info(self):
        """Get information about all tables in the database"""
        schema_info = {}
        
        # Get list of tables
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(tables_query)
                tables = [row[0] for row in cursor.fetchall()]
            
            # Get info for each table
            for table in tables:
                schema_info[table] = {
                    'exists': True,
                    'columns': self.get_table_info(table)
                }
            
            return schema_info
            
        except Exception as e:
            self.logger.error(f"Error getting database schema info: {str(e)}")
            return {}
    
    def insert_news_link(self, source, link, date, title=None):
        """Insert a single news link into the database"""
        insert_query = """
        INSERT INTO news_links (source, link, date, title, has_processed)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (link) DO NOTHING
        RETURNING id;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, (source, link, date, title, False))
                result = cursor.fetchone()
                self.connection.commit()
                
                if result:
                    self.logger.debug(f"Inserted news link: {link}")
                    return result[0]
                else:
                    self.logger.debug(f"Link already exists: {link}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error inserting news link: {str(e)}")
            self.connection.rollback()
            raise
    
    def insert_news_links_batch(self, links_data):
        """Insert multiple news links in a batch"""
        insert_query = """
        INSERT INTO news_links (source, link, date, title, has_processed)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (link) DO NOTHING;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(insert_query, links_data)
                self.connection.commit()
                self.logger.info(f"Batch inserted {len(links_data)} news links")
                
        except Exception as e:
            self.logger.error(f"Error batch inserting news links: {str(e)}")
            self.connection.rollback()
            raise
    
    def insert_news_article(self, source, published_date, title, summary, content, tags, link_id):
        """Insert a news article into the database"""
        insert_query = """
        INSERT INTO news (source, published_date, title, summary, content, tags, link_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, (source, published_date, title, summary, content, tags, link_id))
                result = cursor.fetchone()
                self.connection.commit()
                
                if result:
                    self.logger.debug(f"Inserted news article: {title[:50] if title else 'No title'}...")
                    return result[0]
                    
        except Exception as e:
            self.logger.error(f"Error inserting news article: {str(e)}")
            self.connection.rollback()
            raise
    
    def get_unprocessed_links(self, source=None, limit=None):
        """Get unprocessed news links"""
        query = "SELECT * FROM news_links WHERE has_processed = FALSE"
        params = []
        
        if source:
            query += " AND source = %s"
            params.append(source)
        
        query += " ORDER BY created_at ASC"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
                
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
            with self.connection.cursor() as cursor:
                cursor.execute(update_query, (link_id,))
                self.connection.commit()
                self.logger.debug(f"Marked link {link_id} as processed")
                
        except Exception as e:
            self.logger.error(f"Error marking link as processed: {str(e)}")
            self.connection.rollback()
            raise
    
    def get_news_articles(self, source=None, limit=None):
        """Get news articles from database"""
        query = "SELECT * FROM news"
        params = []
        
        if source:
            query += " WHERE source = %s"
            params.append(source)
        
        query += " ORDER BY created_at DESC"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"Error fetching news articles: {str(e)}")
            raise
    
    def get_news_statistics(self):
        """Get statistics about news and links"""
        stats_query = """
        SELECT 
            (SELECT COUNT(*) FROM news_links) as total_links,
            (SELECT COUNT(*) FROM news_links WHERE has_processed = TRUE) as processed_links,
            (SELECT COUNT(*) FROM news_links WHERE has_processed = FALSE) as unprocessed_links,
            (SELECT COUNT(*) FROM news) as total_articles,
            (SELECT COUNT(DISTINCT source) FROM news) as sources_count
        """
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(stats_query)
                return cursor.fetchone()
                
        except Exception as e:
            self.logger.error(f"Error fetching statistics: {str(e)}")
            raise 