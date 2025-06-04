#!/usr/bin/env python3
"""
Database Migration Script for ISNA News Crawler

This script creates and manages database tables for the news crawler system.
It can be run independently or imported as a module.

Usage:
    python migrate.py [options]

Options:
    --check-only    Only check table status without creating
    --force-create  Drop and recreate tables (WARNING: destroys data)
    --info          Show detailed table information
    --help          Show this help message

Examples:
    python migrate.py                    # Create tables if they don't exist (default)
    python migrate.py --check-only       # Only check table status
    python migrate.py --force-create     # Drop and recreate all tables
    python migrate.py --info             # Show detailed table information
"""

import sys
import argparse
import logging
from database_manager import DatabaseManager
from config import settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    """Handles database migrations and schema management"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        
    def connect(self):
        """Connect to database"""
        try:
            self.db_manager.connect()
            logger.info("Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.db_manager:
            self.db_manager.close()
    
    def check_tables_status(self):
        """Check the status of all required tables"""
        logger.info("Checking table status...")
        
        required_tables = ['news_links', 'news']
        table_status = {}
        
        for table in required_tables:
            exists = self.db_manager.table_exists(table)
            table_status[table] = exists
            
            if exists:
                logger.info(f"✅ Table '{table}' exists")
            else:
                logger.warning(f"❌ Table '{table}' does not exist")
        
        return table_status
    
    def create_tables(self, force=False):
        """Create all required tables"""
        logger.info("Starting table creation process...")
        
        if force:
            logger.warning("Force mode enabled - this will drop existing tables!")
            response = input("Are you sure you want to continue? (yes/no): ")
            if response.lower() != 'yes':
                logger.info("Migration cancelled by user")
                return False
            
            self.drop_tables()
        
        try:
            # Create tables
            success = self.db_manager.create_tables_if_not_exist()
            
            if success:
                logger.info("✅ All tables created successfully")
                return True
            else:
                logger.error("❌ Failed to create tables")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during table creation: {str(e)}")
            return False
    
    def drop_tables(self):
        """Drop all tables (WARNING: destroys data)"""
        logger.warning("Dropping all tables...")
        
        drop_queries = [
            "DROP TABLE IF EXISTS news CASCADE;",
            "DROP TABLE IF EXISTS news_links CASCADE;"
        ]
        
        try:
            with self.db_manager.connection.cursor() as cursor:
                for query in drop_queries:
                    cursor.execute(query)
                self.db_manager.connection.commit()
            
            logger.info("✅ All tables dropped successfully")
            
        except Exception as e:
            logger.error(f"❌ Error dropping tables: {str(e)}")
            self.db_manager.connection.rollback()
            raise
    
    def show_table_info(self):
        """Show detailed information about all tables"""
        logger.info("Gathering table information...")
        
        schema_info = self.db_manager.get_database_schema_info()
        
        if not schema_info:
            logger.warning("No tables found or error retrieving schema info")
            return
        
        print("\n" + "="*60)
        print("DATABASE SCHEMA INFORMATION")
        print("="*60)
        
        for table_name, info in schema_info.items():
            print(f"\n📋 Table: {table_name}")
            print("-" * 40)
            
            if info['columns']:
                print("Columns:")
                for col in info['columns']:
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                    print(f"  • {col['column_name']}: {col['data_type']} {nullable}{default}")
            else:
                print("  No column information available")
        
        print("\n" + "="*60)
    
    def validate_schema(self):
        """Validate that the schema matches expected structure"""
        logger.info("Validating database schema...")
        
        # Expected schema definition
        expected_schema = {
            'news_links': {
                'required_columns': ['id', 'source', 'link', 'date', 'has_processed', 'title', 'created_at', 'updated_at']
            },
            'news': {
                'required_columns': ['id', 'source', 'published_date', 'title', 'summary', 'content', 'tags', 'link_id', 'created_at', 'updated_at']
            }
        }
        
        validation_passed = True
        
        for table_name, expected in expected_schema.items():
            if not self.db_manager.table_exists(table_name):
                logger.error(f"❌ Required table '{table_name}' is missing")
                validation_passed = False
                continue
            
            # Get actual columns
            table_info = self.db_manager.get_table_info(table_name)
            actual_columns = [col['column_name'] for col in table_info]
            
            # Check required columns
            missing_columns = set(expected['required_columns']) - set(actual_columns)
            if missing_columns:
                logger.error(f"❌ Table '{table_name}' missing columns: {', '.join(missing_columns)}")
                validation_passed = False
            else:
                logger.info(f"✅ Table '{table_name}' has all required columns")
        
        if validation_passed:
            logger.info("✅ Schema validation passed")
        else:
            logger.error("❌ Schema validation failed")
        
        return validation_passed
    
    def run_migration(self, check_only=False, force_create=False, show_info=False):
        """Run the complete migration process"""
        logger.info("Starting database migration...")
        
        # Connect to database
        if not self.connect():
            return False
        
        try:
            # Show table info if requested
            if show_info:
                self.show_table_info()
                return True
            
            # Check current status
            table_status = self.check_tables_status()
            
            if check_only:
                logger.info("Check-only mode - no changes made")
                return all(table_status.values())
            
            # Create tables if needed
            if not all(table_status.values()) or force_create:
                success = self.create_tables(force=force_create)
                if not success:
                    return False
            else:
                logger.info("All tables already exist")
            
            # Validate schema
            validation_success = self.validate_schema()
            
            if validation_success:
                logger.info("🎉 Migration completed successfully!")
                return True
            else:
                logger.error("💥 Migration completed with validation errors")
                return False
                
        except Exception as e:
            logger.error(f"💥 Migration failed: {str(e)}")
            return False
        
        finally:
            self.close()

def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(
        description="Database Migration Script for ISNA News Crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate.py                    # Create tables if they don't exist (default)
  python migrate.py --check-only       # Only check table status
  python migrate.py --force-create     # Drop and recreate all tables
  python migrate.py --info             # Show detailed table information
        """
    )
    
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Only check table status without creating tables'
    )
    
    parser.add_argument(
        '--force-create',
        action='store_true',
        help='Drop and recreate tables (WARNING: destroys existing data)'
    )
    
    parser.add_argument(
        '--info',
        action='store_true',
        help='Show detailed table information'
    )
    
    args = parser.parse_args()
    
    # Show configuration info
    print("🗞️  ISNA News Crawler - Database Migration")
    print("=" * 50)
    print(f"Database: {settings.db_name.db_name}")
    print(f"Host: {settings.db_name.host}:{settings.db_name.port}")
    print(f"User: {settings.db_name.user}")
    print("=" * 50)
    
    # Run migration
    migrator = DatabaseMigrator()
    success = migrator.run_migration(
        check_only=args.check_only,
        force_create=args.force_create,
        show_info=args.info
    )
    
    if success:
        print("\n✅ Migration completed successfully!")
        print("🚀 You can now run the news crawler!")
        sys.exit(0)
    else:
        print("\n❌ Migration failed!")
        print("💡 Check the logs above for details")
        sys.exit(1)

if __name__ == "__main__":
    main() 