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

import logging
import sys

from config import settings
from database_manager import DatabaseManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseMigrator:
    """Handles database migrations and schema management"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def create_tables(self):
        """Create all required tables"""
        logger.info("Starting table creation process...")

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


def main():
    """Main function for command-line usage"""

    # Show configuration info
    print("🗞️  ISNA News Crawler - Database Migration")
    print("=" * 50)
    print(f"Database: {settings.database.db_name}")
    print(f"Host: {settings.database.host}:{settings.database.port}")
    print(f"User: {settings.database.user}")
    print("=" * 50)

    db_manager = DatabaseManager(host=settings.database.host,
                                 port=settings.database.port,
                                 db_name=settings.database.db_name,
                                 user=settings.database.user,
                                 password=settings.database.password,
                                 min_conn=settings.database.min_conn,
                                 max_conn=settings.database.max_conn)
    migrator = DatabaseMigrator(db_manager=db_manager)
    success = migrator.create_tables()

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
