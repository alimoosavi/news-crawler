"""
Create Qdrant Indexes Script
===========================

This script creates the necessary payload indexes for the News RAG system.
It is idempotent (safe to run multiple times).

Indexes Created:
1. source (KEYWORD): For filtering by publisher (e.g., "IRNA")
2. published_timestamp (INTEGER): For date range queries (e.g., "Last 30 days")
3. status (KEYWORD): For system maintenance (e.g., "Find all failed items")
4. keywords (KEYWORD): For tag-based filtering

Usage:
    python create_qdrant_indexes.py
"""
import logging
import sys
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse

# Import your settings
try:
    from config import settings
except ImportError:
    print("❌ Critical Error: Could not import 'settings' from config.py")
    print("   Make sure you are running this script from the project root.")
    sys.exit(1)

# Setup Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("IndexManager")

def create_indexes():
    """
    Connects to Qdrant and ensures all required indexes exist.
    """
    collection_name = settings.qdrant.collection_name
    qdrant_host = settings.qdrant.host
    qdrant_port = settings.qdrant.port

    logger.info("=" * 60)
    logger.info(f"🚀 Starting Index Creation for Collection: '{collection_name}'")
    logger.info(f"📡 Connecting to Qdrant at {qdrant_host}:{qdrant_port}...")
    logger.info("=" * 60)

    # 1. Initialize Client
    try:
        # Force HTTP (prefer_grpc=False) to match your fix in other files
        client = QdrantClient(
            host=qdrant_host,
            port=qdrant_port,
            prefer_grpc=False,
            timeout=30  # Generous timeout for admin operations
        )
        
        # Verify connection
        client.get_collections()
        logger.info("✅ Connected to Qdrant successfully.")

    except Exception as e:
        logger.critical(f"❌ Failed to connect to Qdrant: {e}")
        logger.info("   Is the Qdrant Docker container running?")
        sys.exit(1)

    # 2. Verify Collection Exists
    if not client.collection_exists(collection_name):
        logger.critical(f"❌ Collection '{collection_name}' does not exist!")
        logger.info("   Please run the ingestion scheduler first to create the collection.")
        sys.exit(1)

    # 3. Define Indexes to Create
    # Format: (Field Name, Schema Type, Description)
    indexes_to_create = [
        (
            "source", 
            models.PayloadSchemaType.KEYWORD, 
            "Enables filtering by News Source (IRNA, ISNA, etc.)"
        ),
        (
            "published_timestamp", 
            models.PayloadSchemaType.INTEGER, 
            "Enables Date Range filtering (Start Date -> End Date)"
        ),
        (
            "status", 
            models.PayloadSchemaType.KEYWORD, 
            "Enables maintenance filtering (find failed/pending items)"
        ),
        (
            "keywords", 
            models.PayloadSchemaType.KEYWORD, 
            "Enables filtering by tags/keywords JSON array"
        )
    ]

    # 4. Create Indexes Loop (Idempotent / Crash-Proof)
    success_count = 0
    
    for field_name, schema_type, description in indexes_to_create:
        logger.info(f"👉 Processing Index: '{field_name}' ({schema_type})")
        logger.info(f"   Context: {description}")
        
        try:
            client.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=schema_type,
            )
            logger.info(f"   ✅ Index creation command sent.")
            success_count += 1
            
        except UnexpectedResponse as e:
            # Handle "Index already exists" gracefully
            error_str = str(e).lower()
            if "already exists" in error_str or "conflict" in error_str:
                logger.info(f"   ℹ️  Index already exists. Skipping.")
                success_count += 1 # Count as success since the state is what we want
            else:
                logger.error(f"   ❌ Failed to create index: {e}")
                
        except Exception as e:
            logger.error(f"   ❌ Unexpected error: {e}")

    # 5. Final Summary
    logger.info("=" * 60)
    if success_count == len(indexes_to_create):
        logger.info("✅ SUCCESS: All indexes are present and ready.")
        logger.info("   Your RAG filtering queries will now be blazing fast! ⚡")
    else:
        logger.warning(f"⚠️  WARNING: Only {success_count}/{len(indexes_to_create)} indexes were verified.")
    logger.info("=" * 60)

if __name__ == "__main__":
    create_indexes()