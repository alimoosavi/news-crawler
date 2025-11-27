#!/usr/bin/env python3
"""
Emergency Collection Delete

WARNING: This deletes ALL your embeddings!
Use ONLY if you need to switch embedding models.
"""
import sys
from qdrant_client import QdrantClient

try:
    from config import settings
except ImportError:
    print("ERROR: Cannot import config")
    sys.exit(1)


def delete_collection():
    """Delete the collection after double confirmation"""
    client = QdrantClient(host=settings.qdrant.host, port=settings.qdrant.port)
    collection_name = settings.qdrant.collection_name
    
    if not client.collection_exists(collection_name):
        print(f"\n✅ Collection '{collection_name}' doesn't exist. Nothing to delete.\n")
        return
    
    # Get info
    info = client.get_collection(collection_name)
    article_count = info.points_count
    dimension = info.config.params.vectors.size
    
    print("\n" + "=" * 80)
    print("⚠️  EMERGENCY COLLECTION DELETE")
    print("=" * 80)
    print(f"Collection: {collection_name}")
    print(f"Articles to delete: {article_count:,}")
    print(f"Current dimension: {dimension}")
    print()
    print("After deletion, you must:")
    print("  1. Update .env with new EMBEDDING_* settings")
    print("  2. Run: python news_historical_embedding_scheduler.py")
    print("  3. Wait 8-10 hours for re-embedding")
    print("=" * 80)
    print()
    
    # Double confirmation
    print("⚠️  THIS CANNOT BE UNDONE!")
    print(f"⚠️  You will lose {article_count:,} embeddings!")
    print()
    confirmation = input(f"Type collection name '{collection_name}' to confirm: ")
    
    if confirmation != collection_name:
        print("\n❌ Cancelled. Name didn't match.\n")
        return
    
    final = input("\nType 'DELETE' in capitals to proceed: ")
    
    if final != 'DELETE':
        print("\n❌ Cancelled.\n")
        return
    
    # Delete
    try:
        client.delete_collection(collection_name)
        print(f"\n✅ Collection '{collection_name}' deleted")
        print("\nNext steps:")
        print("  1. Update .env with new embedding settings")
        print("  2. Run: python news_historical_embedding_scheduler.py")
        print()
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    delete_collection()