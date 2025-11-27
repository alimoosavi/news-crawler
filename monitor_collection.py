#!/usr/bin/env python3
"""
Simple Collection Monitor
View information about your Qdrant collection.
"""
import sys
from qdrant_client import QdrantClient

try:
    from config import settings
except ImportError:
    print("ERROR: Cannot import config")
    sys.exit(1)


def show_info():
    """Display collection information"""
    client = QdrantClient(host=settings.qdrant.host, port=settings.qdrant.port)
    collection_name = settings.qdrant.collection_name
    
    print("\n" + "=" * 80)
    print("QDRANT COLLECTION INFO")
    print("=" * 80)
    
    if not client.collection_exists(collection_name):
        print(f"❌ Collection '{collection_name}' does not exist")
        print("\nIt will be created automatically when you run:")
        print("  python news_embedding_scheduler.py")
        print("=" * 80 + "\n")
        return
    
    try:
        info = client.get_collection(collection_name)
        existing_dim = info.config.params.vectors.size
        expected_dim = settings.embedding.embedding_dim
        
        print(f"Name: {collection_name}")
        print(f"Status: {info.status}")
        print(f"Articles: {info.points_count:,}")
        print(f"Dimension: {existing_dim}")
        print(f"Distance: {info.config.params.vectors.distance}")
        print()
        print(f"Config Provider: {settings.embedding.provider}")
        if settings.embedding.provider == 'openai':
            print(f"Config Model: {settings.embedding.openai_model}")
        else:
            print(f"Config Model: {settings.embedding.ollama_model}")
        print(f"Expected Dimension: {expected_dim}")
        
        if existing_dim == expected_dim:
            print(f"\n✅ Dimension matches - compatible")
        else:
            print(f"\n❌ WARNING: Dimension mismatch!")
            print(f"   Collection: {existing_dim}")
            print(f"   Config: {expected_dim}")
            print(f"\n   You changed embedding settings in .env!")
            print(f"   Either revert .env or run: python delete_collection.py")
        
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    show_info()