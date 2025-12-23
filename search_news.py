#!/usr/bin/env python3
"""
Simple Semantic Search Script using Rayen & Qdrant
=================================================
Fetches the top 10 most similar news articles for a hardcoded query.
Uses the project's existing configuration and embedding service.

Usage:
    python search_news.py
"""
import logging
import sys
from typing import List

from qdrant_client import QdrantClient

# Import from your existing project files
try:
    from config import settings
    from embedding_service import create_embedding_service
except ImportError:
    print("❌ Error: Could not import project modules.")
    print("   Make sure you are running this script from the project root.")
    sys.exit(1)

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("SearchScript")

# ---------------------------------------------------------
# 📝 YOUR HARDCODED QUERY HERE
# ---------------------------------------------------------
QUERY_TEXT = "تاثیر تحریم ها بر بازار ارز"  # Example: "Impact of sanctions on currency market"


def main():
    print("=" * 60)
    print(f"🔍 Searching for: '{QUERY_TEXT}'")
    print("=" * 60)

    # 1. Initialize Embedding Service (Rayen)
    # ---------------------------------------
    # We force the provider to 'rayen' here to ensure we use what you asked for,
    # even if .env has 'openai' or 'ollama' set as default.
    try:
        embedding_service = create_embedding_service(
            provider="rayen",  # FORCE Rayen
            logger=logger,
            # Pass credentials from config.py (loaded from .env)
            rayen_api_key=settings.embedding.rayen_api_key,
            rayen_base_url=settings.embedding.rayen_base_url,
            rayen_model=settings.embedding.rayen_model,
        )
    except Exception as e:
        logger.critical(f"❌ Failed to start Rayen service: {e}")
        print("   (Did you set EMBEDDING_RAYEN_API_KEY in your .env file?)")
        sys.exit(1)

    # 2. Generate Embedding
    # ---------------------
    print("⚡ Generating embedding with Rayen...")
    try:
        # returns List[List[float]], we take the first one
        query_vector = embedding_service.embed_documents([QUERY_TEXT])[0]
    except Exception as e:
        logger.critical(f"❌ Embedding generation failed: {e}")
        sys.exit(1)

    # 3. Connect to Qdrant
    # --------------------
    client = QdrantClient(
        host=settings.qdrant.host,
        port=settings.qdrant.port,
        prefer_grpc=False,
    )
    
    collection_name = settings.qdrant.collection_name
    
    # 4. Perform Search
    # -----------------
    print(f"📡 Searching Qdrant collection: '{collection_name}'...")
    
    search_results = client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=10,  # Top 10 results
        with_payload=True # We need the Title/Link to print
    )

    # 5. Display Results
    # ------------------
    print("\n" + "=" * 60)
    print(f"TOP 10 RESULTS FOR: {QUERY_TEXT}")
    print("=" * 60)

    if not search_results:
        print("⚠️  No results found. Is the collection empty?")
        return

    for i, hit in enumerate(search_results, 1):
        payload = hit.payload
        score = hit.score
        
        # Safe access to payload fields
        source = payload.get('source', 'Unknown')
        title = payload.get('title', 'No Title')
        link = payload.get('link', '#')
        published = payload.get('published_datetime', 'N/A')
        
        print(f"\n{i}. [{score:.4f}] {title}")
        print(f"   Source: {source} | Date: {published}")
        print(f"   🔗 {link}")

if __name__ == "__main__":
    main()