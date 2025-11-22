from qdrant_client import QdrantClient
from config import settings

# --- Config ---
COLLECTION_NAME = settings.qdrant.collection_name
FIELD_NAME = "keywords"  # the field you want to remove index from

# --- Connect to Qdrant ---
client = QdrantClient(
    host=settings.qdrant.host,
    port=settings.qdrant.port,
    check_compatibility=False
)

try:
    print(f"Attempting to remove index on field '{FIELD_NAME}' in collection '{COLLECTION_NAME}'...")
    client.delete_payload_index(
        collection_name=COLLECTION_NAME,
        field_name=FIELD_NAME,
    )
    print(f"SUCCESS: Index on field '{FIELD_NAME}' removed from collection '{COLLECTION_NAME}'.")
except Exception as e:
    print(f"FAILED: Could not remove index. Error: {e}")
