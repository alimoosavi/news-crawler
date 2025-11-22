from qdrant_client import QdrantClient, models
from config import settings

COLLECTION_NAME = settings.qdrant.collection_name

client = QdrantClient(
    host=settings.qdrant.host,
    port=settings.qdrant.port,
    check_compatibility=False
)

# Create a keyword index for the "keywords" field
client.create_payload_index(
    collection_name=COLLECTION_NAME,
    field_name="keywords",
    field_schema=models.PayloadSchemaType.KEYWORD,  # <-- direct schema type
    wait=True
)

print("✅ Keyword index created on 'keywords' field.")
