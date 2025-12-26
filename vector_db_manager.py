"""
Vector Database Manager for newsLens

Manages embeddings and Qdrant vector storage.
Updates:
- Configures Indexing schema at creation time.
- Optimizes for low-RAM usage (on_disk_payload).
"""
import logging
import uuid
from typing import List, Tuple
from datetime import datetime
from dataclasses import asdict

from prometheus_client import Summary
from qdrant_client import QdrantClient
from qdrant_client.http import models  # Importing models for Schema definitions
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

try:
    from config import QdrantConfig, EmbeddingConfig
    from schema import NewsData
    from embedding_service import create_embedding_service, EmbeddingService
except ImportError as e:
    print(f"FATAL: Missing required import: {e}")
    raise

# --------------------
# Metrics
# --------------------
QDRANT_UPSERT_LATENCY = Summary(
    'qdrant_upsert_latency_seconds',
    'Latency of Qdrant upsert operations'
)


class VectorDBManager:
    """
    Manages embeddings and Qdrant vector storage.
    
    Features:
    - Auto-creates collection with INDEXES for filtering (Source, Time, Keywords)
    - Optimizes storage for low-resource environments (Disk usage preferred over RAM)
    - Validates dimension compatibility
    """

    def __init__(
        self,
        qdrant_config: QdrantConfig,
        embedding_config: EmbeddingConfig,
        logger: logging.Logger,
        collection_name: str = None
    ):
        self.logger = logger
        self.collection_name = collection_name or qdrant_config.collection_name

        self.qdrant_client = QdrantClient(
            host=qdrant_config.host,
            port=qdrant_config.port,
            grpc_port=qdrant_config.grpc_port,
            timeout=60,
            prefer_grpc=True,
            check_compatibility=False
        )

        # Initialize Embedding Service
        try:
            self.embedding_service = create_embedding_service(
                provider=embedding_config.provider,
                logger=self.logger,
                openai_api_key=embedding_config.openai_api_key,
                openai_model=embedding_config.openai_model,
                ollama_host=embedding_config.ollama_host,
                ollama_model=embedding_config.ollama_model,
                max_workers=embedding_config.max_concurrent_requests,
                chunk_size=embedding_config.chunk_size
            )
            self.embedding_dim = self.embedding_service.get_dimension()
            
            self.logger.info(
                f"✅ VectorDBManager initialized with {self.embedding_service.get_provider_name()} "
                f"(dimension: {self.embedding_dim})"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize embedding service: {e}")
            raise
    
    def ensure_collection_exists(self):
        """
        Creates the collection if it doesn't exist, applying:
        1. Correct Vector Dimension
        2. Low-RAM optimizations (HNSW on disk, Payload on disk)
        3. Search Indexes (Source, Date, Keywords)
        """
        try:
            if not self.qdrant_client.collection_exists(collection_name=self.collection_name):
                self.logger.info(f"🔹 Creating collection '{self.collection_name}'...")
                
                # 1. Create Collection with Low-RAM config
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.embedding_dim,
                        distance=Distance.COSINE
                    ),
                    # Optimization: Store payload on disk to save RAM
                    on_disk_payload=True,
                    # Optimization: Store HNSW graph on disk (slower but very RAM efficient)
                    hnsw_config=models.HnswConfigDiff(
                        on_disk=True,
                        m=16,
                        ef_construct=100
                    )
                )

                # 2. Create Indexes immediately
                # This ensures queries like "Filter by Source" are fast from day one
                self._create_collection_indexes()

                self.logger.info(f"✅ Collection '{self.collection_name}' created and indexed.")
            else:
                # 3. Validation for existing collection
                collection_info = self.qdrant_client.get_collection(self.collection_name)
                existing_dim = collection_info.config.params.vectors.size
                
                if existing_dim != self.embedding_dim:
                    self._handle_dimension_mismatch(existing_dim)
                
                self.logger.info(
                    f"✅ Collection '{self.collection_name}' ready: "
                    f"{collection_info.points_count:,} articles, dim {self.embedding_dim}"
                )
                
        except ValueError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to ensure collection exists: {e}")
            raise

    def _create_collection_indexes(self):
        """Defines and creates the schema indexes."""
        self.logger.info("🔹 Creating Search Indexes...")
        
        # Index definition list
        indexes = [
            # 1. Source: Exact match filtering (e.g. "Only IRNA")
            {"field": "source", "type": models.PayloadSchemaType.KEYWORD},
            
            # 2. Keywords: Array filtering (e.g. "News containing 'Economy'")
            {"field": "keywords", "type": models.PayloadSchemaType.KEYWORD},
            
            # 3. Published Timestamp: Range queries (e.g. "Last 24 hours") - CRITICAL for sorting
            {"field": "published_timestamp", "type": models.PayloadSchemaType.INTEGER},
            
            # 4. Published Datetime: String matching (ISO format)
            {"field": "published_datetime", "type": models.PayloadSchemaType.KEYWORD},
        ]

        for idx in indexes:
            try:
                self.qdrant_client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=idx["field"],
                    field_schema=idx["type"]
                )
                self.logger.info(f"   + Index created: {idx['field']}")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Could not create index for {idx['field']}: {e}")

    def _handle_dimension_mismatch(self, existing_dim):
        """Helper to raise clear error on dimension mismatch"""
        error_msg = (
            f"\n{'='*80}\n"
            f"❌ CRITICAL ERROR: EMBEDDING MODEL MISMATCH\n"
            f"Collection '{self.collection_name}' dim: {existing_dim} != Model dim: {self.embedding_dim}\n"
            f"Please DELETE the collection or REVERT your .env settings.\n"
            f"{'='*80}\n"
        )
        self.logger.critical(error_msg)
        raise ValueError(f"Dimension mismatch: {existing_dim} != {self.embedding_dim}")

    def extract_plain_text(self, news: NewsData) -> str:
        """Extract plain string text from a NewsData object for embedding."""
        title = str(news.title or "")
        summary = str(news.summary or "")
        content = str(news.content or "")
        
        text = f"{title}. {summary}".strip()
        if not text or text == ".":
            text = content
        
        return str(text).replace("\n", " ").strip()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts: return []
        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts: return []

        try:
            vectors = self.embedding_service.embed_documents(non_empty_texts)
            for vector in vectors:
                if len(vector) != self.embedding_dim:
                    raise ValueError(f"Expected dim {self.embedding_dim}, got {len(vector)}")
            return vectors
        except Exception as e:
            self.logger.error(f"Batch embedding failed: {e}")
            raise

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @QDRANT_UPSERT_LATENCY.time()
    def _upsert_points_with_retry(self, points: List[PointStruct]):
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=points,
            wait=True
        )

    def persist_news_batch(self, news_batch: List[NewsData]) -> int:
        if not news_batch: return 0

        articles_to_embed = []
        texts_for_embedding = []

        for news in news_batch:
            embedding_text = self.extract_plain_text(news)
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, news.link))

            if embedding_text:
                articles_to_embed.append((news, point_id, embedding_text))
                texts_for_embedding.append(embedding_text)

        if not articles_to_embed: return 0

        self.logger.info(f"Generating embeddings for batch of {len(texts_for_embedding)} articles.")
        try:
            vectors = self.get_embeddings_batch(texts_for_embedding)
        except Exception:
            return 0

        points = []
        for i, (news, point_id, _) in enumerate(articles_to_embed):
            payload = asdict(news)

            if 'published_datetime' in payload and isinstance(payload['published_datetime'], datetime):
                payload['published_datetime'] = payload['published_datetime'].isoformat()

            # STRICT INTEGER CASTING FOR TIMESTAMP (Crucial for Indexing)
            if 'published_timestamp' in payload and payload['published_timestamp'] is not None:
                try:
                    payload['published_timestamp'] = int(payload['published_timestamp'])
                except (ValueError, TypeError):
                    payload['published_timestamp'] = 0

            try:
                point = PointStruct(id=point_id, vector=vectors[i], payload=payload)
                points.append(point)
            except IndexError:
                continue

        if not points: return 0

        try:
            self.logger.info(f"Upserting {len(points)} points to Qdrant.")
            self._upsert_points_with_retry(points)
            return len(points)
        except Exception as e:
            self.logger.error(f"Upsert failed: {e}")
            return 0