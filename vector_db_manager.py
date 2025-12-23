"""
Vector Database Manager for newsLens - PRODUCTION FIXED
"""
import logging
import uuid
from typing import List, Tuple, Optional, Any
from datetime import datetime
from dataclasses import asdict

from prometheus_client import Summary, Counter
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

try:
    from config import QdrantConfig, EmbeddingConfig
    from schema import NewsData
    from embedding_service import (
        create_embedding_service, 
        EmbeddingService, 
        EmbeddingValidationError
    )
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
QDRANT_UPSERT_FAILURES = Counter(
    'qdrant_upsert_failures_total',
    'Total number of Qdrant upsert failures',
    ['error_type']
)

class VectorDBManager:
    def __init__(
        self,
        qdrant_config: QdrantConfig,
        embedding_config: EmbeddingConfig,
        logger: logging.Logger,
        collection_name: str = None
    ):
        self.logger = logger
        self.collection_name = collection_name or qdrant_config.collection_name

        # ✅ FIX 1: Force HTTP and increase timeout (Solves _InactiveRpcError)
        self.qdrant_client = QdrantClient(
            host=qdrant_config.host,
            port=qdrant_config.port,
            grpc_port=qdrant_config.grpc_port,
            timeout=100,             
            prefer_grpc=False,       # 🔴 DISABLE gRPC (Force HTTP)
            check_compatibility=False
        )

        # Create embedding service
        try:
            self.embedding_service = create_embedding_service(
                provider=embedding_config.provider,
                logger=self.logger,
                openai_api_key=embedding_config.openai_api_key,
                openai_model=embedding_config.openai_model,
                ollama_host=embedding_config.ollama_host,
                ollama_model=embedding_config.ollama_model,
                rayen_api_key=getattr(embedding_config, 'rayen_api_key', None),
                rayen_base_url=getattr(embedding_config, 'rayen_base_url', None),
                rayen_model=getattr(embedding_config, 'rayen_model', None),
                max_workers=embedding_config.max_concurrent_requests,
                chunk_size=embedding_config.chunk_size
            )
            
            # Retrieve dimension safely
            try:
                self.embedding_dim = self.embedding_service.get_dimension()
            except AttributeError:
                self.logger.warning("Could not get dimension from service attribute, defaulting to 768")
                self.embedding_dim = 768
            
            self.logger.info(
                f"✅ VectorDBManager initialized with {self.embedding_service.get_provider_name()} "
                f"(dimension: {self.embedding_dim})"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to initialize embedding service: {e}")
            raise
    
    def ensure_collection_exists(self):
        """Ensure collection exists and matches dimension."""
        try:
            if not self.qdrant_client.collection_exists(collection_name=self.collection_name):
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.embedding_dim,
                        distance=Distance.COSINE,
                        # ✅ FIX 2: Store vectors on Disk (mmap) to save RAM
                        on_disk=True  
                    )
                )
                self.logger.info(f"✅ Collection '{self.collection_name}' created (dim: {self.embedding_dim}, on_disk: True)")
            else:
                collection_info = self.qdrant_client.get_collection(self.collection_name)
                existing_dim = collection_info.config.params.vectors.size
                
                if existing_dim != self.embedding_dim:
                    raise ValueError(
                        f"Dimension mismatch! Collection: {existing_dim}, Model: {self.embedding_dim}. "
                        "You must DROP the collection or switch models."
                    )
                self.logger.info(f"✅ Collection '{self.collection_name}' ready (points: {collection_info.points_count})")
                
        except Exception as e:
            self.logger.error(f"Failed to ensure collection exists: {e}")
            raise

    def extract_plain_text(self, news: NewsData) -> str:
        """Extract text for embedding."""
        title = str(news.title or "")
        summary = str(news.summary or "")
        content = str(news.content or "")
        text = f"{title}. {summary}".strip()
        if not text or len(text) < 10:
            text = content
        return str(text).replace("\n", " ").strip()

    def _validate_vector_for_qdrant(self, vector: Any, index: int) -> Tuple[bool, Optional[str]]:
        """Strict validation of vector structure."""
        if vector is None:
            return False, f"Vector at index {index} is None"
        
        try:
            if len(vector) != self.embedding_dim:
                return False, f"Dim mismatch: expected {self.embedding_dim}, got {len(vector)}"
        except TypeError:
            return False, f"Vector at index {index} is not iterable (type: {type(vector)})"

        return True, None

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        try:
            return self.embedding_service.embed_documents(texts)
        except Exception as e:
            self.logger.error(f"Batch embedding generation failed: {e}")
            raise

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
    def _upsert_points_with_retry(self, points: List[PointStruct]):
        """Upsert points directly."""
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=points,
            # ✅ FIX 3: Fire and forget (faster, less prone to timeouts)
            wait=False
        )

    def persist_news_batch(self, news_batch: List[NewsData]) -> int:
        if not news_batch:
            return 0

        # 1. Prepare texts
        articles_to_embed = []
        texts_for_embedding = []

        for news in news_batch:
            text = self.extract_plain_text(news)
            if text and len(text) > 5:
                point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, news.link))
                articles_to_embed.append((news, point_id))
                texts_for_embedding.append(text)
            else:
                self.logger.warning(f"Skipping empty article: {news.link}")

        if not articles_to_embed:
            return 0

        # 2. Generate Embeddings
        try:
            vectors = self.get_embeddings_batch(texts_for_embedding)
        except Exception:
            return 0

        if len(vectors) != len(articles_to_embed):
            self.logger.error(f"Count mismatch: {len(vectors)} vectors for {len(articles_to_embed)} articles")
            return 0

        # 3. Construct Points
        points: List[PointStruct] = []
        
        for i, (news, point_id) in enumerate(articles_to_embed):
            raw_vector = vectors[i]
            
            is_valid, error = self._validate_vector_for_qdrant(raw_vector, i)
            if not is_valid:
                self.logger.error(f"Invalid vector for {news.link}: {error}")
                continue

            # Force cast to list[float] to ensure serialization safety
            try:
                safe_vector = [float(x) for x in raw_vector]
            except Exception as e:
                self.logger.error(f"Failed to cast vector to float list: {e}")
                continue

            payload = asdict(news)
            if 'published_datetime' in payload and isinstance(payload['published_datetime'], datetime):
                payload['published_datetime'] = payload['published_datetime'].isoformat()
            if 'keywords' in payload and payload['keywords'] is None:
                payload['keywords'] = []

            points.append(PointStruct(id=point_id, vector=safe_vector, payload=payload))

        if not points:
            return 0

        # 4. Upsert
        try:
            self._upsert_points_with_retry(points)
            self.logger.info(f"✅ Successfully upserted {len(points)} points")
            return len(points)
        except Exception as e:
            self.logger.critical(f"❌ Failed to upsert batch: {e}")
            QDRANT_UPSERT_FAILURES.labels(error_type='upsert_exception').inc()
            return 0

    def health_check(self) -> dict:
        """
        Perform health check on the vector database and embedding service.
        """
        status = {
            'qdrant_connected': False,
            'collection_exists': False,
            'collection_dimension': 0,
            'collection_points': 0,
            'embedding_service': self.embedding_service.get_provider_name(),
            'embedding_dimension': self.embedding_dim,
            'test_embedding_ok': False,
        }
        
        try:
            # Check Qdrant connection
            self.qdrant_client.get_collections()
            status['qdrant_connected'] = True
            
            # Check collection
            if self.qdrant_client.collection_exists(self.collection_name):
                status['collection_exists'] = True
                info = self.qdrant_client.get_collection(self.collection_name)
                status['collection_dimension'] = info.config.params.vectors.size
                status['collection_points'] = info.points_count
            else:
                status['collection_points'] = 0

            # Test embedding generation
            try:
                test_vectors = self.embedding_service.embed_documents(["health check test"])
                if test_vectors and len(test_vectors) > 0 and len(test_vectors[0]) == self.embedding_dim:
                    status['test_embedding_ok'] = True
            except Exception as e:
                status['test_embedding_error'] = str(e)
                
        except Exception as e:
            status['error'] = str(e)
        
        return status