"""
Vector Database Manager for newsLens - FIXED VERSION

Manages embeddings and Qdrant vector storage with support for multiple
embedding providers (OpenAI, Ollama) with concurrent processing.

Key Fixes:
- Removed deprecated Batch() wrapper (causes ResponseHandlingException)
- Added proper timeout configuration
- Chunked large batch uploads to prevent timeouts
- Improved error handling and logging
- Better retry logic with exponential backoff

Features:
- Auto-creates Qdrant collection on first run
- Validates dimension compatibility (fails fast if model changed)
- Batch embedding for performance with concurrent processing
- Retry logic with exponential backoff
- Chunked uploads for large batches
"""
import logging
import uuid
from typing import List, Tuple, Optional
from datetime import datetime
from dataclasses import asdict

from prometheus_client import Summary, start_http_server
from qdrant_client import QdrantClient
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

QDRANT_EMBEDDING_LATENCY = Summary(
    'qdrant_embedding_latency_seconds',
    'Latency of embedding generation'
)


class VectorDBManager:
    """
    Manages embeddings and Qdrant vector storage.
    
    Automatically detects embedding dimension based on configured provider.
    Fails fast if you try to use a different embedding model than what
    the collection was created with.
    
    Supports concurrent embedding generation for improved performance.
    
    FIXED: Compatible with Qdrant client v1.7+ (removed deprecated Batch API)
    """

    def __init__(
        self,
        qdrant_config: QdrantConfig,
        embedding_config: EmbeddingConfig,
        logger: logging.Logger,
        collection_name: str = None,
        chunk_size: int = 100
    ):
        """
        Initialize VectorDBManager with flexible embedding provider.
        
        Args:
            qdrant_config: Qdrant configuration
            embedding_config: Embedding configuration (provider, model, concurrency, etc.)
            logger: Logger instance
            collection_name: Optional collection name (overrides config)
            chunk_size: Maximum points to upsert in single batch (default: 100)
        """
        self.logger = logger
        self.collection_name = collection_name or qdrant_config.collection_name
        self.chunk_size = chunk_size

        # Initialize Qdrant client with proper timeouts
        try:
            self.qdrant_client = QdrantClient(
                host=qdrant_config.host,
                port=qdrant_config.port,
                grpc_port=qdrant_config.grpc_port,
                timeout=60,  # 60 second timeout for HTTP requests
                prefer_grpc=True,  # Use gRPC for better performance with large batches
            )
            self.logger.info(
                f"✅ Qdrant client initialized: {qdrant_config.host}:{qdrant_config.port}"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize Qdrant client: {e}")
            raise

        # Create embedding service based on provider with concurrent processing support
        try:
            self.embedding_service = create_embedding_service(embedding_config)
            
            # Get embedding dimension from the service
            self.embedding_dim = self.embedding_service.get_dimension()
            provider_name = self.embedding_service.get_provider_name()
            
            self.logger.info(
                f"✅ Embedding service initialized: {provider_name} "
                f"(dimension: {self.embedding_dim})"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to initialize embedding service: {e}")
            raise

        # Ensure collection exists with correct dimension
        self.ensure_collection_exists()

        # Start Prometheus metrics server on port 8000
        try:
            start_http_server(8000)
            self.logger.info("✅ Prometheus metrics server started on port 8000")
        except OSError as e:
            self.logger.warning(f"Prometheus server already running or port occupied: {e}")

    def ensure_collection_exists(self):
        """
        Ensure the Qdrant collection exists with the correct embedding dimension.
        
        Behavior:
        - If collection doesn't exist: Creates it with correct dimension
        - If collection exists with correct dimension: Continues normally
        - If collection exists with WRONG dimension: FAILS FAST with clear error
        
        This prevents mixing embeddings from different models, which would
        result in meaningless similarity searches.
        """
        try:
            if not self.qdrant_client.collection_exists(collection_name=self.collection_name):
                # Create new collection
                self.logger.info(f"Creating collection '{self.collection_name}'...")
                self.qdrant_client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.embedding_dim,
                        distance=Distance.COSINE
                    )
                )
                self.logger.info(
                    f"✅ Collection '{self.collection_name}' created successfully"
                )
                self.logger.info(
                    f"   Provider: {self.embedding_service.get_provider_name()}"
                )
                self.logger.info(
                    f"   Dimension: {self.embedding_dim}"
                )
            else:
                # Verify existing collection dimension
                collection_info = self.qdrant_client.get_collection(self.collection_name)
                existing_dim = collection_info.config.params.vectors.size
                
                if existing_dim != self.embedding_dim:
                    # FAIL FAST - dimension mismatch is a critical error
                    error_msg = (
                        f"\n{'='*80}\n"
                        f"❌ CRITICAL ERROR: EMBEDDING MODEL MISMATCH\n"
                        f"{'='*80}\n"
                        f"Collection '{self.collection_name}' was created with dimension {existing_dim}\n"
                        f"but your current embedding model requires dimension {self.embedding_dim}.\n\n"
                        f"This means you changed EMBEDDING_PROVIDER or EMBEDDING_*_MODEL in .env\n"
                        f"after already embedding articles with a different model.\n\n"
                        f"YOU CANNOT MIX EMBEDDING MODELS!\n\n"
                        f"Your options:\n"
                        f"  1. REVERT .env to original embedding settings (recommended)\n"
                        f"  2. DELETE collection and re-embed ALL articles:\n"
                        f"     Delete collection in Qdrant UI or via API\n"
                        f"     (This deletes all {collection_info.points_count:,} embeddings!)\n"
                        f"     Then re-run: python news_historical_embedding_scheduler.py\n\n"
                        f"Details:\n"
                        f"  Collection: {self.collection_name}\n"
                        f"  Existing dimension: {existing_dim}\n"
                        f"  Required dimension: {self.embedding_dim}\n"
                        f"  Current provider: {self.embedding_service.get_provider_name()}\n"
                        f"{'='*80}\n"
                    )
                    self.logger.critical(error_msg)
                    raise ValueError(f"Collection dimension mismatch: {existing_dim} != {self.embedding_dim}")
                
                # Dimension matches - all good
                self.logger.info(
                    f"✅ Collection '{self.collection_name}' ready: "
                    f"{collection_info.points_count:,} articles, "
                    f"dimension {self.embedding_dim}"
                )
                
        except ValueError:
            # Re-raise dimension mismatch errors
            raise
        except Exception as e:
            self.logger.error(f"Failed to ensure collection exists: {e}")
            raise

    def extract_plain_text(self, news: NewsData) -> str:
        """
        Extract plain string text from a NewsData object for embedding.
        
        Priority: title + summary > content
        """
        title = str(news.title or "")
        summary = str(news.summary or "")
        content = str(news.content or "")
        
        # Prefer title + summary, fallback to content
        text = f"{title}. {summary}".strip()
        if not text or text == ".":
            text = content
        
        return str(text).replace("\n", " ").strip()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @QDRANT_EMBEDDING_LATENCY.time()
    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts using the configured provider.
        
        This method now supports concurrent processing for improved performance.
        The concurrency is handled internally by the embedding service.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors
            
        Raises:
            ValueError: If embedding dimension doesn't match expected
            Exception: On embedding generation failure
        """
        if not texts:
            return []

        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []

        try:
            vectors = self.embedding_service.embed_documents(non_empty_texts)

            # Validate dimensions
            for idx, vector in enumerate(vectors):
                if len(vector) != self.embedding_dim:
                    raise ValueError(
                        f"Expected embedding dimension {self.embedding_dim}, "
                        f"got {len(vector)} at index {idx}"
                    )

            return vectors
            
        except Exception as e:
            self.logger.error(f"Batch embedding generation failed: {e}")
            raise

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @QDRANT_UPSERT_LATENCY.time()
    def _upsert_points_with_retry(self, points: List[PointStruct]):
        """
        Upsert a list of points into Qdrant in a single batch operation.
        
        FIXED: Removed deprecated Batch() wrapper that caused ResponseHandlingException
        in Qdrant client v1.7+
        
        Args:
            points: List of PointStruct objects to upsert
            
        Raises:
            Exception: On upsert failure after retries
        """
        try:
            # Direct upsert without Batch() wrapper (compatible with v1.7+)
            self.qdrant_client.upsert(
                collection_name=self.collection_name,
                points=points,  # Pass points directly as list
                wait=True  # Wait for indexing to complete
            )
            
            self.logger.debug(f"✅ Successfully upserted {len(points)} points to Qdrant")
            
        except Exception as e:
            self.logger.error(
                f"Upsert failed: {type(e).__name__}: {e}\n"
                f"Collection: {self.collection_name}, Points: {len(points)}"
            )
            raise

    def persist_news_batch(self, news_batch: List[NewsData]) -> int:
        """
        Persist a batch of NewsData objects into Qdrant.
        
        IMPROVED: Now chunks large batches to prevent timeouts on collections
        with many existing vectors (e.g., 471K+).
        
        Args:
            news_batch: List of NewsData objects to embed and store
            
        Returns:
            Number of articles successfully persisted
        """
        if not news_batch:
            return 0

        # 1. Extract texts and prepare mapping
        articles_to_embed: List[Tuple[NewsData, str, str]] = []
        texts_for_embedding: List[str] = []

        for news in news_batch:
            embedding_text = self.extract_plain_text(news)
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, news.link))

            if embedding_text:
                articles_to_embed.append((news, point_id, embedding_text))
                texts_for_embedding.append(embedding_text)
            else:
                self.logger.warning(
                    f"Skipping article with link '{news.link}' due to empty content"
                )

        if not articles_to_embed:
            self.logger.warning("No valid articles to embed in batch")
            return 0

        # 2. Get embeddings in a single batch with concurrent processing
        self.logger.info(
            f"Generating embeddings for batch of {len(texts_for_embedding)} articles"
        )
        
        vectors: List[List[float]] = []
        try:
            vectors = self.get_embeddings_batch(texts_for_embedding)
        except Exception as e:
            self.logger.error(f"Failed to get embeddings for batch: {e}")
            return 0

        if len(vectors) != len(articles_to_embed):
            self.logger.error(
                f"Vector count mismatch: got {len(vectors)}, expected {len(articles_to_embed)}"
            )
            return 0

        # 3. Construct Points
        points: List[PointStruct] = []
        
        for i, (news, point_id, _) in enumerate(articles_to_embed):
            # Convert dataclass to dict
            payload = asdict(news)

            # Ensure datetime objects are JSON-compatible
            if 'published_datetime' in payload and isinstance(payload['published_datetime'], datetime):
                payload['published_datetime'] = payload['published_datetime'].isoformat()

            # Ensure published_timestamp is string
            if 'published_timestamp' in payload and payload['published_timestamp'] is not None:
                payload['published_timestamp'] = str(payload['published_timestamp'])

            try:
                point = PointStruct(
                    id=point_id,
                    vector=vectors[i],
                    payload=payload
                )
                points.append(point)
                
            except IndexError:
                self.logger.error(
                    f"Vector missing for article {news.link} at index {i} "
                    f"during point construction"
                )
                continue

        if not points:
            self.logger.warning("No valid points constructed from batch")
            return 0

        # 4. Upsert Points in chunks to avoid timeouts
        total_inserted = 0
        num_chunks = (len(points) - 1) // self.chunk_size + 1
        
        self.logger.info(
            f"Upserting {len(points)} points in {num_chunks} chunk(s) "
            f"(chunk_size={self.chunk_size})"
        )
        
        for i in range(0, len(points), self.chunk_size):
            chunk = points[i:i + self.chunk_size]
            chunk_num = i // self.chunk_size + 1
            
            try:
                self.logger.info(
                    f"Upserting chunk {chunk_num}/{num_chunks} ({len(chunk)} points) "
                    f"to Qdrant..."
                )
                
                self._upsert_points_with_retry(chunk)
                total_inserted += len(chunk)
                
                self.logger.info(
                    f"✅ Chunk {chunk_num}/{num_chunks} inserted successfully "
                    f"({total_inserted}/{len(points)} total)"
                )
                
            except RetryError as e:
                self.logger.critical(
                    f"❌ Chunk {chunk_num}/{num_chunks} failed after all retries: "
                    f"{e.last_attempt.exception()}"
                )
                # Continue with next chunk instead of failing entire batch
                continue
                
            except Exception as e:
                self.logger.error(
                    f"❌ Unexpected error during chunk {chunk_num}/{num_chunks} upsert: {e}"
                )
                # Continue with next chunk
                continue
        
        # 5. Log final results
        if total_inserted < len(points):
            self.logger.warning(
                f"⚠️  Partial success: {total_inserted}/{len(points)} points inserted"
            )
        else:
            self.logger.info(
                f"✅ All {total_inserted} points inserted successfully"
            )
        
        return total_inserted

    def get_collection_stats(self) -> dict:
        """
        Get statistics about the current collection.
        
        Returns:
            Dictionary with collection statistics
        """
        try:
            collection_info = self.qdrant_client.get_collection(self.collection_name)
            
            return {
                'collection_name': self.collection_name,
                'points_count': collection_info.points_count,
                'segments_count': collection_info.segments_count,
                'vectors_count': collection_info.vectors_count,
                'indexed_vectors_count': collection_info.indexed_vectors_count,
                'status': collection_info.status,
                'dimension': collection_info.config.params.vectors.size,
                'distance': collection_info.config.params.vectors.distance,
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get collection stats: {e}")
            return {}

    def health_check(self) -> bool:
        """
        Check if Qdrant is healthy and collection is accessible.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Check if collection exists
            exists = self.qdrant_client.collection_exists(self.collection_name)
            
            if not exists:
                self.logger.error(f"Collection '{self.collection_name}' does not exist")
                return False
            
            # Try to get collection info
            self.qdrant_client.get_collection(self.collection_name)
            
            self.logger.info(f"✅ Health check passed for collection '{self.collection_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False