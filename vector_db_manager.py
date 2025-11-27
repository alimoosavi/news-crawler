"""
Vector Database Manager for newsLens

Manages embeddings and Qdrant vector storage with support for multiple
embedding providers (OpenAI, Ollama).

Key Features:
- Auto-creates Qdrant collection on first run
- Validates dimension compatibility (fails fast if model changed)
- Batch embedding for performance
- Retry logic with exponential backoff
"""
import logging
import uuid
from typing import List, Tuple
from datetime import datetime
from dataclasses import asdict

from prometheus_client import Summary, start_http_server
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Batch
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
    
    Automatically detects embedding dimension based on configured provider.
    Fails fast if you try to use a different embedding model than what
    the collection was created with.
    """

    def __init__(
        self,
        qdrant_config: QdrantConfig,
        embedding_config: EmbeddingConfig,
        logger: logging.Logger,
        collection_name: str = None
    ):
        """
        Initialize VectorDBManager with flexible embedding provider.
        
        Args:
            qdrant_config: Qdrant configuration
            embedding_config: Embedding configuration (provider, model, etc.)
            logger: Logger instance
            collection_name: Optional collection name (overrides config)
        """
        self.logger = logger
        self.collection_name = collection_name or qdrant_config.collection_name

        # Initialize Qdrant client
        self.qdrant_client = QdrantClient(
            host=qdrant_config.host,
            port=qdrant_config.port,
            grpc_port=qdrant_config.grpc_port
        )

        # Create embedding service based on provider
        try:
            self.embedding_service = create_embedding_service(
                provider=embedding_config.provider,
                logger=logger,
                openai_api_key=embedding_config.openai_api_key,
                openai_model=embedding_config.openai_model,
                ollama_host=embedding_config.ollama_host,
                ollama_model=embedding_config.ollama_model,
            )
            
            # Get embedding dimension from the service
            self.embedding_dim = self.embedding_service.get_dimension()
            provider_name = self.embedding_service.get_provider_name()
            
            self.logger.info(
                f"✅ VectorDBManager initialized with {provider_name} "
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
            self.logger.info("Prometheus metrics server started on port 8000.")
        except OSError as e:
            self.logger.warning(f"Failed to start Prometheus server on port 8000: {e}")

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
                        f"     python delete_collection.py\n"
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
        """Extract plain string text from a NewsData object for embedding."""
        title = str(news.title or "")
        summary = str(news.summary or "")
        content = str(news.content or "")
        
        # Prefer title + summary, fallback to content
        text = f"{title}. {summary}".strip()
        if not text or text == ".":
            text = content
        
        return str(text).replace("\n", " ").strip()

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts using the configured provider.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors
        """
        if not texts:
            return []

        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []

        try:
            vectors = self.embedding_service.embed_documents(non_empty_texts)

            # Validate dimensions
            for vector in vectors:
                if len(vector) != self.embedding_dim:
                    raise ValueError(
                        f"Expected embedding dimension {self.embedding_dim}, "
                        f"got {len(vector)} in batch."
                    )

            return vectors
        except Exception as e:
            self.logger.error(f"Batch embedding generation failed: {e}")
            raise

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @QDRANT_UPSERT_LATENCY.time()
    def _upsert_points_with_retry(self, points: List[PointStruct]):
        """Upsert a list of points into Qdrant in a single batch operation."""
        self.qdrant_client.upsert(
            collection_name=self.collection_name,
            points=Batch(
                ids=[p.id for p in points],
                vectors=[p.vector for p in points],
                payloads=[p.payload for p in points]
            ),
            wait=True
        )

    def persist_news_batch(self, news_batch: List[NewsData]) -> int:
        """
        Persist a batch of NewsData objects into Qdrant.
        
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
                self.logger.warning(f"Skipping article with link '{news.link}' due to empty content.")

        if not articles_to_embed:
            return 0

        # 2. Get embeddings in a single batch
        self.logger.info(f"Generating embeddings for batch of {len(texts_for_embedding)} articles.")
        vectors: List[List[float]] = []
        try:
            vectors = self.get_embeddings_batch(texts_for_embedding)
        except Exception as e:
            self.logger.error(f"Failed to get embeddings for batch: {e}")
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
                point = PointStruct(id=point_id, vector=vectors[i], payload=payload)
                points.append(point)
            except IndexError:
                self.logger.error(f"Vector missing for article {news.link} during point construction.")

        if not points:
            return 0

        # 4. Upsert Points in a single batch
        try:
            self.logger.info(f"Upserting batch of {len(points)} points to Qdrant.")
            self._upsert_points_with_retry(points)
            return len(points)
        except RetryError as e:
            self.logger.critical(f"Failed to upsert batch after retries: {e.last_attempt.exception}")
            return 0
        except Exception as e:
            self.logger.error(f"Unexpected error during Qdrant upsert: {e}")
            return 0