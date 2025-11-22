import logging
import uuid
import os
from typing import List, Dict, Any, Tuple
from datetime import datetime  # Required for type checking
from dataclasses import asdict  # <-- CORRECTED: Import asdict for dataclass conversion

from langchain_openai import OpenAIEmbeddings
from prometheus_client import Summary, start_http_server
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct, Batch
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

try:
    # Assuming config.py, schema.py, QdrantConfig, OpenAIConfig, and NewsData are available
    from config import settings
    from config import QdrantConfig, OpenAIConfig
    from schema import NewsData
except ImportError as e:
    print(f"FATAL: Missing required import: {e}. Ensure config.py and schema.py are available.")
    raise

# --------------------
# Constants and Metrics
# --------------------
EMBEDDING_MODEL = settings.openai.embedding_model_name
EMBEDDING_DIM = settings.openai.embedding_dim
COLLECTION_NAME = settings.qdrant.collection_name

OPENAI_EMBEDDING_LATENCY = Summary('openai_embedding_latency_seconds', 'Latency of OpenAI embedding calls')
QDRANT_UPSERT_LATENCY = Summary('qdrant_upsert_latency_seconds', 'Latency of Qdrant upsert operations')
QDRANT_SEARCH_LATENCY = Summary('qdrant_search_latency_seconds', 'Latency of Qdrant search operations')


class VectorDBManager:
    """
    Manages LangChain OpenAI embeddings and Qdrant vector storage.
    Optimized for batch processing.
    """

    def __init__(self, qdrant_config: QdrantConfig, openai_config: OpenAIConfig, logger: logging.Logger):
        self.logger = logger

        # Qdrant client
        self.qdrant_client = QdrantClient(
            host=qdrant_config.host,
            port=qdrant_config.port,
            grpc_port=qdrant_config.grpc_port
        )

        # LangChain OpenAI embeddings
        if not openai_config.api_key:
            self.logger.warning("OPENAI_API_KEY is missing. Embedding functionality disabled.")
            self.embeddings = None
        else:
            # Set the environment variable for LangChain
            os.environ['OPENAI_API_KEY'] = openai_config.api_key

            # Initialize embeddings (LangChain now reads key from env)
            self.embeddings = OpenAIEmbeddings(model=EMBEDDING_MODEL)

        self.ensure_collection_exists()

        # Start Prometheus metrics server on port 8000
        try:
            start_http_server(8000)
            self.logger.info("Prometheus metrics server started on port 8000.")
        except OSError as e:
            self.logger.warning(f"Failed to start Prometheus server on port 8000: {e}")

    def ensure_collection_exists(self):
        """Ensure the Qdrant collection exists."""
        try:
            if not self.qdrant_client.collection_exists(collection_name=COLLECTION_NAME):
                self.qdrant_client.create_collection(
                    collection_name=COLLECTION_NAME,
                    vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE)
                )
                self.logger.info(f"Collection '{COLLECTION_NAME}' created with dimension {EMBEDDING_DIM}.")
            else:
                self.logger.info(f"Collection '{COLLECTION_NAME}' already exists and is ready.")
        except Exception as e:
            self.logger.error(f"Failed to ensure collection exists: {e}")

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
    @OPENAI_EMBEDDING_LATENCY.time()
    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts using LangChain's bulk method.
        """
        if not self.embeddings:
            raise RuntimeError("Embeddings not initialized. Check OPENAI_API_KEY.")

        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []

        try:
            # This is the key performance improvement: calling LangChain's bulk method
            vectors = self.embeddings.embed_documents(non_empty_texts)

            # Simple validation
            for vector in vectors:
                if len(vector) != EMBEDDING_DIM:
                    raise ValueError(f"Expected embedding dimension {EMBEDDING_DIM}, got {len(vector)} in batch.")

            return vectors
        except Exception as e:
            self.logger.error(f"LangChain batch embedding generation failed: {e}")
            raise

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @QDRANT_UPSERT_LATENCY.time()
    def _upsert_points_with_retry(self, points: List[PointStruct]):
        """Upsert a list of points into Qdrant in a single batch operation."""
        self.qdrant_client.upsert(
            collection_name=COLLECTION_NAME,
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
        Returns the number of articles successfully persisted.
        """
        if not news_batch:
            return 0

        # 1. Extract texts and prepare mapping
        articles_to_embed: List[Tuple[NewsData, str, str]] = []  # (NewsData, unique_id, text)
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

        # 2. Get embeddings in a single batch API call
        self.logger.info(f"Generating embeddings for a batch of {len(texts_for_embedding)} articles.")
        vectors: List[List[float]] = []
        try:
            vectors = self.get_embeddings_batch(texts_for_embedding)
        except Exception as e:
            self.logger.error(f"Failed to get embeddings for batch: {e}")
            return 0  # Fail the entire batch if embedding fails

        # 3. Construct Points
        points: List[PointStruct] = []
        for i, (news, point_id, _) in enumerate(articles_to_embed):

            # 🐛 FIX: Use dataclasses.asdict() instead of Pydantic's model_dump()
            payload = asdict(news)

            # Ensure datetime objects are converted to ISO format for JSON/Qdrant compatibility
            if 'published_datetime' in payload and isinstance(payload['published_datetime'], datetime):
                payload['published_datetime'] = payload['published_datetime'].isoformat()

            # Ensure published_timestamp is stored as string if it's an integer for consistency
            if 'published_timestamp' in payload and payload['published_timestamp'] is not None:
                payload['published_timestamp'] = str(payload['published_timestamp'])

            try:
                point = PointStruct(id=point_id, vector=vectors[i], payload=payload)
                points.append(point)
            except IndexError:
                self.logger.error(f"Vector missing for article {news.link} during point construction.")

        if not points:
            return 0

        # 4. Upsert Points in a single batch Qdrant call
        try:
            self.logger.info(f"Upserting batch of {len(points)} points to Qdrant.")
            self._upsert_points_with_retry(points)
            return len(points)
        except RetryError as e:
            self.logger.critical(f"Failed to upsert batch after retries. Error: {e.last_attempt.exception}")
            return 0
        except Exception as e:
            self.logger.error(f"Unexpected error during Qdrant upsert: {e}")
            return 0
