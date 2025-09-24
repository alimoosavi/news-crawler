import logging
from typing import List, Dict
import uuid

from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from prometheus_client import Summary, start_http_server

# Assume these are defined elsewhere
from config import QdrantConfig, OpenAIConfig
from schema import NewsData

# --------------------
# Constants and Metrics
# --------------------
EMBEDDING_DIM = 1536
COLLECTION_NAME = "news_articles"

# Initialize Prometheus metrics
OPENAI_LATENCY = Summary('openai_embedding_latency_seconds', 'Latency of OpenAI embedding calls')
QDRANT_UPSERT_LATENCY = Summary('qdrant_upsert_latency_seconds', 'Latency of Qdrant upsert operations')
QDRANT_SEARCH_LATENCY = Summary('qdrant_search_latency_seconds', 'Latency of Qdrant search operations')


class VectorDBManager:
    """
    Manages connections to OpenAI for embeddings and Qdrant for vector storage/search,
    with added retry and monitoring capabilities.
    """

    def __init__(self, qdrant_config: QdrantConfig, openai_config: OpenAIConfig, logger: logging.Logger):
        self.qdrant_client = QdrantClient(host=qdrant_config.host, port=qdrant_config.port)
        self.openai_client = OpenAI(api_key=openai_config.api_key)
        self.logger = logger
        self.ensure_collection_exists()
        # Start a simple Prometheus HTTP server to expose metrics
        try:
            start_http_server(8000)
            self.logger.info("Prometheus metrics server started on port 8000.")
        except OSError as e:
            self.logger.warning(f"Failed to start Prometheus server, port 8000 might be in use: {e}")

    def ensure_collection_exists(self):
        """
        Creates the Qdrant collection if it doesn't already exist.
        """
        try:
            if not self.qdrant_client.collection_exists(collection_name=COLLECTION_NAME):
                self.qdrant_client.create_collection(
                    collection_name=COLLECTION_NAME,
                    vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE)
                )
                self.logger.info(f"Collection '{COLLECTION_NAME}' created in Qdrant.")
            else:
                self.logger.info(f"Collection '{COLLECTION_NAME}' already exists.")
        except Exception as e:
            self.logger.error(f"Failed to ensure Qdrant collection exists: {e}")

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @OPENAI_LATENCY.time()
    def get_embedding(self, text: str) -> List[float]:
        """
        Generates a vector embedding for the given text using OpenAI.
        Includes retry logic for transient failures and tracks latency.
        """
        if not text:
            return []
        try:
            response = self.openai_client.embeddings.create(
                input=text,
                model="text-embedding-3-small"
            )
            return response.data[0].embedding
        except Exception as e:
            self.logger.error(f"OpenAI embedding generation failed: {e}")
            raise  # Re-raise the exception to trigger the retry decorator

    @retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
    @QDRANT_UPSERT_LATENCY.time()
    def _upsert_point_with_retry(self, point: PointStruct):
        """
        Internal method to handle upserting a single point with retry logic.
        """
        self.qdrant_client.upsert(
            collection_name=COLLECTION_NAME,
            points=[point]
        )

    def persist_news_vector(self, news: NewsData, vector: List[float]):
        """
        Persists a single news vector and its payload to Qdrant.
        The payload includes original news data for easy retrieval later.
        Includes robust error handling.
        """
        if not vector:
            self.logger.warning(f"Skipping persistence for '{news.link}' due to empty vector.")
            return

        try:
            point = PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_URL, news.link)),
                vector=vector,
                payload={
                    "source": news.source,
                    "title": news.title,
                    "link": news.link,
                    "published_datetime": news.published_datetime.isoformat(),
                    "summary": news.summary,
                    "keywords": news.keywords,
                    "entities": news.entities,
                }
            )
            self.logger.info(f"Attempting to persist vector for news article: {news.link}")
            self._upsert_point_with_retry(point)
            self.logger.info(f"Successfully persisted vector for news article: {news.link}")
        except RetryError as e:
            self.logger.critical(
                f"Failed to persist vector for '{news.link}' after multiple retries. This item should be sent to a Dead-Letter Queue (DLQ). Last error: {e.last_attempt.exception()}")
        except Exception as e:
            self.logger.error(f"Unexpected error during persistence for '{news.link}': {e}")

    @QDRANT_SEARCH_LATENCY.time()
    def search_news_vectors(self, query_text: str, limit: int = 5) -> List[Dict]:
        """
        Generates an embedding for a query and searches Qdrant for similar news.
        Includes retry logic for transient failures and tracks latency.
        """
        try:
            query_vector = self.get_embedding(query_text)
            if not query_vector:
                return []

            search_results = self.qdrant_client.search(
                collection_name=COLLECTION_NAME,
                query_vector=query_vector,
                limit=limit,
                with_payload=True,
            )
            return [{"score": result.score, **result.payload} for result in search_results]
        except Exception as e:
            self.logger.error(f"Qdrant search failed: {e}")
            return []