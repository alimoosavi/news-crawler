"""
Optimized Abstract Embedding Service with Concurrent Processing
Supports: OpenAI API, Ollama (local models) with parallel batch processing
"""
import logging
from abc import ABC, abstractmethod
from typing import List
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from prometheus_client import Summary, Histogram

# Metrics
EMBEDDING_LATENCY = Summary(
    'embedding_generation_latency_seconds', 
    'Latency of embedding generation', 
    ['provider']
)
EMBEDDING_BATCH_SIZE = Histogram(
    'embedding_batch_size',
    'Number of texts in embedding batch',
    ['provider'],
    buckets=[1, 5, 10, 20, 50, 100]
)
CONCURRENT_REQUESTS = Histogram(
    'embedding_concurrent_requests',
    'Number of concurrent embedding requests',
    ['provider'],
    buckets=[1, 2, 5, 10, 20]
)


class EmbeddingService(ABC):
    """Abstract base class for embedding services"""
    
    def __init__(self, logger: logging.Logger, max_workers: int = 10, chunk_size: int = 5):
        self.logger = logger
        self.max_workers = max_workers
        self.chunk_size = chunk_size
    
    @abstractmethod
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts.
        
        Args:
            texts: List of text strings to embed
            
        Returns:
            List of embedding vectors
        """
        pass
    
    @abstractmethod
    def get_dimension(self) -> int:
        """Return the embedding dimension for this provider"""
        pass
    
    @abstractmethod
    def get_provider_name(self) -> str:
        """Return the provider name for logging/metrics"""
        pass


class OpenAIEmbeddingService(EmbeddingService):
    """OpenAI API-based embedding service with batch optimization"""
    
    def __init__(
        self, 
        api_key: str, 
        model_name: str, 
        logger: logging.Logger,
        max_workers: int = 10,
        chunk_size: int = 5
    ):
        super().__init__(logger, max_workers, chunk_size)
        
        if not api_key:
            raise ValueError("OpenAI API key is required for OpenAIEmbeddingService")
        
        # Set environment variable for LangChain
        os.environ['OPENAI_API_KEY'] = api_key
        
        # Import here to avoid dependency if not using OpenAI
        try:
            from langchain_openai import OpenAIEmbeddings
        except ImportError:
            raise ImportError(
                "langchain-openai not installed. Run: pip install langchain-openai"
            )
        
        self.model_name = model_name
        self.embeddings = OpenAIEmbeddings(model=model_name)
        
        # Determine dimension
        self.dimension = self._get_dimension_from_model()
        
        self.logger.info(
            f"✅ OpenAI Embedding Service initialized: model={model_name}, "
            f"dim={self.dimension}, max_workers={max_workers}"
        )
    
    def _get_dimension_from_model(self) -> int:
        """Get embedding dimension based on OpenAI model name"""
        dims = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }
        return dims.get(self.model_name, 1536)
    
    @EMBEDDING_LATENCY.labels(provider='openai').time()
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings using OpenAI API with batch processing.
        OpenAI API supports batch requests natively, so we use that.
        """
        if not texts:
            return []
        
        # Filter out empty strings
        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []
        
        EMBEDDING_BATCH_SIZE.labels(provider='openai').observe(len(non_empty_texts))
        
        try:
            # OpenAI API handles batching efficiently internally
            vectors = self.embeddings.embed_documents(non_empty_texts)
            
            # Validate dimensions
            for vector in vectors:
                if len(vector) != self.dimension:
                    raise ValueError(
                        f"Expected dimension {self.dimension}, got {len(vector)}"
                    )
            
            return vectors
        except Exception as e:
            self.logger.error(f"OpenAI embedding generation failed: {e}")
            raise
    
    def get_dimension(self) -> int:
        return self.dimension
    
    def get_provider_name(self) -> str:
        return f"openai-{self.model_name}"


class OllamaEmbeddingService(EmbeddingService):
    """Ollama local model embedding service with concurrent processing"""
    
    def __init__(
        self, 
        host: str, 
        model_name: str, 
        logger: logging.Logger,
        max_workers: int = 10,
        chunk_size: int = 5
    ):
        super().__init__(logger, max_workers, chunk_size)
        
        self.host = host
        self.model_name = model_name
        
        # Import ollama library
        try:
            import ollama
            self.ollama = ollama
        except ImportError:
            raise ImportError(
                "ollama library not installed. Run: pip install ollama"
            )
        
        # Verify Ollama is running
        try:
            self.ollama.list()
            self.logger.info(f"✅ Ollama is running at {host}")
        except Exception as e:
            raise ConnectionError(
                f"Cannot connect to Ollama at {host}. "
                f"Ensure Ollama is running: ollama serve\nError: {e}"
            )
        
        # Verify model exists
        self._ensure_model_exists()
        
        # Determine dimension by making a test embedding
        self.dimension = self._detect_dimension()
        
        self.logger.info(
            f"✅ Ollama Embedding Service initialized: model={model_name}, "
            f"dim={self.dimension}, max_workers={max_workers}, chunk_size={chunk_size}"
        )
    
    def _ensure_model_exists(self):
        """Check if model exists, pull if not"""
        try:
            models = self.ollama.list()
            model_names = [m['name'] for m in models.get('models', [])]
            
            # Check if our model is in the list
            model_exists = any(
                self.model_name in name or name.startswith(self.model_name)
                for name in model_names
            )
            
            if not model_exists:
                self.logger.warning(
                    f"⚠️  Model {self.model_name} not found. Pulling..."
                )
                self.ollama.pull(self.model_name)
                self.logger.info(f"✅ Model {self.model_name} downloaded")
        except Exception as e:
            raise RuntimeError(f"Error checking/pulling Ollama model: {e}")
    
    def _detect_dimension(self) -> int:
        """Detect embedding dimension by generating a test embedding"""
        try:
            test_response = self.ollama.embeddings(
                model=self.model_name,
                prompt="test"
            )
            test_embedding = test_response['embedding']
            return len(test_embedding)
        except Exception as e:
            self.logger.warning(
                f"Could not detect dimension via test embedding: {e}. Using fallback."
            )
            # Fallback dimensions based on known models
            fallback_dims = {
                "bge-m3": 1024,
                "multilingual-e5-base": 768,
                "multilingual-e5-large": 1024,
                "nomic-embed-text": 768,
                "mxbai-embed-large": 1024,
            }
            for model_key, dim in fallback_dims.items():
                if model_key in self.model_name:
                    return dim
            return 1024  # Default fallback
    
    def _embed_single(self, text: str) -> List[float]:
        """Generate embedding for a single text"""
        try:
            response = self.ollama.embeddings(
                model=self.model_name,
                prompt=text
            )
            embedding = response['embedding']
            
            # Validate dimension
            if len(embedding) != self.dimension:
                raise ValueError(
                    f"Expected dimension {self.dimension}, got {len(embedding)}"
                )
            
            return embedding
        except Exception as e:
            self.logger.error(f"Failed to embed text: {e}")
            raise
    
    @EMBEDDING_LATENCY.labels(provider='ollama').time()
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings using Ollama with concurrent processing.
        
        This method splits the texts into chunks and processes them
        concurrently using ThreadPoolExecutor for significant speedup.
        """
        if not texts:
            return []
        
        # Filter out empty strings
        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []
        
        EMBEDDING_BATCH_SIZE.labels(provider='ollama').observe(len(non_empty_texts))
        
        start_time = time.time()
        
        # Create a mapping to preserve order
        text_to_index = {i: text for i, text in enumerate(non_empty_texts)}
        embeddings = [None] * len(non_empty_texts)
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(self._embed_single, text): idx
                for idx, text in text_to_index.items()
            }
            
            CONCURRENT_REQUESTS.labels(provider='ollama').observe(len(future_to_index))
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    embedding = future.result()
                    embeddings[idx] = embedding
                    completed += 1
                    
                    # Log progress for large batches
                    if completed % 10 == 0 or completed == len(non_empty_texts):
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        self.logger.debug(
                            f"Progress: {completed}/{len(non_empty_texts)} "
                            f"({rate:.1f} emb/s)"
                        )
                except Exception as e:
                    self.logger.error(f"Failed to get embedding for index {idx}: {e}")
                    raise
        
        elapsed = time.time() - start_time
        rate = len(non_empty_texts) / elapsed if elapsed > 0 else 0
        self.logger.info(
            f"✅ Generated {len(non_empty_texts)} embeddings in {elapsed:.2f}s "
            f"({rate:.1f} emb/s)"
        )
        
        return embeddings
    
    def get_dimension(self) -> int:
        return self.dimension
    
    def get_provider_name(self) -> str:
        return f"ollama-{self.model_name}"


def create_embedding_service(
    provider: str,
    logger: logging.Logger,
    # OpenAI params
    openai_api_key: str = None,
    openai_model: str = None,
    # Ollama params
    ollama_host: str = None,
    ollama_model: str = None,
    # Concurrency params
    max_workers: int = 10,
    chunk_size: int = 5,
) -> EmbeddingService:
    """
    Factory function to create the appropriate embedding service.
    
    Args:
        provider: 'openai' or 'ollama'
        logger: Logger instance
        openai_api_key: OpenAI API key (if provider='openai')
        openai_model: OpenAI model name (if provider='openai')
        ollama_host: Ollama host URL (if provider='ollama')
        ollama_model: Ollama model name (if provider='ollama')
        max_workers: Maximum concurrent workers for parallel processing
        chunk_size: Number of texts per chunk (currently unused, for future optimization)
    
    Returns:
        EmbeddingService instance
    
    Raises:
        ValueError: If provider is unknown or required params are missing
    """
    provider = provider.lower()
    
    if provider == "openai":
        if not openai_api_key or not openai_model:
            raise ValueError("OpenAI provider requires api_key and model_name")
        return OpenAIEmbeddingService(
            openai_api_key, 
            openai_model, 
            logger,
            max_workers=max_workers,
            chunk_size=chunk_size
        )
    
    elif provider == "ollama":
        if not ollama_host or not ollama_model:
            raise ValueError("Ollama provider requires host and model_name")
        return OllamaEmbeddingService(
            ollama_host, 
            ollama_model, 
            logger,
            max_workers=max_workers,
            chunk_size=chunk_size
        )
    
    else:
        raise ValueError(
            f"Unknown embedding provider: {provider}. "
            f"Supported providers: 'openai', 'ollama'"
        )