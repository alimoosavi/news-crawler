"""
Abstract Embedding Service with Multiple Provider Implementations
Supports: OpenAI API, Ollama (local models)
"""
import logging
from abc import ABC, abstractmethod
from typing import List
import os

from prometheus_client import Summary

# Metrics
EMBEDDING_LATENCY = Summary('embedding_generation_latency_seconds', 'Latency of embedding generation', ['provider'])


class EmbeddingService(ABC):
    """Abstract base class for embedding services"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
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
    """OpenAI API-based embedding service using LangChain"""
    
    def __init__(self, api_key: str, model_name: str, logger: logging.Logger):
        super().__init__(logger)
        
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
            f"✅ OpenAI Embedding Service initialized: model={model_name}, dim={self.dimension}"
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
        """Generate embeddings using OpenAI API"""
        if not texts:
            return []
        
        # Filter out empty strings
        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []
        
        try:
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
    """Ollama local model embedding service"""
    
    def __init__(self, host: str, model_name: str, logger: logging.Logger):
        super().__init__(logger)
        
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
            f"✅ Ollama Embedding Service initialized: model={model_name}, dim={self.dimension}"
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
    
    @EMBEDDING_LATENCY.labels(provider='ollama').time()
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Ollama"""
        if not texts:
            return []
        
        # Filter out empty strings
        non_empty_texts = [text for text in texts if text.strip()]
        if not non_empty_texts:
            return []
        
        embeddings = []
        
        try:
            # Ollama processes one at a time (could be parallelized in future)
            for text in non_empty_texts:
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
                
                embeddings.append(embedding)
            
            return embeddings
            
        except Exception as e:
            self.logger.error(f"Ollama embedding generation failed: {e}")
            raise
    
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
    
    Returns:
        EmbeddingService instance
    
    Raises:
        ValueError: If provider is unknown or required params are missing
    """
    provider = provider.lower()
    
    if provider == "openai":
        if not openai_api_key or not openai_model:
            raise ValueError("OpenAI provider requires api_key and model_name")
        return OpenAIEmbeddingService(openai_api_key, openai_model, logger)
    
    elif provider == "ollama":
        if not ollama_host or not ollama_model:
            raise ValueError("Ollama provider requires host and model_name")
        return OllamaEmbeddingService(ollama_host, ollama_model, logger)
    
    else:
        raise ValueError(
            f"Unknown embedding provider: {provider}. "
            f"Supported providers: 'openai', 'ollama'"
        )