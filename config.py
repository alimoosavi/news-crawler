from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

# Load environment variables from .env
load_dotenv()


# ---------------------------
# Selenium Config
# ---------------------------
class SeleniumConfig(BaseSettings):
    hub_url: str = Field(default="http://localhost:4444/wd/hub")
    display: str = Field(default=":99")
    node_max_sessions: int = Field(default=10)
    node_override_max_sessions: bool = Field(default=True)
    node_session_timeout: int = Field(default=300)

    class Config:
        env_prefix = "SE_"
        case_sensitive = False


# ---------------------------
# Crawler Config
# ---------------------------
class CrawlerConfig(BaseSettings):
    bulk_size: int = Field(default=20)
    max_workers: int = Field(default=5)
    sleep_interval: int = Field(default=30)
    max_retries: int = Field(default=3)
    retry_delay: int = Field(default=10)

    class Config:
        env_prefix = "CRAWLER_"
        case_sensitive = False


# ---------------------------
# App Config
# ---------------------------
class AppConfig(BaseSettings):
    log_level: str = Field(default="INFO")
    debug: bool = Field(default=False)

    class Config:
        env_prefix = "APP_"
        case_sensitive = False


# ---------------------------
# Redpanda Config (Kafka-compatible)
# ---------------------------
class RedpandaConfig(BaseSettings):
    version: str = Field(default="v23.2.2")
    smp: int = Field(default=1)
    internal_port: int = Field(default=9092)
    external_port: int = Field(default=19092)
    admin_port: int = Field(default=9644)

    # Topics
    news_links_topic: str = Field(default="news_links")
    news_content_topic: str = Field(default="news_contents")

    class Config:
        env_prefix = "REDPANDA_"
        case_sensitive = False


# ---------------------------
# Redpanda Console Config
# ---------------------------
class RedpandaConsoleConfig(BaseSettings):
    version: str = Field(default="v2.3.0")
    port: int = Field(default=18080)

    class Config:
        env_prefix = "REDPANDA_CONSOLE_"
        case_sensitive = False


# ---------------------------
# Redis Config
# ---------------------------
class RedisConfig(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=6379)
    db: int = Field(default=0)

    class Config:
        env_prefix = "REDIS_"
        case_sensitive = False


# ---------------------------
# Qdrant Vector DB Config
# ---------------------------
class QdrantConfig(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=6333)
    grpc_port: int = Field(default=6334)
    log_level: str = Field(default="INFO")
    collection_name: str = Field(default="news")

    class Config:
        env_prefix = "QDRANT_"
        case_sensitive = False


# ---------------------------
# Embedding Config (Provider-agnostic)
# ---------------------------
class EmbeddingConfig(BaseSettings):
    """
    Unified embedding configuration supporting multiple providers.
    
    Providers:
    - 'openai': Use OpenAI API (requires OPENAI_API_KEY)
    - 'ollama': Use local Ollama models (requires Ollama running)
    
    Performance Settings:
    - max_concurrent_requests: Controls parallelism (higher = faster but more CPU)
    - chunk_size: Batch size for processing (affects memory usage)
    
    Model dimensions:
    - text-embedding-3-small: 1536
    - text-embedding-3-large: 3072
    - bge-m3: 1024
    - mxbai-embed-large: 1024
    - multilingual-e5-base: 768
    - multilingual-e5-large: 1024
    """
    
    # Provider selection
    provider: str = Field(
        default="openai",
        description="Embedding provider: 'openai' or 'ollama'"
    )
    
    # OpenAI settings
    openai_api_key: str = Field(default="", description="OpenAI API key")
    openai_model: str = Field(
        default="text-embedding-3-small",
        description="OpenAI embedding model name"
    )
    
    # Ollama settings
    ollama_host: str = Field(
        default="http://localhost:11434",
        description="Ollama API endpoint"
    )
    ollama_model: str = Field(
        default="bge-m3",
        description="Ollama model name (bge-m3, multilingual-e5-base, etc.)"
    )
    
    # Concurrent processing settings (NEW)
    max_concurrent_requests: int = Field(
        default=10,
        description="Maximum concurrent embedding requests (for parallel processing)"
    )
    chunk_size: int = Field(
        default=5,
        description="Number of texts to process in each concurrent chunk"
    )
    
    # Auto-computed embedding dimension based on provider and model
    @property
    def embedding_dim(self) -> int:
        """Automatically determine embedding dimension based on provider and model"""
        if self.provider == "openai":
            return self._get_openai_dimension()
        elif self.provider == "ollama":
            return self._get_ollama_dimension()
        else:
            raise ValueError(f"Unknown embedding provider: {self.provider}")
    
    def _get_openai_dimension(self) -> int:
        """Get OpenAI model embedding dimension"""
        openai_dims = {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }
        return openai_dims.get(self.openai_model, 1536)
    
    def _get_ollama_dimension(self) -> int:
        """Get Ollama model embedding dimension"""
        ollama_dims = {
            "bge-m3": 1024,
            "multilingual-e5-base": 768,
            "multilingual-e5-large": 1024,
            "nomic-embed-text": 768,
            "mxbai-embed-large": 1024,
            "all-minilm": 384,
        }
        # Try exact match first
        if self.ollama_model in ollama_dims:
            return ollama_dims[self.ollama_model]
        
        # Try partial match (for community models like zylonai/multilingual-e5-large)
        for model_key, dim in ollama_dims.items():
            if model_key in self.ollama_model:
                return dim
        
        # Default fallback
        return 1024

    class Config:
        env_prefix = "EMBEDDING_"
        case_sensitive = False


# ---------------------------
# OpenAI Config (Legacy - kept for backward compatibility)
# ---------------------------
class OpenAIConfig(BaseSettings):
    """Legacy OpenAI config - prefer using EmbeddingConfig instead"""
    api_key: str = Field(default="")
    embedding_model_name: str = Field(default="text-embedding-3-small")
    embedding_dim: int = Field(default=1536)

    class Config:
        env_prefix = "OPENAI_"
        case_sensitive = False


# ---------------------------
# Spark Settings
# ---------------------------
class SparkConfig(BaseSettings):
    version: str = Field(default="3.2.1")
    master_url: str = Field(default="spark://spark-master:7077")
    worker_cores: int = Field(default=1)
    worker_memory: str = Field(default="1G")

    class Config:
        env_prefix = "SPARK_"
        case_sensitive = False


# ---------------------------
# Hugging Face Config
# ---------------------------
class HuggingFaceConfig(BaseSettings):
    access_key: str = Field(default="")

    class Config:
        env_prefix = "HUGGING_FACE_"
        case_sensitive = False


# ---------------------------
# Prometheus Config
# ---------------------------
class PrometheusConfig(BaseSettings):
    port: int = Field(default=9090)

    class Config:
        env_prefix = "PROMETHEUS_"
        case_sensitive = False


# ---------------------------
# Grafana Config
# ---------------------------
class GrafanaConfig(BaseSettings):
    port: int = Field(default=3000)
    admin_user: str = Field(default="admin")
    admin_password: str = Field(default="admin-password")

    class Config:
        env_prefix = "GF_SECURITY_"
        case_sensitive = False


# ---------------------------
# News Backlog Postgres Config
# ---------------------------
class DatabaseConfig(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    db: str = Field(default="news")
    user: str = Field(default="news_user")
    password: str = Field(default="news_password")

    class Config:
        env_prefix = "POSTGRES_"
        case_sensitive = False


# ---------------------------
# Main Settings
# ---------------------------
class Settings(BaseSettings):
    selenium: SeleniumConfig = SeleniumConfig()
    crawler: CrawlerConfig = CrawlerConfig()
    app: AppConfig = AppConfig()
    redpanda: RedpandaConfig = RedpandaConfig()
    redpanda_console: RedpandaConsoleConfig = RedpandaConsoleConfig()
    redis: RedisConfig = RedisConfig()
    qdrant: QdrantConfig = QdrantConfig()
    embedding: EmbeddingConfig = EmbeddingConfig()  # Unified embedding config
    openai: OpenAIConfig = OpenAIConfig()  # Legacy support
    spark: SparkConfig = SparkConfig()
    huggingface: HuggingFaceConfig = HuggingFaceConfig()
    prom: PrometheusConfig = PrometheusConfig()
    grafana: GrafanaConfig = GrafanaConfig()
    database: DatabaseConfig = DatabaseConfig()

    class Config:
        case_sensitive = False


# Global settings instance
settings = Settings()