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
# OpenAI Config
# ---------------------------
class OpenAIConfig(BaseSettings):
    api_key: str = Field(default="")

    class Config:
        env_prefix = "OPENAI_"
        case_sensitive = False


# ---------------------------
# Elasticsearch & Kibana Config
# ---------------------------
class ElasticsearchConfig(BaseSettings):
    port: int = Field(default=9200)
    port_transport: int = Field(default=9300)
    memory_limit: str = Field(default="1g")
    java_opts: str = Field(default="-Xms512m -Xmx512m")
    password: str = Field(default="your_strong_password")

    class Config:
        env_prefix = "ELASTIC_"
        case_sensitive = False


class KibanaConfig(BaseSettings):
    port: int = Field(default=5601)
    memory_limit: str = Field(default="512m")

    class Config:
        env_prefix = "KIBANA_"
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
# Main Settings
# ---------------------------
class Settings(BaseSettings):
    selenium: SeleniumConfig = SeleniumConfig()
    crawler: CrawlerConfig = CrawlerConfig()
    app: AppConfig = AppConfig()
    redpanda: RedpandaConfig = RedpandaConfig()
    redpanda_console: RedpandaConsoleConfig = RedpandaConsoleConfig()
    redis: RedisConfig = RedisConfig()
    openai: OpenAIConfig = OpenAIConfig()
    elasticsearch: ElasticsearchConfig = ElasticsearchConfig()
    kibana: KibanaConfig = KibanaConfig()
    spark: SparkConfig = SparkConfig()

    class Config:
        case_sensitive = False


# Global settings instance
settings = Settings()
