from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

# Load environment variables from .env
load_dotenv()


# ---------------------------
# Database Config
# ---------------------------
class DatabaseConfig(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=5433)
    db_name: str = Field(default="news_crawler")
    user: str = Field(default="postgres")
    password: str = Field(default="password")
    min_conn: int = Field(default=5)
    max_conn: int = Field(default=20)

    class Config:
        env_prefix = "DATABASE_"
        case_sensitive = False


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
# Kafka Config
# ---------------------------
class KafkaConfig(BaseSettings):
    advertised_listeners: str = Field(default="PLAINTEXT://localhost:9092")
    broker_id: int = Field(default=1)
    offsets_topic_replication_factor: int = Field(default=1)
    transaction_state_log_min_isr: int = Field(default=1)
    transaction_state_log_replication_factor: int = Field(default=1)
    listeners_port: int = Field(default=9092)
    jmx_port: int = Field(default=9101)
    zookeeper_client_port: int = Field(default=2181)

    # Topics
    news_links_topic: str = Field(default="news_links")
    news_content_topic: str = Field(default="news_content")

    class Config:
        env_prefix = "KAFKA_"
        case_sensitive = False


# ---------------------------
# Kafka UI Config
# ---------------------------
class KafkaUIConfig(BaseSettings):
    port: int = Field(default=8080)
    cluster_name: str = Field(default="local")
    bootstrap_servers: str = Field(default="localhost:9092")
    zookeeper: str = Field(default="localhost:2181")

    class Config:
        env_prefix = "KAFKA_UI_"
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
# Main Settings
# ---------------------------
class Settings(BaseSettings):
    database: DatabaseConfig = DatabaseConfig()
    selenium: SeleniumConfig = SeleniumConfig()
    crawler: CrawlerConfig = CrawlerConfig()
    app: AppConfig = AppConfig()
    kafka: KafkaConfig = KafkaConfig()
    kafka_ui: KafkaUIConfig = KafkaUIConfig()
    redis: RedisConfig = RedisConfig()

    class Config:
        case_sensitive = False


# Global settings instance
settings = Settings()
