from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5433, description="Database port")
    db_name: str = Field(default="news_crawler", description="Database name")
    user: str = Field(default="postgres", description="Database user")
    password: str = Field(default="password", description="Database password")
    min_conn: int = Field(default=5, description="Database min connection")
    max_conn: int = Field(default=20, description="Database max connection")

    class Config:
        env_prefix = "DATABASE_"
        case_sensitive = False


class SeleniumConfig(BaseSettings):
    """Selenium configuration settings"""
    hub_url: str = Field(default="http://localhost:4444/wd/hub", description="Selenium hub URL")
    display: str = Field(default=":99", description="Display setting")
    node_max_sessions: int = Field(default=10, description="Max sessions per node")

    class Config:
        env_prefix = "SE_"
        case_sensitive = False


class CrawlerConfig(BaseSettings):
    """Page crawler configuration settings"""
    bulk_size: int = Field(default=20, description="Number of pages to process in each batch")
    max_workers: int = Field(default=5, description="Maximum concurrent workers")
    sleep_interval: int = Field(default=30, description="Sleep interval between batches (seconds)")
    max_retries: int = Field(default=3, description="Maximum retry attempts for failed pages")
    retry_delay: int = Field(default=10, description="Delay between retries (seconds)")

    class Config:
        env_prefix = "CRAWLER_"
        case_sensitive = False


class AppConfig(BaseSettings):
    """Application configuration settings"""
    log_level: str = Field(default="INFO", description="Logging level")
    debug: bool = Field(default=False, description="Debug mode")

    class Config:
        env_prefix = "APP_"
        case_sensitive = False


class Settings(BaseSettings):
    """Main settings class that combines all configurations"""
    database: DatabaseConfig = DatabaseConfig()
    selenium: SeleniumConfig = SeleniumConfig()
    crawler: CrawlerConfig = CrawlerConfig()
    app: AppConfig = AppConfig()

    class Config:
        case_sensitive = False


# Create a global settings instance
settings = Settings()
