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
    database: str = Field(default="news_crawler", description="Database name")
    user: str = Field(default="postgres", description="Database user")
    password: str = Field(default="password", description="Database password")
    
    class Config:
        env_prefix = "POSTGRES_"
        case_sensitive = False

class SeleniumConfig(BaseSettings):
    """Selenium configuration settings"""
    hub_url: str = Field(default="http://localhost:4444/wd/hub", description="Selenium hub URL")
    display: str = Field(default=":99", description="Display setting")
    node_max_sessions: int = Field(default=10, description="Max sessions per node")
    node_override_max_sessions: bool = Field(default=True, description="Override max sessions")
    node_session_timeout: int = Field(default=300, description="Session timeout in seconds")
    
    class Config:
        env_prefix = "SE_"
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
    app: AppConfig = AppConfig()
    
    class Config:
        case_sensitive = False

# Create a global settings instance
settings = Settings() 