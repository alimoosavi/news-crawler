from typing import Dict, Type
from . import NewsSourceInterface
from .isna_source import ISNANewsSource
from .irna_source import IRNANewsSource

class NewsSourceFactory:
    """Factory for creating news source instances"""
    
    _sources: Dict[str, Type[NewsSourceInterface]] = {
        'ISNA': ISNANewsSource,
        'IRNA': IRNANewsSource,
    }
    
    @classmethod
    def create_source(cls, source_name: str) -> NewsSourceInterface:
        """Create a news source instance"""
        source_name = source_name.upper()
        
        if source_name not in cls._sources:
            available = ', '.join(cls._sources.keys())
            raise ValueError(f"Unsupported news source '{source_name}'. Available: {available}")
        
        return cls._sources[source_name]()
    
    @classmethod
    def register_source(cls, source_name: str, source_class: Type[NewsSourceInterface]):
        """Register a new news source"""
        cls._sources[source_name.upper()] = source_class
    
    @classmethod
    def get_available_sources(cls) -> list:
        """Get list of available news sources"""
        return list(cls._sources.keys())

# Convenience function
def get_news_source(source_name: str) -> NewsSourceInterface:
    """Get a news source instance"""
    return NewsSourceFactory.create_source(source_name) 