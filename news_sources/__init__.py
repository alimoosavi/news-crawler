from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime, date

class NewsSourceInterface(ABC):
    """
    Abstract interface for news source crawlers
    """
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of the news source"""
        pass
    
    @abstractmethod
    def extract_news_content(self, html_content: str, link: str) -> Optional[Dict[str, Any]]:
        """
        Extract news content from individual news pages
        
        Returns:
            Dict with keys: 'title', 'summary', 'content', 'published_date', 
                          'published_datetime', 'tags', 'author'
        """
        pass
    
    def validate_link(self, link: str) -> bool:
        """Validate if a link belongs to this news source"""
        return True  # Default implementation 