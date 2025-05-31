from typing import Dict, Optional, Any
from datetime import datetime, date
from bs4 import BeautifulSoup
from . import NewsSourceInterface

class IRNANewsSource(NewsSourceInterface):
    """
    IRNA News Source Implementation
    """
    
    @property
    def source_name(self) -> str:
        return "IRNA"
    
    def extract_news_content(self, html_content: str, link: str) -> Optional[Dict[str, Any]]:
        """
        Extract news content from IRNA news pages
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract title
            title_tag = soup.find("h1", class_="title")
            title = title_tag.a.get_text(strip=True) if title_tag and title_tag.a else "No Title"
            
            # Extract body
            body_tag = soup.find("div", class_="item-body")
            if not body_tag:
                return None
            
            content = "\n".join([
                self._clean_text(p.get_text()) 
                for p in body_tag.find_all("p") 
                if self._clean_text(p.get_text())
            ])
            
            return {
                'title': title,
                'summary': content[:500] if content else "",
                'content': content,
                'published_date': None,
                'published_datetime': None,
                'tags': [],
                'author': None
            }
            
        except Exception as e:
            print(f"Error extracting IRNA content from {link}: {e}")
            return None
    
    def validate_link(self, link: str) -> bool:
        """Validate if a link belongs to IRNA"""
        return "irna.ir" in link.lower()
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        return ' '.join(text.split()).strip() 