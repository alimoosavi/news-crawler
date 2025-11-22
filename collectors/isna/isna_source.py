from typing import Dict, Optional, Any
from datetime import datetime, date
from bs4 import BeautifulSoup
import re
from news_sources import NewsSourceInterface


class ISNANewsSource(NewsSourceInterface):
    """
    ISNA News Source Implementation with Shamsi date support
    """

    @property
    def source_name(self) -> str:
        return "ISNA"

    def extract_news_content(self, html_content: str, link: str) -> Optional[Dict[str, Any]]:
        """
        Extract news content from ISNA news pages with Shamsi date support
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract title
            title_tag = soup.find("h1", class_="first-title", itemprop="headline")
            title = self._clean_text(title_tag.get_text()) if title_tag else "No Title"

            # Extract content body
            body_tag = soup.find("div", itemprop="articleBody", class_="item-text")
            if not body_tag:
                return None

            # Extract paragraphs
            paragraphs = body_tag.find_all("p")
            content = "\n".join([
                self._clean_text(p.get_text())
                for p in paragraphs
                if self._clean_text(p.get_text())
            ])

            # Extract summary (first paragraph)
            summary = ""
            if paragraphs:
                summary = self._clean_text(paragraphs[0].get_text())[:500]

            # Extract published date/time with Shamsi support
            date_tag = soup.find('time') or soup.find(class_=re.compile(r'date|publish'))
            published_datetime = None
            published_date = None
            shamsi_components = None

            if date_tag:
                date_text = self._clean_text(date_tag.get_text())

                # Extract Shamsi date components
                shamsi_components = self._extract_shamsi_components(date_text)

                # Also try to convert to Gregorian for compatibility
                published_datetime = self._extract_shamsi_datetime(date_text)
                if published_datetime:
                    published_date = published_datetime.date()

            # Extract tags
            tags = []
            tag_elements = soup.find_all(class_=re.compile(r'tag|keyword|category'))
            for tag_elem in tag_elements:
                tag_text = self._clean_text(tag_elem.get_text())
                if tag_text and len(tag_text) < 50:
                    tags.append(tag_text)

            result = {
                'title': title,
                'summary': summary,
                'content': content,
                'published_date': published_date,
                'published_datetime': published_datetime,
                'tags': tags[:10],  # Limit to 10 tags
                'author': None
            }

            # Add Shamsi date components if available
            if shamsi_components:
                result.update({
                    'shamsi_year': shamsi_components[0],
                    'shamsi_month': shamsi_components[1],
                    'shamsi_day': shamsi_components[2],
                    'shamsi_month_name': shamsi_components[3]
                })

            return result

        except Exception as e:
            print(f"Error extracting ISNA content from {link}: {e}")
            return None

    def validate_link(self, link: str) -> bool:
        """Validate if a link belongs to ISNA"""
        return "isna.ir" in link.lower()

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""

        # Remove extra whitespace and normalize
        text = ' '.join(text.split())

        # Remove common unwanted characters
        text = text.replace('\u200c', ' ')  # Zero-width non-joiner
        text = text.replace('\u200d', '')  # Zero-width joiner

        return text.strip()

    def _extract_shamsi_components(self, date_string: str) -> Optional[tuple]:
        """Extract Shamsi date components from date string"""
        try:
            from utils.shamsi_converter import extract_shamsi_components
            return extract_shamsi_components(date_string)
        except Exception:
            return None

    def _extract_shamsi_datetime(self, date_string: str) -> Optional[datetime]:
        """Extract datetime from Shamsi date strings"""
        try:
            from utils.shamsi_converter import parse_shamsi_datetime
            return parse_shamsi_datetime(date_string)
        except Exception:
            return None
