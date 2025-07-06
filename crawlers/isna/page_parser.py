from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import re
from datetime import datetime
import logging

# Configure logging for debugging in distributed systems
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_news_article(html_content: str) -> Dict[str, Optional[str]]:
    """
    Extract news article data from HTML content.
    
    Args:
        html_content (str): Raw HTML content of the news page
        
    Returns:
        Dict containing: published_date, title, summary, content, tags
    """
    result = {
        'title': None,
        'summary': None,
        'content': None,
        'tags': None
    }

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract title from h1 with class "first-title"
        result['title'] = _extract_title(soup)

        # Extract summary from p with class "summary"
        result['summary'] = _extract_summary(soup)

        # Extract content from div with itemprop="articleBody"
        result['content'] = _extract_content(soup)

        # Extract tags from footer with class "tags"
        result['tags'] = _extract_tags(soup)

        logger.info(
            f"Successfully extracted article data: title='{result['title'][:50] if result['title'] else None}...'")

    except Exception as e:
        logger.error(f"Error parsing HTML content: {str(e)}")
        # Return partial results even if some extraction fails

    return result


def _extract_title(soup: BeautifulSoup) -> Optional[str]:
    """Extract title from h1 with class 'first-title'"""
    try:
        title_element = soup.find('h1', class_='first-title')

        if title_element:
            title = title_element.get_text().strip()
            logger.debug(f"Found title: {title}")
            return title

        # Fallback: try to find any h1 with itemprop="headline"
        title_element = soup.find('h1', attrs={'itemprop': 'headline'})
        if title_element:
            title = title_element.get_text().strip()
            logger.debug(f"Found title via itemprop: {title}")
            return title

        logger.warning("Could not find title element")
        return None

    except Exception as e:
        logger.warning(f"Failed to extract title: {str(e)}")
        return None


def _extract_summary(soup: BeautifulSoup) -> Optional[str]:
    """Extract summary from p with class 'summary'"""
    try:
        summary_element = soup.find('p', class_='summary')

        if summary_element:
            summary = summary_element.get_text().strip()
            logger.debug(f"Found summary: {summary[:50]}...")
            return summary

        # Fallback: try to find any element with itemprop="description"
        summary_element = soup.find(attrs={'itemprop': 'description'})
        if summary_element:
            summary = summary_element.get_text().strip()
            logger.debug(f"Found summary via itemprop: {summary[:50]}...")
            return summary

        logger.warning("Could not find summary element")
        return None

    except Exception as e:
        logger.warning(f"Failed to extract summary: {str(e)}")
        return None


def _extract_content(soup: BeautifulSoup) -> Optional[str]:
    """Extract content from div with itemprop='articleBody'"""
    try:
        content_element = soup.find('div', attrs={'itemprop': 'articleBody'})

        if content_element:
            # Get all text content, preserving paragraph breaks
            paragraphs = content_element.find_all('p')
            if paragraphs:
                content_parts = []
                for p in paragraphs:
                    text = p.get_text().strip()
                    if text:
                        content_parts.append(text)

                content = '\n\n'.join(content_parts)
                logger.debug(f"Found content: {content[:100]}...")
                return content
            else:
                # If no paragraphs, get all text
                content = content_element.get_text().strip()
                logger.debug(f"Found content (no paragraphs): {content[:100]}...")
                return content

        # Fallback: try to find content in div with class "item-text"
        content_element = soup.find('div', class_='item-text')
        if content_element:
            content = content_element.get_text().strip()
            logger.debug(f"Found content via item-text: {content[:100]}...")
            return content

        logger.warning("Could not find content element")
        return None

    except Exception as e:
        logger.warning(f"Failed to extract content: {str(e)}")
        return None


def _extract_tags(soup: BeautifulSoup) -> Optional[List[str]]:
    """Extract tags from footer with class 'tags'"""
    try:
        tags_footer = soup.find('footer', class_='tags')

        if tags_footer:
            # Find all links within the tags section
            tag_links = tags_footer.find_all('a')

            if tag_links:
                tags = []
                for link in tag_links:
                    tag_text = link.get_text().strip()
                    if tag_text:
                        tags.append(tag_text)

                logger.debug(f"Found tags: {tags}")
                return tags if tags else None

        # Fallback: look for any element with tags-related classes
        tag_containers = soup.find_all(['div', 'section', 'footer'], class_=re.compile(r'tag', re.I))
        for container in tag_containers:
            tag_links = container.find_all('a')
            if tag_links:
                tags = []
                for link in tag_links:
                    tag_text = link.get_text().strip()
                    if tag_text and len(tag_text) < 50:  # Reasonable tag length
                        tags.append(tag_text)

                if tags:
                    logger.debug(f"Found tags via fallback: {tags}")
                    return tags

        logger.warning("Could not find tags")
        return None

    except Exception as e:
        logger.warning(f"Failed to extract tags: {str(e)}")
        return None


def main():
    with open('./sample_page.html') as file:
        print(extract_news_article(file.read()))


if __name__ == '__main__':
    main()
