from bs4 import BeautifulSoup
from typing import Dict, List, Optional
import re
from datetime import datetime
import logging
from utils.shamsi_converter import convert_shamsi_to_datetime

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
        published_date will be a datetime object or None
    """
    result = {
        'published_date': None,
        'title': None,
        'summary': None,
        'content': None,
        'tags': None
    }

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract published date from calendar icon structure and convert to datetime
        result['published_date'] = _extract_published_date(soup)

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


def _extract_published_date(soup: BeautifulSoup) -> Optional[datetime]:
    """
    Extract published date from the calendar icon structure and convert to datetime.
    
    Expected format: <li><i class="fa fa-calendar-o"></i> <span class="title-meta">جمعه /</span> <span class="text-meta">۹ خرداد ۱۴۰۴ / ۱۹:۳۱</span></li>
    
    Returns:
        datetime object or None if extraction/conversion fails
    """
    try:
        # Look for the calendar icon and its parent li element
        calendar_icon = soup.find('i', class_='fa fa-calendar-o')

        if calendar_icon:
            # Get the parent li element
            li_element = calendar_icon.find_parent('li')

            if li_element:
                # Find the span with class "text-meta" that contains the date
                date_span = li_element.find('span', class_='text-meta')

                if date_span:
                    date_text = date_span.get_text().strip()
                    logger.debug(f"Found date text: {date_text}")

                    # Convert Shamsi string to datetime
                    datetime_obj = convert_shamsi_to_datetime(date_text)
                    if datetime_obj:
                        logger.debug(f"Converted to datetime: {datetime_obj}")
                        return datetime_obj
                    else:
                        logger.warning(f"Failed to convert Shamsi date to datetime: {date_text}")

        # Alternative approach: look for any li containing calendar icon and date pattern
        li_elements = soup.find_all('li')
        for li in li_elements:
            if li.find('i', class_='fa fa-calendar-o'):
                text_meta_span = li.find('span', class_='text-meta')
                if text_meta_span:
                    date_text = text_meta_span.get_text().strip()
                    # Check if it contains Shamsi date pattern (Persian digits and month names)
                    if _is_shamsi_date_pattern(date_text):
                        logger.debug(f"Found Shamsi date: {date_text}")

                        # Convert to datetime
                        datetime_obj = convert_shamsi_to_datetime(date_text)
                        if datetime_obj:
                            logger.debug(f"Converted to datetime: {datetime_obj}")
                            return datetime_obj

        # Fallback: search for any element containing Shamsi date pattern
        all_text_meta = soup.find_all('span', class_='text-meta')
        for span in all_text_meta:
            text = span.get_text().strip()
            if _is_shamsi_date_pattern(text):
                logger.debug(f"Found Shamsi date in text-meta: {text}")

                # Convert to datetime
                datetime_obj = convert_shamsi_to_datetime(text)
                if datetime_obj:
                    logger.debug(f"Converted to datetime: {datetime_obj}")
                    return datetime_obj

        logger.warning("Could not find published date with calendar icon structure")
        return None

    except Exception as e:
        logger.warning(f"Failed to extract published date: {str(e)}")
        return None


def _is_shamsi_date_pattern(text: str) -> bool:
    """
    Check if text contains a Shamsi date pattern.
    
    Expected patterns:
    - ۹ خرداد ۱۴۰۴ / ۱۹:۳۱
    - ۱۰ آبان ۱۴۰۳ / ۱۴:۲۵
    """
    try:
        # Shamsi month names in Persian
        shamsi_months = [
            'فروردین', 'اردیبهشت', 'خرداد', 'تیر', 'مرداد', 'شهریور',
            'مهر', 'آبان', 'آذر', 'دی', 'بهمن', 'اسفند'
        ]

        # Check if text contains any Shamsi month name
        for month in shamsi_months:
            if month in text:
                # Also check for Persian digits and time pattern
                if re.search(r'[۰-۹]', text) and ('/' in text or ':' in text):
                    return True

        return False

    except Exception:
        return False


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


# Example usage and testing function
def test_extractor():
    """Test the extractor with sample HTML."""
    sample_html = """
    <html>
        <li><i class="fa fa-calendar-o"></i> <span class="title-meta">جمعه /</span> <span class="text-meta">۹ خرداد ۱۴۰۴ / ۱۹:۳۱</span></li>
        
        <h1 class="first-title" itemprop="headline">بازدید استاندار از طرح آبرسانی به ۷ روستای رفسنجان و طرح توسعه‌ معدن سنگ‌آهن داوران</h1>
        
        <p class="summary" itemprop="description"><span class="src">ایسنا/کرمان </span>استاندار کرمان از عملیات اجرایی آبرسانی به ۷ روستای رفسنجان بازدید کرد که این طرح جمعیتی بالغ بر ۷۰۰۰ نفر را از نعمت آب شرب پایدار بهره‌مند خواهند کرد.</p>
        
        <div itemprop="articleBody" class="item-text">
            <p>محمدعلی طالبی ۹ خرداد به همراه فرماندار و نماینده مردم رفسنجان و انار در مجلس شورای اسلامی از مراحل اجرای خط انتقال آب به روستاهای فردوسیه آزادگان، عباس آباد آقا غفور، قائمیه، مهدی آباد معاون، اسلامیه، اکبر آباد هجری و ... بازدید کرد.&nbsp;</p>
            <p>انتهای پیام</p>
        </div>
        
        <footer class="tags">
            <i class="fa fa-tags"></i> <span>برچسب‌ها:</span>
            <ul>
                <li><a href="/tag/test1" rel="Index, Tag">استانی-شهرستانها</a></li>
                <li><a href="/tag/test2" rel="Index, Tag">رفسنجان</a></li>
            </ul>
        </footer>
    </html>
    """

    result = extract_news_article(sample_html)

    print("Extraction Results:")
    print(f"Published Date: {result['published_date']} (type: {type(result['published_date'])})")
    print(f"Title: {result['title']}")
    print(f"Summary: {result['summary']}")
    print(f"Content: {result['content'][:100]}..." if result['content'] else "Content: None")
    print(f"Tags: {result['tags']}")


if __name__ == "__main__":
    test_extractor()
