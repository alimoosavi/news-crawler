from datetime import datetime, timedelta, date
import re
import logging
import pytz
from typing import Optional, Union

logger = logging.getLogger(__name__)

class ShamsiDateConverter:
    """
    Converter for Shamsi (Persian) dates to Gregorian dates
    Handles formats like: "۵ خرداد ۰۴ - ۰۹:۳۲"
    """
    
    # Persian to English digit mapping
    PERSIAN_DIGITS = {
        '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
        '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
    }
    
    # Shamsi month names to numbers
    SHAMSI_MONTHS = {
        'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4,
        'مرداد': 5, 'شهریور': 6, 'مهر': 7, 'آبان': 8,
        'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
    }
    
    def __init__(self, default_timezone='Asia/Tehran'):
        self.default_tz = pytz.timezone(default_timezone)
    
    def persian_to_english_digits(self, text: str) -> str:
        """Convert Persian digits to English digits"""
        for persian, english in self.PERSIAN_DIGITS.items():
            text = text.replace(persian, english)
        return text
    
    def parse_shamsi_date_string(self, date_string: str) -> Optional[dict]:
        """
        Parse Shamsi date string like "۵ خرداد ۰۴ - ۰۹:۳۲"
        Returns dict with day, month, year, hour, minute
        """
        try:
            # Convert Persian digits to English
            normalized = self.persian_to_english_digits(date_string.strip())
            
            # Pattern for "day month year - hour:minute"
            pattern = r'(\d+)\s+(\w+)\s+(\d+)\s*-\s*(\d+):(\d+)'
            match = re.search(pattern, normalized)
            
            if not match:
                # Try simpler pattern without time
                pattern = r'(\d+)\s+(\w+)\s+(\d+)'
                match = re.search(pattern, normalized)
                if not match:
                    return None
                
                day, month_name, year = match.groups()
                hour, minute = 0, 0
            else:
                day, month_name, year, hour, minute = match.groups()
            
            # Find month number
            month = None
            for shamsi_month, month_num in self.SHAMSI_MONTHS.items():
                if month_name in shamsi_month or shamsi_month.startswith(month_name):
                    month = month_num
                    break
            
            if month is None:
                return None
            
            return {
                'day': int(day),
                'month': int(month),
                'year': int(year),
                'hour': int(hour),
                'minute': int(minute)
            }
            
        except Exception as e:
            print(f"Error parsing Shamsi date '{date_string}': {e}")
            return None
    
    def shamsi_to_gregorian(self, shamsi_year: int, shamsi_month: int, shamsi_day: int) -> Optional[date]:
        """
        Convert Shamsi date to Gregorian date
        Note: This is a simplified conversion. For production, use a proper library like jdatetime
        """
        try:
            # Simple approximation - for accurate conversion use jdatetime library
            # This assumes the year is in short format (e.g., 04 = 1404)
            if shamsi_year < 100:
                shamsi_year += 1400 if shamsi_year < 50 else 1300
            
            # Approximate conversion (not accurate for all dates)
            # For accurate conversion, install and use jdatetime library
            gregorian_year = shamsi_year + 621
            
            # Simple month/day mapping (approximate)
            if shamsi_month <= 6:
                gregorian_month = shamsi_month + 3
                gregorian_day = shamsi_day
            else:
                gregorian_month = shamsi_month - 6
                gregorian_day = shamsi_day
                gregorian_year += 1
            
            # Adjust for month overflow
            if gregorian_month > 12:
                gregorian_month -= 12
                gregorian_year += 1
            
            return date(gregorian_year, gregorian_month, min(shamsi_day, 28))
            
        except Exception as e:
            print(f"Error converting Shamsi to Gregorian: {e}")
            return None
    
    def parse_shamsi_datetime(self, date_string: str, timezone=None) -> Optional[datetime]:
        """
        Parse Shamsi date string and return timezone-aware datetime
        """
        parsed = self.parse_shamsi_date_string(date_string)
        if not parsed:
            return None
        
        # Convert to Gregorian date
        gregorian_date = self.shamsi_to_gregorian(
            parsed['year'], 
            parsed['month'], 
            parsed['day']
        )
        
        if not gregorian_date:
            return None
        
        # Create datetime with time
        dt = datetime.combine(
            gregorian_date,
            datetime.min.time().replace(
                hour=parsed['hour'],
                minute=parsed['minute']
            )
        )
        
        # Make timezone-aware
        tz = timezone or self.default_tz
        return tz.localize(dt)
    
    def parse_shamsi_date_only(self, date_string: str) -> Optional[date]:
        """
        Parse Shamsi date string and return only the date part
        """
        parsed = self.parse_shamsi_date_string(date_string)
        if not parsed:
            return None
        
        return self.shamsi_to_gregorian(
            parsed['year'], 
            parsed['month'], 
            parsed['day']
        )

# Convenience functions
def parse_shamsi_datetime(date_string: str, timezone='Asia/Tehran') -> Optional[datetime]:
    """Parse Shamsi datetime string to timezone-aware datetime"""
    converter = ShamsiDateConverter(timezone)
    return converter.parse_shamsi_datetime(date_string)

def parse_shamsi_date(date_string: str) -> Optional[date]:
    """Parse Shamsi date string to date object"""
    converter = ShamsiDateConverter()
    return converter.parse_shamsi_date_only(date_string)

# Example usage
if __name__ == "__main__":
    # Test the converter
    test_dates = [
        "۵ خرداد ۰۴ - ۰۹:۳۲",
        "۱۵ مهر ۰۳ - ۱۴:۲۰",
        "۲۸ اسفند ۰۲"
    ]
    
    converter = ShamsiDateConverter()
    
    for test_date in test_dates:
        print(f"Input: {test_date}")
        
        # Parse datetime
        dt = converter.parse_shamsi_datetime(test_date)
        if dt:
            print(f"DateTime: {dt}")
            print(f"UTC: {dt.astimezone(pytz.UTC)}")
        
        # Parse date only
        d = converter.parse_shamsi_date_only(test_date)
        if d:
            print(f"Date: {d}")
        
        print("-" * 40) 