from datetime import datetime, timedelta
import re
import logging

logger = logging.getLogger(__name__)

class ShamsiToDatetimeConverter:
    """Convert Shamsi (Persian) date strings to Python datetime objects"""
    
    # Shamsi month names and their corresponding numbers
    SHAMSI_MONTHS = {
        'فروردین': 1, 'اردیبهشت': 2, 'خرداد': 3, 'تیر': 4, 'مرداد': 5, 'شهریور': 6,
        'مهر': 7, 'آبان': 8, 'آذر': 9, 'دی': 10, 'بهمن': 11, 'اسفند': 12
    }
    
    # Persian to English digit mapping
    PERSIAN_DIGITS = {
        '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
        '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
    }
    
    @classmethod
    def persian_to_english_digits(cls, text):
        """Convert Persian digits to English digits"""
        if not text:
            return text
            
        for persian, english in cls.PERSIAN_DIGITS.items():
            text = text.replace(persian, english)
        return text
    
    @classmethod
    def parse_shamsi_date_string(cls, date_string):
        """
        Parse Shamsi date string and extract components
        
        Expected formats:
        - "۹ خرداد ۱۴۰۴ / ۱۲:۰۳"
        - "۱۰ آبان ۱۴۰۳ / ۱۴:۲۵"
        
        Returns:
            dict: {'day': int, 'month': int, 'year': int, 'hour': int, 'minute': int}
        """
        if not date_string:
            return None
        
        try:
            # Convert Persian digits to English
            normalized_string = cls.persian_to_english_digits(date_string)
            
            # Pattern to match: "day month_name year / hour:minute"
            pattern = r'(\d+)\s+(\w+)\s+(\d+)\s*/\s*(\d+):(\d+)'
            match = re.search(pattern, normalized_string)
            
            if not match:
                logger.warning(f"Could not parse Shamsi date string: {date_string}")
                return None
            
            day_str, month_name, year_str, hour_str, minute_str = match.groups()
            
            # Convert to integers
            day = int(day_str)
            year = int(year_str)
            hour = int(hour_str)
            minute = int(minute_str)
            
            # Find month number
            month = cls.SHAMSI_MONTHS.get(month_name)
            if not month:
                logger.warning(f"Unknown Shamsi month: {month_name}")
                return None
            
            return {
                'day': day,
                'month': month,
                'year': year,
                'hour': hour,
                'minute': minute
            }
            
        except Exception as e:
            logger.error(f"Error parsing Shamsi date string '{date_string}': {str(e)}")
            return None
    
    @classmethod
    def shamsi_to_gregorian(cls, shamsi_year, shamsi_month, shamsi_day):
        """
        Convert Shamsi date to Gregorian date
        
        This is a simplified conversion. For production use, consider using
        a proper Persian calendar library like 'jdatetime' or 'persiantools'
        """
        try:
            # Approximate conversion (this is simplified and may have small errors)
            # For accurate conversion, use a proper Persian calendar library
            
            # Base date: 1 Farvardin 1400 = March 21, 2021
            base_shamsi_year = 1400
            base_gregorian_date = datetime(2021, 3, 21)
            
            # Calculate days difference from base date
            years_diff = shamsi_year - base_shamsi_year
            
            # Approximate days in a Shamsi year (365.2422 days)
            days_from_years = years_diff * 365.2422
            
            # Days from months (approximate)
            days_from_months = 0
            for month in range(1, shamsi_month):
                if month <= 6:
                    days_from_months += 31  # First 6 months have 31 days
                elif month <= 11:
                    days_from_months += 30  # Next 5 months have 30 days
                else:
                    days_from_months += 29  # Last month has 29 days (30 in leap years)
            
            # Add days
            days_from_days = shamsi_day - 1
            
            total_days = days_from_years + days_from_months + days_from_days
            
            # Calculate final Gregorian date
            gregorian_date = base_gregorian_date + timedelta(days=total_days)
            
            return gregorian_date.date()
            
        except Exception as e:
            logger.error(f"Error converting Shamsi to Gregorian: {str(e)}")
            return None
    
    @classmethod
    def convert_shamsi_string_to_datetime(cls, shamsi_string):
        """
        Convert Shamsi date string to Python datetime object
        
        Args:
            shamsi_string (str): Shamsi date string like "۹ خرداد ۱۴۰۴ / ۱۲:۰۳"
            
        Returns:
            datetime: Python datetime object or None if conversion fails
        """
        try:
            # Parse the Shamsi string
            parsed = cls.parse_shamsi_date_string(shamsi_string)
            if not parsed:
                return None
            
            # Convert Shamsi date to Gregorian
            gregorian_date = cls.shamsi_to_gregorian(
                parsed['year'], 
                parsed['month'], 
                parsed['day']
            )
            
            if not gregorian_date:
                return None
            
            # Combine date and time
            result_datetime = datetime.combine(
                gregorian_date,
                datetime.min.time().replace(
                    hour=parsed['hour'],
                    minute=parsed['minute']
                )
            )
            
            logger.debug(f"Converted '{shamsi_string}' to {result_datetime}")
            return result_datetime
            
        except Exception as e:
            logger.error(f"Error converting Shamsi string to datetime: {str(e)}")
            return None

# Convenience function for easy import
def convert_shamsi_to_datetime(shamsi_string):
    """Convert Shamsi date string to datetime object"""
    return ShamsiToDatetimeConverter.convert_shamsi_string_to_datetime(shamsi_string) 