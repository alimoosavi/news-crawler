from datetime import datetime, timedelta
import math

class ShamsiDate:
    """Simple Shamsi (Persian) date utility class"""
    
    # Shamsi month names
    MONTH_NAMES = [
        'فروردین', 'اردیبهشت', 'خرداد', 'تیر', 'مرداد', 'شهریور',
        'مهر', 'آبان', 'آذر', 'دی', 'بهمن', 'اسفند'
    ]
    
    # Days in each Shamsi month (non-leap year)
    MONTH_DAYS = [31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29]
    
    @staticmethod
    def is_leap_year(year):
        """Check if a Shamsi year is leap year"""
        # Simplified leap year calculation for Shamsi calendar
        cycle = year % 128
        if cycle <= 29:
            return cycle % 4 == 1
        elif cycle <= 62:
            return (cycle - 29) % 4 == 0
        elif cycle <= 95:
            return (cycle - 62) % 4 == 1
        else:
            return (cycle - 95) % 4 == 0
    
    @staticmethod
    def get_month_days(year, month):
        """Get number of days in a Shamsi month"""
        if month == 12 and ShamsiDate.is_leap_year(year):
            return 30
        return ShamsiDate.MONTH_DAYS[month - 1]
    
    @staticmethod
    def gregorian_to_shamsi(g_year, g_month, g_day):
        """Convert Gregorian date to Shamsi date (simplified algorithm)"""
        # This is a simplified conversion. For production use, consider using
        # a proper library like 'persiantools' or 'jdatetime'
        
        # Calculate Julian day number
        if g_month <= 2:
            g_year -= 1
            g_month += 12
        
        a = math.floor(g_year / 100)
        b = 2 - a + math.floor(a / 4)
        
        jd = math.floor(365.25 * (g_year + 4716)) + math.floor(30.6001 * (g_month + 1)) + g_day + b - 1524
        
        # Convert Julian day to Shamsi
        jd_shamsi = jd - 1948321  # Shamsi epoch adjustment
        
        # Calculate Shamsi year
        s_year = 1 + math.floor(jd_shamsi / 365.2422)
        
        # Adjust for more accurate calculation
        if s_year < 1:
            s_year = 1
        
        # Calculate remaining days
        year_start_jd = ShamsiDate.shamsi_year_start_jd(s_year)
        days_in_year = jd - year_start_jd + 1948321
        
        if days_in_year <= 0:
            s_year -= 1
            year_start_jd = ShamsiDate.shamsi_year_start_jd(s_year)
            days_in_year = jd - year_start_jd + 1948321
        
        # Calculate month and day
        s_month = 1
        while s_month <= 12:
            month_days = ShamsiDate.get_month_days(s_year, s_month)
            if days_in_year <= month_days:
                break
            days_in_year -= month_days
            s_month += 1
        
        s_day = int(days_in_year)
        
        # Ensure valid ranges
        if s_month > 12:
            s_month = 12
        if s_day < 1:
            s_day = 1
        
        return int(s_year), int(s_month), int(s_day)
    
    @staticmethod
    def shamsi_year_start_jd(year):
        """Calculate Julian day for start of Shamsi year (simplified)"""
        return math.floor(365.2422 * (year - 1)) + 1948321
    
    @staticmethod
    def current_shamsi_date():
        """Get current Shamsi date"""
        now = datetime.now()
        return ShamsiDate.gregorian_to_shamsi(now.year, now.month, now.day)
    
    @staticmethod
    def subtract_days(year, month, day, days_to_subtract):
        """Subtract days from a Shamsi date"""
        current_day = day
        current_month = month
        current_year = year
        
        for _ in range(days_to_subtract):
            current_day -= 1
            
            if current_day <= 0:
                current_month -= 1
                
                if current_month <= 0:
                    current_year -= 1
                    current_month = 12
                
                current_day = ShamsiDate.get_month_days(current_year, current_month)
        
        return current_year, current_month, current_day
    
    @staticmethod
    def get_last_n_days(n_days=10):
        """Get list of last N days in Shamsi calendar"""
        current_year, current_month, current_day = ShamsiDate.current_shamsi_date()
        
        dates = []
        for i in range(n_days):
            year, month, day = ShamsiDate.subtract_days(current_year, current_month, current_day, i)
            dates.append((year, month, day))
        
        return dates 