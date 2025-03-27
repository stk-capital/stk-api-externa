


from datetime import datetime, timedelta
import re
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def is_more_precise_date(new_date_text: str, old_date_text: str) -> bool:
    """
    Determine if the new date text is more specific/precise than the old one.
    
    Args:
        new_date_text: The new date text to evaluate
        old_date_text: The existing date text to compare against
        
    Returns:
        Boolean indicating whether the new date is more precise
    """
    if not new_date_text or not old_date_text:
        return bool(new_date_text and not old_date_text)
    
    # Define precision patterns from most to least precise
    precision_patterns = [
        # Exact datetime with timezone (2024-03-15T14:30:00+00:00)
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}',
        # Exact datetime (March 15, 2024 at 2:30 PM)
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
        r'\w+ \d{1,2}, \d{4} at \d{1,2}:\d{2} [AP]M',
        # Exact date with time (March 15, 2024 2:30 PM)
        r'\w+ \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M',
        # Exact date (March 15, 2024)
        r'\d{4}-\d{2}-\d{2}',
        r'\w+ \d{1,2}, \d{4}',
        r'\d{1,2}/\d{1,2}/\d{4}',
        # Month and year (March 2024)
        r'\w+ \d{4}',
        r'\d{2}/\d{4}',
        # Quarter references (Q1 2024)
        r'Q[1-4] \d{4}',
        r'[Qq]uarter \d \d{4}',
        # Year only (2024)
        r'\d{4}',
        # Fiscal year references
        r'[Ff]iscal [Yy]ear \d{4}',
        r'FY\d{2}',
        r'FY \d{4}'
    ]
    
    # Find the precision level of each date text
    new_precision = len(precision_patterns)  # Default to lowest precision
    old_precision = len(precision_patterns)
    
    for i, pattern in enumerate(precision_patterns):
        if re.search(pattern, new_date_text):
            new_precision = i
            break
            
    for i, pattern in enumerate(precision_patterns):
        if re.search(pattern, old_date_text):
            old_precision = i
            break
    
    # Lower index means higher precision
    return new_precision < old_precision

def normalize_date(date_text: str, reference_date: datetime = None) -> Optional[datetime]:
    """
    Convert date text to normalized datetime object, handling various formats.
    
    Args:
        date_text: The text description of the date to normalize
        reference_date: Optional reference date for resolving relative expressions,
                        defaults to current datetime if not provided
    
    Returns:
        Normalized datetime object or None if parsing fails
    """
    if not date_text:
        return None
        
    # Use current date as default reference if none provided
    if reference_date is None:
        reference_date = datetime.now()
    
    try:
        # Clean up the input text
        cleaned_text = date_text.strip().lower()
        
        # Attempt direct parsing for ISO format dates
        try:
            if 'T' in date_text and ('+' in date_text or 'Z' in date_text):
                # ISO format with timezone: 2024-03-15T14:30:00+00:00 or 2024-03-15T14:30:00Z
                return datetime.fromisoformat(date_text.replace('Z', '+00:00'))
            elif 'T' in date_text:
                # ISO format without timezone: 2024-03-15T14:30:00
                return datetime.fromisoformat(date_text)
        except (ValueError, TypeError):
            pass
            
        # Handle fiscal quarters (Q1, Q2, Q3, Q4)
        quarter_match = re.search(r'q([1-4])\s*(\d{4}|\d{2})', cleaned_text)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year_text = quarter_match.group(2)
            year = int(year_text)
            
            # Handle two-digit years
            if len(year_text) == 2:
                current_century = reference_date.year // 100 * 100
                year = current_century + year
                
            # Map quarter to month
            month = (quarter - 1) * 3 + 1
            return datetime(year, month, 1)
        
        # Handle fiscal year references (FY2024, FY 2024, Fiscal Year 2024)
        fiscal_match = re.search(r'(fy|fiscal year)\s*(\d{4}|\d{2})', cleaned_text)
        if fiscal_match:
            year_text = fiscal_match.group(2)
            year = int(year_text)
            
            # Handle two-digit years
            if len(year_text) == 2:
                current_century = reference_date.year // 100 * 100
                year = current_century + year
                
            # Assuming fiscal year starts in January - adjust as needed
            return datetime(year, 1, 1)
        
        # Handle common month and year formats (January 2024, Jan 2024)
        month_year_pattern = r'(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{4})'
        month_year_match = re.search(month_year_pattern, cleaned_text)
        if month_year_match:
            month_name = month_year_match.group(1)
            year = int(month_year_match.group(2))
            
            # Map month name to number
            month_map = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12
            }
            month = month_map.get(month_name[:3].lower(), 1)
            return datetime(year, month, 1)
        
        # Handle full dates (January 15, 2024; 15 January 2024; 01/15/2024)
        # First, try dateutil parser which handles many formats
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_text, default=reference_date)
            # Only return if year was explicitly specified in the text
            if ('20' in date_text or '19' in date_text) and parsed_date.year != reference_date.year:
                return parsed_date
            # If the month or day changed from the reference, it was probably specified
            elif (parsed_date.month != reference_date.month or 
                  parsed_date.day != reference_date.day):
                return parsed_date
        except (ValueError, TypeError, ImportError):
            pass
        
        # Handle relative date expressions
        today_match = re.search(r'today', cleaned_text)
        if today_match:
            return datetime(reference_date.year, reference_date.month, reference_date.day)
        
        tomorrow_match = re.search(r'tomorrow', cleaned_text)
        if tomorrow_match:
            next_day = reference_date + timedelta(days=1)
            return datetime(next_day.year, next_day.month, next_day.day)
        
        yesterday_match = re.search(r'yesterday', cleaned_text)
        if yesterday_match:
            prev_day = reference_date - timedelta(days=1)
            return datetime(prev_day.year, prev_day.month, prev_day.day)
        
        # Handle "next week", "next month", "next year"
        next_match = re.search(r'next\s+(week|month|year)', cleaned_text)
        if next_match:
            unit = next_match.group(1)
            if unit == 'week':
                next_date = reference_date + timedelta(days=7)
                return datetime(next_date.year, next_date.month, next_date.day)
            elif unit == 'month':
                if reference_date.month == 12:
                    return datetime(reference_date.year + 1, 1, 1)
                else:
                    return datetime(reference_date.year, reference_date.month + 1, 1)
            elif unit == 'year':
                return datetime(reference_date.year + 1, 1, 1)
        
        # Handle X days/weeks/months/years from now
        time_delta_match = re.search(r'(\d+)\s+(day|week|month|year)s?\s+from\s+now', cleaned_text)
        if time_delta_match:
            amount = int(time_delta_match.group(1))
            unit = time_delta_match.group(2)
            
            if unit == 'day':
                next_date = reference_date + timedelta(days=amount)
                return datetime(next_date.year, next_date.month, next_date.day)
            elif unit == 'week':
                next_date = reference_date + timedelta(days=amount * 7)
                return datetime(next_date.year, next_date.month, next_date.day)
            elif unit == 'month':
                month = reference_date.month - 1 + amount
                year = reference_date.year + month // 12
                month = month % 12 + 1
                return datetime(year, month, 1)
            elif unit == 'year':
                return datetime(reference_date.year + amount, reference_date.month, 1)
        
        # Extract year if present
        year_match = re.search(r'\b(20\d{2}|19\d{2})\b', cleaned_text)
        if year_match:
            year = int(year_match.group(1))
            # If only year is specified, return January 1st of that year
            return datetime(year, 1, 1)
        
        # If all else fails, log warning and return None
        logger.warning(f"Could not normalize date text: '{date_text}'")
        return None
        
    except Exception as e:
        logger.error(f"Error normalizing date '{date_text}': {e}")
        return None
    
def relative_time(created_at: datetime) -> str:
    """Convert datetime to relative time string"""
    delta = datetime.now() - created_at
    if delta < timedelta(minutes=1):
        return "Just now"
    elif delta < timedelta(hours=1):
        return f"{delta.seconds//60}m"
    elif delta < timedelta(days=1):
        return f"{delta.seconds//3600}h"
    return f"{delta.days}d"