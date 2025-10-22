from datetime import datetime

from src.config.settings import LOGGER
from src.utils.symbol_matching_helper import match_company_name

import re 


def extract_symbol(issuer_security: str) -> str | None:
    try:
        parts = issuer_security.split(' - ')
        if len(parts) > 1 and len(parts) <=3:
            symbol = parts[-1].strip()
            if symbol:  
                return symbol
    
    except Exception as error:
        LOGGER.error(f"[extract symbol] Failed to extract symbol from split: {error}")

    return None


def matching_symbol(issuer_security: str) -> str | None:
    try:
        company_matched = match_company_name(issuer_security)
        if company_matched:
            return company_matched.get('symbol')
    
    except Exception as error:
        LOGGER.error(f"[matching symbol] Fallback matching symbol failed: {error}")
    
    return None


def safe_convert_float(number_value: str) -> float | None:
    if not number_value:
        return None 
    
    try:
        # Remove leading numbering (e.g., "5. ", "6. ")
        value = re.sub(r'^\d+\.\s+(?!\d)', '', number_value)
        
        # Remove trailing numbering that appears on its own line or after whitespace
        value = re.sub(r'\s*\n\s*\d+\.\s*$', '', value)
        
        # If remains is just "N/A" or similar, return None
        if value.upper() in ['N/A', 'NA', 'NIL', 'NONE', '-', 'NOT APPLICABLE.', 'N.A.']:
            return None
        
        # Check for reference phrases that indicate no actual value
        reference_patterns = [
            r'refer\s+to\s+(?:paragraph|section|item|page|note|schedule|appendix|exhibit)',
            r'see\s+(?:paragraph|section|item|page|note|schedule|appendix|exhibit)',
            r'as\s+(?:described|stated|mentioned)\s+in',
            r'please\s+refer',
            r'refer\s+to\s+the\s+(?:above|below|attached)',
        ]
        
        for pattern in reference_patterns:
            if re.search(pattern, value, re.IGNORECASE):
                return None
        
        # Handle currency pattern first - if found, return immediately
        currency_pattern = r'([\d,]+(?:\.\d+)?)\s*(?:USD|SGD|\$|US\$|S\$)'
        currency_matches = re.findall(currency_pattern, value, re.IGNORECASE)
        if currency_matches:
            return float(currency_matches[0].replace(',', ''))
        
        # Handle shares/units pattern
        shares_pattern = r'([\d,]+(?:\.\d+)?)\s*(?:shares?|units?|securities|stocks?)'
        shares_matches = re.findall(shares_pattern, value, re.IGNORECASE)
        if shares_matches:
            total = 0.0
            for match in shares_matches:
                cleaned = match.replace(",", "")
                total += float(cleaned)
            return total
        
        # Fallback: extract all numbers (but avoid dates in parentheses)
        value_without_dates = re.sub(r'\([^)]*\d{2}/\d{2}/\d{4}[^)]*\)', '', value)
        
        fallback_matches = re.findall(r"([\d,]+(?:\.\d+)?)", value_without_dates)
        
        if not fallback_matches:
            return None
        
        # Sum all numbers found
        total = 0.0
        for match in fallback_matches:
            cleaned = match.replace(",", "")
            total += float(cleaned)
        
        return total
        
    except Exception as error:
        LOGGER.error(f"[safe_convert_float] Error: {error} for value '{number_value}'")
        return None
    

def safe_convert_datetime(date: str) -> str | None: 
    if not date:
        return None 
    
    try:
        date_str = date.strip()
        for format in ("%d/%m/%Y", "%d-%b-%Y"):
            try:
                parsed_date = datetime.strptime(date_str, format)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue

    except Exception as error:
        LOGGER.error(f"[safe_convert_datetime] Error: {error} for value '{date}'")
        return None
    