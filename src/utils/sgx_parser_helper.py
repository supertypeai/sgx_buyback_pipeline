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
        
        currency_pattern = r'([\d,]+(?:\.\d+)?)\s*(?:USD|SGD|\$|US\$|S\$)'
        matches = re.findall(currency_pattern, value, re.IGNORECASE)
        if matches:
            return float(matches[0].replace(',', ''))
        
        pattern = r'([\d,]+(?:\.\d+)?)\s*(?:shares?|units?|securities|stocks?)'
        matches = re.findall(pattern, value, re.IGNORECASE)
        
        if not matches:
            # LOGGER.warning(f"[safe_convert_float] Using fallback pattern for: '{number_value}'")
            matches = re.findall(r"([\d,]+(?:\.\d+)?)", value)

        if not matches:
            return None
        
        total = 0.0
        for match in matches:
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
    