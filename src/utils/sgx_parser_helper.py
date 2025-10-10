from datetime import datetime

from src.config.settings import LOGGER
from src.utils.symbol_matching_helper import match_company_name

import re 


def extract_symbol(issuer_security: str) -> str | None:
    try:
        parts = issuer_security.split('-')
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
        match = re.search(r"([\d,.]+)", number_value)
        if match:
            cleaned = match.group(1).replace(",", "")
            return float(cleaned)
        else:
            return None
        
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
    