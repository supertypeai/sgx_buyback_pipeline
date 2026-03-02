from datetime import datetime

from sgx_scraper.utils.symbol_matching_helper import symbol_from_company_name
import logging


LOGGER = logging.getLogger(__name__)


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
        symbol_matched = symbol_from_company_name(issuer_security)
        if symbol_matched:
            return symbol_matched
    
    except Exception as error:
        LOGGER.error(f"[matching symbol] Fallback matching symbol failed: {error}")
    
    return None
    

def safe_convert_datetime(date: str) -> str | None: 
    if not date:
        return None 
    
    try:
        date_str = date.strip()
        for format in ("%d/%m/%Y", "%d-%b-%Y", "%d %b %Y", "%d %B %Y"):
            try:
                parsed_date = datetime.strptime(date_str, format)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue

    except Exception as error:
        LOGGER.error(f"[safe_convert_datetime] Error: {error} for value '{date}'")
        return None
    