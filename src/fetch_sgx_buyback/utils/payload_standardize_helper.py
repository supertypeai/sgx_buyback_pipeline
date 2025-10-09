from datetime import datetime 

from src.fetch_sgx_buyback.utils.symbol_matching_helper import match_company_name

import re 


def safe_convert_datetime(date: str) -> str: 
    try:
        parsed_date = datetime.strptime(date.strip(), "%d/%m/%Y")
        return parsed_date.strftime("%Y-%m-%d")
    except Exception as error:
        print(f"[safe_convert_datetime] Error: {error} for value '{date}'")
        return None


def safe_convert_float(url: str, number_value: str) -> float:
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
        print(f"[safe_convert_float] Error: {error} for value '{number_value} url: {url}'")
        return None


def build_price_per_share(
        url, price_paid_per_share: str, highest_per_share: str, lowest_per_share: str
) -> dict[str, float]:
    price_paid_per_share = safe_extract_value(price_paid_per_share)
    highest_per_share = safe_extract_value(highest_per_share)
    lowest_per_share = safe_extract_value(lowest_per_share)

    price_per_share = {}
    
    if price_paid_per_share and not highest_per_share and not lowest_per_share: 
        price_per_share['price_paid_per_share'] = safe_convert_float(url, price_paid_per_share)
    elif not price_paid_per_share and highest_per_share and lowest_per_share:
        price_per_share['highest'] = safe_convert_float(url, highest_per_share)
        price_per_share['lowest'] = safe_convert_float(url, lowest_per_share) 

    return price_per_share


def extract_symbol(issuer_security: str):
    try:
        parts = issuer_security.split('-')
        if len(parts) > 1:
            symbol = parts[-1].strip()
            if symbol:  
                return symbol
    except Exception as error:
        print(f"Failed to extract symbol from split: {error}")

    try:
        company_matched = match_company_name(issuer_security)
        if company_matched:
            return company_matched.get('symbol')
    except Exception as error:
        print(f"Fallback matching symbol failed: {error}")

    return None


def safe_extract_value(value: str | list) -> str:  
    try:
        if isinstance(value, list) and value:
            # Get Number not percentage 
            extracted_value = value[0] if value else None
        elif isinstance(value, str) and value:
            extracted_value = value 
        else: 
            extracted_value = None
        return extracted_value
    
    except Exception as e:
        print(f"[safe_extract_value] Error extracting value: {e}")
        return None 
        


