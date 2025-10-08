from datetime import datetime 

from src.config.settings import SUPABASE_CLIENT

import re 


def safe_convert_datetime(date: str) -> str: 
    try:
        parsed_date = datetime.strptime(date.strip(), "%d/%m/%Y")
        return parsed_date.strftime("%Y-%m-%d")
    except Exception as error:
        print(f"[safe_convert_datetime] Error: {error} for value '{date}'")
        return None


def safe_convert_float(number_value: str) -> float:
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
        print(f"[safe_convert_float] Error: {error} for value '{number_value}'")
        return None


def build_price_per_share(
        price_paid_per_share: str, highest_per_share: str, lowest_per_share: str
) -> dict[str, float]:
    price_per_share = {}
    
    if price_paid_per_share and not highest_per_share and not lowest_per_share: 
        price_per_share['price_paid_per_share'] = safe_convert_float(price_paid_per_share)
    elif not price_paid_per_share and highest_per_share and lowest_per_share:
        price_per_share['highest'] = safe_convert_float(highest_per_share)
        price_per_share['lowest'] = safe_convert_float(lowest_per_share) 

    return price_per_share


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
        


