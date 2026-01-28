import re 
import logging


LOGGER = logging.getLogger(__name__)


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
        LOGGER.error(f"[safe_convert_float] Error: {error} for value '{number_value}'")
        return None
    

def build_price_per_share(
        price_paid_per_share: str, 
        highest_per_share: str, lowest_per_share: str
) -> dict[str, float]:
    price_per_share = {}
    
    try:
        if price_paid_per_share and not highest_per_share and not lowest_per_share: 
            price_per_share['price_paid_per_share'] = safe_convert_float(price_paid_per_share)
        
        elif not price_paid_per_share and highest_per_share and lowest_per_share:
            price_per_share['highest'] = safe_convert_float(highest_per_share)
            price_per_share['lowest'] = safe_convert_float(lowest_per_share) 
    
    except Exception as error:
        LOGGER.error(f'[build price per share] Error: {error}')

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
        LOGGER.error(f"[safe_extract_value] Error extracting value: {e}")
        return None 
        

def safe_extract_fallback(key_name: str, section_a: dict | list, section_b) -> str:
    try:
        raw_value_section_a = section_a.get(key_name, None) 
        value_section_a = safe_extract_value(raw_value_section_a)
        if value_section_a:
            return value_section_a
    except Exception as error:
        LOGGER.error(f"[safe extract fallback] Error {error}")

    try:
        raw_value_section_b = section_b.get(key_name, None)
        value_section_b = safe_extract_value(raw_value_section_b)
        if value_section_b:
            return value_section_b
    except Exception as error:
        LOGGER.error(f"[safe extract fallback] Error: {error}")

    return None 


def compute_mandate_remaining(total_mandate: float, cumulative_purchased: float) -> float:
    try: 
        if not total_mandate or not cumulative_purchased:
            return None
        
        mandate_remaining = total_mandate - cumulative_purchased
        return mandate_remaining
     
    except Exception as error:
        LOGGER.error(f"[compute_mandate_remaining] Error: {error}") 
        return None  
