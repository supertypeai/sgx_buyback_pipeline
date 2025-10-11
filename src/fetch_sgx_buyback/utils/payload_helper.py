from src.config.settings import LOGGER
from src.utils.sgx_parser_helper import safe_convert_float


def build_price_per_share(
        url: str, price_paid_per_share: str, 
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