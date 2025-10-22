from src.utils.sgx_parser_helper import safe_convert_float
from src.fetch_sgx_filings.utils.converter_helper import get_latest_currency, calculate_currency_to_sgd
from src.config.settings import LOGGER
from src.fetch_sgx_filings.utils.constants import (
    OTHER_CIRCUMSTANCES_RULES, TRANSACTION_KEYWORDS
)

import re 


def build_price_per_share(raw_value: str, number_of_stock: str) -> float | None:
    if raw_value is None or number_of_stock is None:
        return None
    
    try:
        cleaned_value = raw_value.lower().strip()
      
        # Handle "at a price per share of X" - e.g., "at a price per share of S$0.22"
        at_price_pattern = r'at\s+a?\s*price\s+per\s+(?:share|unit|security|stapled\s+security)\s+of\s+(?:s\$|usd|sgd|\$)?\s*([\d,]+(?:\.\d+)?)'
        at_price_match = re.search(at_price_pattern, cleaned_value, re.IGNORECASE)
        
        if at_price_match:
            per_share_value = at_price_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)
        
        # Handle "@" separator - e.g., "SGD167,958 @ SGD0.042 per share"
        at_pattern = r'@\s*(?:s\$|usd|sgd|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:/share|/unit|per\s+(?:share|unit))'
        at_match = re.search(at_pattern, cleaned_value, re.IGNORECASE)
        
        if at_match:
            per_share_value = at_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)
        
        # Handle Explicit per-share in parentheses with currency - e.g., "(being s$0.2649 per share)"
        explicit_per_share_pattern = r'\((?:being|at|@)?\s*(?:s\$|usd|sgd|\$)?\s*([\d,]+(?:\.\d+)?)\s*per\s+(?:share|unit)\)'
        explicit_match = re.search(explicit_per_share_pattern, cleaned_value, re.IGNORECASE)
        
        if explicit_match:
            per_share_value = explicit_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)

        # Handle "or" separator - e.g., "S$140,114 or S$1.3205/share"
        or_pattern = r'or\s+(?:s\$|usd|sgd|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:/share|/unit|per\s+(?:share|unit))'
        or_match = re.search(or_pattern, cleaned_value, re.IGNORECASE)
        
        if or_match:
            per_share_value = or_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)

        # Handle text contains context
        context_patterns = [
            r'pursuant\s+to',
        ]
        
        has_context = any(re.search(pattern, cleaned_value, re.IGNORECASE) for pattern in context_patterns)
        
        if (
            'share' in cleaned_value or 
            'per unit' in cleaned_value or 
            'per share' in cleaned_value or 
            'per stapled security' in cleaned_value or
            has_context
        ):
            return safe_convert_float(raw_value)
        
        value = safe_convert_float(raw_value)
        
        price_per_share = None
        if value and number_of_stock:
            price_per_share = round(value / number_of_stock, 4)
            return price_per_share
    
    except Exception as error:
        LOGGER.error(f"[build price per share] Error: {error}")
        return None 


def safe_round(value, context="", digits=4):
    if value is None:
        LOGGER.warning(f"[safe_round] Cannot round None {context} {value}")
        return None
    try:
        return round(value, digits)
    except Exception as error:
        LOGGER.error(f"[safe_round] Rounding failed for {value} {context}: {error}")
        return None
    

def build_value(raw_value: str, number_of_stock: float) -> float | None:
    print(f"[build_value] Input - raw_value: {raw_value}, number_of_stock: {number_of_stock}")
    
    if raw_value is None and number_of_stock is None:
        return None
    
    try:
        clean_value = raw_value.lower().strip()

        # Check if "per share/unit/security" is INSIDE parentheses with a currency value
        parentheses_per_share = re.search(
            r'\((?:[^)]*(?:s\$|usd|sgd|\$|being|at))?[^)]*[\d,]+(?:\.\d+)?\s*(?:per\s+(?:share|unit|security|stapled\s+security)|/share|/unit|/security)[^)]*\)', 
            clean_value
        )
        has_per_share_clarification = parentheses_per_share is not None
        
        # Check if "or" pattern exists
        or_per_share = re.search(
            r'\s+or\s+(?:s\$|usd|sgd|\$)?\s*[\d,]+(?:\.\d+)?\s*(?:per\s+(?:share|unit|security|stapled\s+security)|/share|/unit|/security)', 
            clean_value, 
            re.IGNORECASE
        )
        has_or_pattern = or_per_share is not None
        
        # Check if "@" pattern exists
        at_per_share = re.search(
            r'@\s*(?:s\$|usd|sgd|\$)?\s*[\d,]+(?:\.\d+)?\s*(?:per\s+(?:share|unit|security|stapled\s+security)|/share|/unit|/security)',
            clean_value,
            re.IGNORECASE
        )
        has_at_pattern = at_per_share is not None
        
        # Check if "at a price per share/unit/security of" pattern exists
        at_price_per_share = re.search(
            r'at\s+a?\s*price\s+per\s+(?:share|unit|security|stapled\s+security)\s+of\s+(?:s\$|usd|sgd|\$)?\s*[\d,]+(?:\.\d+)?',
            clean_value,
            re.IGNORECASE
        )
        has_at_price_pattern = at_price_per_share is not None
        
        # If any clarification pattern exists, don't multiply
        is_clarification = has_per_share_clarification or has_or_pattern or has_at_pattern or has_at_price_pattern

        # Handle text contains context
        context_patterns = [
            r'pursuant\s+to',
        ]

        has_context = any(re.search(pattern, clean_value, re.IGNORECASE) for pattern in context_patterns)
        should_multiply = (
            'share' in clean_value or 
            'per unit' in clean_value or 
            'security' in clean_value or 
            has_context
        ) and not is_clarification
        print(f'should: {should_multiply}')

        if 'us'in clean_value:
            sgd_rate = get_latest_currency('usd')
            usd_value = safe_convert_float(raw_value)
            value = calculate_currency_to_sgd(usd_value, sgd_rate)
            print(f'value: {value} type: {type(value)}')

            if should_multiply:
                value = float(number_of_stock) * value
                value = safe_round(value, 'usd conversion')
            return value
        
        if 'hk'in clean_value:
            print('Converted to sgd from hkd')
            sgd_rate = get_latest_currency('hkd')
            hk_value = safe_convert_float(raw_value)
            value = calculate_currency_to_sgd(hk_value, sgd_rate)

            if should_multiply:
                value = number_of_stock * value
                value = safe_round(value, 'hkd conversion')
            return value 
        
        if should_multiply:
            value = safe_convert_float(raw_value)
            print(f'Number stock multiplied with value: {value}')
            value = number_of_stock * value
            value = safe_round(value, 'Multiplied')
            return value 

        value_to_return =  safe_convert_float(raw_value)
        value_to_return = safe_round(value_to_return, 'Raw value returned')
        return value_to_return
    
    except Exception as error:
        LOGGER.error(f"[build value] Error: {error}")
        return None 


def get_circumstance_interest(circumstance_interest: dict[str, any]) -> dict[str, any]:
    try:
        for key, value in circumstance_interest.items():
            if key == 'others_specify':
                checked = value.get('checked')
                desc = value.get('description')
                if checked:
                    return {
                        'key': 'others_specify',
                        'checked': checked, 
                        'description': desc
                    }
            else:
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, dict) and 'checked' in sub_value:
                        checked = sub_value.get('checked')
                        desc = sub_value.get('desc')
                        if checked: 
                            return {
                                'key': key, 
                                'specific_key': sub_key,
                                'checked': checked,
                                'description': desc
                            }
                    elif sub_value:
                        return {
                            'key': key,
                            'specific_key': sub_key,
                            'checked': sub_value
                        }

    except Exception as error:
        LOGGER.error(f"[get_transaction_type] Error: {error}")
        return None 


def get_transaction_type_from_desc(description: str) -> str:
    try:
        if not description:
            LOGGER.info(f'[get_transaction_type_from_desc] description is None')
            return None 

        desc_lower = description.lower()

        transaction_type = next(
            (key for key, values in TRANSACTION_KEYWORDS.items()
            if any(value.lower() in desc_lower for value in values)),
            None
        )

        if not transaction_type:
            LOGGER.warning(f"[get_transaction_type_from_desc] No keywords matched for description: '{description}'")

        return transaction_type

    except Exception as error:
        LOGGER.error(f"[get_transaction_type_from_desc] Error: {error}")
        return None


def build_transaction_type(circumstance_interest_raw: dict[str, any]) -> str:
    try:
        circumstance_interest = circumstance_interest_raw.get('results')
        circumstance_interest = get_circumstance_interest(circumstance_interest)
    
        transaction_type = None 
        key = circumstance_interest.get('key')
        checked = circumstance_interest.get('checked')
        specific_key = circumstance_interest.get('specific_key')

        if checked:
            if key == 'others_specify':
                description = circumstance_interest.get('description', None)
                transaction_type = get_transaction_type_from_desc(description)
            elif key == 'acquisition':
                transaction_type = 'buy'
            elif key == 'disposal':
                transaction_type = 'sell'
            elif key == 'other_circumstances':
                lookup_key = specific_key.lower().strip()
                transaction_type = OTHER_CIRCUMSTANCES_RULES.get(lookup_key)

        return transaction_type

    except Exception as error:
        LOGGER.error(f"[build_transaction_type] Error: {error}")
        return None
                
