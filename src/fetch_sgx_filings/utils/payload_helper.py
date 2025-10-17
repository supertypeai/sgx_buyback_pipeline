from src.utils.sgx_parser_helper import safe_convert_float
from src.fetch_sgx_filings.utils.converter_helper import get_latest_currency, calculate_currency_to_sgd
from src.config.settings import LOGGER
from src.fetch_sgx_filings.utils.constants import (
    OTHER_CIRCUMSTANCES_RULES, TRANSACTION_KEYWORDS
)


def build_price_per_share(raw_value: str, number_of_stock: str) -> float | None:
    if raw_value is None or number_of_stock is None:
        return None
    
    try:
        cleaned_value = raw_value.lower().strip()

        if 'share' in cleaned_value or 'per unit' in cleaned_value:
            return safe_convert_float(raw_value)
        
        value = safe_convert_float(raw_value)
        
        price_per_share = None
        if value and number_of_stock:
            price_per_share = round(value / number_of_stock, 4)
            return price_per_share
    
    except Exception as error:
        LOGGER.error(f"[build price per share] Error: {error}")
        return None 


def build_value(raw_value: str, number_of_stock: float) -> float | None:
    print(f"[build_value] Input - raw_value: {repr(raw_value)}, number_of_stock: {number_of_stock}")
    
    if raw_value is None and number_of_stock is None:
        return None
    
    try:
        clean_value = raw_value.lower().strip()

        if 'us'in clean_value:
            sgd_rate = get_latest_currency('usd')
            usd_value = safe_convert_float(raw_value)
            value = calculate_currency_to_sgd(usd_value, sgd_rate)
            
            if 'share' in clean_value or 'per unit':
                value = number_of_stock * value
            return value 
        
        if 'hk'in clean_value:
            sgd_rate = get_latest_currency('hkd')
            hk_value = safe_convert_float(raw_value)
            value = calculate_currency_to_sgd(hk_value, sgd_rate)
            
            if 'share' in clean_value or 'per unit':
                value = number_of_stock * value
            return value 
        
        if 'share' in clean_value or 'per unit':
            return number_of_stock * safe_convert_float(raw_value)
        
        return safe_convert_float(raw_value)
    
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
                
