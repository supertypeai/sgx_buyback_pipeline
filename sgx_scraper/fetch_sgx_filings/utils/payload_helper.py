from sgx_scraper.fetch_sgx_filings.utils.converter_helper import get_latest_currency, calculate_currency_to_sgd
from sgx_scraper.config.settings import LOGGER
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    OTHER_CIRCUMSTANCES_RULES, TRANSACTION_KEYWORDS
)

import re 


def safe_convert_float(number_value: str) -> float | None:
    if not number_value:
        return None 
    
    try:
        # Remove leading numbering like 5. or trailing numbering
        value = re.sub(r'^\d+\.\s+(?!\d)', '', number_value)
        value = re.sub(r'\s*\n\s*\d+\.\s*$', '', value)
        
        # If remains is just "N/A" or similar, return None
        if value.upper() in ['N/A', 'NA', 'NIL', 'NONE', '-', 'NOT APPLICABLE.', 'N.A.']:
            return None
        
        # Check for reference phrases that indicate no actual value
        reference_patterns = [
            r'(refer\s+to\s+(?:paragraph|section|item|page|note|schedule|appendix|exhibit).*)',
            r'(see\s+(?:paragraph|section|item|page|note|schedule|appendix|exhibit).*)',
            r'(as\s+(?:described|stated|mentioned)\s+in.*)',
            r'(please\s+refer.*)',
            r'(refer\s+to\s+the\s+(?:above|below|attached).*)',
        ]

        for pattern in reference_patterns:
            value = re.sub(pattern, '', value, flags=re.IGNORECASE)
        
        # Handle currency pattern first - if found, return immediately
        currency_pattern = currency_pattern = r'(?:(?:USD|SGD|US\$|S\$|\$)\s*)?([\d,]+(?:\.\d+)?)(?:\s*(?:USD|SGD|US\$|S\$|\$))?'
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
        
        # Handle malformed numeric formats like 68.640.19, only if no letters/currency symbols are present
        malformed_pattern = r'\b\d{1,3}(?:\.\d{3})+\.\d{2}\b'
        if re.search(malformed_pattern, value) and not re.search(r'[a-zA-Z$]', value):
            value = re.sub(r'\.(?=\d{3}\.)', '', value)

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
    

def build_price_per_share(raw_value: str, number_of_stock: str) -> float | None:
    if raw_value is None or number_of_stock is None:
        return None
    
    try:
        cleaned_value = raw_value.lower().strip()
      
        # Handle "at a price per share of X" - e.g., "at a price per share of S$0.22"
        at_price_pattern = r'at\s+a?\s*price\s+per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))\s+of\s+(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)'
        at_price_match = re.search(at_price_pattern, cleaned_value, re.IGNORECASE)
        
        if at_price_match:
            per_share_value = at_price_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)
        
        # Handle "@" separator - e.g., "SGD167,958 @ SGD0.042 per share"
        at_pattern = r'@\s*(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:/shares?|/units?|per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies)))'
        at_match = re.search(at_pattern, cleaned_value, re.IGNORECASE)
        
        if at_match:
            per_share_value = at_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)
        
        # Handle Explicit per-share in parentheses with currency - e.g., "(being s$0.2649 per share)"
        explicit_per_share_pattern = r'\((?:being|at|@)?\s*(?:sg\$|s\$|usd|sgd|hkd|\$)?\s*([\d,]+(?:\.\d+)?)\s*per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))\)'
        explicit_match = re.search(explicit_per_share_pattern, cleaned_value, re.IGNORECASE)
        
        if explicit_match:
            per_share_value = explicit_match.group(1).replace(',', '')
            return safe_convert_float(per_share_value)

        # Handle "or" separator - e.g., "S$140,114 or S$1.3205/share"
        or_pattern = r'or\s+(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:/shares?|/units?|per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies)))'
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


def shares_percentage_to_decimal(share_percentage: float) -> float:
    try:
        if share_percentage is None or share_percentage == "":
            return None
        
        decimal_share_before = float(share_percentage) / 100
        decimal_share_before = float(f"{decimal_share_before:.5f}")
        return decimal_share_before
    
    except ValueError as error:
        LOGGER.error(f"[shares_percentage_to_decimal] Error: {error}")
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


def get_transaction_type_from_desc(description: str, value: float | None) -> str:
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
        
        if transaction_type == 'transfer' and value is not None:
            LOGGER.info(f'[get_transaction_type_from_desc] transfer ignore due to value is not None')
            return None 

        if not transaction_type:
            LOGGER.warning(f"[get_transaction_type_from_desc] No keywords matched for description: '{description}'")

        return transaction_type

    except Exception as error:
        LOGGER.error(f"[get_transaction_type_from_desc] Error: {error}")
        return None


def build_transaction_type(
    circumstance_interest_raw: dict[str, any],
    transaction_details: list[dict[str, any]] | float
) -> str:
    try:
        # get total value 
        if isinstance(transaction_details, list):
            value = [value_detail.get('value', None) for value_detail in transaction_details]  
            value = value[0]
        else:
            value = transaction_details 

        circumstance_interest = circumstance_interest_raw.get('results')
        circumstance_interest = get_circumstance_interest(circumstance_interest)
        print(f'\ncircumstance_interest processed: {circumstance_interest}')

        transaction_type = None 
        key = circumstance_interest.get('key')
        checked = circumstance_interest.get('checked')
        specific_key = circumstance_interest.get('specific_key')

        if checked:
            if key == 'others_specify':
                description = circumstance_interest.get('description', None)
                transaction_type = get_transaction_type_from_desc(description, value)
            elif key == 'acquisition':
                transaction_type = 'buy'
            elif key == 'disposal':
                transaction_type = 'sell'
            elif key == 'other_circumstances':
                lookup_key = specific_key.lower().strip()
                transaction_type = OTHER_CIRCUMSTANCES_RULES.get(lookup_key)

        return transaction_type

    except Exception as error:
        LOGGER.error(f"[build_transaction_type] Error: {error}", exc_info=True)
        return None
                

def build_shareholder_name_transfer(
    circumstance_interest_raw: dict[str, any],
    shareholder_name: str
) -> str:
    try:
        circumstance_interest = circumstance_interest_raw.get('results')
        circumstance_interest = get_circumstance_interest(circumstance_interest)
        description = circumstance_interest.get('description', None)

        print(f'\ndescription for transfer: {description}, shareholder name: {shareholder_name}') 

        if not description:
            return shareholder_name 
        
        description_lower = description.lower().strip()

        # Matches: "treasury shares" or "transfer of treasury shares"
        if re.search(r'treasury\s+shares?', description_lower):
            return f"Company Treasury [->] {shareholder_name}"
        
        # Matches: "Tan Sri Datuk Tiong Su Kouk transfer 7,900,000 ordinary shares to his family member"
        name_first_pattern = r'^([A-Z][a-zA-Z\s\.\,]+?)\s+transfer(?:red)?\s+[\d,]+\s+(?:ordinary\s+)?shares?\s+to\s+(?:his|her|their)?\s*(.+?)(?:\.|$)'
        match = re.search(name_first_pattern, description)
        
        if match:
            from_person = match.group(1).strip()
            to_person = match.group(2).strip()
            
            # Keep the original "to_person" text (like "family member")
            return f"{from_person} [->] {to_person}"
        
        # Matches: "from Mr John to Mr Smith", "by Mr John to his son Mr Smith"
        from_to_pattern = r'(?:from|by)\s+([^,]+?)\s+to\s+(?:his|her|their)?\s*(?:son|daughter|spouse|wife|husband|child|children|family|relative)?\s*,?\s*([^,\.]+?)(?:\s+by\s+way|,|\.|\s+pursuant|\s+under)'
        match = re.search(from_to_pattern, description, re.IGNORECASE)
        
        if match:
            from_person = match.group(1).strip()
            to_person = match.group(2).strip()
            
            # Clean up common prefixes/titles (but keep honorifics)
            from_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor)\s+', '', from_person, flags=re.IGNORECASE).strip()
            to_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor)\s+', '', to_person, flags=re.IGNORECASE).strip()
            
            return f"{from_person} [->] {to_person}"
        
        # Matches: "Transfer of 35,000,000 shares by Mr Goh Kim San to his son, Mr Goh Yi Shun, Joshua"
        transfer_by_to_pattern = r'transfer\s+of\s+[\d,]+\s+shares?\s+by\s+([^,]+?)\s+to\s+(?:his|her|their)?\s*(?:son|daughter|spouse|wife|husband|child|children|family|relative)?\s*,?\s*([^,\.]+?)(?:\s+by\s+way|,|\.|\s+pursuant|\s+under)'
        match = re.search(transfer_by_to_pattern, description, re.IGNORECASE)
        
        if match:
            from_person = match.group(1).strip()
            to_person = match.group(2).strip()
            
            # Clean up
            from_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor)\s+', '', from_person, flags=re.IGNORECASE).strip()
            to_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor)\s+', '', to_person, flags=re.IGNORECASE).strip()
            
            return f"{from_person} [->] {to_person}"
        
        # Matches: "John transferred to Smith", "shares by John to Smith"
        transfer_pattern = r'(?:shares?\s+)?(?:transferred\s+)?(?:by\s+)?([A-Z][a-zA-Z\s\.]+?)\s+to\s+([A-Z][a-zA-Z\s\.]+?)(?:\s+by\s+way|,|\.|\s+pursuant|\s+under)'
        match = re.search(transfer_pattern, description)
        
        if match:
            from_person = match.group(1).strip()
            to_person = match.group(2).strip()
            
            # Clean up
            from_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor)\s+', '', from_person, flags=re.IGNORECASE).strip()
            to_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor)\s+', '', to_person, flags=re.IGNORECASE).strip()
            
            return f"{from_person} [->] {to_person}"
        
        # Matches: "transfer from Company" -> "Company [->] shareholder_name"
        from_pattern = r'transfer\s+(?:of\s+shares?\s+)?from\s+([^,\.]+?)(?:\s+to\s+me|,|\.)'
        match = re.search(from_pattern, description, re.IGNORECASE)
        
        if match:
            from_person = match.group(1).strip()
            from_person = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Professor|the)\s+', '', from_person, flags=re.IGNORECASE).strip()
            return f"{from_person} [->] {shareholder_name}"
        
        return None

    except Exception as error:
        LOGGER.error(f"[build_shareholder_name_transfer] Error: {error}")
        return None 