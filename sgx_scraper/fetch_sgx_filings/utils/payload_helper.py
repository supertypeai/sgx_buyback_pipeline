from tavily import TavilyClient
from rapidfuzz import fuzz, process

from sgx_scraper.config.settings import TAVILY_API_KEYS, SUPABASE_CLIENT
from sgx_scraper.fetch_sgx_filings.utils.currency_converter import (
    get_latest_currency,
    calculate_currency_to_sgd
)
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    OTHER_CIRCUMSTANCES_RULES, TRANSACTION_KEYWORDS,
    ACQUISITION_RULES,
    TITLE_BODY_SIDED_TAG_RULES,
    HOLDER_NAME_ACRONYMS,
    HOLDER_SHORT_NAME_PATTERN,
    FILING_EXTRA_FIELDS,
    FILING_TABLE_COLUMNS,
    TITLE_BODY_TAG_PRIORITY,
    TITLE_BODY_RECEIPT_TAGS,
    TITLE_BODY_TAG_RULES,
    TITLE_BODY_CIRCUMSTANCE_RULES,
    TITLE_BODY_TYPE_RULES,
    UNIT_ISSUER_PATTERN,
)
from sgx_scraper.utils.json_helper import open_json

from functools import lru_cache

import re
import logging
import json


LOGGER = logging.getLogger(__name__)

CURRENCY_CODES = (
    "AED",
    "AUD",
    "CAD",
    "CHF",
    "CNY",
    "EUR",
    "GBP",
    "HKD",
    "IDR",
    "INR",
    "JPY",
    "KRW",
    "MYR",
    "NZD",
    "PHP",
    "QAR",
    "SAR",
    "SGD",
    "THB",
    "TWD",
    "USD",
    "VND",
)


def extract_currency_code(raw_value: str) -> str | None:
    normalized_value = raw_value.upper()
    
    detected_currencies = {
        currency_code
        for currency_code in CURRENCY_CODES
        # A code may be written straight against the figure, as in "USD14,584"
        if re.search(rf"(?<![A-Z]){currency_code}(?![A-Z])", normalized_value)
    }

    if "US$" in normalized_value:
        detected_currencies.add("USD")

    if "HK$" in normalized_value:
        detected_currencies.add("HKD")

    if re.search(r"(?<![A-Z])S\$", normalized_value):
        detected_currencies.add("SGD")

    if "RMB" in normalized_value:
        detected_currencies.add("CNY")

    if len(detected_currencies) != 1:
        return None

    return detected_currencies.pop()


def convert_price_to_sgd(
    price: float | None,
    raw_value: str,
    exchange_rate_date: str | None,
) -> float | None:
    if price is None:
        return None

    currency_code = extract_currency_code(raw_value)

    if currency_code is None or currency_code == "SGD":
        return price

    rate_sgd = get_latest_currency(currency_code, exchange_rate_date)

    if rate_sgd is None:
        LOGGER.warning(
            "[currency_converter] Could not convert %s price on %s",
            currency_code,
            exchange_rate_date,
        )
        return None

    return calculate_currency_to_sgd(price, rate_sgd, digits=4)


def get_db(
    table: str, 
    query_modifier,
    columns ="*",
) -> list[dict]:
    query = (
        SUPABASE_CLIENT
        .table(table)
        .select(columns)
    )

    if query_modifier is not None: 
        query = query_modifier(query)

    response = query.execute()
    return response.data 


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
        currency_pattern = r'(?:(?:USD|SGD|US\$|S\$|\$)\s*)?(\d[\d,]*(?:\.\d+)?)(?:\s*(?:USD|SGD|US\$|S\$|\$))?'
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
    

# A figure is a price per security only when the text says so. Words such as
# "shares" or "pursuant to" alone do not make it one: "$3,601.17 paid to
# acquire shares" is a total
PER_SECURITY_PATTERN = re.compile(
    r'per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))'
    r'|/\s*(?:shares?|units?|securit(?:y|ies))',
    re.IGNORECASE,
)


def clean_amount_text(raw_amount: str | None) -> str | None:
    # "110, 700" is 110,700 typed with a stray space
    if not raw_amount:
        return raw_amount

    return re.sub(r'(?<=\d),\s+(?=\d{3}(?!\d))', ',', raw_amount)


def is_percentage_not_money(raw_value: str | None) -> bool:
    # "50% of its management fee" is a share of something, not a sum of
    # money, so it gives no price
    return bool(
        raw_value
        and re.search(r"\d\s*%", raw_value)
        and "$" not in raw_value
        and extract_currency_code(raw_value) is None
    )


def pick_stated_amount(
    raw_amount: str | None,
    share_table_result: dict[str, any],
) -> int | None:
    # An amount field written as a sentence can hold several figures, such as
    # "convert S$357,500 in principal amount of notes and 27,500,000 warrants
    # into total 55,000,000 shares". The figure that equals a movement in the
    # holder's own table is the number of securities. The table only
    # confirms a figure the form states: with no such figure, or more than
    # one, nothing is picked and the first figure is used as before
    figures = {
        int(figure.replace(",", ""))
        for figure in re.findall(r"\d[\d,]*", clean_amount_text(raw_amount) or "")
        if figure.replace(",", "")
    }

    if len(figures) < 2:
        return None

    movements = {
        abs(share_table_result[f"{prefix}_after"] - share_table_result[f"{prefix}_before"])
        for prefix in ("holding", "direct", "deemed")
        if share_table_result.get(f"{prefix}_after") is not None
        and share_table_result.get(f"{prefix}_before") is not None
    } - {0}

    stated = figures & movements

    return stated.pop() if len(stated) == 1 else None


ACQUISITION_WORDING = re.compile(r'\b(?:acquir\w*|purchas\w*|bought)\b', re.IGNORECASE)
DISPOSAL_WORDING = re.compile(r'\b(?:dispos\w*|sold|sale)\b', re.IGNORECASE)


def consideration_contradicts_amount(
    raw_value: str | None,
    raw_amount: str | None,
) -> bool:
    # "$3,601.17 paid to acquire shares" against "Disposal of 268,300 shares":
    # the consideration is for a different leg than the amount reported, so
    # no price can be worked out from the two
    if not raw_value or not raw_amount:
        return False

    value_buys = bool(ACQUISITION_WORDING.search(raw_value))
    value_sells = bool(DISPOSAL_WORDING.search(raw_value))
    amount_buys = bool(ACQUISITION_WORDING.search(raw_amount))
    amount_sells = bool(DISPOSAL_WORDING.search(raw_amount))

    return (
        (value_buys and not value_sells and amount_sells and not amount_buys)
        or (value_sells and not value_buys and amount_buys and not amount_sells)
    )


def normalize_per_unit_wording(value: str) -> str:
    # "per management fee unit" or "per new share" names the same thing as
    # "per unit" or "per share", so the words in between are dropped
    return re.sub(
        r'\bper\s+(?:[a-z\-]+\s+){1,3}(shares?|units?|stapled\s+securit(?:y|ies))\b',
        r'per \1',
        value,
        flags=re.IGNORECASE,
    )


PRICE_TRANCHE_PATTERN = re.compile(
    r'([\d,]{4,})\s*(?:new\s+)?(?:units?|shares?|stapled\s+securit(?:y|ies))\s*'
    r'(?:[a-z]+\s+){0,3}?'
    r'(?:@|at)\s*((?:sg\$|s\$|us\$|usd|sgd|hkd|\$)?\s*[\d,]*\.?\d+)',
    re.IGNORECASE,
)


def build_price_tranches(
    raw_value: str | None,
    number_of_stock: float | None,
    exchange_rate_date: str | None = None,
) -> list[dict] | None:
    # Securities issued at several prices are written as tranches, e.g.
    # "10,277,823 units @ S$1.176 per unit and 7,698,190 units @ S$1.2221 per
    # unit". They are only used when the tranche amounts add up to the
    # transaction amount, so a figure that merely looks like one is ignored
    if not raw_value or not number_of_stock:
        return None

    tranches = []

    for amount_text, price_text in PRICE_TRANCHE_PATTERN.findall(raw_value):
        amount = safe_convert_float(amount_text.replace(',', ''))
        price = convert_price_to_sgd(
            safe_convert_float(re.sub(r'[^\d.]', '', price_text)),
            price_text,
            exchange_rate_date,
        )

        if amount is None or price is None:
            return None

        tranches.append({"price": price, "amount": int(amount)})

    if len(tranches) < 2:
        return None

    if sum(tranche["amount"] for tranche in tranches) != number_of_stock:
        return None

    return tranches


def is_total_too_small_to_be_total(value: float | None, number_of_stock: float | None) -> bool:
    # A bare figure such as "0.275" against millions of shares cannot be the
    # total paid: divided out it rounds to a price of zero. The filer wrote
    # the price per share in the total field, so it is read as the price
    return bool(
        value and number_of_stock and round(value / number_of_stock, 4) == 0
    )


def build_price_per_share(
    raw_value: str,
    number_of_stock: float,
    exchange_rate_date: str | None = None,
) -> float | None:
    if raw_value is None or number_of_stock is None:
        return None

    if is_percentage_not_money(raw_value):
        return None

    cleaned_value = normalize_per_unit_wording(raw_value.lower().strip())

    # Handle "at a price per share of X" - e.g., "at a price per share of S$0.22"
    at_price_pattern = r'at\s+a?\s*price\s+per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))\s+of\s+(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)'
    at_price_match = re.search(at_price_pattern, cleaned_value, re.IGNORECASE)

    if at_price_match:
        per_share_value = at_price_match.group(1).replace(',', '')
        return convert_price_to_sgd(
            safe_convert_float(per_share_value),
            raw_value,
            exchange_rate_date,
        )

    # Handle "@" separator - e.g., "SGD167,958 @ SGD0.042 per share"
    at_pattern = r'@\s*(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:/shares?|/units?|per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies)))'
    at_match = re.search(at_pattern, cleaned_value, re.IGNORECASE)

    if at_match:
        per_share_value = at_match.group(1).replace(',', '')
        return convert_price_to_sgd(
            safe_convert_float(per_share_value),
            raw_value,
            exchange_rate_date,
        )

    # Handle Explicit per-share in parentheses with currency - e.g., "(being s$0.2649 per share)"
    explicit_per_share_pattern = r'\((?:being|at|@)?\s*(?:sg\$|s\$|usd|sgd|hkd|\$)?\s*([\d,]+(?:\.\d+)?)\s*per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))\)'
    explicit_match = re.search(explicit_per_share_pattern, cleaned_value, re.IGNORECASE)

    if explicit_match:
        per_share_value = explicit_match.group(1).replace(',', '')
        return convert_price_to_sgd(
            safe_convert_float(per_share_value),
            raw_value,
            exchange_rate_date,
        )

    # Handle "or" separator - e.g., "S$140,114 or S$1.3205/share"
    or_pattern = r'or\s+(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)\s*(?:/shares?|/units?|per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies)))'
    or_match = re.search(or_pattern, cleaned_value, re.IGNORECASE)

    if or_match:
        per_share_value = or_match.group(1).replace(',', '')
        return convert_price_to_sgd(
            safe_convert_float(per_share_value),
            raw_value,
            exchange_rate_date,
        )

    # Handle direct price per unit format - e.g., "S$0.007 per Rights Unit"
    direct_per_unit_pattern = r'(?:sg\$|s\$|usd|sgd|hkd|us\$|\$)?\s*([\d,]+(?:\.\d+)?)\s*per\s+(?:shares?|units?|rights\s+units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))'
    direct_match = re.search(direct_per_unit_pattern, cleaned_value, re.IGNORECASE)

    if direct_match:
        per_share_value = direct_match.group(1).replace(',', '')
        return convert_price_to_sgd(
            safe_convert_float(per_share_value),
            raw_value,
            exchange_rate_date,
        )

    if PER_SECURITY_PATTERN.search(cleaned_value):
        return convert_price_to_sgd(
            safe_convert_float(raw_value),
            raw_value,
            exchange_rate_date,
        )

    value = safe_convert_float(raw_value)

    if is_total_too_small_to_be_total(value, number_of_stock):
        return convert_price_to_sgd(value, raw_value, exchange_rate_date)

    if value is not None and number_of_stock:
        return convert_price_to_sgd(
            round(value / number_of_stock, 4),
            raw_value,
            exchange_rate_date,
        )

    return None


def safe_round(value, context="", digits=4):
    if value is None:
        # None is an expected, valid input (e.g. an absent price/consideration)
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


def build_value(
    raw_value: str,
    number_of_stock: float,
    exchange_rate_date: str | None = None,
) -> float | None:
    # print(f"[build_value] Input - raw_value: {raw_value}, number_of_stock: {number_of_stock}")
    
    if raw_value is None and number_of_stock is None:
        return None

    if is_percentage_not_money(raw_value):
        return None

    try:
        clean_value = normalize_per_unit_wording(raw_value.lower().strip())

        issue_price_match = re.search(
            r'at\s+(?:an?\s+)?issue\s+price\s+of',
            clean_value,
            re.IGNORECASE,
        )

        if issue_price_match and number_of_stock is not None:
            price_per_share = build_price_per_share(
                raw_value,
                number_of_stock,
                exchange_rate_date,
            )

            if price_per_share is not None:
                return safe_round(
                    number_of_stock * price_per_share,
                    'issue price',
                )

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

        has_context = any(
            re.search(pattern, clean_value, re.IGNORECASE) 
            for pattern in context_patterns
        )

        should_multiply = (
            PER_SECURITY_PATTERN.search(clean_value) is not None or
            is_total_too_small_to_be_total(
                safe_convert_float(raw_value), number_of_stock
            )
        ) and not is_clarification

        currency_code = extract_currency_code(raw_value)

        if currency_code and currency_code != "SGD":
            rate_sgd = get_latest_currency(currency_code, exchange_rate_date)
            currency_value = safe_convert_float(raw_value)

            if rate_sgd is None or currency_value is None:
                return None

            value = calculate_currency_to_sgd(currency_value, rate_sgd, digits=4)

            if should_multiply:
                value = float(number_of_stock) * value
                value = safe_round(value, f'{currency_code} conversion')
            return value
        
        if should_multiply:
            value = safe_convert_float(raw_value)
            # print(f'Number stock multiplied with value: {value}')
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
    if not circumstance_interest:
        return None

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
                    desc = sub_value.get('description')

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

    return None


def get_transaction_type_from_desc(description: str, value: float | None) -> str:
    if not description:
        LOGGER.info('[get_transaction_type_from_desc] description is None')
        return None

    desc_lower = description.lower()

    transaction_type = next(
        (key for key, keywords in TRANSACTION_KEYWORDS.items()
         if any(keyword.lower() in desc_lower for keyword in keywords)),
        None
    )

    if transaction_type == 'transfer' and value is not None:
        LOGGER.info('[get_transaction_type_from_desc] transfer ignore due to having value')
        return None

    # No keyword match returns None on purpose, the LLM fallback classifies
    # transaction_type from the circumstance description in that case
    return transaction_type


def build_transaction_type(
    circumstance_interest_raw: dict[str, any]
) -> str:
    if not circumstance_interest_raw:
        LOGGER.warning(
            "[build_transaction_type] circumstance_interest_raw is None"
        )
        return None

    circumstance_interest = get_circumstance_interest(
        circumstance_interest_raw.get('results')
    )

    if not circumstance_interest:
        return None

    transaction_type = None

    key = circumstance_interest.get('key')
    checked = circumstance_interest.get('checked')
    specific_key = circumstance_interest.get('specific_key')

    if checked:
        if key == 'others_specify':
            description = circumstance_interest.get('description', None)
            # transaction_type = get_transaction_type_from_desc(description, value)
            return None 

        elif key == 'acquisition':
            transaction_type = 'buy'

            if specific_key == 'Securities as part of management':
                transaction_type = 'others'

        elif key == 'disposal':
            transaction_type = 'sell'

        elif key == 'other_circumstances':
            lookup_key = specific_key.lower().strip()
            transaction_type = OTHER_CIRCUMSTANCES_RULES.get(lookup_key)

    return transaction_type
                

def populate_extra_data(
    symbol: str, 
) -> tuple[str, str, str]: 
    cache_path = "data/sgx_companies.json"

    company_lookup = open_json(cache_path)

    if not symbol: 
        return None, None, None 
    
    data = company_lookup.get(symbol, None)
    
    if not data:
        LOGGER.info('symbol not matched with company lookup')
        return None, None, None
    
    company_name =  data.get('name') or None 
    sector = data.get('sector') or None 
    sub_sector = data.get('sub_sector') or None 

    return company_name, sector, sub_sector


def search_tavily(
    holder_name: str,
    company_name: str, 
    type_question: str = "individual"
) -> str:
    api_keys = [key for key in TAVILY_API_KEYS if key]

    if type_question == "individual":
        query = (
            f"Is '{holder_name}', a shareholder in {company_name}, a person or a company? "
            "Answer with this format: {company or person}, explanation"
        )

    elif type_question == "relation":
        query = (f"""
                Determine the identity and relationship of {holder_name} to
                {company_name}

                Return:
                1. relation_to_issuer
                Return one of:
                - "Parent"
                - "Subsidiary"
                - "Affiliate"
                - "Controller"
                - "Manager"
                - "Trustee-Manager"
                - "Responsible Person"
                - "None"

                Only assign a relation when reliable sources explicitly establish a DIRECT
                relationship between the holder and the listed issuer CapitaLand Integrated Commercial Trust.

                For SGX-listed REITs, trusts, or business trusts:
                - "Manager" means the holder is explicitly the manager of the listed REIT/trust.
                - "Trustee-Manager" means the holder is explicitly the trustee-manager of the
                listed issuer.
                - "Responsible Person" means the holder is explicitly identified as the
                responsible person for the listed issuer.

                The relationship must be between 
                {holder_name} and the {company_name} itself.

                Return concise evidence explaining the classification and cite the source used and using this format: 
                (relation), explanation
            """
        )

    last_error = None

    for key_number, api_key in enumerate(api_keys, start=1):
        if not api_key:
            LOGGER.warning("Tavily API key %d is not configured; skipping it", key_number)
            continue

        try:
            client = TavilyClient(api_key)
            response = client.search(
                query=query,
                include_answer="advanced",
                # topic="finance",
                search_depth="advanced",
                max_results=5,
                chunks_per_source=5,
                include_domains=[
                    "https://www.sgx.com/",
                    "https://ranking.fortuneidn.com/",
                    "https://finance.yahoo.com/"
                ] if type_question == "individual" else None 
            )

            answer = response.get("answer")

            if answer:
                return answer

            raise RuntimeError("Tavily returned no answer")

        except Exception as error:
            last_error = error
            LOGGER.warning(
                "Tavily request with API key %d failed: %s. Trying the next key.",
                key_number,
                error,
            )

    raise RuntimeError("All configured Tavily API keys failed") from last_error


def normalize_holder_name_update(
    holder_name: str,
    remove_parenthetical: bool = False,
):
    normalized_name = holder_name.casefold()

    if remove_parenthetical:
        normalized_name = re.sub(
            r"\s*\([^)]*\)",
            " ",
            normalized_name,
        )

    normalized_name = re.sub(
        r"(?<=[a-z])(?=(?:private|pte|ltd|limited|inc|llc|plc)\b)",
        " ",
        normalized_name,
    )

    normalized_name = re.sub(
        r"\b(private|pte|ltd|limited|inc|llc|plc)\b",
        " ",
        normalized_name,
    )

    normalized_name = re.sub(
        r"[^a-z0-9]+",
        " ",
        normalized_name,
    )

    return " ".join(normalized_name.split())


def create_name_variants(holder_name: str):
    normal_name = normalize_holder_name_update(holder_name)

    parenthetical_removed_name = normalize_holder_name_update(
        holder_name,
        remove_parenthetical=True,
    )

    name_variants = {normal_name}

    # avoid creating extremely broad aliases
    if (
        parenthetical_removed_name
        and parenthetical_removed_name != normal_name
        and len(parenthetical_removed_name.split()) >= 2
    ):
        name_variants.add(parenthetical_removed_name)

    return name_variants


def build_reit_candidates(
    reit_profile: dict | None,
) -> list[dict]:
    if not reit_profile:
        return []

    candidates = []

    management = reit_profile.get("management") or {}

    for management_key, company_names in management.items():
        if isinstance(company_names, str):
            company_names = [company_names]

        for company_name in company_names or []:
            if not company_name:
                continue

            for normalized_name in create_name_variants(company_name):
                if not normalized_name:
                    continue

                candidates.append(
                    {
                        "normalized_name": normalized_name,
                        "candidate_type": "management",
                        "management_key": management_key,
                        "matched_name": company_name,
                    }
                )

    board_members = reit_profile.get("board") or []

    for board_member in board_members:
        board_member_name = board_member.get("name")

        if not board_member_name:
            continue

        for normalized_name in create_name_variants(board_member_name):
            if not normalized_name:
                continue

            candidates.append(
                {
                    "normalized_name": normalized_name,
                    "candidate_type": "board",
                    "matched_name": board_member_name,
                    "position": board_member.get("position"),
                }
            )

    return candidates


def match_filing_to_reit_profile(
    filing_holder_name: str | None,
    reit_profile: dict | None,
    filing_name_variants: set[str] | None = None,
    threshold: int = 90,
) -> bool:
    if not filing_holder_name:
        return False

    candidates = build_reit_candidates(reit_profile)

    if not candidates:
        return False

    if filing_name_variants is None:
        filing_name_variants = create_name_variants(filing_holder_name)

    # First attempt an exact normalized match
    for filing_name_variant in filing_name_variants:
        for candidate in candidates:
            if candidate["normalized_name"] == filing_name_variant:
                return True

    candidate_names = [
        candidate["normalized_name"]
        for candidate in candidates
    ]

    for filing_name_variant in filing_name_variants:
        fuzzy_result = process.extractOne(
            filing_name_variant,
            candidate_names,
            scorer=fuzz.ratio,
        )

        if fuzzy_result is None:
            continue

        _, similarity_score, _ = fuzzy_result

        if similarity_score >= threshold:
            return True

    return False


KNOWN_HOLDER_TYPES_PATH = (
    "sgx_scraper/fetch_sgx_filings/data/processed/processed_final.json"
)


def normalize_holder_key(holder_name: str | None) -> str:
    return (clean_holder_name(holder_name) or "").casefold()


@lru_cache(maxsize=1)
def get_known_holder_types() -> dict[tuple[str, str], str]:
    # Holder types already settled for a holder and company, read from the
    # reviewed records. A holder keeps its type on every later run instead of
    # being asked of Tavily again, whose answers vary from run to run
    try:
        records = open_json(KNOWN_HOLDER_TYPES_PATH)

    except (FileNotFoundError, ValueError):
        return {}

    return {
        (normalize_holder_key(record.get("holder_name")), record.get("symbol")): (
            record["holder_type"]
        )
        for record in records or []
        if record.get("holder_type")
    }


def match_table_holder_type(holder_name: str, symbol: str) -> str | None:
    # The holder's type on its latest stored filing for this company
    name_pattern = re.sub(r"([\\%_])", r"\\\1", clean_holder_name(holder_name))

    try:
        records = get_db(
            table="sgx_filings",
            query_modifier=lambda query: (
                query
                .ilike("holder_name", name_pattern)
                .eq("symbol", symbol)
                .not_.is_("holder_type", "null")
                .order("created_at", desc=True)
                .limit(1)
            ),
            columns="holder_type",
        )

    except Exception as error:
        LOGGER.warning("[holder_type] table lookup failed for %s: %s", holder_name, error)
        return None

    return next(
        (record["holder_type"] for record in records if record.get("holder_type")),
        None,
    )


def match_known_holder_type(holder_name: str, filing_symbol: str | None) -> str | None:
    # A holder keeps the type it already has for this company: first from the
    # reviewed file, then from the sgx_filings table
    if not holder_name or not filing_symbol:
        return None

    symbol = f"{filing_symbol.removesuffix('.SI')}.SI"

    return (
        get_known_holder_types().get((normalize_holder_key(holder_name), symbol))
        or match_table_holder_type(holder_name, symbol)
    )


@lru_cache(maxsize=None)
def classify_holder_type(
    parser_form: str,
    company_name: str,
    filing_symbol: str,
    holder_name: str,
) -> str:
    # "insider" means connected to the issuer: its directors and CEO (Form 1),
    # its manager (Form 6), individuals, and entities related to it. Any other
    # company is an "institution", however large its stake.
    # Cached because the same holders file repeatedly and each lookup costs
    # two Tavily searches.
    if "form1" in parser_form.lower():
        return "insider"

    known_holder_type = match_known_holder_type(holder_name, filing_symbol)

    if known_holder_type:
        return known_holder_type

    if "form6" in parser_form.lower():
        reit_profiles = get_db(
            table="sgx_reit_profile",
            columns="symbol, management, board",
            query_modifier=None,
        )

        reit_profile = next(
            (
                profile
                for profile in reit_profiles
                if profile.get("symbol") == filing_symbol
            ),
            None,
        )

        is_reit_management = match_filing_to_reit_profile(
            filing_holder_name=holder_name,
            reit_profile=reit_profile, 
        )

        if is_reit_management: 
            return "insider"
    
    tavily_raw_output_individual = search_tavily(holder_name, company_name)
    is_individual = tavily_raw_output_individual.split(",", 1)[0].strip().lower()

    if is_individual == "person":
        return "insider"

    tavily_raw_output_relation = search_tavily(
        holder_name, company_name, "relation"
    )
    relation = (
        tavily_raw_output_relation
        .split(",", 1)[0]
        .strip()
        .lower()
        .strip("()")
        .strip()
    )

    LOGGER.info(
        "[holder_type] holder=%s | entity=%s | relation=%s",
        holder_name,
        is_individual,
        relation,
    )

    if relation in (
        "parent",
        "subsidiary",
        "affiliate",
        "controller",
        "manager",
        "trustee-manager",
        "responsible person",
    ):
        return "insider"

    return "institution"


def clean_holder_name(holder_name: str | None) -> str | None:
    if not holder_name:
        return holder_name

    # Remove a bracketed short name such as ("FCAML"), ('JSS') or (the "Manager").
    # These always carry quotation marks; brackets that are part of the legal
    # name, such as (Private) or (Singapore), have none and are kept
    holder_name = re.sub(
        HOLDER_SHORT_NAME_PATTERN, ' ', holder_name
    ).strip()
    holder_name = re.sub(r'\s+', ' ', holder_name).strip()

    # Drop a sentence-style full stop, but not one that closes an abbreviation
    # made of initials, such as "L.P."
    if not re.search(r'\b[A-Za-z]\.[A-Za-z]\.$', holder_name):
        holder_name = holder_name.rstrip('.')

    return holder_name


def format_holder_name(holder_name: str | None) -> str:
    if not holder_name:
        return "A shareholder"

    holder_name = holder_name.replace(" [->] ", " to ")

    # SGX forms are often filled in capitals
    if holder_name.isupper():
        holder_name = " ".join(
            word if word.strip(".,()") in HOLDER_NAME_ACRONYMS else word.title()
            for word in holder_name.split(" ")
        )

    return holder_name


def get_display_holder_name(record: dict[str, any], key: str = "holder_name") -> str:
    # Once the casing step has run, a name still in capitals is one the LLM
    # kept that way (an acronym such as "NTCP SPV VI"), so it is not
    # title-cased again
    holder_name = record.get(key)

    if holder_name and record.get("_holder_name_cased"):
        return holder_name.replace(" [->] ", " to ")

    return format_holder_name(holder_name)


def format_percentage(value: float | None) -> str | None:
    if value is None:
        return None

    # A real but tiny stake would otherwise print as 0.00%
    if 0 < value * 100 < 0.005:
        return "less than 0.01%"

    return f"{value * 100:.2f}%"


def format_price(value: float) -> str:
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def get_interest_change_scope(record: dict[str, any]) -> str | None:
    # Which interest moved: "direct", "deemed", "both", or None when neither
    # moved or the share table is incomplete
    direct_before = record.get("direct_before")
    direct_after = record.get("direct_after")
    deemed_before = record.get("deemed_before")
    deemed_after = record.get("deemed_after")

    if None in (direct_before, direct_after, deemed_before, deemed_after):
        return None

    direct_changed = direct_before != direct_after
    deemed_changed = deemed_before != deemed_after

    if direct_changed and deemed_changed:
        return "both"

    if deemed_changed:
        return "deemed"

    if direct_changed:
        return "direct"

    return None


def get_percentage_change(
    record: dict[str, any],
    prefix: str,
) -> tuple[str, str] | None:
    # The before and after percentages as filed, or None when either is
    # missing or they are equal once formatted
    before = format_percentage(record.get(f"{prefix}_percentage_before"))
    after = format_percentage(record.get(f"{prefix}_percentage_after"))

    if before is None or after is None or before == after:
        return None

    return before, after


def get_consistent_holder_side(record: dict[str, any]) -> str | None:
    # The side comes from the filing's wording. The share table never chooses
    # it, but it can veto it: "gives" while the total rose, or "receives"
    # while it fell, contradicts the form, so no side is asserted. This
    # happens when one remark describes two transactions of the same filing
    holder_side = record.get("_holder_side")
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    if holder_side is None or holding_before is None or holding_after is None:
        return holder_side

    contradicts_table = (
        (holder_side == "gives" and holding_after > holding_before)
        or (holder_side == "receives" and holding_after < holding_before)
    )

    return None if contradicts_table else holder_side


def select_sided_tag_rule(
    tag: str,
    tag_rules: dict[str, any],
    transaction_type: str | None,
    circumstance_key: str | None,
    holder_side: str | None,
) -> dict[str, any] | None:
    # Returns the wording for a tag whose side depends on the filing's free
    # text, or None when the tag's ordinary rule applies
    sided_rules = TITLE_BODY_SIDED_TAG_RULES.get(tag)

    if not sided_rules or transaction_type in tag_rules:
        return None

    checkbox_rule = (
        ACQUISITION_RULES.get(circumstance_key)
        or OTHER_CIRCUMSTANCES_RULES.get(circumstance_key)
        or {}
    )

    # A ticked box fixes the side, so only free-text tags are looked at
    if tag in checkbox_rule.get("tags", []) or holder_side == "receives":
        return None

    if holder_side == "gives" and "gives" in sided_rules:
        return sided_rules["gives"]

    return sided_rules["unstated"]


def select_title_body_rule(
    transaction_type: str | None,
    tags: list[str],
    circumstance_key: str | None,
    holder_side: str | None = None,
) -> dict[str, any]:
    for tag in TITLE_BODY_TAG_PRIORITY:
        if tag not in tags:
            continue

        tag_rules = TITLE_BODY_TAG_RULES[tag]

        if (
            transaction_type == "sell"
            and "sell" not in tag_rules
            and tag in TITLE_BODY_RECEIPT_TAGS
        ):
            continue

        sided_rule = select_sided_tag_rule(
            tag, tag_rules, transaction_type, circumstance_key, holder_side
        )

        if sided_rule:
            return sided_rule

        return tag_rules.get(transaction_type) or tag_rules["default"]

    circumstance_rules = TITLE_BODY_CIRCUMSTANCE_RULES.get(circumstance_key) or {}
    rule = (
        circumstance_rules.get(transaction_type)
        or circumstance_rules.get("default")
    )

    if rule:
        return rule

    return (
        TITLE_BODY_TYPE_RULES.get(transaction_type)
        or TITLE_BODY_TYPE_RULES["default"]
    )


def get_security_nouns(
    security_noun: str | None,
    company_name: str | None,
) -> tuple[str, str]:
    # Plural and singular words for the securities. The parser reads the noun
    # from the form (stapled securities, units, or shares); the company name is
    # only a fallback when the form could not be read
    if security_noun is None:
        is_unit_issuer = re.search(
            UNIT_ISSUER_PATTERN, company_name or "", re.IGNORECASE
        )
        security_noun = "units" if is_unit_issuer else "shares"

    if security_noun == "stapled securities":
        return security_noun, "stapled security"

    return security_noun, security_noun[:-1]


def build_notification_title_and_sentence(
    record: dict[str, any],
    holder_name: str,
    company_name: str,
    unit: str,
) -> tuple[str | None, str]:
    # Form 3 states when a holder becomes or ceases to be substantial, which
    # is the event the filing reports
    notification_type = record.get("notification_type")
    role = f"substantial {unit}holder"

    if notification_type == "becoming":
        title = f"{holder_name} becomes {role} of {company_name}"
        stake = format_percentage(record.get("share_percentage_after"))

        if stake is not None:
            title += f" with {stake} stake"

        return title, f" With this, the holder becomes a {role} of {company_name}."

    if notification_type == "ceasing":
        return (
            f"{holder_name} ceases to be {role} of {company_name}",
            f" With this, the holder ceases to be a {role} of {company_name}.",
        )

    return None, ""


NUMBER_WORDS = {2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}


def build_transaction_sentence(
    record: dict[str, any],
    units: str,
    unit: str,
    received: bool,
) -> tuple[str, str]:
    # Returns the amount text and the price clause that follows it.
    # A form that states the amount as 0 or "Nil" gives no amount
    amount = record.get("amount_transaction") or None
    price_per_share = record.get("price_per_share")
    transaction_value = record.get("transaction_value")

    amount_text = f"{amount:,} {units}" if amount is not None else units

    if not price_per_share:
        return amount_text, ""

    tranches = record.get("_price_components") or []

    # Several tranches at different prices: the first sentence gives the
    # total, and a second sentence, closed by the caller's full stop, lists
    # each tranche
    if len(tranches) > 1 and all(
        tranche.get("price") is not None and tranche.get("amount") is not None
        for tranche in tranches
    ):
        total_text = ""

        if transaction_value:
            total_text = (
                f", valued at S${transaction_value:,} in total"
                if received
                else f", for a total of S${transaction_value:,}"
            )

        tranche_texts = [
            f"{tranche['amount']:,} at S${format_price(tranche['price'])} per {unit}"
            for tranche in tranches
        ]
        tranche_count = NUMBER_WORDS.get(len(tranches), str(len(tranches)))

        return amount_text, (
            f"{total_text}. The {units} came in {tranche_count} tranches: "
            f"{', '.join(tranche_texts[:-1])} and {tranche_texts[-1]}"
        )

    price = f"S${format_price(price_per_share)} per {unit}"

    if received:
        price_text = f", valued at {price}"

        if transaction_value:
            price_text += f", or S${transaction_value:,} in total"

        return amount_text, price_text

    price_text = f" at {price}"

    if transaction_value:
        price_text += f", for a total of S${transaction_value:,}"

    return amount_text, price_text


def build_holdings_sentence(
    record: dict[str, any],
    units: str,
    stake_already_stated: bool,
) -> str:
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    if holding_before is None or holding_after is None:
        return ""

    percentage_before = format_percentage(record.get("share_percentage_before"))
    percentage_after = format_percentage(record.get("share_percentage_after"))
    percentage_change = get_percentage_change(record, "share")

    if holding_after == holding_before:
        sentence = f" The number of {units} held is unchanged at {holding_after:,}"

        if percentage_change and not stake_already_stated:
            sentence += (
                f", while the total stake moves from {percentage_change[0]} "
                f"to {percentage_change[1]}"
            )

        return sentence + "."

    direction = "rises" if holding_after > holding_before else "falls"
    before_text = f"{holding_before:,} {units}"
    after_text = f"{holding_after:,} {units}"

    if percentage_before is None or percentage_after is None:
        return f" Total interest {direction} from {before_text} to {after_text}."

    if percentage_before == percentage_after:
        # A move too small to change the rounded percentage. A stake the
        # form gives as 0% is not quoted, since "unchanged at 0.00%" says less
        stake_text = "" if percentage_after == "0.00%" else f" at {percentage_after}"

        return (
            f" Total interest {direction} from {holding_before:,} to "
            f"{after_text}, with the stake unchanged{stake_text}."
        )

    return (
        f" Total interest {direction} from {before_text} ({percentage_before}) "
        f"to {after_text} ({percentage_after})."
    )


def format_compact_number(value: float) -> str:
    # Headline figures: exact below 100,000, then 250K, 1.8M, 1.2B
    if value < 100_000:
        return f"{round(value):,}"

    if round(value / 1_000) < 1_000:
        return f"{value / 1_000:.0f}K"

    if round(value / 1_000_000, 1) < 1_000:
        return f"{value / 1_000_000:.1f}M"

    return f"{value / 1_000_000_000:.1f}B"


def get_title_quantity(
    record: dict[str, any],
    rule: dict[str, any],
) -> tuple[str | None, bool]:
    # What a title says about the size of the transaction, and whether it is
    # a value. A purchase or sale is stated by its total value when the
    # filing gives one. Securities that were received, and trades with no
    # price, are stated by the number of securities
    amount = record.get("amount_transaction") or None
    transaction_value = record.get("transaction_value") or None

    if (
        transaction_value
        and not rule["received"]
        and record.get("transaction_type") in ("buy", "sell")
    ):
        return f"S${format_compact_number(transaction_value)}", True

    if amount:
        return format_compact_number(amount), False

    return None, False


def build_action_title(
    rule: dict[str, any],
    names: dict[str, str],
    quantity: str | None,
    is_value: bool,
) -> str:
    # Title for a holder who acted directly, with the quantity worked into
    # the rule's own title
    template = rule["title"]

    if quantity is None:
        return template.format(**names)

    if "{units} of {company}" in template:
        replacement = (
            f"{quantity} of {{company}} {{units}}"
            if is_value
            else f"{quantity} {{units}} of {{company}}"
        )
        template = template.replace("{units} of {company}", replacement, 1)

    elif "{company} {units}" in template:
        template = template.replace(
            "{company} {units}", f"{quantity} {{company}} {{units}}", 1
        )

    elif rule.get("title_with_quantity") and not is_value:
        template = rule["title_with_quantity"].replace("{quantity}", quantity)

    return template.format(**names)


def build_deemed_reason(
    rule: dict[str, any],
    names: dict[str, str],
    quantity: str | None,
    is_value: bool,
) -> str:
    # The "after ..." part of a deemed-interest title, with the quantity:
    # "after S$8.0M share sale", "after purchase of 1.8M shares"
    template = rule["reason"]

    if quantity is None:
        return template.format(**names)

    if is_value:
        # "after S$8.0M share sale", but "after exercise of warrants worth
        # S$37.3M" where the value cannot sit in front
        if re.search(r"(?:purchase|sale|subscription)$", template):
            template = template.replace(" after ", f" after {quantity} ", 1)

        else:
            template += f" worth {quantity}"

    elif "{unit} purchase" in template or "{unit} sale" in template:
        template = (
            template
            .replace("{unit} purchase", f"purchase of {quantity} {{units}}")
            .replace("{unit} sale", f"sale of {quantity} {{units}}")
        )

    elif "{unit} award" in template:
        template = template.replace("{unit} award", f"award of {quantity} {{units}}")

    elif re.search(r" (?:in|of|on) \{units\}", template):
        template = template.replace("{units}", f"{quantity} {{units}}", 1)

    elif re.search(
        r"(?:purchase|sale|subscription|placement|dividend|specie|inheritance)$",
        template,
    ):
        template += f" of {quantity} {{units}}"

    return template.format(**names)


def get_primary_holder(record: dict[str, any]) -> str | None:
    # The holder in the same filing that holds the securities directly, when
    # it carried out the whole transaction. Part IV gives one amount for
    # every holder, and several funds of a group may each have done part of
    # it, so the direct holder is named only when its own total moved by
    # that amount
    amount = record.get("amount_transaction") or None
    change = record.get("_primary_holder_change")

    if amount is not None and change is not None and amount != change:
        return None

    return record.get("_primary_holder")


def is_group_amount(record: dict[str, any]) -> bool:
    # Part IV of a multi-holder Form 3 gives one amount for all the holders
    # giving notice. A holder whose own total moved by a different number
    # did only part of it, so the amount is the group's and not its own
    amount = record.get("amount_transaction") or None
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    return bool(
        record.get("filing_type") == "form_3_part_iii_iv"
        and amount is not None
        and holding_before is not None
        and holding_after is not None
        and abs(holding_after - holding_before) != amount
    )


def is_unmoved_joint_holder(record: dict[str, any]) -> bool:
    # A holder named on a multi-holder Form 3 whose direct, deemed and total
    # holdings are all unchanged. Part IV's one transaction, and its ticked
    # box, belong to the other holders giving notice
    holdings = [
        record.get(f"{prefix}_{section}")
        for prefix in ("direct", "deemed", "holding")
        for section in ("before", "after")
    ]

    return (
        record.get("filing_type") == "form_3_part_iii_iv"
        and None not in holdings
        and holdings[0::2] == holdings[1::2]
    )


# Stands in for the acting party when the filing does not name one
UNNAMED_ACTING_PARTY = "a party linked to the holder"


def needs_acting_party(record: dict[str, any]) -> bool:
    # True when only the holder's deemed interest moved in a transaction a
    # related party carried out, and no other holder in the filing is known
    # to be that party. The body then has nobody to name
    rule = select_title_body_rule(
        transaction_type=record.get("transaction_type"),
        tags=record.get("tags") or [],
        circumstance_key=record.get("circumstance_key"),
        holder_side=get_consistent_holder_side(record),
    )

    return bool(
        rule["verb"]
        and get_interest_change_scope(record) == "deemed"
        and not get_primary_holder(record)
    )


def generate_title_and_body(record: dict[str, any]) -> tuple[str, str]:
    holder_name = get_display_holder_name(record)
    company_name = record.get("company_name")
    amount = record.get("amount_transaction") or None

    units, unit = get_security_nouns(record.get("_security_noun"), company_name)

    rule = select_title_body_rule(
        transaction_type=record.get("transaction_type"),
        tags=record.get("tags") or [],
        circumstance_key=record.get("circumstance_key"),
        holder_side=get_consistent_holder_side(record),
    )

    names = {
        "holder": holder_name,
        "company": company_name,
        "units": units,
        "unit": unit,
    }
    detail = rule["detail"].format(**names)
    reason = rule["reason"].format(**names)

    interest_scope = get_interest_change_scope(record)
    deemed_only = interest_scope == "deemed"
    amount_text, price_text = build_transaction_sentence(
        record, units, unit, rule["received"]
    )
    stake_already_stated = False
    quantity, quantity_is_value = get_title_quantity(record, rule)
    group_sentence = ""

    if rule["verb"] and is_group_amount(record):
        # The title and the first sentence state no figure, and the amount
        # is given as the group's
        group_sentence = (
            f" The holders giving notice together {rule['verb']} "
            f"{amount_text}{price_text}."
        )
        quantity, amount_text, price_text = None, units, ""

    # A ticked acquisition or disposal box that no longer gives a type is one
    # that normalize_classification took from a holder who did not take part
    ticked_trade_box = (
        ACQUISITION_RULES.get(record.get("circumstance_key"), {})
        .get("transaction_type") == "buy"
    )

    if (
        ticked_trade_box
        and record.get("transaction_type") == "others"
        and is_unmoved_joint_holder(record)
    ):
        title = f"{holder_name} reports no change in interest in {company_name}"
        body = (
            f"{holder_name} reported no change in interest in {company_name}. "
            "The notice was given jointly with other holders, whose "
            "interests changed."
        )

    elif rule["verb"] and not deemed_only:
        # The holder acted directly
        title = build_action_title(rule, names, quantity, quantity_is_value)
        body = (
            f"{holder_name} {rule['verb']} {amount_text} of {company_name}"
            f"{detail}{price_text}."
        )

    elif rule["verb"]:
        # A related party acted, so only the holder's deemed interest moved
        past_direction = "rose" if rule["direction"] == "rises" else "fell"
        title = (
            f"{holder_name}'s deemed interest in {company_name} "
            f"{rule['direction']}"
            f"{build_deemed_reason(rule, names, quantity, quantity_is_value)}"
        )
        # The party that acted: another holder in the same filing that holds
        # the securities directly, or the party the filing's text names. A
        # filing that names nobody still says the holder did not act
        acting_party = (
            get_display_holder_name(record, "_primary_holder")
            if get_primary_holder(record)
            else record.get("acting_party") or UNNAMED_ACTING_PARTY
        )

        body = (
            f"{holder_name}'s deemed interest in {company_name} "
            f"{past_direction} after {acting_party} "
            f"{rule['verb']} {amount_text}{detail}{price_text}."
        )

    else:
        # No stated direction, so report the stake percentages as filed
        stake = "deemed interest" if deemed_only else "stake"
        percentage_prefix = "deemed" if deemed_only else "share"
        percentage_change = get_percentage_change(record, percentage_prefix)

        # An untagged "others" keeps a plain title; the body gives the reason
        is_untagged = rule is TITLE_BODY_TYPE_RULES["default"]

        if percentage_change and not is_untagged:
            before, after = percentage_change
            increased = (
                record[f"{percentage_prefix}_percentage_after"]
                > record[f"{percentage_prefix}_percentage_before"]
            )
            direction = "rises" if increased else "falls"
            title = (
                f"{holder_name}'s {stake} in {company_name} {direction} "
                f"to {after} from {before}{reason}"
            )
            body = (
                f"{holder_name}'s {stake} in {company_name} moved from "
                f"{before} to {after}{detail}."
            )
            stake_already_stated = not deemed_only

        else:
            interest = "deemed interest" if deemed_only else "interest"
            title = (
                f"{holder_name} reports change in {interest} in "
                f"{company_name}{reason}"
            )
            body = (
                f"{holder_name} reported a change in {interest} in "
                f"{company_name}{detail}."
            )

        # With no tag, nothing in the wording above says what happened, so the
        # reason read from the filing is given
        change_reason = record.get("change_reason")

        if change_reason and not (record.get("tags") or []):
            body += f" The filing attributes the change to {change_reason}."

        if amount is not None:
            # A holder whose direct and deemed interests both moved, by
            # different numbers, may state only its own securities as the
            # amount. Securities moved between the two are one number
            direct_only = (
                interest_scope == "both"
                and amount == abs(record["direct_after"] - record["direct_before"])
                and amount != abs(record["deemed_after"] - record["deemed_before"])
            )
            held = " held directly" if direct_only else ""

            body += f" The transaction involved {amount_text}{held}{price_text}."

    body += group_sentence

    notification_title, notification_sentence = (
        build_notification_title_and_sentence(
            record, holder_name, company_name, unit
        )
    )

    if notification_title:
        title = notification_title

    if deemed_only:
        direct_after = record.get("direct_after")

        if direct_after:
            body += (
                f" The direct holding is unchanged at {direct_after:,} {units}."
            )

        else:
            body += f" No {units} are held directly."

    elif interest_scope == "both":
        body += (
            f" The direct interest moved from {record.get('direct_before'):,} "
            f"to {record.get('direct_after'):,} {units} and the deemed interest "
            f"from {record.get('deemed_before'):,} to "
            f"{record.get('deemed_after'):,} {units}."
        )

    body += build_holdings_sentence(record, units, stake_already_stated)
    body += notification_sentence

    # A name ending in a full stop ("Pte. Ltd.") must not double the sentence end
    body = body.replace("..", ".")

    return title, body


def get_percentage_movement(
    percentage_before: float | None,
    percentage_after: float | None,
) -> float | None:
    if percentage_before is None or percentage_after is None:
        return None

    return round(abs(percentage_after - percentage_before), 5)


def build_filing_payload(record: dict[str, any]) -> dict[str, any]:
    # Shapes a finished record for the sgx_filings table: the before and
    # after share table becomes interest_breakdown, the percentage moved is
    # given per interest, and the symbol carries its exchange suffix. The
    # FILING_EXTRA_FIELDS follow the table's own columns and are dropped by
    # keep_table_columns when batches are merged into a processed file
    symbol = record.get("symbol")

    # (stored key, flat amount prefix, flat percentage prefix)
    interests = [
        ("total", "holding", "share_percentage"),
        ("deemed", "deemed", "deemed_percentage"),
        ("direct", "direct", "direct_percentage"),
    ]

    payload = {
        "source": record.get("source"),
        "timestamp": record.get("timestamp"),
        "transaction_type": record.get("transaction_type"),
        "holder_name": record.get("holder_name"),
        "transaction_value": record.get("transaction_value"),
        "share_percentage_transaction": {
            interest: get_percentage_movement(
                record.get(f"{percentage_prefix}_before"),
                record.get(f"{percentage_prefix}_after"),
            )
            for interest, _, percentage_prefix in interests
        },
        "interest_breakdown": {
            section: {
                interest: {
                    "amount": record.get(f"{amount_prefix}_{section}"),
                    "percentage": record.get(f"{percentage_prefix}_{section}"),
                }
                for interest, amount_prefix, percentage_prefix in interests
            }
            for section in ("after", "before")
        },
        "symbol": f"{symbol}.SI" if symbol else None,
        "price_per_share": record.get("price_per_share"),
        "holder_type": record.get("holder_type"),
        "title": record.get("title"),
        "body": record.get("body"),
        "sector": record.get("sector"),
        "sub_sector": record.get("sub_sector"),
        "tags": record.get("tags") or [],
        "source_is_manual": record.get("source_is_manual"),
    }

    for field in FILING_EXTRA_FIELDS:
        payload[field] = record.get(field)

    return payload


def keep_table_columns(payload: dict[str, any]) -> dict[str, any]:
    return {
        column: payload.get(column)
        for column in FILING_TABLE_COLUMNS
    }


def contains_any_keyword(text: str, keywords: list) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def filter_duplicate(payload: list[dict[str, any]]) -> list[dict]:
    if not payload:
        LOGGER.info(f'Payload is empty, skipping filter duplicate')
        return []

    cleaned_payload = []
    seen_keys = set()

    for row in payload:
        # interest_breakdown and price_per_share are nested, so they are
        # compared as text
        unique_key = (
            row.get('source'),
            row.get('holder_name'),
            row.get('timestamp'),
            json.dumps(row.get('interest_breakdown'), sort_keys=True),
            json.dumps(row.get('price_per_share'), sort_keys=True),
        )

        if unique_key in seen_keys:
            LOGGER.info(f"Dropping duplicate record found in payload: \n{json.dumps(row, indent=2)}")
            continue

        seen_keys.add(unique_key)
        cleaned_payload.append(row)

    return cleaned_payload
