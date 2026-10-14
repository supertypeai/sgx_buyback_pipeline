from sgx_scraper.fetch_sgx_filings.utils.currency_converter import (
    get_latest_currency,
    calculate_currency_to_sgd,
    convert_price_to_sgd,
    extract_currency_code,
)

import re
import logging


LOGGER = logging.getLogger(__name__)


PRICE_TRANCHE_PATTERN = re.compile(
    r'([\d,]{4,})\s*(?:new\s+)?(?:units?|shares?|stapled\s+securit(?:y|ies))\s*'
    r'(?:[a-z]+\s+){0,3}?'
    r'(?:@|at)\s*((?:sg\$|s\$|us\$|usd|sgd|hkd|\$)?\s*[\d,]*\.?\d+)',
    re.IGNORECASE,
)

# A figure is a price per security only when the text says so. Words such as
# "shares" or "pursuant to" alone do not make it one: "$3,601.17 paid to
# acquire shares" is a total
PER_SECURITY_PATTERN = re.compile(
    r'per\s+(?:shares?|units?|securit(?:y|ies)|stapled\s+securit(?:y|ies))'
    r'|/\s*(?:shares?|units?|securit(?:y|ies))',
    re.IGNORECASE,
)

ACQUISITION_WORDING = re.compile(r'\b(?:acquir\w*|purchas\w*|bought)\b', re.IGNORECASE)

DISPOSAL_WORDING = re.compile(r'\b(?:dispos\w*|sold|sale)\b', re.IGNORECASE)



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
