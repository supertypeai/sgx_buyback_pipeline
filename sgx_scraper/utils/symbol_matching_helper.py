from rapidfuzz import process, fuzz

from .json_helper import open_json

import logging
import re 


LOGGER = logging.getLogger(__name__)


def get_sgx_company_names():
    companies_path = "data/sgx_companies.json"

    companies = open_json(path=companies_path)

    company_names = [
        value.get('name').strip().lower()
        for _, value in companies.items()
    ]

    return company_names, companies


def symbol_from_company_name(input_name: str, threshold: int = 90) -> str:
    company_names, companies = get_sgx_company_names()

    cleaned_name = re.sub(r'\s*\([^)]*\)', '', input_name)
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()

    input_name_lower = cleaned_name.lower()

    if 'public company' in input_name_lower:
        input_name_lower = input_name_lower.replace('public company', '').strip()

    if 'corporation' in input_name_lower:
        input_name_lower = input_name_lower.replace('corporation', 'corp').strip()
        if '("ifast")' in input_name_lower:
            input_name_lower = input_name_lower.replace('("ifast")', '').strip()

    if 'limited' in input_name_lower:
        if 'the' in input_name_lower:
            input_name_lower = input_name_lower.replace('the', '').strip()
        input_name_lower = input_name_lower.replace('limited', 'ltd').strip()

    input_name_lower = re.sub(r'\s+', ' ', input_name_lower).strip()
    
    try:
        scorers = [
            fuzz.ratio,
            fuzz.partial_ratio,
            fuzz.token_sort_ratio,
            fuzz.token_set_ratio
        ]
        
        for scorer in scorers:
            result = process.extractOne(
                input_name_lower, 
                company_names,
                scorer=scorer
            )
            
            if not result:
                continue
            
            match, score, _ = result
            
            if round(score) >= threshold:
                LOGGER.info(f'Matched with {scorer.__name__}: {result}')
                matched = next(
                    value.get('symbol') 
                    for _, value in companies.items() 
                    if value.get('name').lower() == match
                )

                return matched
        
        LOGGER.info(f'No match company name found above threshold {threshold}')
        
        return None
        
    except (TypeError, ValueError) as error:
        return LOGGER.error(f"[symbol_matching_helper] TypeError or ValueError occurred: {input_name} {error}")
    
    except Exception as error:
        return LOGGER.error(f"[symbol_matching_helper] Error: {error}")


def extract_symbol(issuer_security: str) -> str | None:
    try:
        parts = issuer_security.split(' - ')

        if len(parts) > 1 and len(parts) <= 3:
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


if __name__ == '__main__':
    company = symbol_from_company_name("17live group limited")
    print(company)
    # print(SGX_COMPANY_NAMES[:5])


# uv run -m sgx_scraper.utils.symbol_matching_helper