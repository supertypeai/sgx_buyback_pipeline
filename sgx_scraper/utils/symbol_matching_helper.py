from rapidfuzz import process, fuzz

import json
import os
import logging
import re 


LOGGER = logging.getLogger(__name__)


CACHE_PATH = "data/sgx_companies.json"

if not os.path.exists(CACHE_PATH):
    raise FileNotFoundError("sgx_companies.json not found.")

with open(CACHE_PATH, "r", encoding="utf-8") as file:
    SGX_COMPANIES = json.load(file)

SGX_COMPANY_NAMES = [
    value.get('name').strip().lower()
    for _, value in SGX_COMPANIES.items()
]


def symbol_from_company_name(input_name: str, threshold: int = 90) -> str:
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
                SGX_COMPANY_NAMES,
                scorer=scorer
            )
            
            if not result:
                continue
            
            match, score, _ = result
            
            if round(score) >= threshold:
                LOGGER.info(f'Matched with {scorer.__name__}: {result}')
                matched = next(
                    value.get('symbol') 
                    for _, value in SGX_COMPANIES.items() if value.get('name').lower() == match
                )
                return matched
        
        LOGGER.info(f'No match company name found above threshold {threshold}')
        return None
        
    except (TypeError, ValueError) as error:
        return LOGGER.error(f"[symbol_matching_helper] TypeError or ValueError occurred: {input_name} {error}")
    except Exception as error:
        return LOGGER.error(f"[symbol_matching_helper] Error: {error}")
    

if __name__ == '__main__':
    company = symbol_from_company_name("17live group limited")
    print(company)
    # print(SGX_COMPANY_NAMES[:5])


# uv run -m sgx_scraper.utils.symbol_matching_helper