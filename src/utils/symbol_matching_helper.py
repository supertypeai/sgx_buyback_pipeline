from rapidfuzz import process

from src.config.settings import LOGGER

import json
import os

CACHE_PATH = "data/sgx_companies.json"

if not os.path.exists(CACHE_PATH):
    raise FileNotFoundError("sgx_companies.json not found. Run company_refresher.py first.")

with open(CACHE_PATH, "r", encoding="utf-8") as file:
    SGX_COMPANIES = json.load(file)

SGX_COMPANY_NAMES = [company_name.get('name').lower() for company_name in SGX_COMPANIES]


def match_company_name(input_name: str, threshold: int = 90):
    input_name_lower = input_name.lower()
    if 'limited' in input_name_lower:
        if 'the' in input_name_lower:
            input_name_lower = input_name_lower.replace('the', '').strip()
        input_name_lower = input_name_lower.replace('limited', 'ltd').strip()

    try:
        result = process.extractOne(input_name_lower, SGX_COMPANY_NAMES)
        
        if not result:
            return None
        
        match, score, _ = result
        if round(score) >= threshold:
            matched = next(sgx_company for sgx_company in SGX_COMPANIES if sgx_company.get("name").lower() == match)
            return matched
        
    except (TypeError, ValueError) as error:
        return LOGGER.error(f"[symbol_matching_helper] TypeError or ValueError occurred: {input_name} {error}")
    except Exception as error:
        return LOGGER.error(f"[symbol_matching_helper] Error: {error}")
    

if __name__ == '__main__':
    company = match_company_name("17live group limited")
    print(company)

