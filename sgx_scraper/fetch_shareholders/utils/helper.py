from rapidfuzz import fuzz, process

from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.refresh_sgx_companies import get_sgx_companies

import logging 
import re 


LOGGER = logging.getLogger(__name__)

COUNTRY_ABBREVIATIONS = {
    '(S)': '(Singapore)',
    '(HK)': '(Hong Kong)',
    '(M)': '(Malaysia)',
    '(US)': '(United States)',
    '(UK)': '(United Kingdom)',
    '(AU)': '(Australia)',
    '(JP)': '(Japan)',
    '(CN)': '(China)',
    '(IN)': '(India)',
}


# def match_shareholder_name(screener_name: str, db_name: str, threshold: int = 92) -> bool:
#     normalized_screener_name = screener_name.lower().strip()
#     normalized_db_name = db_name.lower().strip()
    
#     score = fuzz.token_sort_ratio(normalized_screener_name, normalized_db_name)
    
#     LOGGER.info(
#         'Matching "%s" vs "%s" | score: %d | result: %s',
#         screener_name,
#         db_name,
#         score,
#         'matched' if score >= threshold else 'no match'
#     )
    
#     return score >= threshold


def find_matched_db_shareholder(
    filing_name: str,
    db_shareholders: list[dict],
    threshold: int = 95
) -> dict | None:    
    lookup_shareholder_by_name = {
        shareholder.get('name', ''): shareholder
        for shareholder in db_shareholders
    }

    shareholder_names = list(lookup_shareholder_by_name.keys())

    result = process.extractOne(
        clean_name_titles(filing_name),
        shareholder_names,
        scorer=fuzz.WRatio
    )

    if not result or result[1] < threshold:
        return None

    matched_name = result[0]
    similarity_score = result[1]

    LOGGER.info(
        'Matching "%s" vs "%s" | score: %d | result: matched',
        filing_name,
        matched_name,
        similarity_score
    )

    return lookup_shareholder_by_name[matched_name]


def get_current_shareholders() -> list[dict]:
    try: 
        response = (
            SUPABASE_CLIENT 
            .table('sgx_companies')
            .select('symbol, shareholders')
            .execute()
        ) 

        return response.data 

    except Exception as error:
        LOGGER.error('Error fetching shareholders db: %s', error)
        return None 


def clean_name_titles(name: str) -> str:
    name = re.sub(r'^(Ir|Drs?|Dr)\.?\s+', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\.([a-zA-Z])(?=\s|$)', r' \1', name)
    name = re.sub(r'(?<=\s)([a-zA-Z])\.', r'\1', name)
    return ' '.join(name.split())


def expand_country_abbreviations(company_name: str) -> str:
    for abbreviation, full_name in COUNTRY_ABBREVIATIONS.items():
        company_name = company_name.replace(abbreviation, full_name)

    return company_name


def remove_pte_parentheses(company_name: str) -> str:
    return re.sub(r'\s*\(Pte\)\s*', ' ', company_name).strip()


def clean_company_name(company_name: str) -> str:
    company_name = company_name.replace('.', '')
    company_name = expand_country_abbreviations(company_name)
    company_name = remove_pte_parentheses(company_name)

    return company_name


def enrich(payload: list[dict]) -> list[dict]:
    companies = get_sgx_companies()
    
    companies_lookup = {
        record.get('symbol'): record.get('investing_symbol') 
        for record in companies 
    }

    for record in payload: 
        symbol = record.get('symbol')

        investing_symbol = companies_lookup.get(symbol)
        
        record['investing_symbol'] = investing_symbol 

    return payload