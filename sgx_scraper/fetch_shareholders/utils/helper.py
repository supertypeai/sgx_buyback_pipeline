from rapidfuzz import fuzz

from sgx_scraper.config.settings import SUPABASE_CLIENT

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


def match_shareholder_name(screener_name: str, db_name: str, threshold: int = 92) -> bool:
    normalized_screener_name = screener_name.lower().strip()
    normalized_db_name = db_name.lower().strip()
    
    score = fuzz.token_sort_ratio(normalized_screener_name, normalized_db_name)
    
    LOGGER.info(
        'Matching "%s" vs "%s" | score: %d | result: %s',
        screener_name,
        db_name,
        score,
        'matched' if score >= threshold else 'no match'
    )
    
    return score >= threshold


def find_matched_db_shareholder(
    filing_name: str,
    db_shareholders: list[dict],
) -> dict | None:
    for db_shareholder in db_shareholders:
        if match_shareholder_name(filing_name, db_shareholder.get('name', '')):
            return db_shareholder
    
    return None


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