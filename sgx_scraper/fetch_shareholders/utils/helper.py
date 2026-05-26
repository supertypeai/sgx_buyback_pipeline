from rapidfuzz import fuzz, process

from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.utils.cli_helper import open_json

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


def matching(input: str, input_to_match: list):
    result = process.extractOne(
        input,
        input_to_match,
        scorer=fuzz.WRatio
    )   

    return result


def find_matched_db_shareholder(
    filing_name: str,
    db_shareholders: dict,
    threshold: int = 95
) -> dict | None:    
    lookup_shareholder_by_name = {
        shareholder.get('name', ''): shareholder
        for shareholder in db_shareholders
    }

    shareholder_names = list(lookup_shareholder_by_name.keys())

    clean_filing_name = clean_name_titles(filing_name)

    result = process.extractOne(
        clean_filing_name,
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


def matched_db_management(
    filing_name: str, 
    list_managements: list[str], 
    threshold: int = 90
) -> bool:
    clean_filing_name = clean_name_titles(filing_name)

    result = matching(clean_filing_name, list_managements)

    if not result or result[1] < threshold:
        return False

    matched_name = result[0]
    similarity_score = result[1]

    LOGGER.info(
        'Matching against management "%s" vs "%s" | score: %d | result: matched',
        clean_filing_name,
        matched_name,
        similarity_score
    )

    return True


def get_current_shareholders(is_refresh: bool = False) -> dict[str, dict]:
    try: 
        if is_refresh:
            response = (
                SUPABASE_CLIENT 
                .table('sgx_companies')
                .select('symbol, shareholders, management')
                .execute()
            ) 

            lookup_response = {
                record.get('symbol'): record  
                for record in response.data
            }

            return lookup_response
        
        else: 
            companies = open_json('data/sgx_companies.json')
            return companies 

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
    companies = open_json('data/sgx_companies.json')

    for record in payload: 
        investing_symbol = companies.get(record.get('symbol')).get('investing_symbol')
        record['investing_symbol'] = investing_symbol 

    return payload