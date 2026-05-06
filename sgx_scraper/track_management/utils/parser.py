from bs4 import BeautifulSoup 

from sgx_scraper.utils.cli_helper import open_json
from sgx_scraper.utils.symbol_matching_helper import symbol_from_company_name

import re 


def extract_field(soup: BeautifulSoup, label: str) -> str | None:
    for dt in soup.find_all('dt'):
        if dt.get_text(strip=True).lower() == label.lower():
            dd = dt.find_next_sibling('dd')

            if dd:
                return dd.get_text(strip=True)
            
    return None


def parse_appointment_date(raw_date: str | None) -> str | None:
    if not raw_date:
        return None
    
    match = re.match(r'(\d{2})/(\d{2})/(\d{4})', raw_date)
    
    if not match:
        return None
    
    day, month, year = match.groups()
    return f'{year}-{month}-{day}'


def extract_symbol(issuers: list) -> str | None:
    sgx_companies = open_json('data/sgx_companies.json')

    for issuer in issuers:
        stock_code = issuer.get('stock_code')

        if stock_code and stock_code in sgx_companies:
            return stock_code

    for issuer in issuers:
        issuer_name = issuer.get('issuer_name')

        if issuer_name:
            return symbol_from_company_name(issuer_name)

    return None