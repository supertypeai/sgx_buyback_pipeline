from bs4 import BeautifulSoup 

from sgx_scraper.utils.constant import HEADERS
from sgx_scraper.fetch_managements.utils.announcement_helper import (
    extract_field, 
    extract_symbol, 
    parse_appointment_date
)

import requests
import logging


LOGGER = logging.getLogger(__name__)


def get_cessation(
    api_response: dict,
    fallback_symbol: str | None = None,
) -> dict | None:
    url = api_response.get('url', '')
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    if not soup: 
        return None  

    symbol = extract_symbol(api_response.get('issuers')) or fallback_symbol

    if not symbol:
        return None 

    name = extract_field(soup, 'Name of person')
    position = extract_field(soup, 'Job title (e.g. Lead ID, AC Chairman, AC Member etc.)')
    age = extract_field(soup, 'Age')
    end_date_raw = extract_field(soup, 'If yes, please provide the date')

    if end_date_raw is None:
        end_date_raw = extract_field(
            soup,
            'If yes, please provide the date.',
        )

    end_date = parse_appointment_date(end_date_raw)
    start_date = parse_appointment_date(extract_field(soup, "Date of appointment to current position")) 

    return {
        "symbol": symbol,
        "name": name,
        "position": position,
        "age": age,
        "end_date": end_date,
        "start_date": start_date
    }
