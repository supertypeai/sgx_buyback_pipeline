from bs4 import BeautifulSoup

from sgx_scraper.fetch_managements.utils.announcement_helper import (
    extract_field,
    extract_symbol,
    parse_appointment_date,
)
from sgx_scraper.utils.constant import HEADERS

import logging
import requests


LOGGER = logging.getLogger(__name__)


def get_appointment(api_response: dict) -> dict | None:
    url = api_response.get("url", "")

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    symbol = extract_symbol(api_response.get("issuers"))

    if not soup or not symbol:
        return None

    name = extract_field(soup, "Name of person")

    position = extract_field(
        soup,
        "Job title (e.g. Lead ID, AC Chairman, AC Member etc.)",
    )
    
    age = extract_field(soup, "Age")

    start_date = parse_appointment_date(
        extract_field(soup, "Date of appointment")
    )

    return {
        "symbol": symbol,
        "name": name,
        "position": position,
        "age": age,
        "start_date": start_date,
    }
