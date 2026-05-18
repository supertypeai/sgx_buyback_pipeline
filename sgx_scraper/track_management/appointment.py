import logging

import requests
from bs4 import BeautifulSoup

from sgx_scraper.track_management.utils.helper import (
    extract_field,
    extract_symbol,
    parse_appointment_date,
)
from sgx_scraper.utils.constant import HEADERS


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


if __name__ == "__main__":
    api_response = {
        "ref_id": "SG260504OTHRJNWK",
        "sub": "ANNC03",
        "category_name": "Announcement of Appointment",
        "submitted_by": "Karen Teo/Samantha Teong",
        "title": (
            "Change - Announcement of Appointment::"
            "Appointment of Non-Executive Independent Director"
        ),
        "announcer_name": None,
        "issuers": [
            {
                "isin_code": "SG1U68934629",
                "stock_code": "BN4",
                "security_name": "KEPPEL LTD.",
                "issuer_name": "KEPPEL LTD.",
                "ibm_code": "1U68",
            }
        ],
        "security_name": "KEPPEL LTD.",
        "url": (
            "https://links.sgx.com/1.0.0/corporate-announcements/"
            "HAE4K891C8R2VP4O/"
            "6d4072ddb4fccc56eb51e3a667a1b4979fa859367e6aff24d4f463fb851b84e1"
        ),
        "issuer_name": "KEPPEL LTD.",
        "submission_date": "20260504",
        "submission_date_time": 1777851037000,
        "broadcast_date_time": 1777851037000,
        "xml": None,
        "submission_time": None,
        "cat": "ANNC",
        "id": "HAE4K891C8R2VP4O",
        "sn": None,
        "product_category": None,
    }

    appointment = get_appointment(api_response)

    print(appointment)