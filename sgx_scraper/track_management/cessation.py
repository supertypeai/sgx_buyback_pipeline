from bs4 import BeautifulSoup 

from sgx_scraper.utils.constant import HEADERS
from sgx_scraper.track_management.utils.parser import (
    extract_field, 
    extract_symbol, 
    parse_appointment_date
)
import requests
import logging


LOGGER = logging.getLogger(__name__)


def get_cessation(api_response: dict) -> dict | None:
    url = api_response.get('url', '')
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    if not soup: 
        return None  

    symbol =  extract_symbol(api_response.get('issuers', []))

    if not symbol:
        return None 

    name = extract_field(soup, 'Name of person')
    position = extract_field(soup, 'Job title (e.g. Lead ID, AC Chairman, AC Member etc.)')
    age = extract_field(soup, 'Age')
    end_date = parse_appointment_date(extract_field(soup, 'If yes, please provide the date.'))
    # announcement_ref = extract_field(soup, 'Announcement Reference'),

    return {
        'symbol': symbol,
        'name': name,
        'position': position,
        'age': age,
        'end_date': end_date,
        # 'category': 'cessation'
        # 'announcement_ref': announcement_ref
    }


if __name__ == '__main__':
    api = {
      "ref_id": "SG260504OTHR0U4Z",
      "sub": "ANNC04",
      "category_name": "Announcement of Cessation",
      "submitted_by": "Siaw Ken Ket @ Danny Siaw",
      "title": "Change - Announcement of Cessation::Retirement of Executive Director and Cost Director at Annual General Meeting",
      "announcer_name": None,
      "issuers": [
        {
          "isin_code": "SG2G36998349",
          "stock_code": "5F4",
          "security_name": "FIGTREE HOLDINGS LIMITED",
          "issuer_name": "FIGTREE HOLDINGS LIMITED",
          "ibm_code": "2G36"
        }
      ],
      "security_name": "FIGTREE HOLDINGS LIMITED",
      "url": "https://links.sgx.com/1.0.0/corporate-announcements/PV80RZ0R9XL2UHPP/807e8ee084622cce576290f6cbdc005150f2c751dadf668d4820319d4cac6997",
      "issuer_name": "FIGTREE HOLDINGS LIMITED",
      "submission_date": "20260504",
      "submission_date_time": 1777850359000,
      "broadcast_date_time": 1777850359000,
      "xml": None,
      "submission_time": None,
      "cat": "ANNC",
      "id": "PV80RZ0R9XL2UHPP",
      "sn": None,
      "product_category": None
    }

    cessation = get_cessation(api)
    print(cessation)

# uv run -m sgx_scraper.track_management.cessation 