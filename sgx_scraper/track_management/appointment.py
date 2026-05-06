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


def get_appointment(api_response: dict) -> dict | None:
    url = api_response.get('url', '')
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    if not soup: 
        return None 
    
    symbol = extract_symbol(api_response.get('issuers'))

    if not symbol:
        return None 

    name = extract_field(soup, 'Name of person') 
    position =  extract_field(soup, 'Job title (e.g. Lead ID, AC Chairman, AC Member etc.)')
    age = extract_field(soup, 'Age') 
    start_date = parse_appointment_date(extract_field(soup, 'Date of appointment'))
    #announcement_ref = extract_field(soup, 'Announcement Reference')

    return {
        'symbol': symbol,
        'name': name,
        'position': position,
        'age': age,
        'start_date': start_date,
        # 'category': 'appointment'
        # 'announcement_ref': announcement_ref
    }


if __name__ == '__main__':
    url_two = 'https://links.sgx.com/1.0.0/corporate-announcements/MJPBYFU357NJCLLL/a15afb26438407f97ac0b674a391501a2119ab94c7878af734051614f172f265'
    url = 'https://links.sgx.com/1.0.0/corporate-announcements/XLOPWG11Y7C6RBXK/e147e187d66955d5b6aba2f57e4bf707af24d3045899140a74544ee86c33ee0d'
    api = {
      "ref_id": "SG260504OTHR99WU",
      "sub": "ANNC04",
      "category_name": "Announcement of Cessation",
      "submitted_by": "Maria Crisselda T. Torcuator",
      "title": "Change - Announcement of Cessation::Retirement of Ms. Fe Irma A. Ramirez",
      "announcer_name": None,
      "issuers": [
        {
          "isin_code": "XS3178401793",
          "stock_code": "MRBB",
          "security_name": "PETRON CORP US$475M7.35%PCS",
          "issuer_name": "PETRON CORPORATION",
          "ibm_code": "5MM5"
        }
      ],
      "security_name": "PETRON CORP US$475M7.35%PCS",
      "url": "https://links.sgx.com/1.0.0/corporate-announcements/XE63ZTSZKT5NO7CA/2320a19f04846bcea013e6adbe614bc6c5f6f42899a441fd8712b2863af3c3d7",
      "issuer_name": "PETRON CORPORATION",
      "submission_date": "20260504",
      "submission_date_time": 1777864249000,
      "broadcast_date_time": 1777864249000,
      "xml": None,
      "submission_time": None,
      "cat": "ANNC",
      "id": "XE63ZTSZKT5NO7CA",
      "sn": None,
      "product_category": None
    }
    api_two = {
      "ref_id": "SG260504OTHRJNWK",
      "sub": "ANNC03",
      "category_name": "Announcement of Appointment",
      "submitted_by": "Karen Teo/Samantha Teong",
      "title": "Change - Announcement of Appointment::Appointment of Non-Executive Independent Director",
      "announcer_name": None,
      "issuers": [
        {
          "isin_code": "SG1U68934629",
          "stock_code": "BN4",
          "security_name": "KEPPEL LTD.",
          "issuer_name": "KEPPEL LTD.",
          "ibm_code": "1U68"
        }
      ],
      "security_name": "KEPPEL LTD.",
      "url": "https://links.sgx.com/1.0.0/corporate-announcements/HAE4K891C8R2VP4O/6d4072ddb4fccc56eb51e3a667a1b4979fa859367e6aff24d4f463fb851b84e1",
      "issuer_name": "KEPPEL LTD.",
      "submission_date": "20260504",
      "submission_date_time": 1777851037000,
      "broadcast_date_time": 1777851037000,
      "xml": None,
      "submission_time": None,
      "cat": "ANNC",
      "id": "HAE4K891C8R2VP4O",
      "sn": None,
      "product_category": None
    }

    appointment = get_appointment(api_two)
    print(appointment)

# uv run -m sgx_scraper.track_management.appointment 
