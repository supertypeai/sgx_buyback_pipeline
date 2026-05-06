from rapidfuzz import process, fuzz

from sgx_scraper.track_management.appointment import get_appointment 
from sgx_scraper.track_management.cessation import get_cessation

import logging 


LOGGER = logging.getLogger(__name__)


def get_management_update(api_response: dict, top_100_companies: list[dict]):
    registry = {
        'announcement of appointment': get_appointment, 
        'announcement of cessation': get_cessation
    }

    category = api_response.get('category_name', '').strip().lower()
    handler = registry.get(category)

    if not handler:
        LOGGER.warning(f'[management] unknown category: {category}')
        return None
    
    announcement = handler(api_response=api_response)

    db_symbols = {record.get('symbol'): record for record in top_100_companies}

    symbol = announcement.get('symbol')

    if symbol not in db_symbols:
        LOGGER.info(f'announcement {category} for symbol: {symbol} not in the top 100 companies')
        return None  

    db_management = db_symbols[symbol].get('management', [])

    if category == 'announcement of appointment':
        db_management.append({
            'name': announcement.get('name'),
            'position': announcement.get('position'),
            'age': announcement.get('age'),
            'start_date': announcement.get('start_date'),
        })
        
        LOGGER.info(f'[appointment] added {announcement.get("name")} as {announcement.get("position")} for {symbol}')

    elif category == 'announcement of cessation':
        matched = False

        for record in db_management:
            score = fuzz.token_sort_ratio(record.get('name', ''), announcement.get('name', ''))

            if score >= 88:
                record['end_date'] = announcement.get('end_date')
                LOGGER.info(f'[cessation] matched {announcement.get("name")} -> {record.get("name")} (score: {score}) for {symbol}')
                matched = True
                break

        if not matched:
            LOGGER.warning(f'[cessation] no match found for {announcement.get("name")} in {symbol} management')

    updated_record = {
        'symbol': symbol, 
        'management': db_management
    }
    
    return [updated_record]


if __name__ == '__main__':
    api = {
      "ref_id": "SG260504OTHRCMBL",
      "sub": "ANNC03",
      "category_name": "Announcement of Appointment",
      "submitted_by": "Dr Zhang Jian",
      "title": "Change - Announcement of Appointment::Appointment of Dr Tan Wei Jie (\"Dr Tan\") as Chief Executive Officer and Executive Director",
      "announcer_name": None,
      "issuers": [
        {
          "isin_code": "SG0584008601",
          "stock_code": "K71U",
          "security_name": "AJJ MEDTECH HOLDINGS LIMITED",
          "issuer_name": "AJJ MEDTECH HOLDINGS LIMITED",
          "ibm_code": "K71U"
        }
      ],
      "security_name": "AJJ MEDTECH HOLDINGS LIMITED",
      "url": "https://links.sgx.com/1.0.0/corporate-announcements/XLOPWG11Y7C6RBXK/e147e187d66955d5b6aba2f57e4bf707af24d3045899140a74544ee86c33ee0d",
      "issuer_name": "AJJ MEDTECH HOLDINGS LIMITED",
      "submission_date": "20260504",
      "submission_date_time": 1777904731000,
      "broadcast_date_time": 1777904731000,
      "xml": None,
      "submission_time": None,
      "cat": "ANNC",
      "id": "XLOPWG11Y7C6RBXK",
      "sn": None,
      "product_category": None
    }

    db_management = [
        {
            "symbol":"K71U",
            "name":"DBS Group Holdings Ltd",
            "market_cap":166143508480,
            'management': [
                {
                "name": "Magdalene Tan",
                "position": "Head of Internal Audit",
                "age": None,
                "start_date": "2025-02-01"
                },
                {
                "name": "Jason Chua",
                "position": "Director of Asset Management of Keppel REIT Management Limited",
                "age": 40,
                "start_date": None
                }
            ]
        }

    ]

    updated_management_record = get_management_update(api, db_management)
    print(updated_management_record)


# uv run -m sgx_scraper.track_management.tracking