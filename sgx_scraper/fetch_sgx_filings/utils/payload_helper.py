from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    FILING_EXTRA_FIELDS,
    FILING_TABLE_COLUMNS,
)
from sgx_scraper.utils.json_helper import open_json

import json
import logging


LOGGER = logging.getLogger(__name__)


def get_db(
    table: str, 
    query_modifier,
    columns ="*",
) -> list[dict]:
    query = (
        SUPABASE_CLIENT
        .table(table)
        .select(columns)
    )

    if query_modifier is not None: 
        query = query_modifier(query)

    response = query.execute()
    return response.data 


def get_circumstance_interest(circumstance_interest: dict[str, any]) -> dict[str, any]:
    if not circumstance_interest:
        return None

    for key, value in circumstance_interest.items():
        if key == 'others_specify':
            checked = value.get('checked')
            desc = value.get('description')

            if checked:
                return {
                    'key': 'others_specify',
                    'checked': checked,
                    'description': desc
                }
            
        else:
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict) and 'checked' in sub_value:
                    checked = sub_value.get('checked')
                    desc = sub_value.get('description')

                    if checked:
                        return {
                            'key': key,
                            'specific_key': sub_key,
                            'checked': checked,
                            'description': desc
                        }
                    
                elif sub_value:
                    return {
                        'key': key,
                        'specific_key': sub_key,
                        'checked': sub_value
                    }

    return None


def populate_extra_data(
    symbol: str, 
) -> tuple[str, str, str]: 
    cache_path = "data/sgx_companies.json"

    company_lookup = open_json(cache_path)

    if not symbol: 
        return None, None, None 
    
    data = company_lookup.get(symbol, None)
    
    if not data:
        LOGGER.info('symbol not matched with company lookup')
        return None, None, None
    
    company_name =  data.get('name') or None 
    sector = data.get('sector') or None 
    sub_sector = data.get('sub_sector') or None 

    return company_name, sector, sub_sector


def get_percentage_movement(
    percentage_before: float | None,
    percentage_after: float | None,
) -> float | None:
    if percentage_before is None or percentage_after is None:
        return None

    return round(abs(percentage_after - percentage_before), 5)


def build_filing_payload(record: dict[str, any]) -> dict[str, any]:
    # Shapes a finished record for the sgx_filings table: the before and
    # after share table becomes interest_breakdown, the percentage moved is
    # given per interest, and the symbol carries its exchange suffix. The
    # FILING_EXTRA_FIELDS follow the table's own columns and are dropped by
    # keep_table_columns when batches are merged into a processed file
    symbol = record.get("symbol")

    # (stored key, flat amount prefix, flat percentage prefix)
    interests = [
        ("total", "holding", "share_percentage"),
        ("deemed", "deemed", "deemed_percentage"),
        ("direct", "direct", "direct_percentage"),
    ]

    payload = {
        "source": record.get("source"),
        "timestamp": record.get("timestamp"),
        "transaction_type": record.get("transaction_type"),
        "holder_name": record.get("holder_name"),
        "transaction_value": record.get("transaction_value"),
        "share_percentage_transaction": {
            interest: get_percentage_movement(
                record.get(f"{percentage_prefix}_before"),
                record.get(f"{percentage_prefix}_after"),
            )
            for interest, _, percentage_prefix in interests
        },
        "interest_breakdown": {
            section: {
                interest: {
                    "amount": record.get(f"{amount_prefix}_{section}"),
                    "percentage": record.get(f"{percentage_prefix}_{section}"),
                }
                for interest, amount_prefix, percentage_prefix in interests
            }
            for section in ("after", "before")
        },
        "symbol": f"{symbol}.SI" if symbol else None,
        "price_per_share": record.get("price_per_share"),
        "holder_type": record.get("holder_type"),
        "title": record.get("title"),
        "body": record.get("body"),
        "sector": record.get("sector"),
        "sub_sector": record.get("sub_sector"),
        "tags": record.get("tags") or [],
        "source_is_manual": record.get("source_is_manual"),
    }

    for field in FILING_EXTRA_FIELDS:
        payload[field] = record.get(field)

    return payload


def keep_table_columns(payload: dict[str, any]) -> dict[str, any]:
    return {
        column: payload.get(column)
        for column in FILING_TABLE_COLUMNS
    }


def contains_any_keyword(text: str, keywords: list) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def filter_duplicate(payload: list[dict[str, any]]) -> list[dict]:
    if not payload:
        LOGGER.info(f'Payload is empty, skipping filter duplicate')
        return []

    cleaned_payload = []
    seen_keys = set()

    for row in payload:
        # interest_breakdown and price_per_share are nested, so they are
        # compared as text
        unique_key = (
            row.get('source'),
            row.get('holder_name'),
            row.get('timestamp'),
            json.dumps(row.get('interest_breakdown'), sort_keys=True),
            json.dumps(row.get('price_per_share'), sort_keys=True),
        )

        if unique_key in seen_keys:
            LOGGER.info(f"Dropping duplicate record found in payload: \n{json.dumps(row, indent=2)}")
            continue

        seen_keys.add(unique_key)
        cleaned_payload.append(row)

    return cleaned_payload
