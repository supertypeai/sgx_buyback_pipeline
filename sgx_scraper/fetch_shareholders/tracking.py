from sgx_scraper.utils.cli_helper import open_json
from sgx_scraper.utils.constant import SGX_FILINGS_PATH_TOP_100 
from .utils.helper import find_matched_db_shareholder, enrich, matched_db_management

import logging 
import json 


LOGGER = logging.getLogger(__name__)


def get_latest_filing_by_name(filings: list[dict]) -> list[dict]: 
    latest_by_name = {}

    for filing in filings:
        name = filing.get('shareholder_name')
        transaction_date = filing.get('transaction_date')

        if not name or not transaction_date:
            continue

        if name not in latest_by_name:
            latest_by_name[name] = filing 
        
        else: 
            existing_date = latest_by_name[name].get('transaction_date') 
            
            if transaction_date > existing_date: 
                latest_by_name[name] = filing 

    return list(latest_by_name.values())


def get_filings() -> list[dict]:
    payload = open_json(SGX_FILINGS_PATH_TOP_100)
    final_payload = get_latest_filing_by_name(payload)
    return final_payload 


def get_shareholders_update(filing_payload: list[dict], shareholders_db: dict[str, dict]) -> list[dict]:
    result_by_symbol = {}

    for filing in filing_payload:
        filing_symbol = filing.get('symbol')
        filing_shareholder = filing.get('shareholder_name')
        filing_share_percentage = filing.get('shares_after_percentage')
        filing_share_amount = filing.get('shares_after')

        if filing_symbol not in shareholders_db:
            continue

        db_shareholders = shareholders_db.get(filing_symbol, [])

        if not db_shareholders:
            LOGGER.warning(f'[matching] No db shareholders found for symbol: {filing_symbol}')
            continue

        if filing_symbol not in result_by_symbol:
            result_by_symbol[filing_symbol] = list(db_shareholders.get('shareholders'))

        matched_shareholder = find_matched_db_shareholder(filing_shareholder, result_by_symbol[filing_symbol])

        if matched_shareholder:
            matched_shareholder['share_amount'] = filing_share_amount
            matched_shareholder['share_percentage'] = filing_share_percentage

        else:
            db_management = db_shareholders.get('management')

            list_managements = {
                record.get('name') 
                for record in db_management
            }

            is_management = matched_db_management(filing_shareholder, list_managements)

            if not is_management and filing_share_percentage < 0.05: 
                LOGGER.info(
                    'shareholder name: %s not in board management: %s', filing_shareholder, ', '.join(list_managements)
                )
                continue 

            result_by_symbol[filing_symbol].append({
                'name': filing_shareholder,
                'share_amount': filing_share_amount,
                'share_percentage': filing_share_percentage,
            })

    payload_update = [
        {'symbol': symbol, 'shareholders': shareholders}
        for symbol, shareholders in result_by_symbol.items()
    ]

    final_payload = enrich(payload_update)

    LOGGER.info('Check payload updated: %s', json.dumps(final_payload, indent=2))
    return final_payload


