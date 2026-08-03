from .utils.helper import find_matched_db_shareholder

import logging 
import json 


LOGGER = logging.getLogger(__name__)


def get_latest_filing_by_name(filings: list[dict]) -> list[dict]: 
    latest_by_name = {}

    for filing in filings:
        name = filing.get('holder_name')
        transaction_date = filing.get('timestamp')

        if not name or not transaction_date:
            continue

        if name not in latest_by_name:
            latest_by_name[name] = filing 
        
        else: 
            existing_date = latest_by_name[name].get('timestamp')
            
            if transaction_date > existing_date: 
                latest_by_name[name] = filing 

    return list(latest_by_name.values())


def get_shareholders_update(
    filing_payload: list[dict],
    shareholders_db: dict[str, dict],
) -> list[dict]:
    latest_filings = get_latest_filing_by_name(filing_payload)
    result_by_symbol = {}

    for filing in latest_filings:
        filing_symbol = filing.get('symbol')
        filing_shareholder = filing.get('holder_name')
        filing_share_amount = filing.get('direct_after')
        filing_share_percentage = filing.get("direct_percentage_after")

        if filing_symbol not in shareholders_db:
            continue

        db_shareholders = shareholders_db.get(filing_symbol, {})
        existing_shareholders = db_shareholders.get('shareholders')

        if not existing_shareholders:
            continue

        result_by_symbol.setdefault(filing_symbol, list(existing_shareholders))

        matched_shareholder = find_matched_db_shareholder(
            filing_shareholder,
            result_by_symbol[filing_symbol],
        )

        is_zero_direct_position = (
            filing_share_amount == 0
            and filing_share_percentage == 0
        )

        if matched_shareholder:
            if is_zero_direct_position:
                result_by_symbol[filing_symbol].remove(matched_shareholder)
                continue

            if filing_share_amount is not None:
                matched_shareholder['share_amount'] = filing_share_amount

            if filing_share_percentage is not None:
                matched_shareholder['share_percentage'] = filing_share_percentage

        else:
            result_by_symbol[filing_symbol].append({
                'name': filing_shareholder,
                'share_amount': filing_share_amount,
                'share_percentage': filing_share_percentage,
            })

    LOGGER.info(
        'Check payload updated: %s',
        json.dumps(result_by_symbol, indent=2)
    )

    return [
        {'symbol': symbol, 'shareholders': shareholders}
        for symbol, shareholders in result_by_symbol.items()
    ]
