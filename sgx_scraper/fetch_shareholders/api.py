from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict

from .utils.helper import find_matched_db_shareholder, clean_company_name, enrich

import requests 
import logging
import time 
import random 
import json 


LOGGER = logging.getLogger(__name__)


def build_http_session() -> requests.Session:
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_randomized_headers(base_headers: dict) -> dict:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    ]
    
    return {
        **base_headers,
        "User-Agent": random.choice(user_agents),
    }


def fetch_api(symbol: str) -> dict[str, any]: 
    base_headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'authorizationtoken': '',
        'origin': 'https://www.sgx.com',
        'referer': 'https://www.sgx.com/',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
    }

    headers = get_randomized_headers(base_headers)
  
    api_url = f'https://api.sgx.com/shareholdersreports/v2.0/stockCode/{symbol}?params=investorName,investorType,investorHoldingsDate,pctOfSharesOutstanding,sharesHeld,sharesHeldChange,turnoverRating'
    
    session = build_http_session()

    try:
        response = session.get(url=api_url, headers=headers)
        response.raise_for_status()

        return response.json()

    except Exception as error:
        LOGGER.error('Fetch api shareholders error: %s', error, exc_info=True)
        return None 


def clean_api_response(payload: dict[str, any], symbol: str) -> dict[str, list]: 
    data = payload.get('data')
    
    final_payload = defaultdict(list)

    for record in data:
        shareholder_name = record.get('investorName')
        share_amount = record.get('sharesHeld')
        share_percentage = record.get('pctOfSharesOutstanding')

        if any(value is None for value in [shareholder_name, share_amount, share_percentage]):
            print('some data is none, skipped for symbol: %s', symbol)
            continue 

        final_payload[symbol].append(
            {
                'name': clean_company_name(shareholder_name), 
                'share_amount': share_amount, 
                'share_percentage': round(share_percentage / 100, 3) 
            }
        )

    return final_payload


def get_screener_shareholders(symbols: list[str]) -> dict[str, list]:
    next_long_break_at = random.randint(8, 15)

    final = {}

    for index, symbol in enumerate(symbols, start=1):
        LOGGER.info('processing %d/%d', index, len(symbols))

        response = fetch_api(symbol=symbol) 
        result = clean_api_response(response, symbol) 

        final.update(result)

        if index == next_long_break_at: 
            sleep_duration = random.randint(20, 30)
            LOGGER.info('long break at request %d, sleeping %ds', index, sleep_duration)

            time.sleep(sleep_duration)
            next_long_break_at = index + random.randint(8, 15)

        else:
            time.sleep(random.randint(2, 10))

    return final 


def sync_with_db(
    screener_shareholders_by_symbol: dict[str, list],
    db_records: list[dict],
) -> list[dict]:
    result = []

    db_lookup = {
        record.get('symbol'): record.get('shareholders')
        for record in db_records
    }

    for symbol, screener_shareholders in screener_shareholders_by_symbol.items():
        if symbol not in db_lookup:
            continue

        existing_db_shareholders = db_lookup[symbol]
        merged_shareholders = []
        matched_db_shareholders = set()

        for screener_shareholder in screener_shareholders:
            screener_name = screener_shareholder.get('name')
            screener_share_amount = screener_shareholder.get('share_amount')
            screener_share_percentage = screener_shareholder.get('share_percentage')

            matched_db_shareholder = find_matched_db_shareholder(screener_name, existing_db_shareholders)

            if matched_db_shareholder:
                if (
                    matched_db_shareholder.get('share_amount') != screener_share_amount or
                    matched_db_shareholder.get('share_percentage') != screener_share_percentage
                ):
                    matched_db_shareholder['share_amount'] = screener_share_amount
                    matched_db_shareholder['share_percentage'] = screener_share_percentage

                matched_db_shareholders.add(matched_db_shareholder.get('name'))
                merged_shareholders.append(matched_db_shareholder)  

            else:
                merged_shareholders.append(screener_shareholder)  

        for db_shareholder in existing_db_shareholders:
            if db_shareholder.get('name') not in matched_db_shareholders:
                merged_shareholders.append(db_shareholder)

        result.append({
            'symbol': symbol,
            'shareholders': merged_shareholders
        })

    final_result = enrich(result)

    LOGGER.info('Check payload synced: %s', json.dumps(final_result, indent=2))
    return final_result 

