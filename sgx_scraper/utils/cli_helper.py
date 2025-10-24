from datetime import datetime 

from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.config.settings import LOGGER

import json 


def normalize_datetime(date: str | datetime) -> str: 
    if isinstance(date, datetime):
        return date.strftime("%Y%m%d")
     
    try:
        if '-' in date: 
            dt = datetime.strptime(date, "%Y-%m-%d")
        else:
            dt = datetime.strptime(date, "%Y%m%d")
        
        return dt.strftime("%Y%m%d") 
    
    except ValueError:
        LOGGER.error("Invalid date format. Use YYYY-MM-DD or YYYYMMDD.")
    

def push_to_db(sgx_payload: list[dict[str]], table_name: str) -> bool:
    if not sgx_payload:
        LOGGER.info(f'[sgx_payload] is empty, skipping push to DB')
        return 
    
    try:
        is_succes = False 

        response = (
            SUPABASE_CLIENT
            .table(table_name)
            .insert(sgx_payload)
            .execute()
        )

        if response.data:
            LOGGER.info(f"[sgx_payload] Successfully pushed {len(sgx_payload)} records to DB, table: {table_name}")
            is_succes = True 
            return is_succes 
        
        return is_succes
    
    except Exception as error:
        LOGGER.error(f"[push_to_db] Failed to push data: {error}")
        return None
    

def clean_payload_sgx_buyback(payload: list[dict]) -> list[dict]:
    if not payload:
        LOGGER.info(f'[sgx_buyback] is empty, skipping clean payload')
        return []
    
    for row in payload:
        for key in [
            "total_shares_purchased",
            "cumulative_purchased",
            "treasury_shares_after_purchase"
        ]:
            if key in row and row[key] is not None:
                try:
                    row[key] = int(float(row[key]))
                except (ValueError, TypeError):
                    LOGGER.error(f"Failed to convert {key} with value {row[key]} to int.")
                    row[key] = None
    
    return payload


def clean_payload_sgx_filings(payload: list[dict[str, any]]) -> list[dict]:
    if not payload:
        LOGGER.info(f'[sgx_filings] is empty, skipping clean payload')
        return [], []

    for row in payload:
        shareholder_name = row.get('shareholder_name')
        if shareholder_name.isupper():
            row['shareholder_name'] = shareholder_name.title()

        for key in [
            "number_of_stock",
            "shares_before",
            "shares_after"
        ]:
            if key in row and row[key] is not None:
                try:
                    row[key] = int(float(row[key]))
                except (ValueError, TypeError):
                    LOGGER.error(f"Failed to convert {key} with value {row[key]} to int.")
                    row[key] = None
    
    return payload 


def remove_duplicate(path_today: str, path_yesterday: str) -> list[dict]:
    sgx_today_datas = open_json(path_today)
    sgx_yesterday_datas = open_json(path_yesterday) 

    if not sgx_yesterday_datas:
        LOGGER.info('Skip removing duplicate, sgx yesterday data is empty, returning sgx today')
        return sgx_today_datas
    
    urls_yesterday = {item.get("url") for item in sgx_yesterday_datas}

    unique_data_today = [
        item for item in sgx_today_datas
        if item.get('url') not in urls_yesterday
    ]

    LOGGER.info(f'Length data after duplicate removing: {len(unique_data_today)}')
    return unique_data_today


def write_to_json(path: str, payload_sgx: list[dict[str, any]]):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload_sgx, file, ensure_ascii=False, indent=2)
    
    LOGGER.info(f"Saved all sgx scraped to {path}")


def open_json(path: str) -> list[dict[str, any]]:
    with open(path, "r", encoding="utf-8") as file:
        sgx_data = json.load(file)
    return sgx_data
    
