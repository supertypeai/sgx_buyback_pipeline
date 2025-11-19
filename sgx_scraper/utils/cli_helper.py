from datetime import datetime 

from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.config.settings import LOGGER

import json 
import os 
import pandas as pd 


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
        return []

    cleaned_payload = []
    seen_keys = set()

    for row in payload:
        shareholder_name = row.get('shareholder_name')
        price_per_share = row.get('price_per_share')
        number_of_stock = row.get('number_of_stock')
        value = row.get('value')

        # Exclude missing data
        if not (price_per_share and number_of_stock and value):
            LOGGER.info(f'Dropping row with missing values:\n{json.dumps(row, indent=2)}')
            continue

        if shareholder_name.isupper():
            row['shareholder_name'] = shareholder_name.title()

        # Convert share counts to int
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
        
        # Check duplicate data with composite keys
        unique_key = {
            row.get('url'),
            row.get('shareholder_name'),
            row.get('transaction_date'),
            row.get('shares_before'),
            row.get('shares_after')
        }

        if unique_key in seen_keys:
            LOGGER.info(f"Dropping duplicate record found in payload: \n{json.dumps(row, indent=2)}")
            continue
        
        seen_keys.add(unique_key)
        cleaned_payload.append(row)

    return cleaned_payload 


def remove_duplicate(path_today: str, path_yesterday: str) -> list[dict]:
    sgx_today_datas = open_json(path_today)
    sgx_yesterday_datas = open_json(path_yesterday) 

    if sgx_yesterday_datas is None or len(sgx_yesterday_datas) == 0:
        LOGGER.info('Skip removing duplicate, sgx yesterday data is empty, returning sgx today')
        return sgx_today_datas
    
    urls_yesterday = {item.get("url") for item in sgx_yesterday_datas}

    unique_data_today = [
        item for item in sgx_today_datas
        if item.get('url') not in urls_yesterday
    ]

    LOGGER.info(f'Length data after duplicate removing: {len(unique_data_today)}')
    return unique_data_today


def filter_top_70_companies(clean_payload: list[dict[str]]) -> tuple:
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_companies')
            .select('symbol, name, market_cap')
            .execute()
        )

        if not response:
            LOGGER.warning('Data sgx_companies not found')
            return [], clean_payload
        
        df_sgx_companies = pd.DataFrame(response.data)

        df_top_sgx = df_sgx_companies.sort_values("market_cap", ascending=False).head(70)
        top_70_symbols = set(df_top_sgx['symbol'].tolist())

        top_70_payload = []
        not_top_70_payload = []

        for payload in clean_payload:
            symbol = payload.get('symbol')
            
            if symbol in top_70_symbols:
                 top_70_payload.append(payload)
            else:
                 not_top_70_payload.append(payload)

        LOGGER.info(f'Length data top_70: {len(top_70_payload)} | Length data not top_70: {len(not_top_70_payload)}')
        return top_70_payload, not_top_70_payload

    except Exception as error:
        LOGGER.error(f'[filter_top_50_companies] Error: {error}')
        return [], [] 


def write_to_csv(path: str, payload_not_top_70: list[dict[str]]):
    df = pd.DataFrame(payload_not_top_70)
    
    if df.empty:
        return

    file_exists = os.path.isfile(path)
    df.to_csv(path, mode='a', index=False, header=not file_exists)

    LOGGER.info(f'Saved payload to {path}')


def write_to_json(path: str, payload_sgx: list[dict[str, any]]):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload_sgx, file, ensure_ascii=False, indent=2)
    
    LOGGER.info(f"Saved all sgx scraped to {path}")


def open_json(path: str):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError:
        LOGGER.warning(f"Failed to decode JSON from {path}, returning empty list")
        return []
