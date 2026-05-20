from datetime import datetime
from pathlib import Path 

from sgx_scraper.config.settings import SUPABASE_CLIENT

import json 
import os 
import pandas as pd 
import logging


LOGGER = logging.getLogger(__name__)


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
        return None 


def push_to_db(sgx_payload: list[dict[str]], table_name: str) -> bool:
    if not sgx_payload:
        LOGGER.info(f'[sgx_payload] is empty, skipping push to DB')
        return 
    
    try:
        is_succes = False 

        sgx_payload = [
            {key: value for key, value in record.items() if key != "issuer_name"}
            for record in sgx_payload
        ]

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
        LOGGER.error(f"[push_to_db] Failed to push data to {table_name}: {error}")
        return None


def upsert_to_db(sgx_payload: list[dict], table_name: str) -> bool:
    if not sgx_payload:
        LOGGER.info('[sgx_payload] is empty, skipping push to DB')
        return False

    try:
        response = (
            SUPABASE_CLIENT
            .table(table_name)
            .upsert(sgx_payload)
            .execute()
        )

        if response.data:
            LOGGER.info(
                '[sgx_payload] successfully upserted %d records to DB, table: %s',
                len(sgx_payload),
                table_name
            )
            return True

        return False

    except Exception as error:
        LOGGER.error('[upsert_to_db] failed to upsert to %s: %s', table_name, error)
        return False
    

def clean_payload_sgx_buyback(payload: list[dict[str, any]]) -> list[dict[str, any]]:
    if not payload:
        LOGGER.info(f'[sgx_buyback] is empty, skipping clean payload')
        return []
    
    for row in payload:
        mandate = row.get('mandate')

        if mandate:
            for key_mandate in ['cumulative_purchased', 'mandate_remaining', 'mandate_total']:

                if key_mandate in mandate and mandate[key_mandate] is not None:
                    try:
                        mandate[key_mandate] = int(float(mandate[key_mandate]))

                    except (ValueError, TypeError):
                        LOGGER.error(f"Failed to convert {key_mandate} with value {mandate[key_mandate]} to int.")
                        mandate[key_mandate] = None

        for key in [
            "total_shares_purchased",
            "treasury_shares_after_purchase"
        ]:
            if key in row and row[key] is not None:
                try:
                    row[key] = int(float(row[key]))

                except (ValueError, TypeError):
                    LOGGER.error(f"Failed to convert {key} with value {row[key]} to int.")
                    row[key] = None

    return payload


def standardize_name(payload: list[dict[str, any]]) -> list[dict[str, any]]:
    for record in payload:
        holder_name = record.pop('shareholder_name', None)
        record['holder_name'] = holder_name.strip() if holder_name is not None else None 
        record['holding_before'] = record.pop('shares_before', None)
        record['holding_after'] = record.pop('shares_after', None)
        record['share_percentage_before'] = record.pop('shares_before_percentage', None)
        record['share_percentage_after'] = record.pop('shares_after_percentage', None)
        record['timestamp'] = record.pop('transaction_date', None)
        record['amount_transaction'] = record.pop('number_of_stock', None)
        record['transaction_value'] = record.pop('value', None) 
        record['source'] = record.pop('url', None) 

        share_pct_after = record.get('share_percentage_after')
        share_pct_before = record.get('share_percentage_before')
        
        if share_pct_after is not None and share_pct_before is not None:
            record['share_percentage_transaction'] = round(abs(share_pct_after - share_pct_before), 7)
        
        else:
            record['share_percentage_transaction'] = None
        
    return payload 


def clean_payload_sgx_filings(payload: list[dict[str, any]]) -> list[dict]:
    if not payload:
        LOGGER.info(f'[sgx_filings] is empty, skipping clean payload')
        return []

    cleaned_payload = []
    seen_keys = set()

    for row in payload:
        shareholder_name = row.get('shareholder_name')
        row.pop('time', None)

        if shareholder_name and shareholder_name.isupper():
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
        unique_key = (
            row.get('url'),
            row.get('shareholder_name'),
            row.get('transaction_date'),
            row.get('shares_before'),
            row.get('shares_after'),
            row.get('price_per_share')
        )

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


def filter_top_n_companies(clean_payload: list[dict[str]], top_n: int = 70) -> tuple:
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_company_report')
            .select('symbol, name, market_cap')
            .execute()
        )

        if not response:
            LOGGER.warning('Data sgx_companies not found')
            return [], clean_payload
        
        df_sgx_companies = pd.DataFrame(response.data)

        df_top_sgx = (
            df_sgx_companies
            .sort_values("market_cap", ascending=False)
            .head(top_n)
        )

        csv_path = Path(f"data/sgx_top_{top_n}_mcap_companies.csv")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df_top_sgx[["symbol", "name", "market_cap"]].to_csv(csv_path, index=False)

        top_n_symbols = set(df_top_sgx['symbol'].tolist())

        top_n_payload = []
        not_top_n_payload = []

        for payload in clean_payload:
            symbol = payload.get('symbol')

            if symbol in top_n_symbols:
                top_n_payload.append(payload)

            else:
                not_top_n_payload.append(payload)

        LOGGER.info(
            "Length data top_%d: %d | Length data not top_%d: %d",
            top_n, len(top_n_payload), top_n, len(not_top_n_payload)
        )

        return top_n_payload, not_top_n_payload

    except Exception as error:
        LOGGER.error("[filter_top_n_companies] Error: %s", error, exc_info=True)
        return [], []


def get_100_top_companies():
    csv_path = Path(f"data/sgx_top_100_mcap_companies.csv")

    if not csv_path.exists():
        LOGGER.warning("CSV not found: %s", csv_path)
        return []

    df_top_n = pd.read_csv(csv_path)
    return df_top_n.to_dict(orient="records")


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
