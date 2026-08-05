from pathlib import Path

from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.utils.json_helper import open_json

import csv
import pandas as pd
import logging


LOGGER = logging.getLogger(__name__)


def push_to_db(
    payload: list[dict[str]],
    table_name: str,
    exclude_columns: set[str] | None = None,
) -> bool:
    if not payload:
        LOGGER.info(f'[payload] is empty, skipping push to DB')
        return

    try:
        is_succes = False

        exclude_columns = exclude_columns or set()

        payload = [
            {
                key: value
                for key, value in record.items()
                if key not in exclude_columns
            }
            for record in payload
        ]

        response = (
            SUPABASE_CLIENT
            .table(table_name)
            .insert(payload)
            .execute()
        )

        if response.data:
            LOGGER.info(f"[payload] Successfully pushed {len(payload)} records to DB, table: {table_name}")
            is_succes = True 
            return is_succes 
        
        return is_succes
    
    except Exception as error:
        LOGGER.error(
            f"[push_to_db] Failed to push data to {table_name}: {error}",
            exc_info=True,
        )
        raise


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
        LOGGER.error(
            '[upsert_to_db] failed to upsert to %s: %s', 
            table_name, 
            error,
            exc_info=True
        )
        raise


def remove_duplicate(path_today: str, path_yesterday: str) -> list[dict]:
    sgx_today_datas = open_json(path_today)
    sgx_yesterday_datas = open_json(path_yesterday) 

    if sgx_yesterday_datas is None or len(sgx_yesterday_datas) == 0:
        LOGGER.info('Skip removing duplicate, sgx yesterday data is empty, returning sgx today')
        return sgx_today_datas
    
    urls_yesterday = {
        item.get("source") 
        for item in sgx_yesterday_datas
    }

    unique_data_today = [
        item 
        for item in sgx_today_datas
        if item.get('source') not in urls_yesterday
    ]

    LOGGER.info(f'Length data after duplicate removing: {len(unique_data_today)}')
    return unique_data_today


def filter_top_n_companies(clean_payload: list[dict[str]], top_n: int = 70) -> tuple:
    try:
        response = (
            SUPABASE_CLIENT
            .table('sgx_company_report')
            .select('symbol, name, market_cap')
            .not_.is_('market_cap', 'null')
            .order('market_cap', desc=True)
            .limit(top_n)
            .execute()
        )

        if not response.data:
            LOGGER.warning('Data sgx_companies not found')
            return [], clean_payload

        top_companies = response.data

        csv_path = Path(f"data/sgx_top_{top_n}_mcap_companies.csv")
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        with csv_path.open('w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(
                file, fieldnames=['symbol', 'name', 'market_cap']
            )
            writer.writeheader()
            writer.writerows(top_companies)

        top_n_symbols = {
            company['symbol'] 
            for company in top_companies
        }

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


def get_top_companies(csv_path: str | Path) -> list[dict]:
    csv_path = Path(csv_path)
    
    if not csv_path.exists():
        LOGGER.warning("CSV not found: %s", csv_path)
        return []

    df_top_n = pd.read_csv(csv_path)
    return df_top_n.to_dict(orient="records")


def get_100_top_companies():
    top_companies = get_top_companies(
        "data/sgx_top_100_mcap_companies.csv"
    )

    # The top-100 CSV intentionally contains only ranking data.  Management
    # tracking needs the existing management list, so need to open from local list
    companies = open_json('data/sgx_companies.json')
    
    if not isinstance(companies, dict):
        LOGGER.warning('Company snapshot is unavailable; management updates will start empty')
        return top_companies

    for company in top_companies:
        cached_company = companies.get(company.get('symbol'), {})
        company['management'] = cached_company.get('management') or []

    return top_companies
