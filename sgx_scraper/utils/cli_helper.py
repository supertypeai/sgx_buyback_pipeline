from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.utils.json_helper import open_json
from sgx_scraper.utils.sgx_symbol_helper import (
    add_sgx_suffix,
    strip_sgx_suffix,
)

import logging


LOGGER = logging.getLogger(__name__)


def prepare_payload_for_db(payload: list[dict]) -> list[dict]:
    prepared_payload = []

    for record in payload:
        prepared_record = record.copy()

        if prepared_record.get("symbol"):
            prepared_record["symbol"] = add_sgx_suffix(
                prepared_record["symbol"]
            )

        if prepared_record.get("symbols"):
            prepared_record["symbols"] = [
                add_sgx_suffix(symbol)
                for symbol in prepared_record["symbols"]
            ]

        prepared_payload.append(prepared_record)

    return prepared_payload


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
        payload = prepare_payload_for_db(payload)

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
        top_companies = get_db(
            table='sgx_company_report',
            columns='symbol, name, market_cap',
            query=lambda query: (
                query
                .not_.is_('market_cap', 'null')
                .order('market_cap', desc=True)
                .limit(top_n)
            ),
        )

        if not top_companies:
            LOGGER.warning('Data sgx_companies not found')
            return [], clean_payload

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


def upsert_to_db(
    payload: list[dict[str]],
    table_name: str,
    on_conflict: str | None = None,
    exclude_columns: set[str] | None = None,
) -> bool:
    if not payload:
        LOGGER.info('[payload] is empty, skipping upsert to DB')
        return False

    exclude_columns = exclude_columns or set()

    payload = [
        {
            key: value
            for key, value in record.items()
            if key not in exclude_columns
        }
        for record in payload
    ]

    payload = prepare_payload_for_db(payload)

    try:
        response = (
            SUPABASE_CLIENT
            .table(table_name)
            .upsert(payload, **({'on_conflict': on_conflict} if on_conflict else {}))
            .execute()
        )

        if response.data:
            LOGGER.info(f"[payload] Successfully upserted {len(payload)} records to DB, table: {table_name}")
            return True

        return False

    except Exception as error:
        LOGGER.error(
            f"[upsert_to_db] Failed to upsert data to {table_name}: {error}",
            exc_info=True,
        )
        raise


def get_db(table: str, columns: str = "*", query=None):
    db_query = (
        SUPABASE_CLIENT
        .table(table)
        .select(columns)
    )

    if query:
        db_query = query(db_query)

    response = db_query.execute()

    records = response.data

    for record in records:
        if record.get("symbol"):
            record["symbol"] = strip_sgx_suffix(record["symbol"])

    return records
