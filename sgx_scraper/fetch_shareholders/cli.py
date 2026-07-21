from datetime import datetime
from typing import Annotated

from sgx_scraper.utils.cli_helper import upsert_to_db, get_100_top_companies
from sgx_scraper.utils.json_helper import open_json
from sgx_scraper.utils.constant import SGX_FILINGS_PATH_TOP_100, OUTPUT_DIR_SHAREHOLDERS
from sgx_scraper.fetch_shareholders.tracking import get_shareholders_update
from sgx_scraper.fetch_shareholders.api import sync_with_db, get_screener_shareholders
from sgx_scraper.fetch_shareholders.utils.helper import get_current_shareholders

import typer
import json
import logging


app = typer.Typer(help="SGX shareholders tracking pipeline")


@app.command(name='track_shareholders')
def run_tracking_shareholders(
    is_push_db: Annotated[bool, typer.Option(help='Flag to upsert to db or not')] = True
):
    existing_db_shareholders = get_current_shareholders()
    filings = open_json(SGX_FILINGS_PATH_TOP_100)

    payload_updated = get_shareholders_update(
        filing_payload=filings,
        shareholders_db=existing_db_shareholders
    )

    if is_push_db:
        upsert_to_db(sgx_payload=payload_updated, table_name='sgx_companies')


@app.command(name='sync_screener_shareholders')
def run_sync_screener_shareholders(
    is_push_db: Annotated[bool, typer.Option(help='Flag to upsert to db or not')] = True
):
    top_100_companies = get_100_top_companies()

    symbols = [
        record.get('symbol') 
        for record in top_100_companies
    ]

    existing_db_shareholders = get_current_shareholders()
    screener_shareholders = get_screener_shareholders(symbols=symbols)

    payload_updated = sync_with_db(
        screener_shareholders_by_symbol=screener_shareholders,
        db_records=existing_db_shareholders
    )

    OUTPUT_DIR_SHAREHOLDERS.mkdir(parents=True, exist_ok=True)
    output_filename = OUTPUT_DIR_SHAREHOLDERS / f"{datetime.today().strftime('%Y-%m-%d')}_sync_screener_shareholders.json"

    with output_filename.open('w') as file:
        json.dump(payload_updated, file, indent=2)

    if is_push_db:
        upsert_to_db(sgx_payload=payload_updated, table_name='sgx_companies')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    app()
