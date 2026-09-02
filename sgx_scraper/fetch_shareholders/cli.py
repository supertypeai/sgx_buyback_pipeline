from datetime import datetime
from typing import Annotated

from sgx_scraper.utils.cli_helper import get_db
from sgx_scraper.utils.cli_helper import upsert_to_db
from sgx_scraper.utils.json_helper import open_json, write_json
from sgx_scraper.utils.constant import SGX_FILINGS_PATH_TODAY, OUTPUT_DIR_SHAREHOLDERS
from sgx_scraper.fetch_shareholders.tracking import get_shareholders_update
from sgx_scraper.fetch_shareholders.api import sync_with_db, get_screener_shareholders
from sgx_scraper.fetch_shareholders.utils.helper import (
    get_current_shareholders,
    remove_shareholder_dates,
)

import typer
import logging


app = typer.Typer(help="SGX shareholders tracking pipeline")


@app.command(name='track_shareholders')
def run_tracking_shareholders(
    is_push_db: Annotated[bool, typer.Option(help='Flag to upsert to db or not')] = True
):
    logger = logging.getLogger(__name__)

    existing_db_shareholders = get_current_shareholders(is_refresh=True)
    filings = open_json(SGX_FILINGS_PATH_TODAY)

    payload_updated = get_shareholders_update(
        filing_payload=filings,
        shareholders_db=existing_db_shareholders,
    )

    payload_updated = remove_shareholder_dates(payload_updated)

    if not payload_updated:
        logger.info("[TRACK SHAREHOLDERS] No payload generated, stopping.")
        return

    if is_push_db:
        upsert_to_db(
            payload=payload_updated,
            table_name='sgx_companies'
        )


@app.command(name='sync_screener_shareholders')
def run_sync_screener_shareholders(
    is_push_db: Annotated[bool, typer.Option(help='Flag to upsert to db or not')] = True
):
    logger = logging.getLogger(__name__)
    
    top_200_companies = get_db(
        table="sgx_company_report",
        columns="name,symbol,market_cap",
        query=lambda query: (
            query
            .not_.is_('market_cap', 'null')
            .order('market_cap', desc=True)
            .limit(200)
        ),
    )

    symbols = [
        record.get('symbol') 
        for record in top_200_companies
        if record.get('symbol')
    ]

    existing_db_shareholders = get_current_shareholders(is_refresh=True)
    screener_shareholders = get_screener_shareholders(symbols=symbols)

    payload_updated = sync_with_db(
        screener_shareholders_by_symbol=screener_shareholders,
        db_records=existing_db_shareholders
    )

    payload_updated = remove_shareholder_dates(payload_updated)

    if not payload_updated:
        logger.info("[SYNC SCREENER SHAREHOLDERS] No payload generated, stopping.")
        return

    OUTPUT_DIR_SHAREHOLDERS.mkdir(parents=True, exist_ok=True)
    output_filename = OUTPUT_DIR_SHAREHOLDERS / f"{datetime.today().strftime('%Y-%m-%d')}_sync_screener_shareholders.json"

    write_json(
        path=output_filename,
        payload=payload_updated,
    )

    if is_push_db:
        upsert_to_db(
            payload=payload_updated,
            table_name='sgx_companies'
        )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    app()
