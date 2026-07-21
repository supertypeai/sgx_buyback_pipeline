from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import upsert_to_db, get_100_top_companies
from sgx_scraper.track_management.tracking import get_management_update

import typer
import logging
import time
import random


app = typer.Typer(help="SGX management tracking pipeline")


@app.command(name='track_management')
def run_tracking_management(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    logger = logging.getLogger(__name__)

    payload_management = []

    top_100_companies = get_100_top_companies()

    announcements = iter_sgx_announcements(
        sub_category="ANNC03%2CANNC04",
        flag_log="Management",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    for announcement in announcements:
        try:
            updated_management_record = get_management_update(
                api_response=announcement,
                top_100_companies=top_100_companies
            )

            time.sleep(random.uniform(1, 3))

            if not updated_management_record:
                continue

            payload_management.extend(updated_management_record)

        except Exception as error:
            logger.error(f'[Management] Error processing announcement: {error}', exc_info=True)
            continue

    logger.info(f'total length of payload management: {len(payload_management)}')
    logger.info(f'payload management to upsert: {payload_management}')

    if is_push_db:
        upsert_to_db(sgx_payload=payload_management, table_name='sgx_companies')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s')
    
    app()
