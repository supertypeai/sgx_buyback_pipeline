from dataclasses import asdict

from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import push_to_db, remove_duplicate, filter_top_n_companies
from sgx_scraper.utils.json_helper import write_json, write_to_csv
from sgx_scraper.utils.constant import (
    SGX_BUYBACKS_PATH_TODAY,
    SGX_BUYBACKS_PATH_YESTERDAY,
    SGX_BUYBACKS_PATH_NOT_TOP_200,
)
from sgx_scraper.fetch_sgx_buyback.parser import get_sgx_buybacks
from sgx_scraper.fetch_sgx_buyback.utils.payload_helper import clean_payload_sgx_buyback

import typer
import logging
import time
import random


app = typer.Typer(help="SGX buyback scraper pipeline")


@app.command(name='scraper_buybacks')
def run_sgx_buyback_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    logger = logging.getLogger(__name__)

    payload_sgx_buybacks = []

    announcements = iter_sgx_announcements(
        sub_category="ANNC13",
        flag_log="Buybacks",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    for sgx_announcement in announcements:
        detail_url = sgx_announcement.get('url', None)
        issuer_name = sgx_announcement.get("issuer_name")

        if not detail_url:
            logger.info(f'[SGX BUYBACK] Skipping {issuer_name}, no detail url.')
            continue

        try:
            sgx_announcement_details = get_sgx_buybacks(detail_url)

            sgx_announcement_details = asdict(sgx_announcement_details)
            payload_sgx_buybacks.append(sgx_announcement_details)

        except Exception as error:
            logger.error(f'[SGX BUYBACK] Failed parsing {issuer_name} - {detail_url}: {error}', exc_info=True)
            continue

        time.sleep(random.uniform(1, 3))

    logger.info(f"[SGX_BUYBACK] Scraping completed. Total records: {len(payload_sgx_buybacks)}")

    payload_sgx_buybacks_clean = clean_payload_sgx_buyback(payload_sgx_buybacks)

    payload_top_200, payload_not_top_200 = filter_top_n_companies(
        payload_sgx_buybacks_clean,
        top_n=200
    )

    write_to_csv(SGX_BUYBACKS_PATH_NOT_TOP_200, payload_not_top_200)
    write_json(SGX_BUYBACKS_PATH_TODAY, payload_top_200)

    if SGX_BUYBACKS_PATH_YESTERDAY.exists():
        logger.info('Processing remove duplicate data')
        new_payload_sgx_buybacks = remove_duplicate(
            SGX_BUYBACKS_PATH_TODAY, 
            SGX_BUYBACKS_PATH_YESTERDAY
        )

    else:
        logger.info('First run detected, all Top 200 filings are new')
        new_payload_sgx_buybacks = payload_top_200

    write_json(SGX_BUYBACKS_PATH_YESTERDAY, payload_top_200)

    if is_push_db:
        push_to_db(
            new_payload_sgx_buybacks, 
            'sgx_buybacks'
        )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    app()
