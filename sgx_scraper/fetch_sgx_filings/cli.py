from dataclasses import asdict

from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import push_to_db, remove_duplicate, filter_top_n_companies
from sgx_scraper.utils.json_helper import write_json, write_to_csv
from sgx_scraper.utils.constant import (
    SGX_FILINGS_PATH_TODAY,
    SGX_FILINGS_PATH_YESTERDAY,
    SGX_FILINGS_PATH_NOT_TOP_200,
    SGX_FILINGS_PATH_TOP_100,
    SGX_FILINGS_PATH_INSERTABLE,
    SGX_FILINGS_PATH_NOT_INSERTABLE,
)
from sgx_scraper.fetch_sgx_filings.parser import get_sgx_filings
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    clean_payload_sgx_filings,
    standardize_name,
)
from sgx_scraper.fetch_sgx_filings.news.builder import generate_news
from sgx_scraper.alerting.filter_data_alert import get_data_alert
from sgx_scraper.alerting.mailer import send_sgx_filings_alert

import typer
import logging
import time
import random


app = typer.Typer(help="SGX filings scraper pipeline")


@app.command(name='scraper_filings')
def run_sgx_filings_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
    is_send_news: bool = typer.Option(True, help='Flag to send to idx_news or not')
):
    logger = logging.getLogger(__name__)

    payload_sgx_filings = []

    announcements = iter_sgx_announcements(
        sub_category="ANNC14",
        flag_log="Filings",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    for sgx_announcement in announcements:
        detail_url = sgx_announcement.get('url', None)
        issuer_name = sgx_announcement.get("issuer_name")

        logger.info(f'Processing url: {detail_url}')

        if not detail_url:
            logger.info(f'[SGX FILINGS] Skipping {issuer_name}, no detail url.')
            continue

        try:
            sgx_filings_details = get_sgx_filings(detail_url)

            if not sgx_filings_details:
                logger.info(f'[SGX FILINGS] Data not valid found for issuer name: {issuer_name} detail url: {detail_url}')
                continue

            for sgx_filing_detail in sgx_filings_details:
                sgx_filing_data = asdict(sgx_filing_detail)
                payload_sgx_filings.append(sgx_filing_data)

        except Exception as error:
            logger.error(f'[SGX FILINGS] Failed parsing {issuer_name} - {detail_url}: {error}', exc_info=True)
            continue

        time.sleep(random.uniform(1, 3))

    logger.info(f"[SGX FILINGS] Scraping completed. Total records: {len(payload_sgx_filings)}")

    payload_clean = clean_payload_sgx_filings(payload_sgx_filings)

    payload_top_200, payload_not_top_200 = filter_top_n_companies(
        payload_clean, top_n=200
    )
    payload_top_100, _ = filter_top_n_companies(payload_clean, top_n=100)

    write_to_csv(SGX_FILINGS_PATH_NOT_TOP_200, payload_not_top_200)
    write_json(SGX_FILINGS_PATH_TODAY, payload_top_200)
    write_json(SGX_FILINGS_PATH_TOP_100, payload_top_100)

    if SGX_FILINGS_PATH_YESTERDAY.exists():
        logger.info('Processing remove duplicate data')
        new_payload = remove_duplicate(SGX_FILINGS_PATH_TODAY, SGX_FILINGS_PATH_YESTERDAY)

    else:
        logger.info('First run detected, all Top 200 filings are new')
        new_payload = payload_top_200

    write_json(SGX_FILINGS_PATH_YESTERDAY, payload_top_200)

    standardized_payload = standardize_name(new_payload)

    payload_insertable, payload_not_insertable = get_data_alert(standardized_payload)

    if is_send_news:
        news_payload = generate_news(payload_insertable)
        push_to_db(news_payload, 'sgx_news')

    write_json(SGX_FILINGS_PATH_NOT_INSERTABLE, payload_not_insertable)
    write_json(SGX_FILINGS_PATH_INSERTABLE, payload_insertable)

    send_sgx_filings_alert(payload_not_insertable, [str(SGX_FILINGS_PATH_NOT_INSERTABLE)])

    if is_push_db:
        push_to_db(payload_insertable, 'sgx_filings')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )
    
    app()
