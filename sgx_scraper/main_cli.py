from dataclasses import asdict
from datetime import datetime, timedelta

from sgx_scraper.utils.cli_helper import (
    normalize_datetime, push_to_db, 
    clean_payload_sgx_buyback, clean_payload_sgx_filings, 
    write_to_json, remove_duplicate, 
    filter_top_70_companies, write_to_csv
)
from sgx_scraper.utils.constant import (
    SGX_BUYBACKS_PATH_YESTERDAY, SGX_BUYBACKS_PATH_TODAY, SGX_BUYBACKS_PATH_NOT_TOP_70,
    SGX_FILINGS_PATH_INSERTABLE, SGX_FILINGS_PATH_NOT_INSERTABLE, 
    SGX_FILINGS_PATH_TODAY, SGX_FILINGS_PATH_YESTERDAY, SGX_FILINGS_PATH_NOT_TOP_70
)
from sgx_scraper.sgx_api.scraper_sgx_api import get_auth, run_scrape_api
from sgx_scraper.fetch_sgx_buyback.parser_sgx_buyback import get_sgx_buybacks
from sgx_scraper.fetch_sgx_filings.parser_sgx_filings import get_sgx_filings
from sgx_scraper.alerting.filter_data_alert import get_data_alert 
from sgx_scraper.alerting.mailer import send_sgx_filings_alert

import typer 
import os 
import time 
import random 
import logging 
import sys 


def setup_logging():
    """Configures logging for the whole application"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("scraper.log") 
        ]
    )

    # suppress noisy third-party loggers
    logging.getLogger('WDM').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire2').setLevel(logging.WARNING)
    logging.getLogger('mitmproxy').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


app = typer.Typer(
    help='A CLI for managing scraper sgx buybacks and filings',
    no_args_is_help=True
)


@app.callback()
def main():
    """
    SGX Scraper CLI.
    
    This callback function treats this as a multi-command app
    """
    setup_logging()


@app.command(name='scraper_buybacks')
def run_sgx_buyback_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(20, help="Number of records per page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not')
):
    logger = logging.getLogger(__name__)

    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

    logger.info(f"Scraping from {period_start} to {period_end}...")

    page_start = 0
    payload_sgx_buybacks = []

    today = datetime.now()
    yesterday = today - timedelta(days=2)

    start_date_source = period_start if period_start is not None else yesterday
    end_date_source = period_end if period_end is not None else today

    normalized_start = normalize_datetime(start_date_source)
    normalized_end = normalize_datetime(end_date_source)

    logger.info(f"Start scraping from start date: {normalized_start} to {normalized_end}")

    while True:
        logger.info(f'page_start: {page_start}')
        typer.echo(f'page start: {page_start}')
        try:
            url = (
                f"{api_url}?periodstart={normalized_start}_160000"
                f"&periodend={normalized_end}_155959"
                f"&cat=ANNC&sub=ANNC13"
                f"&pagestart={page_start}"
                f"&pagesize={page_size}"
            )
            
            sgx_announcements = run_scrape_api(api_url=url, flag_log='Buybacks', headers=headers)
           
            if sgx_announcements is None:
                logger.info("No more announcements found — stopping pagination.")
                headers = get_auth(proxy=None)
                sgx_announcements = run_scrape_api(api_url=url, flag_log='Buybacks', headers=headers)
                
            if sgx_announcements is None:
                logger.info("No more announcements found on this page — stopping pagination.")
                break

            for sgx_announcement in sgx_announcements:
                detail_url = sgx_announcement.get('url', None)
                issuer_name = sgx_announcement.get("issuer_name")

                if not detail_url:
                    logger.info(
                        f'[SGX BUYBACK] Skipping extracting data in details for issuer name: {issuer_name}'
                    )
                    continue

                sgx_announcement_details = get_sgx_buybacks(detail_url)
                sgx_announcement_details = asdict(sgx_announcement_details)
                payload_sgx_buybacks.append(sgx_announcement_details)
                time.sleep(random.uniform(1, 6))

            page_start += 1
            time.sleep(random.uniform(1, 8))

        except Exception as error:
            logger.error(f'[SGX BUYBACK] Unexpected error on page {page_start}: {error}', exc_info=True)
            break 

    logger.info(f"[SGX_BUYBACK] Scraping completed. Total records: {len(payload_sgx_buybacks)}")

    payload_sgx_buybacks_clean = clean_payload_sgx_buyback(payload_sgx_buybacks)
    
    payload_top_70, payload_not_top_70 = filter_top_70_companies(payload_sgx_buybacks_clean)
    write_to_csv(SGX_BUYBACKS_PATH_NOT_TOP_70, payload_not_top_70)

    write_to_json(SGX_BUYBACKS_PATH_TODAY, payload_top_70)

    if os.path.exists(SGX_BUYBACKS_PATH_YESTERDAY):   
        logger.info('Processing remove duplicate data') 
        new_payload_sgx_buybacks = remove_duplicate(SGX_BUYBACKS_PATH_TODAY, SGX_BUYBACKS_PATH_YESTERDAY)
    else: 
        logger.info('First run detected, all Top 70 filings are new')
        new_payload_sgx_buybacks = payload_top_70

    write_to_json(SGX_BUYBACKS_PATH_YESTERDAY, payload_top_70)

    if is_push_db:
        push_to_db(new_payload_sgx_buybacks, 'sgx_buybacks')


@app.command(name='scraper_filings')
def run_sgx_filings_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(20, help="Number of records per page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not')
):
    logger = logging.getLogger(__name__)
    
    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

    logger.info(f"Scraping from {period_start} to {period_end}...")

    page_start = 0
    payload_sgx_filings = []

    today = datetime.now()
    yesterday = today - timedelta(days=2)

    start_date_source = period_start if period_start is not None else yesterday
    end_date_source = period_end if period_end is not None else today

    normalized_start = normalize_datetime(start_date_source)
    normalized_end = normalize_datetime(end_date_source)

    logger.info(f"Start scraping from start date: {normalized_start} to {normalized_end}")

    while True:
        logger.info(f'page_start: {page_start}')
        typer.echo(f'page start: {page_start}')
        try:
            url = (
                f"{api_url}?periodstart={normalized_start}_160000"
                f"&periodend={normalized_end}_155959"
                f"&cat=ANNC&sub=ANNC14"
                f"&pagestart={page_start}"
                f"&pagesize={page_size}"
            )
            
            sgx_announcements = run_scrape_api(api_url=url, flag_log='Filings', headers=headers)
           
            if sgx_announcements is None:
                logger.info("No more announcements found — stopping pagination.")
                headers = get_auth(proxy=None)
                sgx_announcements = run_scrape_api(api_url=url, flag_log='Filings', headers=headers)
                
            if sgx_announcements is None:
                logger.info("No more announcements found on this page — stopping pagination.")
                break

            for sgx_announcement in sgx_announcements:
                detail_url = sgx_announcement.get('url', None)
                issuer_name = sgx_announcement.get("issuer_name")

                if not detail_url:
                    logger.info(
                        f'[SGX FILINGS] Skipping extracting data in details for issuer name: {issuer_name}'
                    )
                    continue

                sgx_filings_details = get_sgx_filings(detail_url)

                if not sgx_filings_details:
                    logger.info( f'[SGX FILINGS] Data not valid found for issuer name: {issuer_name} detail url: {detail_url}')
                    continue
                
                for sgx_filing_detail in sgx_filings_details:
                    sgx_filing_data = asdict(sgx_filing_detail)
                    payload_sgx_filings.append(sgx_filing_data)
                
                time.sleep(random.uniform(1, 6))

            page_start += 1
            time.sleep(random.uniform(1, 8))

        except Exception as error:
            logger.error(f'[SGX FILINGS] Unexpected error on page {page_start}: {error}', exc_info=True)
            break 

    logger.info(f"[SGX FILINGS] Scraping completed. Total records: {len(payload_sgx_filings)}")

    payload_sgx_filings_clean = clean_payload_sgx_filings(payload_sgx_filings)

    payload_top_70, payload_not_top_70 = filter_top_70_companies(payload_sgx_filings_clean)
    write_to_csv(SGX_FILINGS_PATH_NOT_TOP_70, payload_not_top_70)

    write_to_json(SGX_FILINGS_PATH_TODAY, payload_top_70)

    if os.path.exists(SGX_FILINGS_PATH_YESTERDAY):    
        logger.info('Processing remove duplicate data') 
        new_payload_sgx_filings = remove_duplicate(SGX_FILINGS_PATH_TODAY, SGX_FILINGS_PATH_YESTERDAY)
    else:
        logger.info('First run detected, all Top 70 filings are new')
        new_payload_sgx_filings = payload_top_70

    write_to_json(SGX_FILINGS_PATH_YESTERDAY, payload_top_70)

    sgx_filings_insertable, sgx_filings_not_insertable = get_data_alert(new_payload_sgx_filings)

    write_to_json(SGX_FILINGS_PATH_NOT_INSERTABLE, sgx_filings_not_insertable)
    write_to_json(SGX_FILINGS_PATH_INSERTABLE, sgx_filings_insertable)

    send_sgx_filings_alert(sgx_filings_not_insertable, [str(SGX_FILINGS_PATH_NOT_INSERTABLE)])

    if is_push_db:
        push_to_db(sgx_filings_insertable, 'sgx_filings') 


if __name__ == '__main__':
    app()

