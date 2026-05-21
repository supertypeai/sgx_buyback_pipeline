from dataclasses import asdict
from datetime import datetime, timedelta
from collections import Counter
from typing import Annotated
 
from sgx_scraper.utils.cli_helper import (
    normalize_datetime, 
    push_to_db, 
    upsert_to_db,
    clean_payload_sgx_buyback, 
    clean_payload_sgx_filings, 
    write_to_json, 
    remove_duplicate, 
    filter_top_n_companies, 
    write_to_csv, 
    standardize_name, 
    get_100_top_companies,
    open_json
)
from sgx_scraper.utils.constant import (
    SGX_BUYBACKS_PATH_YESTERDAY, 
    SGX_BUYBACKS_PATH_TODAY, 
    SGX_BUYBACKS_PATH_NOT_TOP_70,
    SGX_FILINGS_PATH_INSERTABLE, 
    SGX_FILINGS_PATH_NOT_INSERTABLE, 
    SGX_FILINGS_PATH_TODAY, 
    SGX_FILINGS_PATH_YESTERDAY, 
    SGX_FILINGS_PATH_NOT_TOP_70,
    SGX_FILINGS_PATH_TOP_100,
    OUTPUT_DIR_SHAREHOLDERS
)
from sgx_scraper.sgx_api.scraper_sgx_api import get_auth, run_scrape_api
from sgx_scraper.fetch_sgx_buyback.parser_sgx_buyback import get_sgx_buybacks
from sgx_scraper.fetch_sgx_filings.parser_sgx_filings import get_sgx_filings
from sgx_scraper.fetch_sgx_filings.news.builder import generate_news
from sgx_scraper.alerting.filter_data_alert import get_data_alert 
from sgx_scraper.alerting.mailer import send_sgx_filings_alert
from sgx_scraper.track_management.tracking import get_management_update
from sgx_scraper.fetch_shareholders.tracking import get_shareholders_update 
from sgx_scraper.fetch_shareholders.api import sync_with_db, get_screener_shareholders
from sgx_scraper.fetch_shareholders.utils.helper import get_current_shareholders

import typer 
import os 
import time 
import random 
import logging 
import sys 
import json 


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
    logging.getLogger('httpx').setLevel(logging.WARNING)


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
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    logger = logging.getLogger(__name__)

    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

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
       
        try:
            url = (
                f"{api_url}?periodstart={normalized_start}_160000"
                f"&periodend={normalized_end}_155959"
                f"&cat=ANNC&sub=ANNC13"
                f"&pagestart={page_start}"
                f"&pagesize={page_size}"
            )
            
            sgx_announcements = run_scrape_api(
                api_url=url, 
                flag_log='Buybacks', 
                headers=headers, 
                is_proxy=is_proxy
            ) 

            if not sgx_announcements:
                logger.info("No more announcements found, stopping pagination.")
                break

        except Exception as error:
            logger.error(f'[SGX BUYBACK] Fatal API error on page {page_start}: {error}', exc_info=True)
            raise

        for sgx_announcement in sgx_announcements:
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

        page_start += 1
        time.sleep(random.uniform(1, 8))

    logger.info(f"[SGX_BUYBACK] Scraping completed. Total records: {len(payload_sgx_buybacks)}")

    payload_sgx_buybacks_clean = clean_payload_sgx_buyback(payload_sgx_buybacks)
    
    payload_top_70, payload_not_top_70 = filter_top_n_companies(payload_sgx_buybacks_clean)
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
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
    is_send_news: bool = typer.Option(True, help='Flag to send to idx_news or not')
):
    logger = logging.getLogger(__name__)
    
    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

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
    
        try:
            url = (
                f"{api_url}?periodstart={normalized_start}_160000"
                f"&periodend={normalized_end}_155959"
                f"&cat=ANNC&sub=ANNC14"
                f"&pagestart={page_start}"
                f"&pagesize={page_size}"
            )
            
            sgx_announcements = run_scrape_api(
                api_url=url, 
                flag_log='Filings',
                headers=headers, 
                is_proxy=is_proxy
            )

            if not sgx_announcements:
                logger.info("No more announcements found, stopping pagination.")
                break
        
        except Exception as error:
            logger.error(f'[SGX FILINGS] Fatal API error on page {page_start}: {error}', exc_info=True)
            raise

        for sgx_announcement in sgx_announcements:
            detail_url = sgx_announcement.get('url', None)
            issuer_name = sgx_announcement.get("issuer_name")
           
            if not detail_url:
                logger.info(f'[SGX FILINGS] Skipping {issuer_name}, no detail url.')
                continue
            
            try: 
                sgx_filings_details = get_sgx_filings(detail_url)

                if not sgx_filings_details:
                    logger.info( f'[SGX FILINGS] Data not valid found for issuer name: {issuer_name} detail url: {detail_url}')
                    continue
                
                for sgx_filing_detail in sgx_filings_details:
                    sgx_filing_data = asdict(sgx_filing_detail)
                    payload_sgx_filings.append(sgx_filing_data)
            
            except Exception as error:
                logger.error(f'[SGX FILINGS] Failed parsing {issuer_name} - {detail_url}: {error}', exc_info=True)
                continue

            time.sleep(random.uniform(1, 3))

        page_start += 1
        time.sleep(random.uniform(1, 8))

    logger.info(f"[SGX FILINGS] Scraping completed. Total records: {len(payload_sgx_filings)}")

    filing_dates = Counter(
        record.get('time', '')
        for record in payload_sgx_filings
    )
        
    logger.info(f"Filing dates distribution: {dict(sorted(filing_dates.items()))}")

    payload_clean = clean_payload_sgx_filings(payload_sgx_filings)

    payload_top_70, payload_not_top_70 = filter_top_n_companies(payload_clean)
    payload_top_100, _ = filter_top_n_companies(payload_clean, top_n=100)

    write_to_csv(SGX_FILINGS_PATH_NOT_TOP_70, payload_not_top_70)
    write_to_json(SGX_FILINGS_PATH_TODAY, payload_top_70)
    write_to_json(SGX_FILINGS_PATH_TOP_100, payload_top_100)

    if os.path.exists(SGX_FILINGS_PATH_YESTERDAY):    
        logger.info('Processing remove duplicate data') 
        new_payload = remove_duplicate(SGX_FILINGS_PATH_TODAY, SGX_FILINGS_PATH_YESTERDAY)

    else:
        logger.info('First run detected, all Top 70 filings are new')
        new_payload = payload_top_70

    write_to_json(SGX_FILINGS_PATH_YESTERDAY, payload_top_70)

    standardized_payload = standardize_name(new_payload)

    payload_insertable, payload_not_insertable = get_data_alert(standardized_payload)

    if is_send_news:
        news_payload = generate_news(payload_insertable)
        push_to_db(news_payload, 'idx_news')

    write_to_json(SGX_FILINGS_PATH_NOT_INSERTABLE, payload_not_insertable)
    write_to_json(SGX_FILINGS_PATH_INSERTABLE, payload_insertable)

    send_sgx_filings_alert(payload_not_insertable, [str(SGX_FILINGS_PATH_NOT_INSERTABLE)])

    if is_push_db:
        push_to_db(payload_insertable, 'sgx_filings') 


@app.command(name='track_management')
def run_tracking_management(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(20, help="Number of records per page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    logger = logging.getLogger(__name__)

    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

    page_start = 0
    payload_management = []

    today = datetime.now()
    yesterday = today - timedelta(days=2)

    start_date_source = period_start if period_start is not None else yesterday
    end_date_source = period_end if period_end is not None else today

    normalized_start = normalize_datetime(start_date_source)
    normalized_end = normalize_datetime(end_date_source)

    logger.info(f"Start scraping from start date: {normalized_start} to {normalized_end}")

    top_100_companies = get_100_top_companies()

    while True:
        logger.info(f'page_start: {page_start}')
       
        try:
            url = (
                f"{api_url}?periodstart={normalized_start}_160000"
                f"&periodend={normalized_end}_155959"
                f"&cat=ANNC&sub=ANNC03%2CANNC04"
                f"&pagestart={page_start}"
                f"&pagesize={page_size}"
            )
            
            logger.info(f'url constructed: {url}')

            announcements = run_scrape_api(
                api_url=url, 
                flag_log='Management', 
                headers=headers, 
                is_proxy=is_proxy
            ) 

            if not announcements:
                logger.info("No more announcements found, stopping pagination.")
                break

        except Exception as error:
            logger.error(f'[Management] Fatal API error on page {page_start}: {error}', exc_info=True)
            raise
        
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

        page_start += 1
        time.sleep(random.uniform(1, 8))
    
    logger.info(f'total length of payload management: {len(payload_management)}')
    logger.info(f'payload management to upsert: {payload_management}')

    if is_push_db: 
        upsert_to_db(sgx_payload=payload_management, table_name='sgx_companies')


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
    symbols = [record.get('symbol') for record in top_100_companies]

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
    app()

