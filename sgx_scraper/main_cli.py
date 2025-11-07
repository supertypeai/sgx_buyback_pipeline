from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from sgx_scraper.config.settings import LOGGER
from sgx_scraper.utils.cli_helper import (
    normalize_datetime, push_to_db, 
    clean_payload_sgx_buyback, clean_payload_sgx_filings, 
    write_to_json, remove_duplicate, 
    filter_top_70_companies, write_to_csv
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
    pass


@app.command(name='scraper_buybacks')
def run_sgx_buyback_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(20, help="Number of records per page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not')
):
    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

    LOGGER.info(f"Scraping from {period_start} to {period_end}...")

    page_start = 0
    payload_sgx_buybacks = []

    today = datetime.now()
    yesterday = today - timedelta(days=2)

    start_date_source = period_start if period_start is not None else yesterday
    end_date_source = period_end if period_end is not None else today

    normalized_start = normalize_datetime(start_date_source)
    normalized_end = normalize_datetime(end_date_source)

    LOGGER.info(f"Start scraping from start date: {normalized_start} to {normalized_end}")

    while True:
        LOGGER.info(f'page_start: {page_start}')
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
                LOGGER.info("No more announcements found — stopping pagination.")
                headers = get_auth(proxy=None)
                sgx_announcements = run_scrape_api(api_url=url, flag_log='Buybacks', headers=headers)
                
            if sgx_announcements is None:
                LOGGER.info("No more announcements found on this page — stopping pagination.")
                break

            for sgx_announcement in sgx_announcements:
                detail_url = sgx_announcement.get('url', None)
                issuer_name = sgx_announcement.get("issuer_name")

                if not detail_url:
                    LOGGER.info(
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
            LOGGER.error(f'[SGX BUYBACK] Unexpected error on page {page_start}: {error}', exc_info=True)
            break 

    LOGGER.info(f"[SGX_BUYBACK] Scraping completed. Total records: {len(payload_sgx_buybacks)}")
    
    base_dir = Path("data/scraper_output/sgx_buyback")
    base_dir.mkdir(parents=True, exist_ok=True)

    path_today = base_dir / "sgx_buybacks_today.json"
    path_yesterday = base_dir / "sgx_buybacks_yesterday.json"
    path_data_not_top_70 = base_dir / "sgx_buybacks_not_top_70.csv"

    payload_sgx_buybacks_clean = clean_payload_sgx_buyback(payload_sgx_buybacks)
    
    payload_top_70, payload_not_top_70 = filter_top_70_companies(payload_sgx_buybacks_clean)
    write_to_csv(path_data_not_top_70, payload_not_top_70)

    write_to_json(path_today, payload_top_70)

    if os.path.exists(path_yesterday):   
        LOGGER.info('Processing remove duplicate data') 
        new_payload_sgx_buybacks = remove_duplicate(path_today, path_yesterday)
    else: 
        LOGGER.info('First run detected, all Top 70 filings are new')
        new_payload_sgx_buybacks = payload_top_70

    write_to_json(path_yesterday, payload_top_70)

    if is_push_db:
        push_to_db(new_payload_sgx_buybacks, 'sgx_buybacks')


@app.command(name='scraper_filings')
def run_sgx_filings_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(20, help="Number of records per page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not')
):
    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

    LOGGER.info(f"Scraping from {period_start} to {period_end}...")

    page_start = 0
    payload_sgx_filings = []

    today = datetime.now()
    yesterday = today - timedelta(days=2)

    start_date_source = period_start if period_start is not None else yesterday
    end_date_source = period_end if period_end is not None else today

    normalized_start = normalize_datetime(start_date_source)
    normalized_end = normalize_datetime(end_date_source)

    LOGGER.info(f"Start scraping from start date: {normalized_start} to {normalized_end}")

    while True:
        LOGGER.info(f'page_start: {page_start}')
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
                LOGGER.info("No more announcements found — stopping pagination.")
                headers = get_auth(proxy=None)
                sgx_announcements = run_scrape_api(api_url=url, flag_log='Filings', headers=headers)
                
            if sgx_announcements is None:
                LOGGER.info("No more announcements found on this page — stopping pagination.")
                break

            for sgx_announcement in sgx_announcements:
                detail_url = sgx_announcement.get('url', None)
                issuer_name = sgx_announcement.get("issuer_name")

                if not detail_url:
                    LOGGER.info(
                        f'[SGX FILINGS] Skipping extracting data in details for issuer name: {issuer_name}'
                    )
                    continue

                sgx_filings_details = get_sgx_filings(detail_url)

                if not sgx_filings_details:
                    LOGGER.info( f'[SGX FILINGS] Data not valid found for issuer name: {issuer_name} detail url: {detail_url}')
                    continue
                
                for sgx_filing_detail in sgx_filings_details:
                    sgx_filing_data = asdict(sgx_filing_detail)
                    payload_sgx_filings.append(sgx_filing_data)
                
                time.sleep(random.uniform(1, 6))

            page_start += 1
            time.sleep(random.uniform(1, 8))

        except Exception as error:
            LOGGER.error(f'[SGX FILINGS] Unexpected error on page {page_start}: {error}', exc_info=True)
            break 

    LOGGER.info(f"[SGX FILINGS] Scraping completed. Total records: {len(payload_sgx_filings)}")
    
    base_dir = Path("data/scraper_output/sgx_filing")
    base_dir.mkdir(parents=True, exist_ok=True)

    path_today = base_dir / "sgx_filings_today.json"
    path_yesterday = base_dir / "sgx_filings_yesterday.json"
    path_insertable = base_dir / "sgx_filings_insertable.json"
    path_not_insertable = base_dir / "sgx_filings_not_insertable.json"
    path_not_top_70 = base_dir / "sgx_filings_not_top_70.csv"

    payload_sgx_filings_clean = clean_payload_sgx_filings(payload_sgx_filings)

    payload_top_70, payload_not_top_70 = filter_top_70_companies(payload_sgx_filings_clean)
    write_to_csv(path_not_top_70, payload_not_top_70)

    write_to_json(path_today, payload_top_70)

    if os.path.exists(path_yesterday):    
        LOGGER.info('Processing remove duplicate data') 
        new_payload_sgx_filings = remove_duplicate(path_today, path_yesterday)
    else:
        LOGGER.info('First run detected, all Top 70 filings are new')
        new_payload_sgx_filings = payload_top_70

    write_to_json(path_yesterday, payload_top_70)

    sgx_filings_insertable, sgx_filings_not_insertable = get_data_alert(new_payload_sgx_filings)

    write_to_json(path_not_insertable, sgx_filings_not_insertable)
    write_to_json(path_insertable, sgx_filings_insertable)

    send_sgx_filings_alert(sgx_filings_not_insertable, [str(path_not_insertable)])

    if is_push_db:
        push_to_db(sgx_filings_insertable, 'sgx_filings') 


if __name__ == '__main__':
    app()

