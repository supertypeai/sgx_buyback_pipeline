from dataclasses import asdict
from datetime import datetime, timedelta

from src.config.settings import LOGGER
from src.utils.cli_helper import normalize_datetime, push_to_db, clean_payload
from src.sgx_api.scraper_sgx_api import get_auth, run_scrape_api
from src.fetch_sgx_buyback.parser_sgx_buyback import get_sgx_announcements

import json 
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
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD_HHMMSS"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD_HHMMSS"),
    page_size: int = typer.Option(20, help="Number of records per page"),
    is_saved_json: bool = typer.Option(True, help='Flag to write to json or not'),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not')
):
    api_url = "https://api.sgx.com/announcements/v1.1/"
    headers = get_auth(proxy=None)

    LOGGER.info(f"Scraping from {period_start} to {period_end}...")

    page_start = 0
    payload_sgx_announcements = []

    today = datetime.now()
    yesterday = today - timedelta(days=1)

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
            
            sgx_announcements = run_scrape_api(api_url=url,headers=headers)
           
            if sgx_announcements is None:
                LOGGER.info("No more announcements found — stopping pagination.")
                headers = get_auth(proxy=None)
                sgx_announcements = run_scrape_api(api_url=url, headers=headers)
                
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

                sgx_announcement_details = get_sgx_announcements(detail_url)
                sgx_announcement_details = asdict(sgx_announcement_details)
                payload_sgx_announcements.append(sgx_announcement_details)
                time.sleep(random.uniform(1, 6))

            page_start += 1
            time.sleep(random.uniform(1, 8))

        except Exception as error:
            LOGGER.error(f'[SGX BUYBACK] Unexpected error on page {page_start}: {error}', exc_info=True)
            break 

    LOGGER.info(f"[SGX_BUYBACK] Scraping completed. Total records: {len(payload_sgx_announcements)}")
    
    payload_sgx_announcements = clean_payload(payload_sgx_announcements)
    
    if is_saved_json:
        os.makedirs('data/scraper_output', exist_ok=True)
        with open("data/scraper_output/sgx_buybacks.json", "w", encoding="utf-8") as file:
            json.dump(payload_sgx_announcements, file, ensure_ascii=False, indent=2)

        LOGGER.info("Saved all announcements to data/scraper_output/sgx_buybacks.json")

    if is_push_db:
        push_to_db(payload_sgx_announcements)


if __name__ == '__main__':
    app()

