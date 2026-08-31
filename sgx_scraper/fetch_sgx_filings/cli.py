from itertools import islice

from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import push_to_db, remove_duplicate, filter_top_n_companies
from sgx_scraper.utils.json_helper import write_json
from sgx_scraper.utils.constant import (
    SGX_FILINGS_PATH_TODAY,
    SGX_FILINGS_PATH_YESTERDAY,
    SGX_FILINGS_PATH_INSERTABLE,
    SGX_FILINGS_PATH_NOT_INSERTABLE,
)
from sgx_scraper.fetch_sgx_filings.parser import get_sgx_filings
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import filter_duplicate
from sgx_scraper.news.builder import generate_news
from sgx_scraper.alerting.filter_data_alert import get_data_alert
from sgx_scraper.alerting.mailer import send_sgx_alert_email
from sgx_scraper.alerting.build_template import render_filing_email_content

import typer
import logging
import time
import random


LOGGER = logging.getLogger(__name__)


app = typer.Typer(help="SGX filings scraper pipeline")


def scrape_filings(
    period_start: str | None,
    period_end: str | None,
    page_size: int,
    is_proxy: bool | None,
    limit: int | None = None,
) -> list[dict]:
    payload = []

    announcements = iter_sgx_announcements(
        sub_category="ANNC14",
        flag_log="Filings",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    if limit:
        announcements = islice(announcements, limit)

    for index, sgx_announcement in enumerate(announcements, start=1):
        detail_url = sgx_announcement.get('url')
        issuer_name = sgx_announcement.get('issuer_name')

        LOGGER.info(f'Processing {index} | url: {detail_url}')

        if not detail_url:
            LOGGER.info(f'[SGX FILINGS] Skipping {issuer_name}, no detail url.')
            continue

        try:
            filings_details = get_sgx_filings(detail_url)

            if filings_details:
                payload.extend(filings_details)

        except Exception as error:
            LOGGER.error(f'[SGX FILINGS] Failed parsing {issuer_name}: {error}', exc_info=True)

        time.sleep(random.uniform(1, 3))

    LOGGER.info(f"[SGX FILINGS] Scraping completed. Total records: {len(payload)}")

    return payload


def resolve_new_records(top_200: list[dict]) -> list[dict]:
    if SGX_FILINGS_PATH_YESTERDAY.exists():
        LOGGER.info('Processing remove duplicate data')
        new_records = remove_duplicate(SGX_FILINGS_PATH_TODAY, SGX_FILINGS_PATH_YESTERDAY)

    else:
        LOGGER.info('First run detected, all Top 200 filings are new')
        new_records = top_200

    write_json(SGX_FILINGS_PATH_YESTERDAY, top_200)

    return new_records


def dispatch(
    insertable: list[dict],
    not_insertable: list[dict],
    is_send_news: bool,
    is_send_email: bool,
    is_push_db: bool,
) -> None:
    if is_send_news:
        news_payload = generate_news(insertable)
        push_to_db(news_payload, 'sgx_news')

    write_json(SGX_FILINGS_PATH_NOT_INSERTABLE, not_insertable)
    write_json(SGX_FILINGS_PATH_INSERTABLE, insertable)

    if is_send_email:
        subject, body_text, body_html = render_filing_email_content(
            alerts=not_insertable,
            title="SGX Non-Insertable Transaction Alerts",
        )

        send_sgx_alert_email(
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachments_path=[str(SGX_FILINGS_PATH_NOT_INSERTABLE)]
        )

    if is_push_db:
        exclude_columns = {
            "circumstances_desc", 
            "company_name"
        }
        
        push_to_db(
            insertable, 
            'sgx_filings',
            exclude_columns=exclude_columns
        )


@app.command(name='scraper_filings')
def run_sgx_filings_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    limit: int = typer.Option(None, help="Cap total announcements processed (for testing)"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
    is_send_email: bool = typer.Option(True, help="Sending flagged records to email"),
    is_send_news: bool = typer.Option(True, help='Flag to send to idx_news or not'),
):
    payload = scrape_filings(
        period_start, 
        period_end, 
        page_size, 
        is_proxy, 
        limit
    )

    payload_clean = filter_duplicate(payload)

    top_200, _ = filter_top_n_companies(payload_clean, top_n=200)

    write_json(SGX_FILINGS_PATH_TODAY, top_200)

    new_records = resolve_new_records(top_200)
    insertable, not_insertable = get_data_alert(new_records)

    dispatch(
        insertable, 
        not_insertable, 
        is_send_news, 
        is_send_email, 
        is_push_db
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    app()
