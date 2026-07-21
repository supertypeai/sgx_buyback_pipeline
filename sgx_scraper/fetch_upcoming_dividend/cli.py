from datetime import date, timedelta

from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import upsert_to_db
from sgx_scraper.utils.constant import UPCOMING_DIVIDEND
from sgx_scraper.utils.json_helper import write_json
from .parser import get_upcoming_dividend
from .utils.db_helper import delete_past_dividends

import typer
import logging
import time
import random


app = typer.Typer(help="Upcoming dividend scraper pipeline")


@app.command(name="upcoming_dividend")
def run_sgx_buyback_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    lookback_days: int = typer.Option(60, help="Broadcast lookback window in days (wide enough to re-fetch long-lead dividends)"),
    future_n_days: int = typer.Option(14, help="Only keep dividends with an ex-date within the next N days"),
    is_push_db: bool = typer.Option(True, help="Flag to push to db or not"),
    is_proxy: bool = typer.Option(None, help="Flag to use proxy or not"),
):
    logger = logging.getLogger(__name__)

    payload_upcoming_dividend = []

    today = date.today()
    start_date = today.isoformat()
    end_date = (today + timedelta(days=future_n_days)).isoformat()

    # Default the broadcast window to [today - lookback_days, today] so long-lead
    # dividends stay fetchable on the day their ex-date enters the 14-day window
    if period_start is None:
        period_start = (today - timedelta(days=lookback_days)).strftime("%Y%m%d")

    if period_end is None:
        period_end = today.strftime("%Y%m%d")

    announcements = iter_sgx_announcements(
        category="CACT",
        sub_category="CACT06",
        flag_log="Upcoming Dividend",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    for sgx_announcement in announcements:
        detail_url = sgx_announcement.get('url', None)
        issuer_name = sgx_announcement.get("issuer_name")

        if not detail_url:
            logger.info('[Upcoming Dividend] Skipping %s, no detail url.', issuer_name)
            continue

        try:
            payload = get_upcoming_dividend(detail_url)

            if not payload:
                logger.info('[Upcoming Dividend] Skipping %s, no data extracted.', detail_url)
                continue

            ex_date = payload.get("ex_date")

            if not ex_date or not (start_date <= ex_date <= end_date):
                logger.info(
                    '[Upcoming Dividend] Skipping %s, ex_date %s outside window %s to %s.',
                    issuer_name,
                    ex_date,
                    start_date,
                    end_date,
                )
                continue

            payload_upcoming_dividend.append(payload)

        except Exception as error:
            logger.error(
                '[Upcoming Dividend] Failed parsing %s - %s: %s',
                issuer_name,
                detail_url,
                error,
                exc_info=True,
            )
            continue

        time.sleep(random.uniform(1, 3))

    write_json(
        path=UPCOMING_DIVIDEND,
        payload=payload_upcoming_dividend
    )

    if is_push_db:
        upsert_to_db(
            sgx_payload=payload_upcoming_dividend,
            table_name="sgx_upcoming_dividend"
        )

        delete_past_dividends(
            table_name="sgx_upcoming_dividend",
            retention_days=14,
        )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    app()
