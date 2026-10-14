from sgx_scraper.sgx_api.announcements import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import push_to_db, remove_duplicate, get_db
from sgx_scraper.utils.json_helper import write_json
from sgx_scraper.utils.constant import (
    SGX_FILINGS_PATH_TODAY,
    SGX_FILINGS_PATH_YESTERDAY,
    SGX_FILINGS_PATH_INSERTABLE,
)
from sgx_scraper.fetch_sgx_filings.parser import (
    get_sgx_filings,
    get_symbols_top_200,
    is_top_200_announcement,
    get_announcement_symbol,
)
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    filter_duplicate,
    keep_table_columns,
)
from sgx_scraper.news.builder import generate_news

from itertools import islice

import typer
import logging
import time
import random
import json
import re


LOGGER = logging.getLogger(__name__)

FILINGS_TABLE = "sgx_filings"
NEWS_TABLE = "sgx_news"
FILINGS_MODELS = ["gpt-6-luna"]

ANNOUNCEMENT_KEY_PATTERN = re.compile(r"corporate-announcements/([A-Z0-9]{16})")
STORED_LOOKUP_CHUNK = 40


app = typer.Typer(help="SGX filings scraper pipeline")


def get_announcement_key(url: str | None) -> str | None:
    match = ANNOUNCEMENT_KEY_PATTERN.search(url or "")

    return match.group(1) if match else None


def get_stored_announcement_keys(announcement_keys: list[str]) -> set[str]:
    stored_keys = set()

    for start in range(0, len(announcement_keys), STORED_LOOKUP_CHUNK):
        chunk = announcement_keys[start:start + STORED_LOOKUP_CHUNK]

        records = get_db(
            table=FILINGS_TABLE,
            columns="source",
            query=lambda query: query.or_(
                ",".join(f"source.ilike.*{key}*" for key in chunk)
            ),
        )

        stored_keys.update(
            get_announcement_key(record.get("source"))
            for record in records
        )

    return stored_keys - {None}


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

    symbols_top_200 = get_symbols_top_200()

    announcements = [
        announcement
        for announcement in announcements
        if announcement.get('url')
        and is_top_200_announcement(announcement, symbols_top_200)
    ]

    stored_keys = get_stored_announcement_keys(
        [
            get_announcement_key(announcement['url'])
            for announcement in announcements
            if get_announcement_key(announcement['url'])
        ]
    )

    LOGGER.info(
        "[SGX FILINGS] %d top 200 announcements, %d already stored",
        len(announcements),
        sum(
            get_announcement_key(announcement["url"]) in stored_keys
            for announcement in announcements
        ),
    )

    for index, sgx_announcement in enumerate(announcements, start=1):
        detail_url = sgx_announcement.get("url")
        issuer_name = sgx_announcement.get("issuer_name")

        if get_announcement_key(detail_url) in stored_keys:
            LOGGER.info(f"[SGX FILINGS] Skipping {issuer_name}, already stored: {detail_url}")
            continue

        LOGGER.info(f"Processing {index} | url: {detail_url}")

        try:
            filings_details = get_sgx_filings(
                detail_url,
                models=FILINGS_MODELS,
                fallback_symbol=get_announcement_symbol(
                    sgx_announcement, symbols_top_200
                ),
            )

            if filings_details:
                payload.extend(filings_details)

        except Exception as error:
            LOGGER.error(f"[SGX FILINGS] Failed parsing {issuer_name}: {error}", exc_info=True)

        time.sleep(random.uniform(1, 3))

    LOGGER.info(f"[SGX FILINGS] Scraping completed. Total records: {len(payload)}")

    return payload


def resolve_new_records(records: list[dict]) -> list[dict]:
    write_json(SGX_FILINGS_PATH_TODAY, records)

    if SGX_FILINGS_PATH_YESTERDAY.exists():
        LOGGER.info("Processing remove duplicate data")
        new_records = remove_duplicate(SGX_FILINGS_PATH_TODAY, SGX_FILINGS_PATH_YESTERDAY)

    else:
        LOGGER.info("First run detected, all filings are new")
        new_records = records

    write_json(SGX_FILINGS_PATH_YESTERDAY, records)

    return new_records


def build_news_records(records: list[dict]) -> list[dict]:
    # Every holder of a joint filing reports the same transaction, so news is
    # written once per transaction with the other holders' records attached
    groups = {}

    for record in records:
        group_key = (
            record.get('source'),
            record.get('transaction_reference')
            or json.dumps(
                [record.get('timestamp'), record.get('price_per_share')],
                sort_keys=True,
            ),
        )
        groups.setdefault(group_key, []).append(record)

    news_records = []

    for group in groups.values():
        direct_holders = [
            record
            for record in group
            if record['interest_breakdown']['before']['direct']
            != record['interest_breakdown']['after']['direct']
        ]
        main_record = direct_holders[0] if len(direct_holders) == 1 else group[0]

        news_records.append({
            **main_record,
            'related_records': [
                record for record in group if record is not main_record
            ],
        })

    return news_records


def dispatch(
    records: list[dict],
    is_send_news: bool,
    is_push_db: bool,
) -> None:
    write_json(SGX_FILINGS_PATH_INSERTABLE, records)

    if is_push_db:
        push_to_db(
            [keep_table_columns(record) for record in records],
            FILINGS_TABLE,
        )

    if is_send_news:
        news_payload = generate_news(
            payload=build_news_records(records),
            generate_type="filing",
            models=FILINGS_MODELS,
        )
        push_to_db(news_payload, NEWS_TABLE)


@app.command(name="scraper_filings")
def run_sgx_filings_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    limit: int = typer.Option(None, help="Cap total announcements processed (for testing)"),
    is_push_db: bool = typer.Option(True, help="Flag to push to db or not"),
    is_proxy: bool = typer.Option(None, help="Flag to use proxy or not"),
    is_send_news: bool = typer.Option(True, help="Flag to send to sgx_news or not"),
):
    payload = scrape_filings(
        period_start,
        period_end,
        page_size,
        is_proxy,
        limit
    )

    payload_clean = filter_duplicate(payload)
    new_records = resolve_new_records(payload_clean)

    dispatch(
        new_records,
        is_send_news,
        is_push_db
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    app()
