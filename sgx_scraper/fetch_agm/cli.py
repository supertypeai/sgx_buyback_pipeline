from dataclasses import asdict

from sgx_scraper.fetch_agm.constant import (
    AGM_PATH_SEEN,
    AGM_PATH_TODAY,
    ON_CONFLICT,
    SIAS_DEFAULT_PAGES,
    SUB_CATEGORIES,
    TABLE_NAME,
)
from sgx_scraper.fetch_agm.models import AgmMeeting
from sgx_scraper.fetch_agm.parser import (
    extract_detail_fields,
    resolve_results_document,
    summarise_results,
)
from sgx_scraper.fetch_agm.utils.payload_helper import (
    resolve_place_desc,
    split_meeting_datetime,
)
from sgx_scraper.fetch_agm.utils.sias_helper import build_sias_index, build_qa, find_sias_entry
from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import upsert_to_db
from sgx_scraper.utils.symbol_matching_helper import add_sgx_suffix
from sgx_scraper.utils.date_helper import to_iso_date
from sgx_scraper.utils.json_helper import open_json, write_json

import logging
import random
import time
import typer


app = typer.Typer(help="SGX AGM and EGM meeting scraper pipeline")

LOGGER = logging.getLogger(__name__)

DB_EXCLUDED_COLUMNS = {"ref_id"}


def load_seen_refs() -> set[str]:
    if not AGM_PATH_SEEN.exists():
        return set()

    return set(open_json(AGM_PATH_SEEN) or [])


def deduplicate(payload: list[dict]) -> list[dict]:
    """The feed repeats a filing, and postgres rejects a batch that conflicts
    with itself, so the richer row wins."""
    best = {}

    for record in payload:
        key = (record["symbol"], record["agm_date"], record["meeting_type"])
        current = best.get(key)

        if current is None or (record["summary"] and not current["summary"]):
            best[key] = record

    return list(best.values())


def resolve_symbol(announcement: dict) -> str | None:
    issuers = announcement.get("issuers") or []

    if not issuers:
        return None

    return add_sgx_suffix(issuers[0].get("stock_code"))


def build_record(
    announcement: dict,
    symbol: str,
    meeting_type: str,
    fields: dict,
    summary: str | None,
    tags: list[str] | None,
    sias_entry: dict,
    qa: list[dict],
) -> AgmMeeting:
    agm_date, agm_time = split_meeting_datetime(fields.get("Meeting Date and Time"))
    venue = fields.get("Meeting Venue")

    return AgmMeeting(
        symbol=symbol,
        # submission_date is when the announcement thread opened.
        recording_date=to_iso_date(announcement.get("submission_date")),
        agm_date=agm_date,
        meeting_type=meeting_type,
        agm_time=agm_time,
        agm_place=venue,
        agm_place_desc=resolve_place_desc(venue),
        summary=summary,
        tags=tags,
        sias_questions_pdf=sias_entry.get("questions_pdf"),
        sias_response_pdf=sias_entry.get("response_pdf"),
        qa=qa or None,
        source_link=announcement.get("url"),
        ref_id=announcement.get("ref_id"),
    )


@app.command(name="scraper_agm")
def run_agm_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    model_name: str = typer.Option("laguna-s-2.1", help="Model key in MODEL_CONFIG"),
    limit: int = typer.Option(None, help="Stop after this many meeting filings"),
    sias_pages: int = typer.Option(SIAS_DEFAULT_PAGES, help="SIAS listing pages to index"),
    ignore_seen: bool = typer.Option(False, help="Reprocess filings already in the seen list"),
    is_push_db: bool = typer.Option(True, help="Flag to push to db or not"),
    is_proxy: bool = typer.Option(None, help="Flag to use proxy or not"),
):
    seen_refs = load_seen_refs()
    sias_index = build_sias_index(sias_pages) if sias_pages else {}
    processed = 0
    payload = []

    for sub_category, meeting_type in SUB_CATEGORIES.items():
        announcements = iter_sgx_announcements(
            sub_category=sub_category,
            flag_log=f"AGM {meeting_type}",
            period_start=period_start,
            period_end=period_end,
            page_size=page_size,
            is_proxy=is_proxy,
        )

        for announcement in announcements:
            symbol = resolve_symbol(announcement)
            ref_id = announcement.get("ref_id")
            detail_url = announcement.get("url")

            if not symbol or not detail_url:
                continue

            if ref_id in seen_refs and not ignore_seen:
                LOGGER.info(f"[AGM] Already processed {ref_id}, skipping")
                continue

            try:
                fields = extract_detail_fields(detail_url)

            except Exception as error:
                LOGGER.error(
                    f"[AGM] Failed reading detail page {symbol} - {detail_url}: {error}",
                    exc_info=True,
                )
                continue

            agm_date, _ = split_meeting_datetime(fields.get("Meeting Date and Time"))

            if not agm_date:
                LOGGER.warning(f"[AGM] No meeting date on {ref_id}, skipping")
                continue

            results_url = resolve_results_document(detail_url)

            summary, tags = (
                summarise_results(results_url, model_name)
                if results_url else (None, None)
            )

            sias_entry = find_sias_entry(
                index=sias_index,
                issuer_name=announcement.get("issuer_name"),
                security_name=announcement.get("security_name"),
                meeting_date=agm_date,
                meeting_type=meeting_type,
            )

            qa = build_qa(sias_entry, model_name) if sias_entry else []

            payload.append(
                asdict(build_record(
                    announcement, symbol, meeting_type, fields,
                    summary, tags, sias_entry, qa,
                ))
            )

            # Left unseen until the results are filed.
            if summary:
                seen_refs.add(ref_id)

            processed += 1

            if limit and processed >= limit:
                LOGGER.info(f"[AGM] Reached limit of {limit} filings")
                break

            time.sleep(random.uniform(1, 3))

        if limit and processed >= limit:
            break

    payload = deduplicate(payload)

    LOGGER.info(f"[AGM] Scraping completed. Total records: {len(payload)}")

    write_json(AGM_PATH_TODAY, payload)
    write_json(AGM_PATH_SEEN, sorted(seen_refs))

    if not is_push_db:
        LOGGER.info(f"[AGM] Dry run, {len(payload)} records written to file only")
        return

    upsert_to_db(
        payload=payload,
        table_name=TABLE_NAME,
        on_conflict=ON_CONFLICT,
        exclude_columns=DB_EXCLUDED_COLUMNS,
    )
