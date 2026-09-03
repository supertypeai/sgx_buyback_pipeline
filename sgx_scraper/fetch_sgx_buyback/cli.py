from dataclasses import asdict

from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import push_to_db, remove_duplicate, filter_top_n_companies
from sgx_scraper.utils.json_helper import write_json, write_to_csv
from sgx_scraper.utils.constant import (
    SGX_BUYBACKS_PATH_TODAY,
    SGX_BUYBACKS_PATH_YESTERDAY,
    SGX_BUYBACKS_PATH_NOT_TOP_200,
)
from sgx_scraper.fetch_sgx_buyback.parser import get_sgx_buybacks, detect_replacement_announcement
from sgx_scraper.fetch_sgx_buyback.utils.payload_helper import clean_payload_sgx_buyback
from sgx_scraper.config.settings import SUPABASE_CLIENT

import typer
import logging
import time
import random


app = typer.Typer(help="SGX buyback scraper pipeline")


@app.command(name="scraper_buybacks")
def run_sgx_buyback_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help="Flag to push to db or not"),
    is_proxy: bool = typer.Option(None, help="Flag to use proxy or not"),
):
    logger = logging.getLogger(__name__)

    payload_sgx_buybacks = []

    announcements = sorted(
        iter_sgx_announcements(
            sub_category="ANNC13",
            flag_log="Buybacks",
            period_start=period_start,
            period_end=period_end,
            page_size=page_size,
            is_proxy=is_proxy,
        ),
        key=lambda announcement: announcement["broadcast_date_time"],
        reverse=True,
    )

    seen_references = set()
    
    for announcement in announcements:
        detail_url = announcement.get("url", None)
        reference = announcement.get("ref_id")
        issuer_name = announcement.get("issuer_name")
        issuers = announcement.get("issuers")

        if not detail_url:
            logger.info(
                "[SGX BUYBACK] Skipping %s, no detail url."
            ),
            detail_url
            continue

        if reference in seen_references:
            continue

        seen_references.add(reference)
        
        try:
            sgx_announcement_details = get_sgx_buybacks(
                url=detail_url,
                reference=reference,
                issuer_name=issuer_name,
                issuers=issuers,
            )

            sgx_announcement_details = asdict(sgx_announcement_details)

            payload_sgx_buybacks.append(sgx_announcement_details)

        except Exception as error:
            logger.error(
                "[SGX BUYBACK] Failed parsing url: %s | error: %s", 
                detail_url,
                error,
                exc_info=True
            )
            continue

        time.sleep(random.uniform(1, 3))

    if not payload_sgx_buybacks:
        logger.info("No buyback payload generated, stopping.")
        return

    logger.info(
        "[SGX_BUYBACK] Total records before cleaning and filtering: %d",
        len(payload_sgx_buybacks)
    )

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
            SGX_BUYBACKS_PATH_YESTERDAY,
            key_name="url",
        )

    else:
        logger.info('First run detected, all Top 200 filings are new')
        new_payload_sgx_buybacks = payload_top_200

    write_json(SGX_BUYBACKS_PATH_YESTERDAY, payload_top_200)

    if not new_payload_sgx_buybacks:
        logger.info("No new buyback records to push, stopping.")
        return

    # detect old record tthat needs to be replaced 
    replacement_ids_to_delete = []

    for record in new_payload_sgx_buybacks:
        is_replacement = "repl" in (record.get("title") or "").lower()

        if is_replacement:
            replacement_ids_to_delete.extend(
                detect_replacement_announcement(record)
            )

    if is_push_db:
        is_pushed = push_to_db(
            new_payload_sgx_buybacks, 
            "sgx_buybacks",
            exclude_columns={"title"},
        )

        if not is_pushed:
            raise RuntimeError("Insert failed; skipping replacement deletion.")

        if replacement_ids_to_delete:
            delete_response = (
                SUPABASE_CLIENT
                .table("sgx_buybacks")
                .delete()
                .in_("id", replacement_ids_to_delete)
                .execute()
            )

            logger.info(
                "Deleted %d replaced buyback records",
                len(delete_response.data),
            )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    app()
