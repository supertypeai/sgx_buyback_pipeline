from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import upsert_to_db, get_100_top_companies
from sgx_scraper.fetch_managements.management import (
    filter_announcements,
    get_management_payload,
)
from sgx_scraper.utils.json_helper import write_json
from sgx_scraper.fetch_managements.tracking import (
    consolidate_management_records,
    get_management_update,
)
from pathlib import Path 

import typer
import logging
import time
import random
import json 


LOGGER = logging.getLogger(__name__)

app = typer.Typer(help="SGX management tracking pipeline")


@app.command(name='track_management')
def run_tracking_management(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    payload_management = []

    # top_100_companies = get_100_top_companies()

    # announcements = list(iter_sgx_announcements(
    #     sub_category="ANNC03",
    #     flag_log="Management",
    #     period_start=period_start,
    #     period_end=period_end,
    #     page_size=page_size,
    #     is_proxy=is_proxy,
    # ))

    
    output_path = Path(
        "sgx_scraper/fetch_managements/data/cessations/"
        "historical.jsonl"
    )

    announcement_count = 0

    with output_path.open("w", encoding="utf-8") as output_file:
        for announcement in iter_sgx_announcements(
            sub_category="ANNC04",
            flag_log="Management",
            period_start=period_start,
            period_end=period_end,
            page_size=page_size,
            is_proxy=is_proxy,
        ):
            output_file.write(
                json.dumps(announcement, ensure_ascii=False)
            )
            output_file.write("\n")
            announcement_count += 1

    LOGGER.info(
        "Saved %d raw announcements to %s", 
        announcement_count, output_path
    )

    # filter_announcements(
    #     path=str(output_path),
    #     output_path=(
    #         "sgx_scraper/fetch_managements/data/appointments/"
    #         "lookup_historical.json"
    #     ),
    #     sort_oldest_first=True,
    # )
        
    # for announcement in announcements:
    #     try:
    #         updated_management_record = get_management_update(
    #             api_response=announcement,
    #             top_100_companies=top_100_companies
    #         )

    #         time.sleep(random.uniform(1, 3))

    #         if not updated_management_record:
    #             continue

    #         payload_management.extend(updated_management_record)

    #     except Exception as error:
    #         LOGGER.error(f'[Management] Error processing announcement: {error}', exc_info=True)
    #         continue

    # payload_management = consolidate_management_records(payload_management)

    # LOGGER.info(f'total unique management payloads to upsert: {len(payload_management)}')
    # LOGGER.info(f'payload management to upsert: {payload_management}')

    # if is_push_db:
    #     upsert_to_db(sgx_payload=payload_management, table_name='sgx_companies')


@app.command(name='scraper_managements')
def run_managements_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    is_push_db: bool = typer.Option(True, help='Flag to push to db or not'),
    is_proxy: bool = typer.Option(None, help='Flag to use proxy or not'),
):
    announcements = list(
        iter_sgx_announcements(
            sub_category="ANNC30",
            flag_log="Annual Reports",
            period_start=period_start,
            period_end=period_end,
            page_size=page_size,
            is_proxy=is_proxy,
        )
    )

    write_json(
        "sgx_scraper/fetch_managements/announcements.json", 
        announcements
    )

    # final_management_payload = []

    # for index, announcement in enumerate(
    #     announcements,
    #     start=1
    # ):
    #     annual_report_url = announcement.get("url")

    #     LOGGER.info(
    #         "Processing %d/%d | annual report url: %s",
    #         index,
    #         len(announcements),
    #         annual_report_url 
    #     )

        # management_payload = get_management_payload(
        #     annual_report_url=annual_report_url,
        #     company_name=announcement.get("issuer_name"),
        # )

        # final_management_payload.append(management_payload)
    
    

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )

    logging.getLogger('WDM').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire2').setLevel(logging.WARNING)
    logging.getLogger('mitmproxy').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    
    app()
