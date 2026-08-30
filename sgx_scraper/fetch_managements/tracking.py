from rapidfuzz import fuzz

from sgx_scraper.fetch_managements.appointment import get_appointment 
from sgx_scraper.fetch_managements.cessation import get_cessation

import logging 
from copy import deepcopy


LOGGER = logging.getLogger(__name__)


def consolidate_management_records(records: list[dict]) -> list[dict]:
    records_by_symbol = {}

    for record in records:
        symbol = record.get('symbol')

        if not symbol:
            LOGGER.warning(
                '[management] skipping update without a symbol: %s', 
                record
            )
            continue

        records_by_symbol[symbol] = record

    return list(records_by_symbol.values())


def get_announcement_id(url: str) -> str | None:
    url_parts = url.rstrip("/").split("/")

    try:
        announcement_index = url_parts.index("corporate-announcements")
    except ValueError:
        return None

    announcement_id_index = announcement_index + 1

    if announcement_id_index >= len(url_parts):
        return None

    return url_parts[announcement_id_index]


def get_annual_report_submission_time(
    source_url: str,
    annual_report_announcements: list[dict],
) -> int | None:
    source_announcement_id = get_announcement_id(source_url)

    if source_announcement_id is None:
        return None

    for announcement in annual_report_announcements:
        if get_announcement_id(announcement.get("url", "")) != source_announcement_id:
            continue

        return announcement.get("submission_date_time")

    return None


def get_success_management_lookup(
    successful_results: list[dict],
) -> dict[str, list[dict]]:
    management_by_symbol = {}

    for result in successful_results:
        for symbol, result_data in result.items():
            management_payload = result_data.get("payload")

            if not management_payload:
                continue

            management_by_symbol[symbol] = deepcopy(management_payload)

    return management_by_symbol


def get_management_update(
    api_response: dict, 
    management_by_symbol: dict[str, list[dict]],
) -> dict | None:
    registry = {
        'announcement of appointment': get_appointment, 
        'announcement of cessation': get_cessation
    }

    category = api_response.get('category_name', '').strip().lower()
    handler = registry.get(category)

    if not handler:
        LOGGER.warning('[management] unknown category: %s', category)
        return None
    
    announcement = handler(api_response=api_response)

    if announcement is None:
        LOGGER.warning(
            '[management] handler returned None for category: %s url: %s',
            category,
            api_response.get('url'),
        )
        return None

    symbol = announcement.get('symbol')
    management = management_by_symbol.get(symbol)

    if management is None:
        LOGGER.info(
            '[management] announcement %s for %s has no annual-report baseline',
            category,
            symbol,
        )
        return None  

    if category == 'announcement of appointment':
        management.append({
            'name': announcement.get('name'),
            'position': announcement.get('position'),
            'age': announcement.get('age'),
            'start_date': announcement.get('start_date'),
        })
        
        LOGGER.info(
            '[appointment] added %s as %s for %s',
            announcement.get('name'),
            announcement.get('position'),
            symbol,
        )

    elif category == 'announcement of cessation':
        matched = False

        for record in management:
            score = fuzz.token_sort_ratio(
                record.get('name', ''), 
                announcement.get('name', '')
            )

            if score >= 88:
                record['end_date'] = announcement.get('end_date')
                LOGGER.info(
                    '[cessation] matched %s -> %s (score: %s) for %s',
                    announcement.get('name'),
                    record.get('name'),
                    score,
                    symbol,
                )
                matched = True
                break

        if not matched:
            LOGGER.warning(
                '[cessation] no match found for %s in %s management',
                announcement.get('name'),
                symbol,
            )

    return {
        'symbol': symbol, 
        'management': management,
    }


def get_management_updates(
    successful_results: list[dict],
    annual_report_lookup: dict[str, list[dict]],
    announcement_lookup: dict[str, list[dict]],
) -> list[dict]:
    management_by_symbol = get_success_management_lookup(successful_results)
    annual_report_submission_times = {}

    for result in successful_results:
        for symbol, result_data in result.items():
            if symbol not in management_by_symbol:
                continue

            annual_report_submission_time = get_annual_report_submission_time(
                source_url=result_data.get('source', ''),
                annual_report_announcements=annual_report_lookup.get(symbol, []),
            )

            if annual_report_submission_time is None:
                LOGGER.warning(
                    '[management] no annual-report submission time for %s',
                    symbol,
                )
                continue

            annual_report_submission_times[symbol] = annual_report_submission_time

    processed_announcement_ids = set()

    for symbol, announcements in announcement_lookup.items():
        annual_report_submission_time = annual_report_submission_times.get(symbol)

        if annual_report_submission_time is None:
            continue

        sorted_announcements = sorted(
            announcements,
            key=lambda announcement: announcement.get('submission_date_time') or 0,
        )

        for announcement in sorted_announcements:
            announcement_submission_time = announcement.get('submission_date_time')

            if (
                announcement_submission_time is None
                or announcement_submission_time <= annual_report_submission_time
            ):
                continue

            announcement_id = announcement.get('ref_id') or announcement.get('url')

            if announcement_id in processed_announcement_ids:
                continue

            processed_announcement_ids.add(announcement_id)

            get_management_update(
                api_response=announcement,
                management_by_symbol=management_by_symbol,
            )

    return [
        {
            'symbol': symbol,
            'management': management,
        }
        for symbol, management in management_by_symbol.items()
    ]


def is_management_still_active(end_date_string):
    from datetime import date

    if end_date_string is None:
        return True

    parsed_end_date = date.fromisoformat(end_date_string)
    today_date = date.today()

    return parsed_end_date > today_date


def drop_former_director(
    path: str = "sgx_scraper/fetch_managements/data/success/result_tracked_v2.json"
): 
    records = open_json(path)

    for record in records: 
        managements = record.get("management")

        cleaned_management = []

        for management in managements: 
            end_date = management.get("end_date") 

            if is_management_still_active(end_date):
                format_management = {
                    "name": management["name"],
                    "age": management.get("age"), 
                    "position": management.get("position"), 
                    "start_date": management.get("start_date")
                }
                cleaned_management.append(format_management)

        record["management"] = cleaned_management

    write_json(
        path="sgx_scraper/fetch_managements/data/tracked/result_tracked_clean.json", 
        payload=records
    )    


if __name__ == "__main__":
    import sys

    from sgx_scraper.utils.json_helper import open_json, write_json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("sgx_scraper/fetch_managements/tracking.log"),
        ],
    )

    # successful_results = (
    #     open_json("sgx_scraper/fetch_managements/data/success/result_parsed_v2.json")
    #     + open_json(
    #         "sgx_scraper/fetch_managements/data/failed_rerun/success/result_v2.json"
    #     )
    # )
    # print(len(successful_results))

    # annual_report_lookup = open_json(
    #     "sgx_scraper/fetch_managements/cleaned_announcements.json"
    # )
    # announcement_lookup = open_json(
    #     "sgx_scraper/fetch_managements/cleaned_announcements_appointment_cessation.json"
    # )

    # management_updates = get_management_updates(
    #     successful_results=successful_results,
    #     annual_report_lookup=annual_report_lookup,
    #     announcement_lookup=announcement_lookup,
    # )

    # write_json(
    #     "sgx_scraper/fetch_managements/data/success/result_tracked_v2.json",
    #     management_updates,
    # )

    # LOGGER.info("saved tracked management records: %d", len(management_updates))


    # drop_former_director(
    #     path="sgx_scraper/fetch_managements/data/success/result_tracked_v2.json"
    # )
