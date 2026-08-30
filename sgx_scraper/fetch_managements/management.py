from collections import defaultdict
from datetime import datetime, date 
from rapidfuzz import fuzz, process 
from tavily import TavilyClient

from sgx_scraper.config.settings import TAVILY_API_KEYS, SUPABASE_CLIENT
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.sgx_announcement_html import resolve_annual_report
from sgx_scraper.utils.symbol_matching_helper import symbol_from_company_name 
from sgx_scraper.utils.json_helper import open_json, write_json
from sgx_scraper.fetch_managements.parser.board_parser import (
    extract_board_of_director,
    find_board_pdf_pages_from_ocr,
    get_range_pages,
)
from sgx_scraper.fetch_managements.utils.pdf_helper import (
    parse_native_pages,
    parse_pages,
    resolve_report_pages_to_pdf,
)
from sgx_scraper.utils.cli_helper import get_db
from .appointment import get_appointment
from .cessation import get_cessation
from .process_annual_report import filter_annual_report_url, filter_lookup_announcements

import re 
import logging


LOGGER = logging.getLogger(__name__)

EXHAUSTED_TAVILY_KEY_INDEXES = set()


def enrich_management_records(
    primary_records: list[dict],
    fallback_records: list[dict],
    threshold: int = 88,
) -> list[dict]:
    fallback_records_by_name = {}

    for fallback_record in fallback_records:
        fallback_name = fallback_record.get("name")

        if not fallback_name:
            continue

        normalized_name = re.sub(
            r"[^a-z0-9]+",
            " ",
            fallback_name.casefold(),
        ).strip()

        fallback_records_by_name.setdefault(
            normalized_name,
            [],
        ).append(fallback_record)

    fallback_names = list(fallback_records_by_name)

    for primary_record in primary_records:
        primary_name = primary_record.get("name")

        if not primary_name or not fallback_names:
            continue

        normalized_name = re.sub(
            r"[^a-z0-9]+",
            " ",
            primary_name.casefold(),
        ).strip()

        match_result = process.extractOne(
            normalized_name,
            fallback_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )

        if match_result is None:
            continue

        matched_name = match_result[0]
        matched_records = fallback_records_by_name[matched_name]

        if len(matched_records) != 1:
            continue

        fallback_record = matched_records[0]

        if (
            primary_record.get("age") is None
            and fallback_record.get("age") is not None
        ):
            primary_record["age"] = fallback_record["age"]

        if (
            primary_record.get("start_date") is None
            and fallback_record.get("start_date") is not None
        ):
            primary_record["start_date"] = fallback_record["start_date"]

    return primary_records


def get_table_contents_page(
    pdf_texts: str,
    pdf_bytes: bytes | None = None,
) -> int:
    fallback_page = 4

    contents_pattern = re.compile(
        r"(?im)^[ \t]*#*[ \t]*(?:table[ \t]+of[ \t]+)?contents?[ \t]*$"
    )
    contents_entry_pattern = re.compile(
        r"(?im)^\s*(?:\d+\.\s+|part\s+[ivxlcdm]+\b|item\s+\d+\b).*\b\d{1,3}\s*$"
    )
    page_separator_pattern = re.compile(
        r"(?m)^[ \t]*-{5,}[ \t]*$|\f"
    )
    marker_page_separator_pattern = re.compile(
        r"(?m)^\{(\d+)\}-{5,}[ \t]*$"
    )

    marker_page_matches = list(marker_page_separator_pattern.finditer(pdf_texts))

    if marker_page_matches:
        pages_with_numbers = []

        for index, marker_page_match in enumerate(marker_page_matches):
            page_text_end = (
                marker_page_matches[index + 1].start()
                if index + 1 < len(marker_page_matches)
                else len(pdf_texts)
            )
            page_number = int(marker_page_match.group(1)) + 1
            page_text = pdf_texts[marker_page_match.end():page_text_end]
            pages_with_numbers.append((page_number, page_text))

    else:
        page_texts = page_separator_pattern.split(pdf_texts)
        pages_with_numbers = enumerate(page_texts, start=1)

    candidate_page = None

    for page_number, page_text in pages_with_numbers:
        if contents_pattern.search(page_text):
            if len(contents_entry_pattern.findall(page_text)) >= 3:
                return page_number

            candidate_page = candidate_page or page_number

    if pdf_bytes is not None:
        native_pages_text = parse_native_pages(
            pdf_bytes=pdf_bytes,
            page_spec="1-10",
        )
        native_page_parts = re.split(
            r"(?m)^--- PDF page (\d+) ---\n",
            native_pages_text,
        )

        for index in range(1, len(native_page_parts), 2):
            page_number = int(native_page_parts[index])
            page_text = native_page_parts[index + 1]

            if contents_pattern.search(page_text):
                if len(contents_entry_pattern.findall(page_text)) >= 3:
                    return page_number

                candidate_page = candidate_page or page_number

    return candidate_page or fallback_page


def get_contents_page_spec(
    pdf_bytes: bytes,
    table_of_content_page: int,
) -> str:
    next_page_text = parse_native_pages(
        pdf_bytes=pdf_bytes,
        page_spec=str(table_of_content_page + 1),
    )
    contents_entry_pattern = re.compile(
        r"(?im)^\s*(?:\d+\.\s+|part\s+[ivxlcdm]+\b|item\s+\d+\b).*\b\d{1,3}\s*$"
    )
    contents_entry_count = len(
        contents_entry_pattern.findall(next_page_text)
    )

    if contents_entry_count >= 3:
        return f"1-{table_of_content_page + 1}"

    return f"1-{table_of_content_page}"


def get_board_page_range(
    pdf_bytes: bytes,
    table_of_content_page: int,
    models: list[str],
    is_fallback: bool = False,
    is_liteparse: bool = True,
) -> str | None:
    result_pages = get_range_pages(
        pdf_bytes=pdf_bytes,
        content_page=get_contents_page_spec(
            pdf_bytes=pdf_bytes,
            table_of_content_page=table_of_content_page,
        ),
        models=models,
        is_fallback=is_fallback,
        is_liteparse=is_liteparse,
    )

    if is_fallback and (
        result_pages.get("page_start") is None
        or result_pages.get("page_end") is None
    ):
        LOGGER.info(
            "No complete fallback board page range from table of contents"
        )
        return None

    if result_pages.get("page_start") is None:
        LOGGER.info(
            "No board page range from table of contents, using OCR fallback"
        )

        board_page_spec = find_board_pdf_pages_from_ocr(
            pdf_bytes=pdf_bytes,
            page_start=table_of_content_page + 1,
            is_liteparse=is_liteparse,
        )
        
        LOGGER.info("OCR board PDF page range: %s", board_page_spec)
        return board_page_spec

    return resolve_report_pages_to_pdf(
        pdf_bytes=pdf_bytes,
        report_page_start=result_pages.get("page_start"),
        report_page_end=result_pages.get("page_end"),
    )


def management_parser(
    pdf_bytes: bytes,
    company_name: str, 
    models: list[str]= [
        "nvidia-nemotron-3-ultra", 
        "deepsek-v4-flash"
    ],
    effort: str = "low",
    is_fallback: bool = False,
    is_liteparse: bool = True,
) -> list[dict]:
    pdf_text = parse_pages(
        pdf_bytes,
        is_liteparse=is_liteparse,
    )

    table_of_content_page = get_table_contents_page(
        pdf_texts=pdf_text,
        pdf_bytes=pdf_bytes,
    )

    target_page_spec = get_board_page_range(
        pdf_bytes=pdf_bytes,
        table_of_content_page=table_of_content_page,
        models=models,
        is_fallback=is_fallback,
        is_liteparse=is_liteparse,
    )

    if target_page_spec is None:
        return None

    page_splitted = target_page_spec.split("-")

    if len(page_splitted) == 2:
        start_page, end_page = page_splitted
        ranges = abs(int(end_page) - int(start_page))

        if ranges >= 30:
            LOGGER.info(
                "Range differences between start and end is too large: %d pages",
                ranges,
            )
            return None

    target_pdf_text = parse_pages(
        pdf_bytes=pdf_bytes,
        page_spec=target_page_spec,
        is_liteparse=is_liteparse,
    )

    result_bod = extract_board_of_director(
        pdf_text=target_pdf_text,
        company_name=company_name,
        models=models,
        effort=effort,
    )

    return result_bod


def get_management_payload(
    annual_report_url: str,
    models: list[str] = ["nvidia-nemotron-3-ultra", "deepsek-v4-flash"],
    company_name: str | None = None,
    effort: str = "low",
    is_fallback: bool = False,
    is_liteparse: bool = True  
) -> dict[str, str] | None:
    # pdf_url, resolved_company_name = resolve_annual_report(
    #     annual_report_url,
    #     company_name=company_name,
    # )

    # if not pdf_url:
    #     return None

    # LOGGER.info(
    #     '[Management] Processing annual report company: %s',
    #     resolved_company_name or 'unknown',
    # )

    pdf_url = annual_report_url
    resolved_company_name = company_name

    response = HTTPCLIENT.get(pdf_url)
    response.raise_for_status()
    pdf_bytes = response.content

    result = management_parser(
        pdf_bytes=pdf_bytes,
        company_name=resolved_company_name,
        models=models,
        effort=effort, 
        is_fallback=is_fallback,
        is_liteparse=is_liteparse,
    )

    return result


def reprocess_failed(
    failed_path: str,  # failed_records
    filter_symbols: list[str] | None = None, 
    models: list[str] = ["nvidia-nemotron-3-ultra", "deepsek-v4-flash"],
    is_liteparse: bool = False 
): 
    failed_records = open_json(failed_path)

    cleaned_announcements = open_json(
        "sgx_scraper/fetch_managements/cleaned_announcements.json"
    )

    if filter_symbols is not None: 
        failed_records = [
            record 
            for record in failed_records
            if record["symbol"] in filter_symbols
        ]

    companies = open_json("data/sgx_companies.json")

    rerun_success_path = "sgx_scraper/fetch_managements/data/failed_rerun/success/result_v2.json"
    rerun_failed_path = "sgx_scraper/fetch_managements/data/failed_rerun/failed/result_v2.json"

    success_result = open_json(rerun_success_path)
    failed_result = open_json(rerun_failed_path)

    processed_success_symbols = [
        symbol
        for record in success_result
        for symbol, _ in record.items()
    ]
    
    for index, record in enumerate(failed_records, start=1): 
        symbol = record["symbol"]
        pdf_url = None

        if symbol in processed_success_symbols: 
            LOGGER.info(
                "[RERUN FAILED] Skipping success processed symbols: %s", 
                symbol
            )
            continue 

        for announcement in cleaned_announcements.get(symbol) or []:
            pdf_url, _ = resolve_annual_report(
                announcement_url=announcement["url"],
                company_name=announcement.get("security_name"),
            )

            if pdf_url:
                break

        LOGGER.info(
            "[RERUN FAILED] Processing: %d/%d | symbol:%s | url:%s",
            index, 
            len(failed_records), 
            symbol,
            pdf_url
        )

        if pdf_url:
            result = get_management_payload(
                annual_report_url=pdf_url, 
                models=models,
                company_name=companies[symbol]["name"],
                is_liteparse=is_liteparse,
                effort="low"
            )

        else:
            result = None

        payload = result.get("bod_payload") if result else None

        if payload:
            need_fallback = any(
                record.get("start_date") is None
                for record in payload
            )

            if need_fallback:
                fallback_result = get_management_payload(
                    annual_report_url=pdf_url,
                    models=["nvidia-nemotron-3-ultra", "deepsek-v4-flash"],
                    company_name=companies[symbol]["name"],
                    is_fallback=True,
                )
                
                fallback_payload = (
                    fallback_result.get("bod_payload")
                    if fallback_result
                    else None
                )

                if fallback_payload:
                    payload = enrich_management_records(
                        primary_records=payload,
                        fallback_records=fallback_payload,
                    )
        
        if not payload: 
            failed_result.append({
                "symbol": symbol, 
                "pdf_url": pdf_url,
            })

        else: 
            success_result.append({
                symbol: {
                    "payload": payload,
                    "source": pdf_url,
                }
            })

        write_json(
            rerun_success_path,
            success_result
        )

        write_json(
            rerun_failed_path, 
            failed_result
        )


def prepare_announcement_records(records: list[dict]) -> list[dict]:
    appointment_records = []

    for record in records:
        try:
            appointment_record = get_appointment(api_response=record)

        except Exception as error:
            LOGGER.warning(
                "Could not parse appointment announcement %s: %s",
                record.get("id"),
                error,
            )
            continue

        if appointment_record is None or not appointment_record.get("name"):
            continue

        appointment_record["submission_date"] = record.get("submission_date")
        appointment_record["announcement_id"] = record.get("id")
        appointment_records.append(appointment_record)

    return appointment_records


def is_complete_start_date(start_date: str | None) -> bool:
    if not start_date:
        return False

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", start_date):
        return True

    return len(start_date.split()) >= 3


def get_estimated_current_age(
    appointment_age: str | int | None,
    submission_date: str | None,
) -> int | None:
    if appointment_age is None or not submission_date:
        return None

    try:
        appointment_age_number = int(appointment_age)
        announcement_year = int(submission_date[:4])
    except (TypeError, ValueError):
        return None

    return appointment_age_number + (datetime.now().year - announcement_year)


def tavily_caller(query: str) -> str | None:
    for key_index, tavily_api_key in enumerate(TAVILY_API_KEYS):
        if key_index in EXHAUSTED_TAVILY_KEY_INDEXES:
            continue

        try:
            client = TavilyClient(api_key=tavily_api_key)

            response = client.search(
                query=query,
                include_answer="advanced",
                search_depth="advanced",
                max_results=7,
                chunks_per_source=5
            )

            if response:
                response = response.get("answer")
                response_split = response.split(",")
                return response_split[0]

        except Exception as error: 
            LOGGER.error("[TAVILY] error: %s", error)

            error_message = str(error).lower()
            if (
                "usage limit" in error_message
                or "rate limit" in error_message
                or "too many requests" in error_message
                or "api key" in error_message
                or "unauthorized" in error_message
            ):
                EXHAUSTED_TAVILY_KEY_INDEXES.add(key_index)

            continue 

    return None 


def search_appointed_date_with_tavily(
    name: str, 
    position: str,
    company_name: str 
) -> str: 
    query = (
        f"What is the first appointment date of {name} as a board director of {company_name}, " 
        f"where currently serves as {position}? Answer using this format: "
        "date (yyyy-mm-dd) or null, brief explanation of where you found the date."
        "for example response may look like this: "
        "2025-06-14, explanation"
    )

    response = tavily_caller(query)
    return response


def search_is_name_match(
    management_name: str, 
    announcement_name: str,
    company_name: str,        
):
    query = (
        f"Is {management_name} and {announcement_name} are the same person serves in {company_name} "
        "Answer using this format: "
        "{yes or no}, brief explanation of how you found the answer. "
        "for example response may look like this: "
        "yes, explanation"
    )

    response = tavily_caller(query)
    return response


def enrich_announcements(
    lookup_ann_path: str,
    management_path: str | list,
    output_path: str = (
        "sgx_scraper/fetch_managements/data/success/"
        "result_enriched_appointments.json"
    ),
) -> list[dict]:
    lookup_announcements = open_json(lookup_ann_path)

    if isinstance(management_path, str):
        records_managements = open_json(management_path)

    elif isinstance(management_path, list):
        records_managements = [
            {
                tracked_record["symbol"]: {
                    "payload": tracked_record.get("management") or [],
                },
            }
            for tracked_record in management_path
        ]

    processed_managements = open_json(output_path) or []

    processed_symbols = {
        symbol
        for processed_record in processed_managements
        for symbol in processed_record
    }

    companies = open_json("data/sgx_companies.json")

    for index, bod_record in enumerate(records_managements, start=1):
        for symbol, management_result in bod_record.items():
            if symbol in processed_symbols:
                LOGGER.info("Skipping processed symbol: %s", symbol)
                continue

            LOGGER.info(
                "Processing symbol: %s | %d/%d", 
                symbol, 
                index, 
                len(records_managements)
            )

            payload = management_result.get("payload") or []
            company_name = companies.get(symbol, {}).get("name")

            appointment_records = prepare_announcement_records(
                lookup_announcements.get(symbol, []),
            )

            for management_record in payload:
                if is_complete_start_date(
                    management_record.get("start_date")
                ):
                    continue

                management_name = management_record.get("name")
                management_position = management_record.get("position")

                is_update = False 

                for appointment_record in appointment_records:
                    appointment_position = appointment_record.get("position") 
                    appointment_name = appointment_record.get("name")
                    appointment_start_date = appointment_record.get("start_date")

                    score = fuzz.ratio(
                        management_name,
                        appointment_name,
                    )

                    if score < 90:
                        position_score = fuzz.token_sort_ratio(
                            management_position, 
                            appointment_position
                        )

                        if position_score < 90:
                            continue 
                        
                        is_name_match = (
                            search_is_name_match(
                                management_name=management_name,
                                announcement_name=appointment_name,
                                company_name=company_name,
                            )
                            or ""
                        ).strip().lower()

                        if not is_name_match.startswith("yes"):
                            continue

                        if not appointment_start_date:
                            continue 

                        LOGGER.info(
                            "[FALLBACK TAVILY MATCH NAME]Enriched %s for %s from appointment announcement %s",
                            management_name,
                            symbol,
                            appointment_record.get("announcement_id"),
                        )

                        is_update = True 
                        break
                        
                    else:
                        if not appointment_start_date:
                            continue

                        LOGGER.info(
                            "[ENRICHED WITH ANNOUNCEMENTS] %s for %s from appointment announcement %s",
                            management_name,
                            symbol,
                            appointment_record.get("announcement_id"),
                        )

                        is_update = True 
                        break

                if is_update: 
                    management_record["start_date"] = appointment_start_date

                else:
                    tavily_date = search_appointed_date_with_tavily(
                        name=management_name, 
                        position=management_position, 
                        company_name=company_name
                    )

                    if tavily_date: 
                        LOGGER.info(
                            "[TAVILY FALLBACK DATE] name: %s gets date from tavily: %s", 
                            management_name, 
                            tavily_date
                        )
                        management_record["start_date"] = tavily_date

            processed_managements.append(
                {symbol: management_result}
            )
            processed_symbols.add(symbol)

            write_json(
                output_path,
                processed_managements,
            )

    return processed_managements


def upsert_management(
    path: str 
): 
    records = open_json(path)

    # structured = []

    # for record in records:
    #     former_managements = record.get("former_managements")

    #     if not former_managements:
    #         continue

    #     seen_role_keys = set()
    #     unique_former_managements = []

    #     for former_management in former_managements:
    #         role_key = (
    #             record["symbol"],
    #             former_management.get("name"),
    #             former_management.get("position"),
    #             former_management.get("start_date"),
    #             former_management.get("end_date"),
    #         )

    #         if role_key in seen_role_keys:
    #             continue

    #         seen_role_keys.add(role_key)
    #         unique_former_managements.append(former_management)

    #     structured.append(
    #         {
    #             "symbol": record["symbol"],
    #             "former_managements": unique_former_managements,
    #         }
    #     )

    # write_json(
    #     "sgx_scraper/fetch_managements/data/former_management/result_clean.json",
    #     structured
    # )
    
    # structured = []
    # today = date.today()

    # for record in records:
    #     for symbol, payload in record.items():
    #         managements = []

    #         for management in payload["payload"]:
    #             formatted_management = management.copy()
    #             start_date = formatted_management.get("start_date")

    #             if start_date and date.fromisoformat(start_date) > today:
    #                 continue

    #             age = formatted_management.get("age")

    #             if isinstance(age, str):
    #                 formatted_management["age"] = int(age)

    #             managements.append(formatted_management)

    #         if not managements:
    #             continue

    #         structured.append(
    #             {
    #                 "symbol": f"{symbol}.SI",
    #                 "management": managements,
    #             }
    #         )

    # symbols = [
    #     record["symbol"]
    #     for record in structured 
    # ]

    # top_200 = open_json("sgx_scraper/fetch_managements/top200_companies.json")

    # symbols_200 = [
    #     f"{record["symbol"]}.SI"
    #     for record in top_200 
    # ]

    # null_management = set(symbols_200) - set(symbols)

    # LOGGER.info(
    #     f"have no management: {len(null_management)} "
    #     f"| no management symbols: {null_management}"
    # )

    # db_management_symbols = open_json("sgx_scraper/fetch_managements/db_managements.json")

    # db_management_symbols = [
    #     record["symbol"]
    #     for record in db_management_symbols
    # ]

    # for symbol_db in db_management_symbols:
    #     if symbol_db not in symbols:
    #         print(f"symbol db: {symbol_db} will not get updated")

    # for record in records:
    #     for former_management in record["former_management"]:
    #         former_management["age"] = int(former_management["age"])

    # structured = records
    # print(structured)

    # write_json(
    #     "sgx_scraper/fetch_managements/data/success/update_age_int.json", 
    #     structured
    # )

    structured = records

    response = (
        SUPABASE_CLIENT
        .table("sgx_companies")
        .upsert(
            structured,
            on_conflict="symbol",
        )
        .execute()
    )

    LOGGER.info("length upserted: %d", len(response.data))


def build_former_management(path: str):
    records = open_json(path)

    backfill_start_date = date(2021, 8, 28)
    today = date.today()

    result = []

    for index, (symbol, records_by_symbol) in enumerate(records.items(), start=1):
        former_managements = []

        LOGGER.info(
            "processing symbol: %s | %d/%d",
            symbol,
            index,
            len(records),
        )

        for sub_record in records_by_symbol:
            # category_name = (
            #     sub_record.get("category_name") or ""
            # ).strip().lower()

            # if (
            #     "cessation" not in category_name
            #     and "resignation" not in category_name
            # ):
            #     continue

            submission_date_raw = sub_record.get("submission_date")

            if not submission_date_raw:
                continue

            try:
                submission_date = datetime.strptime(
                    submission_date_raw,
                    "%Y%m%d",
                ).date()

            except ValueError:
                LOGGER.warning(
                    "invalid submission date: %s | url: %s",
                    submission_date_raw,
                    sub_record.get("url"),
                )
                continue

            if not backfill_start_date <= submission_date <= today:
                continue

            LOGGER.info(
                "processing cessation: %s",
                sub_record["url"],
            )

            cessation_record = get_cessation(
                api_response=sub_record,
                fallback_symbol=symbol,
            )

            if cessation_record is None:
                continue

            end_date_raw = cessation_record.get("end_date")

            if end_date_raw is None:
                continue

            try:
                end_date = date.fromisoformat(end_date_raw)

            except ValueError:
                LOGGER.warning(
                    "invalid cessation end date: %s | url: %s",
                    end_date_raw,
                    sub_record["url"],
                )
                continue

            # Announced already, but the person has not ceased yet.
            if end_date > today:
                continue

            former_managements.append(
                {
                    "name": cessation_record.get("name"),
                    "position": cessation_record.get("position"),
                    "age": cessation_record.get("age"),
                    "start_date": cessation_record.get("start_date"),
                    "end_date": end_date_raw,
                }
            )

        result.append(
            {
                "symbol": f"{symbol}.SI",
                "former_managements": former_managements,
            }
        )

    write_json(
        path=(
            "sgx_scraper/fetch_managements/data/"
            "former_management/result.json"
        ),
        payload=result,
    )


if __name__ == "__main__":
    import sys 

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                "sgx_scraper/fetch_managements"
                "/former_management.log"
            ) 
        ]
    )

    # build lookup
    # filter_announcements("sgx_scraper/fetch_managements/announcements_appointment_cessation.json")

    # build lookup
    # filter_pdf_url("sgx_scraper/fetch_managements/cleaned_announcements.json")

    # main_success = open_json("sgx_scraper/fetch_managements/data/failed_rerun/success/result_v2.json")
    # fallback_success = open_json("sgx_scraper/fetch_managements/data/success/result_parsed_v2.json")
    # success_payload = main_success + fallback_success

    # print(f"fallback succes: {len(fallback_success)}")
    # print(f"main succes: {len(main_success)}")
    # print(f"length all success: {len(success_payload)}")

    # tracking with announcements
    # enrich_announcements(
    #     lookup_ann_path=(
    #         "sgx_scraper/fetch_managements/data/appointments/"
    #         "lookup_historical.json"
    #     ),
    #     management_path=open_json(
    #         "sgx_scraper/fetch_managements/data/tracked/"
    #         "result_tracked_clean.json"
    #     ),
    #     output_path=(
    #         "sgx_scraper/fetch_managements/data/success/"
    #         "result_enriched_appointment_v3.json"
    #     )
    # )

    # ***** BUILD FORMER MANAGEMENT *****
    # filter_announcements(
    #     path='sgx_scraper/fetch_managements/data/cessations/historical.jsonl',
    #     output_path='sgx_scraper/fetch_managements/data/cessations/lookup_historical.json',
    #     sort_oldest_first=True,
    # )

    # build_former_management(
    #     "sgx_scraper/fetch_managements/data/cessations/lookup_historical.json"
    # )

    # ***** UPSERT MANAGEMENT AND FORMER MANAGEMENT *****
    management_path = "sgx_scraper/fetch_managements/data/success/update_age_int.json"
    former_management = "sgx_scraper/fetch_managements/data/former_management/result_clean.json"

    upsert_management(
        path=management_path
    )

    # ***** RUN PROCESSING *****
    run_process = False
    rerun_failed = False   

    # failed symbols
    failed_records = open_json("sgx_scraper/fetch_managements/data/failed/failed_result_parsed_v2.json")

    failed_symbols = [
        record["symbol"]
        for record in failed_records
        if record["symbol"] not in {"AVP", "BVA"}#{"5LY", "5UF"}
    ][1:]
    
    if rerun_failed: 
        reprocess_failed(
            path="sgx_scraper/fetch_managements/data/failed/failed_result_parsed_v2.json",
            filter_symbols=failed_symbols,
            models=["nvidia-nemotron-3-ultra","deepsek-v4-flash"],
            is_liteparse=False
        )
    
    if run_process:
        base_dir = "sgx_scraper/fetch_managements/data"
        success_path = f"{base_dir}/success/result_parsed_v2.json"
        failed_path = f"{base_dir}/failed/failed_result_parsed_v2.json"
        
        result_parsed = open_json(
            success_path
        ) or []

        failed_parsed = open_json(
            failed_path
        ) or []

        pdf_urls = open_json("sgx_scraper/fetch_managements/pdf_urls.json")
        companies = open_json("data/sgx_companies.json")

        processed_symbols = {
            symbol
            for result_record in result_parsed
            for symbol, result_payload in result_record.items()
            if result_payload.get("payload")
        }

        failed_symbols = {
            record["symbol"]
            for record in failed_parsed
        }

        final_parsed = result_parsed

        for index, record in enumerate(pdf_urls, start=1): 
            symbol = record["symbol"]
            pdf_url = record.get("pdf_url")

            # if symbol != "B61":
            #     continue 

            if symbol in processed_symbols:
                LOGGER.info("skipping processed symbol: %s", symbol)
                continue

            if symbol in failed_symbols: 
                LOGGER.info("skipping processed symbol: %s", symbol)
                continue

            if pdf_url is None: 
                LOGGER.info("skipping none pdf: %s", symbol)
                continue 

            LOGGER.info(
                "Processing %d/%d | symbol: %s | URL: %s", 
                index, len(pdf_urls), symbol, pdf_url
            )
            
            result = get_management_payload(
                annual_report_url=pdf_url, 
                models=["nvidia-nemotron-3-ultra","deepsek-v4-flash"],
                company_name=companies[symbol]["name"],
                effort="medium"
            )

            payload = result.get("bod_payload") if result else None

            if payload:
                need_fallback = any(
                    record.get("start_date") is None
                    for record in payload
                )

                if need_fallback:
                    fallback_result = get_management_payload(
                        annual_report_url=pdf_url,
                        models=["nvidia-nemotron-3-ultra", "deepsek-v4-flash"],
                        company_name=companies[symbol]["name"],
                        is_fallback=True,
                    )
                    
                    fallback_payload = (
                        fallback_result.get("bod_payload")
                        if fallback_result
                        else None
                    )

                    if fallback_payload:
                        payload = enrich_management_records(
                            primary_records=payload,
                            fallback_records=fallback_payload,
                        )

            if not payload:
                failed_parsed.append({
                    "symbol": symbol, 
                    "pdf_url": pdf_url,
                })
            else:
                final_parsed.append({
                    symbol: {
                        "payload": payload,
                        "source": pdf_url,
                    }
                })
                processed_symbols.add(symbol)

            write_json(
                failed_path, 
                failed_parsed
            )

            write_json(
                success_path, 
                final_parsed
            )
            
        print(len(failed_parsed))
        print(len(result_parsed))


# uv run -m sgx_scraper.fetch_managements.management 
