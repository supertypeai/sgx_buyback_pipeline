from rapidfuzz import fuzz, process 
from tavily import TavilyClient

from sgx_scraper.config.settings import TAVILY_API_KEYS
from sgx_scraper.utils.http_client import HTTPCLIENT
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

import re 
import logging


LOGGER = logging.getLogger(__name__)


def enrich_management_records(
    primary_records: list[dict],
    fallback_records: list[dict],
    threshold: int = 90,
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
            scorer=fuzz.ratio,
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
    LOGGER.info("Getting page range, fires")

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
) -> dict:
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
    response = HTTPCLIENT.get(annual_report_url)
    response.raise_for_status()
    pdf_bytes = response.content

    result = management_parser(
        pdf_bytes=pdf_bytes,
        company_name=company_name,
        models=models,
        effort=effort, 
        is_fallback=is_fallback,
        is_liteparse=is_liteparse,
    )

    return result


def tavily_caller(query: str) -> str | None:
    exhausted_tavily_keys = set()

    for key_index, tavily_api_key in enumerate(TAVILY_API_KEYS):
        if key_index in exhausted_tavily_keys:
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
                exhausted_tavily_keys.add(key_index)

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

