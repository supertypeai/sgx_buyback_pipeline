from sgx_scraper.llm.caller import invoke_structured_llm
from sgx_scraper.fetch_managements.parser.board_prompt import (
    BoardMemberDirector,
    BoardPageNumber,
    PromptCollections,
)
from sgx_scraper.fetch_managements.utils.pdf_helper import (
    parse_native_pages,
    parse_pages,
)

import re
import fitz
import logging 


LOGGER = logging.getLogger(__name__)


def find_board_pdf_pages_from_ocr(
    pdf_bytes: bytes,
    page_start: int,
    is_liteparse: bool = True,
) -> str | None:
    pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")

    try:
        page_end = min(page_start + 30, pdf_document.page_count)
    finally:
        pdf_document.close()

    if page_start > page_end:
        return None

    pdf_text = parse_pages(
        pdf_bytes,
        page_spec=f"{page_start}-{page_end}",
        is_liteparse=is_liteparse,
    )
    
    page_separator_pattern = r"(?m)^[ \t]*-{5,}[ \t]*$|\f"
    page_texts = re.split(page_separator_pattern, pdf_text)
    board_pdf_page_start = None
    board_roster_pdf_page = None
    management_heading_pattern = r"#*\s*(?:key management|leadership team)\s*"

    for pdf_page_number, page_text in enumerate(page_texts, start=page_start):
        page_lines = page_text.splitlines()

        has_board_roster_heading = any(
            re.match(
                r"\|\s*board\s+of\s+directors\s*\|",
                line,
                re.IGNORECASE,
            )
            for line in page_lines
        )

        if has_board_roster_heading and board_roster_pdf_page is None:
            board_roster_pdf_page = pdf_page_number

        has_board_heading = any(
            re.match(
                r"#*\s*board\s+of\s+directors\b",
                line,
                re.IGNORECASE,
            )
            for line in page_lines
        )

        if has_board_heading and board_pdf_page_start is None:
            board_pdf_page_start = pdf_page_number
            continue

        has_management_heading = any(
            re.match(management_heading_pattern, line, re.IGNORECASE)
            for line in page_lines
        )

        if board_pdf_page_start is not None and has_management_heading:
            return f"{board_pdf_page_start}-{pdf_page_number - 1}"

    if board_pdf_page_start is None and board_roster_pdf_page is not None:
        return str(board_roster_pdf_page)

    return None


def get_range_pages(
    pdf_bytes: bytes,
    content_page: str,
    models: list[str] = [
        "nvidia-nemotron-3-ultra", 
        "deepsek-v4-flash"
    ],
    is_fallback: bool = False,
    is_liteparse: bool = True,
) -> dict[str, any]:
    prompts = PromptCollections()
    
    page_text = parse_native_pages(
        pdf_bytes=pdf_bytes,
        page_spec=content_page,
    )

    if not page_text:
        page_text = parse_pages(
            pdf_bytes,
            page_spec=content_page,
            is_liteparse=is_liteparse,
        )    

    input_data = {
        "table_of_contents": page_text
    }

    result = invoke_structured_llm(
        pydantic_output=BoardPageNumber, 
        system_prompt=prompts.get_system_board_page_prompt(
            is_fallback=is_fallback,
        ), 
        user_prompt=prompts.get_user_board_page_prompt(
            is_fallback=is_fallback,
        ), 
        log_name="Management BOD Pages", 
        input_data=input_data, 
        models=models, 
        effort="low"
    )

    return result


def extract_board_of_director(
    pdf_text: str,
    company_name: str, 
    models: list[str] = [
        "nvidia-nemotron-3-ultra", 
        "deepsek-v4-flash"
    ],
    effort: str = "low",
) -> dict:
    prompts = PromptCollections()

    input_data = {
        "company_name": company_name, 
        "pdf_text": pdf_text 
    }
    
    result = invoke_structured_llm(
        pydantic_output=BoardMemberDirector, 
        system_prompt=prompts.get_system_management(), 
        user_prompt=prompts.get_user_management(), 
        log_name="Management BOD Extraction", 
        input_data=input_data, 
        models=models, 
        effort=effort
    ) 

    return result
