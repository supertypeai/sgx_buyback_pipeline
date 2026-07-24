from bs4 import BeautifulSoup

from sgx_scraper.fetch_sgx_filings.parser_forms.router import RouterFormParser
from sgx_scraper.utils.http_client import HTTPCLIENT
from .utils.payload_html_helper import extract_section_data
from .llm_parser.fallback import parse_with_llm
from .llm_parser.transfer import resolve_transfer_holder

import logging
import time


LOGGER = logging.getLogger(__name__)


def resolve_document_url(url: str) -> str | None:
    if 'FileOpen' in url or '.ashx' in url:
        return url

    response = HTTPCLIENT.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    attachments = extract_section_data(soup, 'Attachments').get('attachments', [])

    if not attachments:
        LOGGER.info('[sgx_filings] no attachment found for %s', url)
        return None

    return attachments[-1]


def get_sgx_filings(url: str) -> list[dict]:
    pdf_url = resolve_document_url(url)
    
    if not pdf_url:
        return None

    parser = RouterFormParser.get_parser(pdf_url)

    # Voting-shares filtering now happens inside each parser: per-transaction in the
    # builder-based forms, and once per shared Part IV in Form 3 Part III/IV.
    result_parsed = parser.parse_records()

    columns_to_check = [
        "price_per_share",
        "amount_transaction",
        "transaction_type"
    ]

    for record in result_parsed:
        circumstances_desc = record.get("circumstances_desc") or ""

        # Recover any null structured values (verbatim + verified)
        missing_columns = [
            column
            for column in columns_to_check
            if record.get(column) is None
        ]

        if missing_columns:
            filled = parse_with_llm(
                source=parser,
                holder_name=record.get("holder_name"),
                missing_columns=missing_columns,
                circumstances_desc=circumstances_desc,
            )

            for column, value in filled.items():
                record[column] = value

            time.sleep(2)

        # For transfers (possibly only known after step 1), rewrite holder_name
        # as 'transferor [->] transferee'. Left unchanged if it can't be resolved
        if record.get("transaction_type") == "transfer":
            holder = resolve_transfer_holder(
                holder_name=record.get("holder_name"),
                circumstances_desc=circumstances_desc,
            )

            if holder:
                record["holder_name"] = holder

            time.sleep(2)

    return result_parsed

