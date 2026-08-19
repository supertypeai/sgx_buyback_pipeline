from bs4 import BeautifulSoup

from sgx_scraper.fetch_agm.constant import (
    DETAIL_SECTIONS,
    MAX_DOCUMENT_CHARS,
    MEETING_TAGS,
    RESULTS_ATTACHMENT_PATTERN,
)
from sgx_scraper.fetch_agm.llm.prompts import AgmPrompt
from sgx_scraper.fetch_sgx_filings.llm.client import get_llm
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.json_helper import parse_json_reply
from sgx_scraper.utils.pdf_helper import read_pdf, resolve_attachments
from sgx_scraper.utils.sgx_announcement_html import extract_section_data

import logging
import re


LOGGER = logging.getLogger(__name__)

FLAG_LOG = "AGM"


def flatten(value) -> str | None:
    if isinstance(value, (list, tuple)):
        return " | ".join(str(item).strip() for item in value)

    return value.strip() if isinstance(value, str) else value


def extract_detail_fields(detail_url: str) -> dict:
    response = HTTPCLIENT.get(detail_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    fields = {}

    for section in DETAIL_SECTIONS:
        for key, value in (extract_section_data(soup, section) or {}).items():
            fields[key.strip()] = flatten(value)

    return fields


def resolve_results_document(detail_url: str) -> str | None:
    """Nothing is returned while a meeting is still at notice stage."""
    for name, link in resolve_attachments(detail_url):
        if re.search(RESULTS_ATTACHMENT_PATTERN, name, re.I):
            return link

    return None


def summarise_results(results_url: str, model_name: str) -> tuple[str | None, list[str] | None]:
    document_text = re.sub(r"\s+", " ", read_pdf(results_url, FLAG_LOG)).strip()

    if not document_text:
        LOGGER.warning(f"[{FLAG_LOG}] No readable text at {results_url}")
        return None, None

    llm = get_llm(model_name, temperature=0)

    if llm is None:
        return None, None

    try:
        reply = llm.invoke([
            ("system", AgmPrompt.get_system_prompt()),
            ("human", AgmPrompt.get_user_prompt(document_text[:MAX_DOCUMENT_CHARS])),
        ])

        parsed = parse_json_reply(reply.content)
        tags = [tag for tag in (parsed.get("tags") or []) if tag in MEETING_TAGS]

        return parsed.get("summary"), tags or None

    except Exception as error:
        LOGGER.error(f"[{FLAG_LOG}] Summarising failed for {results_url}: {error}")
        return None, None
