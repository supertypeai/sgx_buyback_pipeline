from sgx_scraper.llm.client import get_llm
from sgx_scraper.fetch_reit_transaction.llm.prompts import ReitTransactionPrompt
from sgx_scraper.utils.json_helper import parse_json_reply
from sgx_scraper.utils.pdf_helper import read_pdf, resolve_attachments

import logging
import re


LOGGER = logging.getLogger(__name__)

MAX_ATTACHMENTS = 3
MAX_DOCUMENT_CHARS = 30000


def get_announcement_text(detail_url: str) -> str:
    """A deal's price often sits in the press release while the dates sit in the
    announcement proper, so the attachments are read together."""
    sections = []

    for _, link in resolve_attachments(detail_url)[:MAX_ATTACHMENTS]:
        sections.append(read_pdf(link, "REIT TRANSACTION"))

    return re.sub(r"\s+", " ", "\n".join(sections)).strip()


def extract_properties(detail_url: str, model_name: str) -> list[dict]:
    document_text = get_announcement_text(detail_url)

    if not document_text:
        LOGGER.warning(f"[REIT TRANSACTION] No readable text at {detail_url}")
        return []

    llm = get_llm(model_name, temperature=0)

    if llm is None:
        return []

    try:
        reply = llm.invoke([
            ("system", ReitTransactionPrompt.get_system_prompt()),
            ("human", ReitTransactionPrompt.get_user_prompt(
                document_text[:MAX_DOCUMENT_CHARS]
            )),
        ])

        return parse_json_reply(reply.content).get("properties") or []

    except Exception as error:
        LOGGER.error(f"[REIT TRANSACTION] Extraction failed for {detail_url}: {error}")
        return []
