from bs4 import BeautifulSoup

from sgx_scraper.fetch_sgx_filings.llm.client import get_llm
from sgx_scraper.fetch_reit_transaction.llm.prompts import ReitTransactionPrompt
from sgx_scraper.utils.http_client import HTTPCLIENT

import fitz
import json
import logging
import re


LOGGER = logging.getLogger(__name__)

SGX_BASE = "https://links.sgx.com"
MAX_ATTACHMENTS = 3
MAX_DOCUMENT_CHARS = 30000


def resolve_attachments(detail_url: str) -> list[tuple[str, str]]:
    response = HTTPCLIENT.get(detail_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    return [
        (anchor.get_text(strip=True), SGX_BASE + anchor["href"])
        for anchor in soup.find_all("a", href=True)
        if anchor["href"].startswith("/1.0.0/")
    ]


def extract_pdf_text(pdf_bytes: bytes) -> str:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as document:
        return "\n".join(page.get_text() for page in document)


def get_announcement_text(detail_url: str) -> str:
    """A deal's price often sits in the press release while the dates sit in the
    announcement proper, so the attachments are read together."""
    sections = []

    for name, link in resolve_attachments(detail_url)[:MAX_ATTACHMENTS]:
        try:
            content = HTTPCLIENT.get(link, timeout=90).content

            if content[:4] != b"%PDF":
                continue

            sections.append(extract_pdf_text(content))

        except Exception as error:
            LOGGER.warning(f"[REIT TRANSACTION] Failed reading {name}: {error}")
            continue

    return re.sub(r"\s+", " ", "\n".join(sections)).strip()


def parse_json_reply(reply: str) -> dict:
    cleaned = re.sub(r"^```(?:json)?|```$", "", reply.strip(), flags=re.MULTILINE)
    return json.loads(cleaned.strip())


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
