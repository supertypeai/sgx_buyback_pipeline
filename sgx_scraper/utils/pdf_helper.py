from bs4 import BeautifulSoup

from sgx_scraper.utils.http_client import HTTPCLIENT

import fitz
import logging


LOGGER = logging.getLogger(__name__)

SGX_BASE = "https://links.sgx.com"


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


def read_pdf(url: str, flag_log: str, **kwargs) -> str:
    try:
        content = HTTPCLIENT.get(url, timeout=90, **kwargs).content

        if content[:4] != b"%PDF":
            return ""

        return extract_pdf_text(content)

    except Exception as error:
        LOGGER.warning(f"[{flag_log}] Failed reading {url}: {error}")
        return ""
