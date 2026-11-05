from sgx_scraper.sgx_api.announcements import iter_sgx_announcements
from sgx_scraper.utils.annual_report_helper import filter_annual_report_url, filter_lookup_announcements
from sgx_scraper.utils.http_client import HTTPCLIENT

from datetime import datetime

import re
import asyncio
import logging
import pymupdf


LOGGER = logging.getLogger(__name__)

# The heading reads "Substantial Shareholder(s)", "Substantial Shareholdings" or, for
# stapled trusts like HMN, "Substantial Stapled Securityholders"
SUBSTANTIAL = re.compile(r"substantial\s+(share|unit|stapled\s+security\s?)hold(er|ing)", re.IGNORECASE)
STATISTICS = re.compile(
    r"(share|unit|security)holding\s+statistics|statistics\s+of\s+(stapled\s+)?(share|unit|security)holding",
    re.IGNORECASE,
)
# Governance pages mention substantial holders too, so a page also needs the table's words
TABLE_WORDS = re.compile(
    rf"(direct|deemed)\s+interest|{STATISTICS.pattern}"
    r"|(number|no\.?)\s+of\s+(shares|units)|%\s+of\s+(issued|total)",
    re.IGNORECASE,
)
DIRECT = re.compile(r"direct\s+interest", re.IGNORECASE)
DEEMED = re.compile(r"deemed\s+interest", re.IGNORECASE)
AS_AT = re.compile(r"as\s+(?:at|of)\s+(\d{1,2})\s+([a-z]+)\s+(\d{4})", re.IGNORECASE)
# S68's report has no table, only "the Company had no substantial shareholders"
NO_SUBSTANTIAL = re.compile(
    r"\b(had|has|have|were|are)\s+no\s+substantial\s+(share|unit)holders?",
    re.IGNORECASE,
)
TOP_HOLDERS = re.compile(
    r"twenty\s+largest|top\s+(20|twenty)\s+(share|unit)holders|20\s+largest",
    re.IGNORECASE,
)

# Characters after a heading that its "as at" date may sit in
HEADING_DATE_REACH = 300
STATISTICS_DATE_REACH = 200
OCR_DPI = 300
ANNUAL_REPORT_CATEGORY = "ANNC30"
ANNOUNCEMENT_PAGE_SIZE = 100
PDF_TIMEOUT_SECONDS = 180


def find_report_urls(period_start: str, period_end: str) -> dict[str, dict]:
    announcements = list(
        iter_sgx_announcements(
            sub_category=ANNUAL_REPORT_CATEGORY,
            flag_log="Annual Reports",
            period_start=period_start,
            period_end=period_end,
            page_size=ANNOUNCEMENT_PAGE_SIZE,
        )
    )
    report_urls = filter_annual_report_url(filter_lookup_announcements(announcements))

    # An announcement whose PDF could not be resolved has nothing to read
    return {
        symbol: report
        for symbol, report in report_urls.items()
        if report["pdf_url"]
    }


def is_holders_table_page(page_texts: list[str], page_number: int) -> bool:
    page_text = page_texts[page_number]
    # MIT heads the table on one page and prints it on the next
    has_substantial_heading = any(
        SUBSTANTIAL.search(page_texts[heading])
        for heading in (page_number - 1, page_number)
        if heading >= 0
    )

    if has_substantial_heading and TABLE_WORDS.search(page_text):
        return True

    # ME8U and C52 print the "substantial" heading as an image
    return bool(
        STATISTICS.search(page_text)
        and DIRECT.search(page_text)
        and DEEMED.search(page_text)
    )


def select_shareholding_pages(page_texts: list[str]) -> list[int]:
    matched_pages = [
        page_number
        for page_number in range(len(page_texts))
        if is_holders_table_page(page_texts, page_number)
    ]

    # A table's footnotes often run onto the next page
    return sorted(
        {
            page_number
            for matched_page in matched_pages
            for page_number in (matched_page, matched_page + 1)
            if page_number < len(page_texts)
        }
    )


def find_no_substantial_page(page_texts: list[str]) -> int | None:
    for page_number, page_text in enumerate(page_texts):
        if NO_SUBSTANTIAL.search(re.sub(r"\s+", " ", page_text)):
            return page_number + 1

    return None


def find_date(text: str) -> str | None:
    for day, month, year in AS_AT.findall(text):
        try:
            return datetime.strptime(f"{day} {month.title()} {year}", "%d %B %Y").date().isoformat()
        except ValueError:
            continue

    return None


def find_date_after(heading: re.Pattern, text: str, reach: int) -> str | None:
    for match in heading.finditer(text):
        found = find_date(text[match.end():match.end() + reach])

        if found:
            return found

    return None


def find_table_dates(page_texts: list[str]) -> dict[int, str]:
    # The date beside the substantial heading wins: AJBU's page also dates the directors'
    # register. 5WJ and A31 date the statistics on the page before the table
    texts = [re.sub(r"\s+", " ", text) for text in page_texts]
    dates = {}

    for page_number in select_shareholding_pages(page_texts):
        nearby = [texts[each] for each in (page_number, page_number - 1) if each >= 0]
        statistics_dates = [find_date_after(STATISTICS, text, STATISTICS_DATE_REACH) for text in nearby]
        found = (
            find_date_after(SUBSTANTIAL, texts[page_number], HEADING_DATE_REACH)
            or next((each for each in statistics_dates if each), None)
            or find_date(texts[page_number])
        )

        if found:
            dates[page_number + 1] = found

    return dates


def ocr_blank_pages(
    pdf_document: pymupdf.Document,
    page_texts: list[str],
    plain_texts: list[str],
) -> None:
    # BN2's report is scanned. The statistics sit near the back and OCR is slow, so it
    # reads from the back and stops at the table
    for page_number in reversed(range(len(page_texts))):
        if page_texts[page_number].strip():
            continue

        page = pdf_document[page_number]
        page_texts[page_number] = plain_texts[page_number] = page.get_text(
            "text",
            sort=True,
            textpage=page.get_textpage_ocr(dpi=OCR_DPI, full=True),
        )

        if select_shareholding_pages(page_texts) or find_no_substantial_page(plain_texts):
            return


def read_page_texts(pdf_bytes: bytes) -> tuple[list[str], list[str]]:
    pdf_document = pymupdf.open(stream=pdf_bytes, filetype="pdf")

    try:
        page_texts = [page.get_text("text", sort=True) for page in pdf_document]
        # S68's "no substantial shareholders" line only reads whole in the unsorted text
        plain_texts = [page.get_text("text") for page in pdf_document]

        if not select_shareholding_pages(page_texts) and not find_no_substantial_page(plain_texts):
            ocr_blank_pages(pdf_document, page_texts, plain_texts)
    finally:
        pdf_document.close()

    return page_texts, plain_texts


def format_shareholding_pages(page_texts: list[str]) -> str:
    return "\n\n".join(
        f"--- PDF page {page_number + 1} ---\n{page_texts[page_number].strip()}"
        for page_number in select_shareholding_pages(page_texts)
        if page_texts[page_number].strip()
    )


def fetch_report_pages(symbol: str, report: dict) -> dict | None:
    try:
        response = HTTPCLIENT.get(report["pdf_url"], timeout=PDF_TIMEOUT_SECONDS)
        response.raise_for_status()
        
    except Exception as error:
        LOGGER.error("Fetch annual report error for symbol %s: %s", symbol, error)
        return None

    page_texts, plain_texts = read_page_texts(response.content)
    pages = format_shareholding_pages(page_texts)
    no_substantial_page = None if pages else find_no_substantial_page(plain_texts)

    if not pages and not no_substantial_page:
        LOGGER.info("No shareholding pages found for symbol: %s", symbol)
        return None

    return {
        "symbol": symbol,
        "source_url": report["pdf_url"],
        "submission_date": report["submission_date"],
        "pages": pages,
        # The twenty largest holders list can give a count the footnotes leave out (H30)
        "top_holders": "\n".join(text for text in page_texts if TOP_HOLDERS.search(text)),
        "no_substantial_page": no_substantial_page,
        "page_dates": find_table_dates(page_texts),
    }


async def fetch_report_pages_limited(semaphore: asyncio.Semaphore, symbol: str, report: dict) -> dict | None:
    async with semaphore:
        return await asyncio.to_thread(fetch_report_pages, symbol, report)


async def get_report_pages(report_urls: dict[str, dict], concurrency: int) -> list[dict]:
    semaphore = asyncio.Semaphore(concurrency)
    results = await asyncio.gather(
        *(
            fetch_report_pages_limited(semaphore, symbol, report)
            for symbol, report in report_urls.items()
        )
    )

    return [result for result in results if result is not None]
