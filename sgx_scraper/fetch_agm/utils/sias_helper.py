from bs4 import BeautifulSoup
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from sgx_scraper.fetch_agm.constant import (
    MAX_DOCUMENT_CHARS,
    SIAS_ANSWER_PATTERNS,
    SIAS_BASE_URL,
    SIAS_QUESTION_PATTERN,
)
from sgx_scraper.fetch_agm.llm.prompts import SiasAnswerPrompt
from sgx_scraper.fetch_sgx_filings.llm.client import get_llm
from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.json_helper import parse_json_reply
from sgx_scraper.utils.pdf_helper import read_pdf

import logging
import re


LOGGER = logging.getLogger(__name__)

FLAG_LOG = "AGM SIAS"

COLUMNS = {
    "AGM/EGM Date": "meeting_date",
    "Company": "company",
    "Category": "meeting_type",
    "SIAS Questions": "questions_pdf",
    "Company Response": "response_pdf",
}


def encode_url(url: str) -> str:
    """SIAS hrefs carry spaces and ampersands raw."""
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=quote(unquote(parts.path))))


def read_listing_page(page: int) -> list[dict]:
    url = SIAS_BASE_URL if page == 1 else f"{SIAS_BASE_URL}page/{page}/"

    try:
        response = HTTPCLIENT.get(url, timeout=90)
        response.raise_for_status()

    except Exception as error:
        LOGGER.warning(f"[{FLAG_LOG}] Failed reading listing page {page}: {error}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    tables = [table for table in soup.find_all("table") if len(table.find_all("tr")) > 2]

    if not tables:
        return []

    rows = tables[0].find_all("tr")
    headers = [cell.get_text(" ", strip=True) for cell in rows[0].find_all(["th", "td"])]
    entries = []

    for row in rows[1:]:
        cells = row.find_all("td")

        if len(cells) != len(headers):
            continue

        entry = {}

        for header, cell in zip(headers, cells):
            field = COLUMNS.get(header)

            if not field:
                continue

            if field.endswith("_pdf"):
                anchor = cell.find("a", class_="readmore", href=True)
                entry[field] = anchor["href"] if anchor else None

            else:
                text = cell.get_text(" ", strip=True)
                entry[field] = re.sub(rf"^{re.escape(header)}\s*:?\s*", "", text).strip()

        entries.append(entry)

    return entries


def build_sias_index(pages: int) -> dict[tuple[str, str], dict]:
    """SIAS keys on company name and meeting date, so the index is built on the
    normalised name and matched against the issuer and the security name."""
    index = {}

    for page in range(1, pages + 1):
        for entry in read_listing_page(page):
            meeting_date = safe_convert_datetime(entry.get("meeting_date"))

            if not meeting_date:
                continue

            meeting_type = "EGM" if entry.get("meeting_type") == "EGM" else "AGM"

            for alias in name_aliases(entry.get("company") or ""):
                index[(alias, meeting_date, meeting_type)] = entry

    LOGGER.info(f"[{FLAG_LOG}] Indexed {len(index)} name keys from {pages} listing pages")
    return index


SUFFIX_PATTERN = (
    r"(limited|ltd|pte|plc|inc|incorporated|corporation|corp|company|co|holdings"
    r"|group|berhad|bhd|sa|nv|llc|lp)"
)


def normalise_name(name: str | None) -> str:
    cleaned = re.sub(r"\(.*?\)", " ", (name or "").lower())
    cleaned = re.sub(r"[^a-z0-9 ]", " ", cleaned)

    return " ".join(
        word
        for word in cleaned.split()
        if not re.fullmatch(SUFFIX_PATTERN, word)
    )


def name_aliases(name: str) -> list[str]:
    names = [name] + re.findall(
        r"\((?:formerly|now)\s+known\s+as\s+(.*?)\)", name, re.I
    )

    return [alias for alias in map(normalise_name, names) if alias]


def find_sias_entry(
    index: dict,
    issuer_name: str | None,
    security_name: str | None,
    meeting_date: str,
    meeting_type: str,
) -> dict:
    for name in (issuer_name, security_name):
        entry = index.get((normalise_name(name), meeting_date, meeting_type))

        if entry:
            return entry

    return {}


def split_numbered(text: str, pattern: str, expect: int) -> dict[int, str] | None:
    marks = {}

    for matched in re.finditer(pattern, text):
        marks.setdefault(int(matched.group(1)), (matched.start(), matched.end()))

    if sorted(marks) != list(range(1, expect + 1)):
        return None

    ordered = sorted(marks.items(), key=lambda item: item[1][0])

    if [number for number, _ in ordered] != list(range(1, expect + 1)):
        return None

    sections = {}

    for position, (number, (_, end)) in enumerate(ordered):
        stop = ordered[position + 1][1][0] if position + 1 < len(ordered) else len(text)
        sections[number] = text[end:stop].strip(" .:\n\t")

    return sections


def clean_document(text: str) -> str:
    text = re.sub(r"(?im)^\s*Page \d+( of \d+)?\s*$", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def parse_questions(text: str) -> dict[int, str]:
    body = clean_document(text)
    start = re.search(r"(?im)^\s*Q\s?1[.)]", body)

    if start:
        body = body[start.start():]

    for expect in range(10, 0, -1):
        sections = split_numbered(body, SIAS_QUESTION_PATTERN, expect)

        if sections:
            return sections

    return {}


def parse_answers(text: str, expect: int) -> dict[int, str] | None:
    body = clean_document(text)

    for pattern in SIAS_ANSWER_PATTERNS:
        sections = split_numbered(body, pattern, expect)

        if sections:
            return sections

    return None


def locate_answers_with_llm(text: str, expect: int, model_name: str) -> dict[int, str] | None:
    """The model reports only where each answer starts; the cut is still made
    here, so the stored text is the document's own."""
    llm = get_llm(model_name, temperature=0)

    if llm is None:
        return None

    body = clean_document(text)

    try:
        reply = llm.invoke([
            ("system", SiasAnswerPrompt.get_system_prompt(expect)),
            ("human", SiasAnswerPrompt.get_user_prompt(body[:MAX_DOCUMENT_CHARS])),
        ])

        starts = parse_json_reply(reply.content).get("starts") or {}

    except Exception as error:
        LOGGER.error(f"[{FLAG_LOG}] Locating answers failed: {error}")
        return None

    offsets = []

    for number in range(1, expect + 1):
        phrase = starts.get(str(number))

        if not isinstance(phrase, str) or not phrase.strip():
            continue

        loose = r"\s+".join(re.escape(word) for word in phrase.split()[:6])
        found = re.search(loose, body, re.I)

        if found:
            offsets.append((number, found.start()))

    if len(offsets) < 2:
        return None

    offsets.sort(key=lambda item: item[1])
    sections = {}

    for position, (number, start) in enumerate(offsets):
        stop = offsets[position + 1][1] if position + 1 < len(offsets) else len(body)
        sections[number] = body[start:stop].strip()

    return sections


def tidy_answer(answer: str | None) -> str | None:
    if not answer:
        return answer

    text = answer.lstrip(" ?:.–-\n\t")
    text = re.sub(
        r"^(?:the\s+)?(?:company(?:'|’)?s?\s+)?(?:response|reply|answer)s?\s*[:\-–]?\s*",
        "",
        text,
        flags=re.I,
    )

    return text.strip() or None


def build_qa(entry: dict, model_name: str) -> list[dict]:
    questions_pdf = entry.get("questions_pdf")

    if not questions_pdf:
        return []

    questions = parse_questions(read_pdf(encode_url(questions_pdf), FLAG_LOG))

    if not questions:
        return []

    answers = {}
    response_pdf = entry.get("response_pdf")

    if response_pdf:
        response_text = read_pdf(encode_url(response_pdf), FLAG_LOG)

        if response_text:
            answers = parse_answers(response_text, len(questions)) or {}

            if not answers:
                answers = locate_answers_with_llm(
                    response_text, len(questions), model_name
                ) or {}

    return [
        {
            "n": number,
            "question": questions[number],
            "answer": tidy_answer(answers.get(number)),
        }
        for number in sorted(questions)
    ]
