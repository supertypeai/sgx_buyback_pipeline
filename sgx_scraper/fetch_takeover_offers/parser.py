from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4.element import Tag
from liteparse import LiteParse

from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.sgx_announcement_html import extract_section_data

import json
import logging
import re


LOGGER = logging.getLogger(__name__)

MAX_ATTACHMENT_CONTEXT = 2
MAX_ATTACHMENT_CHARACTERS = 40_000

DISBURSEMENT_SECTION_PATTERN = re.compile(
    r"^(?:Option\s+(\d+)\s*-\s*)?Disbursement Details$",
    re.IGNORECASE,
)

LITEPARSE = LiteParse(
    output_format="markdown",
    image_mode="off",
    extract_links=False,
    max_pages=40,
    quiet=True,
)


def parse_broadcast_datetime(raw_datetime: str | None) -> str | None:
    if not raw_datetime:
        return None

    try:
        parsed_datetime = datetime.strptime(
            raw_datetime.strip(),
            "%d-%b-%Y %H:%M:%S",
        )

        return parsed_datetime.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    except ValueError:
        LOGGER.warning(
            "Unable to parse broadcast datetime: %s",
            raw_datetime,
        )
        return None


def parse_symbol(raw_security: str | None) -> str | None:
    if not raw_security:
        return None

    security_parts = raw_security.rsplit(" - ", 2)

    if len(security_parts) < 2:
        LOGGER.warning(
            "Unexpected SGX security format: %s",
            raw_security,
        )
        return None

    raw_symbol = security_parts[-1].strip()

    if not raw_symbol:
        return None

    return raw_symbol


def parse_offer_type(
    announcement_title: str | None,
) -> str | None:
    if not announcement_title:
        return None

    normalized_title = announcement_title.lower()

    if "mandatory" in normalized_title:
        return "Mandatory"

    if "voluntary" in normalized_title:
        return "Voluntary"

    return None


def get_section_group(
    soup: BeautifulSoup,
    section_title: str,
) -> Tag | None:
    section_headers = soup.find_all(
        "h2",
        class_="announcement-group-header",
    )

    for section_header in section_headers:
        current_title = section_header.get_text(
            " ",
            strip=True,
        )

        if current_title != section_title:
            continue

        return section_header.find_next_sibling(
            "div",
            class_="announcement-group",
        )

    return None


def extract_narrative_rows(
    section_group: Tag | None,
) -> list[dict]:
    if section_group is None:
        return []

    narratives = []

    for table in section_group.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")

            if len(cells) < 2:
                continue

            narrative_type = cells[0].get_text(
                " ",
                strip=True,
            )

            narrative_text = " ".join(
                cell.get_text(" ", strip=True)
                for cell in cells[1:]
                if cell.get_text(" ", strip=True)
            )

            if not narrative_type or not narrative_text:
                continue

            if (
                narrative_type.lower() == "narrative type"
                and narrative_text.lower() == "narrative text"
            ):
                continue

            narratives.append(
                {
                    "type": narrative_type,
                    "text": narrative_text,
                }
            )

    return narratives


def extract_group_fields(
    section_group: Tag | None,
) -> dict:
    if section_group is None:
        return {}

    fields = {}

    for key_element in section_group.find_all("dt"):
        value_element = key_element.find_next_sibling("dd")

        if value_element is None:
            continue

        if value_element.find("table"):
            continue

        key = key_element.get_text(
            " ",
            strip=True,
        )

        value = value_element.get_text(
            " ",
            strip=True,
        )

        if not key or not value:
            continue

        fields[key] = value

    return fields


def extract_disbursement_options(
    soup: BeautifulSoup,
) -> list[dict]:
    disbursement_options = []

    section_headers = soup.find_all(
        "h2",
        class_="announcement-group-header",
    )

    for section_header in section_headers:
        section_title = section_header.get_text(
            " ",
            strip=True,
        )

        title_match = (
            DISBURSEMENT_SECTION_PATTERN.fullmatch(
                section_title
            )
        )

        if title_match is None:
            continue

        section_group = section_header.find_next_sibling(
            "div",
            class_="announcement-group",
        )

        if section_group is None:
            continue

        fields = extract_group_fields(
            section_group
        )

        option_number = (
            int(title_match.group(1))
            if title_match.group(1)
            else None
        )

        disbursement_options.append(
            {
                "option_number": option_number,
                "acceptance_period": fields.get(
                    "Acceptance Period"
                ),
                "closing_time": fields.get(
                    "Closing Time"
                ),
                "disbursement_type": fields.get(
                    "Disbursement Type"
                ),
                "offer_price": fields.get(
                    "Offer Price"
                ),
                "distribution_ratio": fields.get(
                    "Distribution Ratio (New: Old)"
                ),
                "fractional_disposition_method": (
                    fields.get(
                        "Fractional Disposition Method"
                    )
                    or fields.get(
                        "Method of Disposing Fractional Entitlement"
                    )
                ),
                "pay_date": safe_convert_datetime(
                    fields.get("Pay Date")
                ),
                "narrative": extract_narrative_rows(
                    section_group
                ),
            }
        )

    return disbursement_options


def extract_attachments(
    soup: BeautifulSoup,
    announcement_url: str,
) -> list[dict]:
    attachment_section = get_section_group(
        soup=soup,
        section_title="Attachments",
    )

    if attachment_section is None:
        return []

    attachments = []

    for link in attachment_section.find_all(
        "a",
        href=True,
    ):
        attachment_name = link.get_text(
            " ",
            strip=True,
        )

        attachment_href = link.get("href")

        if not attachment_name or not attachment_href:
            continue

        if not attachment_name.lower().endswith(".pdf"):
            continue

        attachments.append(
            {
                "name": attachment_name,
                "url": urljoin(
                    announcement_url,
                    attachment_href,
                ),
            }
        )

    return attachments


def normalize_text(value: str) -> str:
    normalized_value = value.lower()

    normalized_value = normalized_value.replace(
        "_",
        " ",
    )

    normalized_value = normalized_value.replace(
        "-",
        " ",
    )

    return re.sub(
        r"\s+",
        " ",
        normalized_value,
    ).strip()


def get_attachment_relevance_score(
    attachment_name: str,
    announcement_context: str,
) -> int:
    normalized_name = normalize_text(
        attachment_name
    )

    normalized_context = normalize_text(
        announcement_context
    )

    score = 0

    relevance_groups = [
        [
            "ifa",
            "independent financial adviser",
            "independent financial advisor",
        ],
        [
            "compulsory acquisition",
            "compulsory",
        ],
        [
            "close of offer",
            "closing of offer",
            "final level of acceptances",
            "final acceptance",
        ],
        [
            "extension",
            "extended",
            "closing date",
        ],
        [
            "delisting",
            "delist",
        ],
        [
            "regulatory",
            "approval",
        ],
        [
            "offer document",
            "general offer",
            "voluntary general offer",
            "mandatory offer",
            "vgo",
        ],
    ]

    for phrases in relevance_groups:
        context_matches = any(
            phrase in normalized_context
            for phrase in phrases
        )

        attachment_matches = any(
            phrase in normalized_name
            for phrase in phrases
        )

        if context_matches and attachment_matches:
            score += 10

    name_tokens = set(
        re.findall(
            r"[a-z0-9]+",
            normalized_name,
        )
    )

    context_tokens = set(
        re.findall(
            r"[a-z0-9]+",
            normalized_context,
        )
    )

    ignored_tokens = {
        "pdf",
        "announcement",
        "offer",
        "acquisition",
        "takeover",
        "purchase",
        "tender",
        "company",
        "the",
        "of",
        "by",
        "and",
        "for",
        "to",
    }

    useful_name_tokens = (
        name_tokens - ignored_tokens
    )

    useful_context_tokens = (
        context_tokens - ignored_tokens
    )

    score += len(
        useful_name_tokens
        & useful_context_tokens
    )

    return score


def select_relevant_attachments(
    announcement_title: str | None,
    event_narrative: list[dict],
    attachments: list[dict],
) -> list[dict]:
    if not attachments:
        return []

    if len(attachments) == 1:
        return attachments

    narrative_text = " ".join(
        narrative.get("text") or ""
        for narrative in event_narrative
    )

    announcement_context = " ".join(
        [
            announcement_title or "",
            narrative_text,
        ]
    )

    scored_attachments = []

    for attachment in attachments:
        relevance_score = (
            get_attachment_relevance_score(
                attachment_name=(
                    attachment.get("name")
                    or ""
                ),
                announcement_context=(
                    announcement_context
                ),
            )
        )

        if relevance_score <= 0:
            continue

        scored_attachments.append(
            (
                relevance_score,
                attachment,
            )
        )

    scored_attachments.sort(
        key=lambda scored_attachment: (
            scored_attachment[0]
        ),
        reverse=True,
    )

    return [
        attachment
        for _, attachment in scored_attachments[
            :MAX_ATTACHMENT_CONTEXT
        ]
    ]


def parse_attachment_pdf(
    attachment: dict,
) -> dict | None:
    attachment_url = attachment.get("url")
    attachment_name = attachment.get("name")

    if not attachment_url:
        return None

    try:
        response = HTTPCLIENT.get(
            attachment_url
        )

        response.raise_for_status()

        parsed_document = LITEPARSE.parse(
            response.content
        )

        attachment_text = (
            parsed_document.text or ""
        ).strip()

        if not attachment_text:
            LOGGER.warning(
                "No text extracted from attachment: %s",
                attachment_name,
            )
            return None

        return {
            "name": attachment_name,
            "text": attachment_text[
                :MAX_ATTACHMENT_CHARACTERS
            ],
        }

    except Exception as error:
        LOGGER.warning(
            "Failed to parse takeover attachment %s: %s",
            attachment_url,
            error,
            exc_info=True,
        )
        return None


def get_takeover(
    url: str,
) -> dict | None:
    try:
        response = HTTPCLIENT.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        issuer_section = (
            extract_section_data(
                soup,
                "Issuer & Securities",
            )
            or {}
        )

        announcement_section = (
            extract_section_data(
                soup,
                "Announcement Details",
            )
            or {}
        )

        event_dates_section = (
            extract_section_data(
                soup,
                "Event Dates",
            )
            or {}
        )

        withdrawal_section = (
            extract_section_data(
                soup,
                "Reason(s) for Withdrawal",
            )
            or {}
        )

        event_narrative_group = (
            get_section_group(
                soup=soup,
                section_title="Event Narrative",
            )
        )

        event_narrative = (
            extract_narrative_rows(
                event_narrative_group
            )
        )

        security = issuer_section.get(
            "Security"
        )

        symbol = parse_symbol(
            security
        )

        announcement_title = (
            announcement_section.get(
                "Announcement Title"
            )
        )

        attachments = extract_attachments(
            soup=soup,
            announcement_url=url,
        )

        selected_attachments = (
            select_relevant_attachments(
                announcement_title=announcement_title,
                event_narrative=event_narrative,
                attachments=attachments,
            )
        )

        attachment_context = []

        for attachment in selected_attachments:
            parsed_attachment = (
                parse_attachment_pdf(
                    attachment
                )
            )

            if parsed_attachment is None:
                continue

            attachment_context.append(
                parsed_attachment
            )

        payload = {
            "company_name": issuer_section.get(
                "Issuer/ Manager"
            ),
            "symbol": symbol,
            "source": url,
            "timestamp": parse_broadcast_datetime(
                announcement_section.get(
                    "Date &Time of Broadcast"
                )
            ),
            "reference": announcement_section.get(
                "Corporate Action Reference"
            ),
            "announcement_title": announcement_title,
            "status": announcement_section.get(
                "Status"
            ),
            "offer_type": parse_offer_type(
                announcement_title
            ),
            "percentage_sought": (
                announcement_section.get(
                    "Percentage Sought (%)"
                )
            ),
            "event_narrative": event_narrative,
            "record_date": safe_convert_datetime(
                event_dates_section.get(
                    "Record Date"
                )
            ),
            "ex_date": safe_convert_datetime(
                event_dates_section.get(
                    "Ex Date"
                )
            ),
            "disbursement_options": (
                extract_disbursement_options(
                    soup
                )
            ),
            "withdrawal_reason": (
                withdrawal_section.get(
                    "Reason(s) for Withdrawal"
                )
            ),
            "attachments": attachments,
            "attachment_context": (
                attachment_context
            ),
        }

        return payload

    except Exception as error:
        LOGGER.error(
            "Failed to parse takeover announcement %s: %s",
            url,
            error,
            exc_info=True,
        )
        return None


if __name__ == "__main__":
    test_url = (
        "https://links.sgx.com/1.0.0/"
        "corporate-announcements/KIPEZ09JTLRFG3I8"
    )

    result = get_takeover(
        url=test_url
    )

    print(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        )
    )


# uv run -m sgx_scraper.fetch_takeover.parser
