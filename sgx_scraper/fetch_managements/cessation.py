from bs4 import BeautifulSoup 

from sgx_scraper.utils.constant import HEADERS
from sgx_scraper.fetch_managements.utils.announcement_helper import (
    extract_field, 
    extract_symbol, 
    parse_broadcast_date,
    parse_appointment_date
)

import requests
import logging


LOGGER = logging.getLogger(__name__)


def get_cessation(
    api_response: dict,
    fallback_symbol: str | None = None,
) -> dict | None:
    url = api_response.get('url', '')
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    if not soup: 
        return None  

    symbol = extract_symbol(api_response.get('issuers')) or fallback_symbol

    if not symbol:
        return None 

    name = extract_field(soup, 'Name of person')
    position = extract_field(soup, 'Job title (e.g. Lead ID, AC Chairman, AC Member etc.)')
    age = extract_field(soup, 'Age')
    end_date_raw = extract_field(soup, 'If yes, please provide the date')

    if end_date_raw is None:
        end_date_raw = extract_field(
            soup,
            'If yes, please provide the date.',
        )

    end_date = parse_appointment_date(end_date_raw)
    start_date = parse_appointment_date(extract_field(soup, "Date of appointment to current position")) 

    timestamp = extract_field(soup, "Date & Time of Broadcast")

    if timestamp is None:
        timestamp = extract_field(soup, "Date &Time of Broadcast")

    timestamp = parse_broadcast_date(timestamp)

    announcement_subtitle = extract_field(soup, "Announcement Sub Title")
    description = extract_field(
        soup,
        "Description (Please provide a detailed description of the event in the box below)",
    )

    effective_date_known_raw = extract_field(
        soup,
        "Is effective date of cessation known?",
    )

    if effective_date_known_raw is None:
        effective_date_known_raw = extract_field(
            soup,
            "Is effective date of cessation known",
        )

    effective_date_note = extract_field(
        soup,
        "If no, please advise when the date will be announced.",
    )
    cessation_reason = extract_field(soup, "Detailed reason(s) for cessation")
    unresolved_board_difference_raw = extract_field(
        soup,
        "Are there any unresolved differences in opinion on material matters "
        "between the person and the board of directors, including matters which "
        "would have a material impact on the group or its financial reporting?",
    )
    shareholder_attention_required_raw = extract_field(
        soup,
        "Is there any matter in relation to the cessation that needs to be "
        "brought to the attention of the shareholders of the listed issuer?",
    )
    other_relevant_information = extract_field(
        soup,
        "Any other relevant information to be provided to shareholders of the "
        "listed issuer?",
    )

    effective_date_known = None

    if effective_date_known_raw is not None:
        effective_date_known = effective_date_known_raw.strip().lower() == "yes"

    unresolved_board_difference = None

    if unresolved_board_difference_raw is not None:
        unresolved_board_difference = (
            unresolved_board_difference_raw.strip().lower() == "yes"
        )

    shareholder_attention_required = None

    if shareholder_attention_required_raw is not None:
        shareholder_attention_required = (
            shareholder_attention_required_raw.strip().lower() == "yes"
        )

    return {
        "symbol": symbol,
        "name": name,
        "position": position,
        "age": age,
        "end_date": end_date,
        "start_date": start_date,
        "source": url,
        "timestamp": timestamp,
        "announcement_subtitle": announcement_subtitle,
        "description": description,
        "effective_date_known": effective_date_known,
        "effective_date": end_date,
        "effective_date_note": effective_date_note,
        "cessation_reason": cessation_reason,
        "unresolved_board_difference": unresolved_board_difference,
        "shareholder_attention_required": shareholder_attention_required,
        "other_relevant_information": other_relevant_information,
    }
