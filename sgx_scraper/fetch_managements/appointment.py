from bs4 import BeautifulSoup

from sgx_scraper.fetch_managements.utils.announcement_helper import (
    extract_field,
    extract_symbol,
    parse_broadcast_date,
    parse_appointment_date,
)
from sgx_scraper.utils.constant import HEADERS

import logging
import requests


LOGGER = logging.getLogger(__name__)


def get_appointment(api_response: dict) -> dict | None:
    url = api_response.get("url", "")

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    symbol = extract_symbol(api_response.get("issuers"))

    if not soup or not symbol:
        return None

    name = extract_field(soup, "Name of person")

    position = extract_field(
        soup,
        "Job title (e.g. Lead ID, AC Chairman, AC Member etc.)",
    )
    
    age = extract_field(soup, "Age")

    start_date = parse_appointment_date(
        extract_field(soup, "Date of appointment")
    )

    timestamp = extract_field(soup, "Date & Time of Broadcast")

    if timestamp is None:
        timestamp = extract_field(soup, "Date &Time of Broadcast")

    timestamp = parse_broadcast_date(timestamp)

    announcement_subtitle = extract_field(soup, "Announcement Sub Title")
    description = extract_field(
        soup,
        "Description (Please provide a detailed description of the event in the box below)",
    )
    executive_status = extract_field(
        soup,
        "Whether appointment is executive, and if so, the area of responsibility",
    )
    board_comments = extract_field(
        soup,
        "The Board's comments on this appointment (including rationale, selection criteria, board diversity considerations, and the search and nomination process)",
    )
    professional_qualifications = extract_field(
        soup,
        "Professional qualifications",
    )
    recent_work_experience = extract_field(
        soup,
        "Working experience and occupation(s) during the past 10 years",
    )
    shareholding_details = extract_field(soup, "Shareholding details")

    return {
        "symbol": symbol,
        "name": name,
        "position": position,
        "age": age,
        "start_date": start_date,
        "source": url,
        "timestamp": timestamp,
        "announcement_subtitle": announcement_subtitle,
        "description": description,
        "effective_date": start_date,
        "executive_status": executive_status,
        "board_comments": board_comments,
        "professional_qualifications": professional_qualifications,
        "recent_work_experience": recent_work_experience,
        "shareholding_details": shareholding_details,
    }
