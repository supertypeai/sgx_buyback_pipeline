from datetime import datetime

from sgx_scraper.fetch_agm.constant import (
    ELECTRONIC_VENUE_PATTERN,
    PHYSICAL_VENUE_PATTERN,
    PLACE_HYBRID,
    PLACE_ONLINE,
    PLACE_ONSITE,
)

import re


def split_meeting_datetime(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None

    matched = re.match(r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?", value)

    if not matched:
        return None, None

    try:
        parsed = datetime.strptime(matched.group(1), "%d/%m/%Y").date()

    except ValueError:
        return None, None

    return parsed.isoformat(), matched.group(2)


def resolve_place_desc(venue: str | None) -> str | None:
    venue = (venue or "").lower()

    is_electronic = bool(re.search(ELECTRONIC_VENUE_PATTERN, venue))
    is_physical = bool(re.search(PHYSICAL_VENUE_PATTERN, venue))

    if is_electronic and is_physical:
        return PLACE_HYBRID

    if is_electronic:
        return PLACE_ONLINE

    return PLACE_ONSITE if is_physical else None
