from datetime import datetime

import logging


LOGGER = logging.getLogger(__name__)


def normalize_datetime(date: str | datetime) -> str:
    """
    Normalize a date to YYYYMMDD, the format the SGX announcements API expects.
    """
    if isinstance(date, datetime):
        return date.strftime("%Y%m%d")

    try:
        if '-' in date:
            dt = datetime.strptime(date, "%Y-%m-%d")
            
        else:
            dt = datetime.strptime(date, "%Y%m%d")

        return dt.strftime("%Y%m%d")

    except ValueError:
        LOGGER.error("Invalid date format. Use YYYY-MM-DD or YYYYMMDD.")
        return None


def to_iso_date(date: str | datetime) -> str | None:
    """
    Normalize a date to YYYY-MM-DD, the format Postgres date columns expect.
    """
    normalized = normalize_datetime(date)

    if not normalized:
        return None

    return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"


def safe_convert_datetime(date: str) -> str | None:
    """
    Parse a scraped date string in various formats to YYYY-MM-DD for storage.
    """
    if not date:
        return None

    try:
        date_str = date.strip()

        for format in ("%d/%m/%Y", "%d-%b-%Y", "%d %b %Y", "%d %B %Y"):
            try:
                parsed_date = datetime.strptime(date_str, format)

                if date_str[-4:-2] == "00":
                    parsed_date = parsed_date.replace(
                        year=parsed_date.year + 2000
                    )

                return parsed_date.strftime("%Y-%m-%d")

            except ValueError:
                continue

    except Exception as error:
        LOGGER.error(
            "[safe_convert_datetime] Error: %s | input date: %s"
        ),
        error,
        date,
        return None
