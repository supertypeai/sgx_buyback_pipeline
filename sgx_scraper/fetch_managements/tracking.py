from rapidfuzz import fuzz

from sgx_scraper.fetch_managements.appointment import get_appointment 
from sgx_scraper.fetch_managements.cessation import get_cessation

import logging 


LOGGER = logging.getLogger(__name__)


def get_management_update(
    api_response: dict, 
    management_by_symbol: dict[str, dict],
) -> dict | None:
    registry = {
        "announcement of appointment": get_appointment, 
        "announcement of cessation": get_cessation
    }

    category = api_response.get("category_name", "").strip().lower()
    handler = registry.get(category)

    if not handler:
        LOGGER.warning("[TRACKING MANAGEMENT] unknown category: %s", category)
        return None
    
    announcement = handler(api_response=api_response)

    if announcement is None:
        LOGGER.warning(
            "[TRACKING MANAGEMENT] get appointment/cessation returned None for category: %s url: %s",
            category,
            api_response.get("url"),
        )
        return None

    symbol = announcement.get("symbol")

    company_record = management_by_symbol.get(symbol)

    if company_record is None:
        LOGGER.warning(
            "[TRACKING MANAGEMENT] no company record found for symbol: %s",
            symbol,
        )
        return None

    management_db_record = company_record.get("management")

    if management_db_record is None:
        LOGGER.info(
            "[TRACKING MANAGEMENT] announcement type: %s | symbol: %s | do not have management payload, " \
            "returning None",
            category,
            symbol,
        )
        return None  

    former_managements = company_record.get("former_management")

    if former_managements is None:
        former_managements = []
        company_record["former_management"] = former_managements

    if category == "announcement of appointment":
        management_db_record.append({
            "name": announcement.get("name"),
            "position": announcement.get("position"),
            "age": announcement.get("age"),
            "start_date": announcement.get("start_date"),
        })
        
        LOGGER.info(
            "[TRACKING MANAGEMENT] added new diretor name: %s | positioon: %s | symbol: %s",
            announcement.get("name"),
            announcement.get("position"),
            symbol,
        )

    elif category == "announcement of cessation":
        matched = False

        for record_index, record in enumerate(management_db_record):
            score = fuzz.ratio(
                record.get("name", ""), 
                announcement.get("name", "")
            )

            if score >= 90:
                LOGGER.info(
                    "[cessation] matched %s -> %s (score: %s) for %s",
                    announcement.get("name"),
                    record.get("name"),
                    score,
                    symbol,
                )

                former_record = management_db_record.pop(record_index)
                former_record["end_date"] = announcement.get("end_date")
                announcement_start_date = announcement.get("start_date")

                if announcement_start_date is not None:
                    former_record["start_date"] = announcement_start_date  # override from management record

                former_managements.append(former_record)

                matched = True
                break

        if not matched:
            LOGGER.warning(
                "[MANAGEMENT] no match found for name: %s | symbol: %s",
                announcement.get("name"),
                symbol,
            )

    return {
        "management_record": {
            "symbol": symbol,
            "management": management_db_record,
            "former_management": former_managements,
        },
        "announcement": {
            **announcement,
            "company_name": company_record["name"],
            "category": (
                "appointment"
                if category == "announcement of appointment"
                else "cessation"
            ),
        },
    }
