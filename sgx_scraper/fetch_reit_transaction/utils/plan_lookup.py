from datetime import date, timedelta

from sgx_scraper.fetch_reit_transaction.constant import (
    FINANCING_TITLE_PATTERN,
    PLAN_LOOKBACK_DAYS,
    SUB_CATEGORY,
)
from sgx_scraper.fetch_reit_transaction.parser import extract_properties
from sgx_scraper.fetch_reit_transaction.utils.payload_helper import is_same_property
from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements

import logging
import re


LOGGER = logging.getLogger(__name__)

FINANCING_TITLE = re.compile(FINANCING_TITLE_PATTERN, re.IGNORECASE)


def is_completion(title: str) -> bool:
    subtitle = (title or "").split("::", 1)[-1].lower()
    return "completion" in subtitle or "completed" in subtitle


def is_financing(title: str) -> bool:
    return bool(FINANCING_TITLE.search((title or "").split("::", 1)[-1]))


def find_plan_property(
    company: str,
    property_name: str,
    completed_on: date,
    model_name: str,
    is_proxy: bool | None = None,
) -> tuple[dict, str] | None:
    """A completion announcement usually confirms the deal without restating the
    money, so the earlier plan announcement is fetched on demand. Measured lag
    between the two peaks at 195 days.
    """
    window_start = completed_on - timedelta(days=PLAN_LOOKBACK_DAYS)

    candidates = iter_sgx_announcements(
        sub_category=SUB_CATEGORY,
        flag_log="REIT Transaction Plan",
        period_start=window_start.strftime("%Y%m%d"),
        period_end=completed_on.strftime("%Y%m%d"),
        company=company,
        is_proxy=is_proxy,
    )

    best = None

    for announcement in candidates:
        title = announcement.get("title") or ""
        detail_url = announcement.get("url")

        if not detail_url or is_completion(title) or is_financing(title):
            continue

        for candidate in extract_properties(detail_url, model_name):
            if not is_same_property(property_name, candidate.get("property_name")):
                continue

            broadcast = announcement.get("submission_date") or ""

            if best is None or broadcast > best[0]:
                best = (broadcast, candidate, detail_url)

    if best is None:
        LOGGER.info(f"[REIT TRANSACTION] No plan announcement found for {property_name}")
        return None

    return best[1], best[2]
