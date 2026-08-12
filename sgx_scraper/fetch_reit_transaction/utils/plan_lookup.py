from datetime import date, timedelta

from sgx_scraper.fetch_reit_transaction.constant import (
    FINANCING_TITLE_PATTERN,
    MAX_UNNAMED_CANDIDATES,
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
) -> tuple[dict, dict] | None:
    """A completion announcement usually confirms the deal without restating the
    money, so the earlier plan announcement is fetched on demand. Measured lag
    between the two peaks at 195 days.

    The company filter matches SGX's issuer_name, which is the manager rather
    than the trust, so it is taken from the completion announcement itself.
    """
    if not company:
        return None

    window_start = completed_on - timedelta(days=PLAN_LOOKBACK_DAYS)

    candidates = iter_sgx_announcements(
        sub_category=SUB_CATEGORY,
        flag_log="REIT Transaction Plan",
        period_start=window_start.strftime("%Y%m%d"),
        period_end=completed_on.strftime("%Y%m%d"),
        company=company,
        is_proxy=is_proxy,
    )

    plans = [
        announcement
        for announcement in candidates
        if announcement.get("url")
        and not is_completion(announcement.get("title") or "")
        and not is_financing(announcement.get("title") or "")
    ]
    plans.sort(key=lambda item: item.get("submission_date") or "", reverse=True)

    # Titles usually name the property, so those are read first and the rest
    # only if none of them matched. Every candidate read costs an LLM call.
    named = [item for item in plans if is_same_property(property_name, item.get("title"))]

    for batch in (named, [item for item in plans if item not in named][:MAX_UNNAMED_CANDIDATES]):
        for announcement in batch:
            detail_url = announcement["url"]

            for candidate in extract_properties(detail_url, model_name):
                if is_same_property(property_name, candidate.get("property_name")):
                    return candidate, announcement

    LOGGER.info(f"[REIT TRANSACTION] No plan announcement found for {property_name}")

    return None
