from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException
from datetime import datetime, timedelta
from typing import Iterator
from urllib.parse import quote, unquote, urlencode

from sgx_scraper.config.settings import PROXY
from sgx_scraper.utils.json_helper import write_json
from sgx_scraper.utils.date_helper import normalize_datetime

import codecs
import json
import logging
import time 
import random 


LOGGER = logging.getLogger(__name__)

APP_CONFIG_URL = "https://www.sgx.com/config/appconfig.json"

MAX_ATTEMPTS = 3


def get_with_retry(
    url: str,
    headers: dict[str, str] | None = None,
    proxy: str | None = None,
) -> cffi_requests.Response:
    proxies = {"http": proxy, "https": proxy} if proxy else None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = cffi_requests.get(
                url,
                headers=headers,
                proxies=proxies,
                impersonate="chrome",
                verify=not proxy,
                timeout=30,
            )

            if response.status_code >= 500:
                response.raise_for_status()

            return response

        except RequestException as error:
            if attempt == MAX_ATTEMPTS:
                raise

            LOGGER.warning(
                "Request failed on attempt %d/%d, retrying: %s",
                attempt,
                MAX_ATTEMPTS,
                error,
            )
            time.sleep(attempt * 5)


def get_token() -> str:
    # SGX web app reads the token from its CMS and ROT13 decodes it, we do the same
    config_response = get_with_retry(APP_CONFIG_URL)
    config_response.raise_for_status()
    config = config_response.json()

    cms_url = config["endpoints"]["CMS_API_URL"]
    cms_version = config["CMS_VERSION"]
    token_url = f"{cms_url}/?queryId={cms_version}:we_chat_qr_validator"

    token_response = get_with_retry(token_url)
    token_response.raise_for_status()

    raw_token = token_response.json()["data"]["qrValidator"]

    return codecs.encode(raw_token, "rot13")


def get_auth() -> dict[str, str] | None:
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://www.sgx.com",
        "referer": "https://www.sgx.com/",
    }

    try:
        token = get_token()

        headers["authorizationtoken"] = token
        LOGGER.info("Token captured: %s...", token[:20])

        return headers

    except Exception as error:
        LOGGER.error("FAILED getting auth token: %s", error, exc_info=True)
        return None


def fetch_announcements_page(
    api_url: str,
    flag_log: str,
    headers: dict[str, str] | None,
    proxy: str | None = None,
) -> list[dict] | None:
    if not headers:
        raise ValueError("Cannot fetch API, headers are missing.")

    try:
        LOGGER.info("[%s] Fetching data from API", flag_log)

        response = get_with_retry(api_url, headers=headers, proxy=proxy)

        # Token expired, refresh once and update headers in place so next pages reuse it
        if response.status_code == 401:
            LOGGER.info("Token expired, refreshing")
            headers["authorizationtoken"] = get_token()

            response = get_with_retry(api_url, headers=headers, proxy=proxy)

        response.raise_for_status()

        LOGGER.info(
            "[%s] Response status: %s", 
            flag_log, 
            response.status_code
        )

        data = response.json()

        announcements = data.get("data")

        # API returns data null once the page is past the last result
        if announcements is None:
            LOGGER.info(
                "[%s] No announcements on this page", 
                flag_log
            )
            return []

        LOGGER.info(
            "[%s] Fetched %d announcements", 
            flag_log, 
            len(announcements)
        )

        return announcements

    except RequestException as error:
        LOGGER.error("[%s] API request failed: %s", flag_log, error)
        if "response" in locals():
            LOGGER.error("Response: %s", response.text[:200])
        raise

    except json.JSONDecodeError as error:
        LOGGER.error("[%s] JSON decode error: %s", flag_log, error)
        LOGGER.error("Response text: %s", response.text)
        return None


def iter_sgx_announcements(
    sub_category: str,
    flag_log: str,
    period_start: str | None = None,
    period_end: str | None = None,
    page_size: int = 20,
    is_proxy: bool | None = None,
    category: str = "ANNC",
    company: str | None = None,
) -> Iterator[dict]:
    """
    Paginate the SGX announcements API and yield one announcement at a time.

    Owns the shared scraping skeleton (auth, date defaulting, URL construction,
    pagination, per-page throttle). Each pipeline supplies its category/sub-category
    and keeps its own per-record handling by iterating the yielded announcements.
    """
    logger = logging.getLogger(__name__)

    api_url = "https://api.sgx.com/announcements/v1.1/"

    today = datetime.now()
    yesterday = today - timedelta(days=2)

    start_date_source = period_start if period_start is not None else yesterday
    end_date_source = period_end if period_end is not None else today

    normalized_start = normalize_datetime(start_date_source)
    normalized_end = normalize_datetime(end_date_source)

    logger.info(
        "Start scraping from start date: %s to %s",
        normalized_start, 
        normalized_end
    )

    # SGX day D runs from (D-1) 16:00 to D 15:59:59, so the range starts on the previous evening
    start_evening = datetime.strptime(normalized_start, "%Y%m%d") - timedelta(days=1)
    normalized_start = start_evening.strftime("%Y%m%d")

    headers = get_auth()

    page_start = 0

    while True:
        logger.info("page_start: %d", page_start)

        try:
            # Some callers pass several sub-categories already encoded
            query_parameters = {
                "periodstart": f"{normalized_start}_160000",
                "periodend": f"{normalized_end}_155959",
                "cat": category,
                "sub": unquote(sub_category),
                "exactsearch": "true",
                "pagestart": page_start,
                "pagesize": page_size,
            }

            if company:
                query_parameters["value"] = company

            query_string = urlencode(
                query_parameters, 
                quote_via=quote
            )

            url = f"{api_url}company?{query_string}"

            announcements = fetch_announcements_page(
                api_url=url,
                flag_log=flag_log,
                headers=headers,
                proxy=PROXY if is_proxy else None,
            )

            if not announcements:
                logger.info(
                    "No more announcements found, stopping pagination."
                )
                break

        except Exception as error:
            logger.error(
                "[%s] Fatal API error on page %d: %s", 
                flag_log,
                page_start, 
                error,
                exc_info=True
            )
            raise

        yield from announcements

        page_start += 1
        time.sleep(random.uniform(0.5, 2))
