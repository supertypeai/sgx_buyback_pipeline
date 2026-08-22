from bs4 import BeautifulSoup
from datetime import datetime, timezone

from sgx_scraper.utils.sgx_announcement_html import extract_section_data
from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.utils.json_helper import open_json
from sgx_scraper.utils.symbol_matching_helper import add_sgx_suffix, lookup_company_by_symbol
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.fetch_upcoming_dividend.utils.fx_rates_client import fetch_compact_rates

import json
import re
import logging
import time 


LOGGER = logging.getLogger(__name__)


def parse_rate(raw_rate: str) -> dict[str, str | float] | None:
    if not raw_rate:
        return None

    match = re.match(r'([A-Z]{3})\s+([\d,.]+)', raw_rate.strip())

    if not match:
        return None

    currency, value = match.groups()

    return {
        'currency': currency,
        'value': float(value.replace(',', '')),
    }


def parse_broadcast_datetime(raw_datetime: str) -> str | None:
    if not raw_datetime:
        return None

    try:
        parsed = datetime.strptime(raw_datetime.strip(), '%d-%b-%Y %H:%M:%S')
        return parsed.strftime('%Y-%m-%d %H:%M:%S')

    except ValueError as error:
        LOGGER.warning(f'[parse_broadcast_datetime] Error: {error} for value {raw_datetime!r}')
        return None


def parse_exchange_rate(raw_exchange_rate: str) -> float | None:
    if not raw_exchange_rate:
        return None

    try:
        return float(raw_exchange_rate.strip().replace(',', ''))

    except ValueError as error:
        LOGGER.warning(f'[parse_exchange_rate] Error: {error} for value {raw_exchange_rate!r}')
        return None


def update_dividend_currency(records: dict[str, any] | None) -> float | None:
    dividend_amount = records.get("dividend_amount")

    if not dividend_amount:
        return None

    currency = dividend_amount.get("currency")
    value = dividend_amount.get("value")

    if not currency or value is None:
        return None

    if currency.lower() == 'sgd':
        return value

    compact_rates = fetch_compact_rates()

    if currency not in compact_rates:
        LOGGER.warning(
            "No SGD rate for currency=%s, ref=%s, skipping conversion",
            currency,
            records.get('reference'),
        )
        return None

    sgd_exchange = compact_rates[currency].get("SGD")

    if sgd_exchange is None:
        LOGGER.warning(
            "No SGD conversion for currency=%s, ref=%s, skipping conversion",
            currency,
            records.get('reference'),
        )
        return None

    return value * sgd_exchange


def check_companies(record: dict[str, any]) -> dict[str, any] | None:
    companies_db = open_json("data/sgx_companies.json")
    symbol = record.get('symbol')

    if not lookup_company_by_symbol(companies_db, symbol):
        LOGGER.info(
            'Skipping symbol: %s not in sgx_companies json', 
            symbol
        )
        return None

    return record


def extract_all_fields(soup: BeautifulSoup, url: str) -> dict[str, any] | None:
    try:
        issuer_section = extract_section_data(soup, "Issuer & Securities")
        announcement_section = extract_section_data(soup, "Announcement Details")
        event_dates_section = extract_section_data(soup, "Event Dates")
        dividend_section = extract_section_data(soup, "Dividend Details")

        security = issuer_section.get("Security")
        symbol = add_sgx_suffix(security.rsplit(" - ", 2)[-1])

        return {
            "symbol": symbol,
            "reference": announcement_section.get("Corporate Action Reference"),
            "recording_date": safe_convert_datetime(event_dates_section.get("Record Date")),
            "ex_date": safe_convert_datetime(event_dates_section.get("Ex Date")),
            "dividend_amount": parse_rate(dividend_section.get("Gross Rate (Per Share)")),
            "payment_date": safe_convert_datetime(dividend_section.get("Pay Date")),
        }

    except Exception as error:
        LOGGER.error(f"[fetch_upcoming_dividend] Error parsing dividend fields at {url}: {error}", exc_info=True)
        return None


def get_upcoming_dividend(url: str) -> dict[str, any] | None:
    response = HTTPCLIENT.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    records = extract_all_fields(soup=soup, url=url)

    if records is None:
        return None

    records = check_companies(record=records)

    if records is None:
        return None

    records["dividend_amount"] = update_dividend_currency(records=records)
    records["updated_on"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    time.sleep(1.5)
    return records


if __name__ == '__main__':
    url = 'https://links.sgx.com/1.0.0/corporate-announcements/JYG9YK3K7DNB6CLZ/d4720c1aa336a298ec6bb95249aa3a97afbb4940ae571bc4e82fcd9f64e81066'
    url_repl = 'https://links.sgx.com/1.0.0/corporate-announcements/CLBLLDCB0QFV0KHF/2a69420374716d836bfa2d2517a1a3db47ba13cc4b7a6532faf373dffd9c96ee'
    url_valid = 'https://links.sgx.com/1.0.0/corporate-announcements/Q7B5F0HE8XFHUPHM/17d56560f33e5da4d2a2b563dd544e119c77bff8e2a359ee74128f7d5fbb5f05'

    result = get_upcoming_dividend(url_valid)

    print(json.dumps(result, indent=2))


# uv run -m sgx_scraper.fetch_upcoming_dividend.parser
