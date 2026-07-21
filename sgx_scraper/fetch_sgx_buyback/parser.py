from bs4 import BeautifulSoup
from dataclasses import asdict
from dateutil.relativedelta import relativedelta
from datetime import datetime

from sgx_scraper.fetch_sgx_buyback.models import SGXBuyback
from sgx_scraper.fetch_sgx_buyback.utils.payload_helper import (
    build_price_per_share,
    compute_mandate_remaining,
    safe_extract_value,
    safe_extract_fallback,
    safe_convert_float,
)
from sgx_scraper.utils.symbol_matching_helper import extract_symbol, matching_symbol
from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.utils.sgx_announcement_html import extract_section_data
from sgx_scraper.utils.http_client import HTTPCLIENT

import requests
import json
import logging


LOGGER = logging.getLogger(__name__)


def resolve_symbol(issuer_section: dict) -> str | None:
    issuer_security = issuer_section.get("Securities")
    symbol = extract_symbol(issuer_security)

    if not symbol:
        issuer_name = issuer_section.get("Issuer/ Manager")
        symbol = matching_symbol(issuer_name)
    
    return symbol 


def get_buyback_type(section_a: dict[str], section_b: dict[str]) -> str | None:
    on_market = section_a.get("Purchase made by way of market acquisition")
    off_market = section_b.get("Purchase made by way of off-market acquisition on equal access scheme")

    if on_market == "Yes" and off_market == "No":
        return "On Market"
    elif on_market == "No" and off_market == "Yes":
        return "Off Market"
    
    return None 


def parse_date(section_a: dict[str], section_b: dict[str], additional_detail: dict[str]) -> tuple:
    purchase_date = safe_extract_fallback("Date of Purchase", section_a, section_b)
    purchase_date = safe_convert_datetime(purchase_date)

    start_date_raw = additional_detail.get("Start date for mandate of daily share buy-back")
    start_date = safe_convert_datetime(start_date_raw)

    return purchase_date, start_date


def parse_prices(section_a: dict[str], section_b: dict[str]) -> dict[str, float]:
    price_paid_per_share = safe_extract_fallback("Price Paid per share", section_a, section_b)
    
    if not price_paid_per_share:
        price_paid_per_share = safe_extract_fallback("Price Paid or Payable per Share", section_a, section_b)

    highest_per_share = safe_extract_fallback("Highest Price per share", section_a, section_b)
    lowest_per_share = safe_extract_fallback("Lowest Price per share", section_a, section_b)

    price_per_share = build_price_per_share(price_paid_per_share, highest_per_share, lowest_per_share)

    return price_per_share


def parse_total_value(
    section_a: dict[str], section_b: dict[str], 
    section_c: dict[str], section_d: dict[str]
) -> tuple:
    # Total mandate 
    total_mandate = section_a.get('Maximum number of shares authorised for purchase')
    total_mandate = safe_convert_float(total_mandate)

    # Total shares purchased
    total_share_purchased = safe_extract_fallback("Total Number of shares purchased", section_a, section_b)
    total_share_purchased = safe_convert_float(total_share_purchased)

    # Cumulative shares purchased
    cumulative_raw = section_c.get("Total")
    cumulative_share_purchased = safe_extract_value(cumulative_raw)
    cumulative_share_purchased = safe_convert_float(cumulative_share_purchased)

    # Total consideration
    total_consideration = safe_extract_fallback("Total Consideration", section_a, section_b)
    total_consideration = safe_convert_float(total_consideration)

    # Treasury shares after purchase
    treasury_shares_after_purchase_raw = section_d.get("Number of treasury shares held after purchase")
    treasury_shares_after_purchase = safe_convert_float(treasury_shares_after_purchase_raw)

    return total_share_purchased, cumulative_share_purchased, total_consideration, treasury_shares_after_purchase, total_mandate


def extract_all_fields(soup: BeautifulSoup, url: str) -> dict[str, any] | None:
    try:
        # Extract sections
        issuer_section = extract_section_data(soup, "Issuer & Securities")
        additional_detail = extract_section_data(soup, "Additional Details")

        section_a = extract_section_data(soup, "Section A")
        section_b = extract_section_data(soup, "Section B")
        section_c = extract_section_data(soup, "Section C")
        section_d = extract_section_data(soup, "Section D")

        # Symbol extraction
        symbol = resolve_symbol(issuer_section)

        # Buyback type
        buy_back_type = get_buyback_type(section_a, section_b)

        # Dates
        purchase_date, start_date = parse_date(section_a, section_b, additional_detail)

        # Prices
        price_per_share = parse_prices(section_a, section_b)

        # Total shares purchased, Cumulative shares purchased, 
        # Total consideration, Treasury shares after purchase, Total Mandate
        buy_total, buy_cum, cost_total, treasury_after, mandate_total = parse_total_value(
            section_a, section_b, section_c, section_d
        )

        # Computer mandate remaining 
        mandate_remaining = compute_mandate_remaining(mandate_total, buy_cum)

        # Mandate end 
        start_date_object = datetime.strptime(start_date, "%Y-%m-%d")
        mandate_end = start_date_object + relativedelta(years=1)
        mandate_end = mandate_end.strftime("%Y-%m-%d")

        buyback_payload = {
            "symbol": symbol,
            "purchase_date": purchase_date,
            "type": buy_back_type,
            "price_per_share": price_per_share,
            "total_value": cost_total,
            "total_shares_purchased": buy_total,
            "treasury_shares_after_purchase": treasury_after,
        }

        buyback_payload['mandate'] = {
            "mandate_total": mandate_total,
            "cumulative_purchased": buy_cum, 
            "mandate_remaining": mandate_remaining, 
            "mandate_start": start_date,
            "mandate_end": mandate_end
        }

        return buyback_payload
 
    except Exception as error:
        LOGGER.error(f"[extract_buyback_fields] Error parsing SGX buyback at {url}: {error}", exc_info=True)
        return None


def get_sgx_buybacks(url: str) -> SGXBuyback: 
    try:
        print(f"Extracting detail buyback for {url}")

        response = HTTPCLIENT.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        data_extracted = extract_all_fields(soup=soup, url=url)
        if not data_extracted:
            return None
        
        sgx_buybacks = SGXBuyback(url=url, **data_extracted)

        print(json.dumps(asdict(sgx_buybacks), indent=2))
        return sgx_buybacks
    
    except requests.RequestException as error:
        LOGGER.error(f"[sgx buyback] Error fetching SGX buyback for url {url}: {error}", exc_info=True)
        return None
    
    except Exception as error:
        LOGGER.error(f"[sgx buyback] Unexpected Error extracting SGX buyback: {error}", exc_info=True)
        raise None


if __name__ == '__main__':
    test_url = 'https://links.sgx.com/1.0.0/corporate-announcements/IZHHDYW20N2Q5EPO/632276817f55ea1bc49fb3b9137351ce3117adca0630b958cb5c5e6d76b90dd8'
    result = get_sgx_buybacks(test_url)
    # print(json.dumps(asdict(result), indent=2))

    # uv run -m sgx_scraper.fetch_sgx_buyback.parser_sgx_buyback