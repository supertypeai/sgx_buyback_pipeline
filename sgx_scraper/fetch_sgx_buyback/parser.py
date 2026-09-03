from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from datetime import datetime

from sgx_scraper.fetch_sgx_buyback.models import SGXBuyback
from sgx_scraper.utils.cli_helper import get_db
from sgx_scraper.fetch_sgx_buyback.utils.payload_helper import (
    build_price_per_share,
    compute_mandate_remaining,
    safe_extract_value,
    safe_extract_fallback,
    safe_convert_float,
)
from sgx_scraper.utils.symbol_matching_helper import matching_symbol
from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.utils.json_helper import open_json
from sgx_scraper.utils.sgx_announcement_html import extract_section_data
from sgx_scraper.utils.http_client import HTTPCLIENT

import logging


LOGGER = logging.getLogger(__name__)


def resolve_symbol(issuer_name: str, issuers: list) -> str | None:
    symbol = matching_symbol(issuer_name)

    if not symbol: 
        issuers = issuers[0]
        symbol = issuers.get("stock_code")

    if not symbol:
        return None 
    
    return symbol 


def get_buyback_type(
    section_a: dict[str], 
    section_b: dict[str]
) -> str | None:
    on_market = section_a.get(
        "Purchase made by way of market acquisition"
    )
    off_market = section_b.get(
        "Purchase made by way of off-market acquisition on equal access scheme"
    )

    if on_market == "Yes" and off_market == "No":
        return "On Market"

    elif on_market == "No" and off_market == "Yes":
        return "Off Market"
    
    return None 


def parse_date(
    section_a: dict[str], 
    section_b: dict[str], 
    additional_detail: dict[str]
) -> tuple:
    purchase_date = safe_extract_fallback(
        "Date of Purchase", 
        section_a, 
        section_b
    )
    purchase_date = safe_convert_datetime(purchase_date)

    start_date_raw = additional_detail.get(
        "Start date for mandate of daily share buy-back"
    )
    start_date = safe_convert_datetime(start_date_raw)

    return purchase_date, start_date


def parse_prices(
    section_a: dict[str], 
    section_b: dict[str]
) -> dict[str, float | str]:
    price_paid_per_share = safe_extract_fallback(
        "Price Paid per share", 
        section_a, 
        section_b
    )
    
    if not price_paid_per_share:
        price_paid_per_share = safe_extract_fallback(
            "Price Paid or Payable per Share", 
            section_a, 
            section_b
        )

    highest_per_share = safe_extract_fallback(
        "Highest Price per share", 
        section_a, 
        section_b
    )
    lowest_per_share = safe_extract_fallback(
        "Lowest Price per share", 
        section_a, 
        section_b
    )

    price_per_share = build_price_per_share(
        price_paid_per_share, 
        highest_per_share, 
        lowest_per_share
    )

    return price_per_share


def parse_total_value(
    section_a: dict[str], section_b: dict[str], 
    section_c: dict[str], section_d: dict[str]
) -> tuple:
    # Total mandate 
    total_mandate = section_a.get(
        "Maximum number of shares authorised for purchase"
    )
    total_mandate = safe_convert_float(total_mandate)

    # Total shares purchased
    total_share_purchased = safe_extract_fallback(
        "Total Number of shares purchased", 
        section_a, 
        section_b
    )
    total_share_purchased = safe_convert_float(total_share_purchased)

    # Cumulative shares purchased
    cumulative_raw = section_c.get("Total")
    cumulative_share_purchased = safe_extract_value(cumulative_raw)
    cumulative_share_purchased = safe_convert_float(cumulative_share_purchased)

    # Total consideration
    total_consideration = safe_extract_fallback(
        "Total Consideration", 
        section_a, 
        section_b
    )
    total_consideration = safe_convert_float(total_consideration)

    # Treasury shares after purchase
    treasury_shares_after_purchase_raw = section_d.get(
        "Number of treasury shares held after purchase"
    )
    treasury_shares_after_purchase = safe_convert_float(
        treasury_shares_after_purchase_raw
    )

    return (
        total_share_purchased, 
        cumulative_share_purchased, 
        total_consideration, 
        treasury_shares_after_purchase, 
        total_mandate
    )


def convert_currency(record: dict) -> dict:
    rates = open_json("data/quarterly_rates.json")
    quarterly_rates = rates["quarters"]

    price = record["price_per_share"]
    currency = price.get("currency")

    if not currency:
        return record

    price_paid_per_share = price.get("price_paid_per_share")
    price_high = price.get("highest")
    price_low = price.get("lowest")
    purchase_date = record["purchase_date"]

    if currency != "SGD":
        purchase_date_object = datetime.fromisoformat(purchase_date).date()

        closest_quarter_end = min(
            quarterly_rates,
            key=lambda quarter_end: abs(
                datetime.fromisoformat(quarter_end).date()
                - purchase_date_object
            ),
        )

        rate = quarterly_rates[closest_quarter_end][currency]["SGD"]

        LOGGER.info(
            "Converting prices from %s to SGD using %s | url: %s",
            currency,
            closest_quarter_end,
            record["url"],
        )

        if price_paid_per_share is not None:
            price["price_paid_per_share"] = round(
                price_paid_per_share * rate,
                4,
            )

        elif price_high is not None and price_low is not None:
            price["highest"] = round(price_high * rate, 4)
            price["lowest"] = round(price_low * rate, 4)

    price.pop("currency", None)

    return record


def extract_all_fields(
    soup: BeautifulSoup, 
    url: str,
    reference: str,
    issuer_name: str, 
    issuers: list[dict]
) -> dict[str, any] | None:
    try:
        # Extract sections
        additional_detail = extract_section_data(soup, "Additional Details")

        section_a = extract_section_data(soup, "Section A")
        section_b = extract_section_data(soup, "Section B")
        section_c = extract_section_data(soup, "Section C")
        section_d = extract_section_data(soup, "Section D")

        title_tag = soup.find("h1")
        announcement_title = (
            title_tag.get_text(" ", strip=True)
            if title_tag
            else ""
        )

        # Symbol extraction
        symbol = resolve_symbol(
            issuer_name=issuer_name, 
            issuers=issuers
        )

        # Buyback type
        buy_back_type = get_buyback_type(section_a, section_b)

        # Dates
        purchase_date, start_date = parse_date(
            section_a, 
            section_b, 
            additional_detail
        )

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
            "reference": reference,
            "symbol": symbol,
            "title": announcement_title,
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

        return convert_currency(buyback_payload)
 
    except Exception as error:
        LOGGER.error(
            "[extract_buyback_fields] Error parsing SGX buyback at %s: %s",
            url,
            error, 
            exc_info=True
        )
        return None


def detect_replacement_announcement(
    incoming_record: dict,
) -> list[int]:
    reference = incoming_record["reference"]

    db_records = get_db(
        table="sgx_buybacks",
        query=lambda query: query.eq(
            "reference",
            reference,
        ),
    )

    return [
        record["id"]
        for record in db_records
    ]


def get_sgx_buybacks(
    url: str,
    reference: str,
    issuer_name: str, 
    issuers: list[dict]
) -> SGXBuyback: 
    try:
        response = HTTPCLIENT.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
    
        data_extracted = extract_all_fields(
            soup=soup, 
            url=url,
            reference=reference,
            issuer_name=issuer_name,
            issuers=issuers
        )

        if not data_extracted:
            return None
        
        sgx_buybacks = SGXBuyback(url=url, **data_extracted)

        return sgx_buybacks
    
    except Exception as error:
        LOGGER.error(
            "[sgx buyback] Unexpected Error extracting SGX buyback: %s",
            error, 
            exc_info=True
        )
        raise None

