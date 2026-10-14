from bs4 import BeautifulSoup

from sgx_scraper.utils.json_helper import open_json

from datetime import date, datetime
from functools import lru_cache

import logging
import requests


LOGGER = logging.getLogger(__name__)

BASE_URL = "https://eservices.mas.gov.sg/Statistics/msb/ExchangeRates.aspx"

# Quarter-end MAS rates kept in the repository and shared with the buyback
# and REIT transaction pipelines
QUARTERLY_RATES_PATH = "data/quarterly_rates.json"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL,
}

MAS_CURRENCIES = {
    "EUR": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$0",
        "Euro",
        1,
    ),
    "GBP": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$1",
        "Pound Sterling",
        1,
    ),
    "USD": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPerUnitCheckBoxList$2",
        "US Dollar",
        1,
    ),
    "AUD": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$0",
        "Australian Dollar",
        100,
    ),
    "CAD": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$1",
        "Canadian Dollar",
        100,
    ),
    "CNY": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$2",
        "Chinese Renminbi",
        100,
    ),
    "HKD": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$3",
        "Hong Kong Dollar",
        100,
    ),
    "INR": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$4",
        "Indian Rupee",
        100,
    ),
    "IDR": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$5",
        "Indonesian Rupiah",
        100,
    ),
    "JPY": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$6",
        "Japanese Yen",
        100,
    ),
    "KRW": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$7",
        "Korean Won",
        100,
    ),
    "MYR": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$8",
        "Malaysian Ringgit",
        100,
    ),
    "TWD": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$9",
        "New Taiwan Dollar",
        100,
    ),
    "NZD": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$10",
        "New Zealand Dollar",
        100,
    ),
    "PHP": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$11",
        "Philippine Peso",
        100,
    ),
    "QAR": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$12",
        "Qatar Riyal",
        100,
    ),
    "SAR": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$13",
        "Saudi Arabia Riyal",
        100,
    ),
    "CHF": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$14",
        "Swiss Franc",
        100,
    ),
    "THB": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$15",
        "Thai Baht",
        100,
    ),
    "AED": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$16",
        "UAE Dirham",
        100,
    ),
    "VND": (
        "ctl00$ContentPlaceHolder1$EndOfPeriodPer100UnitsCheckBoxList$17",
        "Vietnamese Dong",
        100,
    ),
}


def parse_exchange_rate_date(
    exchange_rate_date: str | None
) -> date | None:
    if exchange_rate_date is None:
        return date.today()

    try:
        return date.fromisoformat(exchange_rate_date[:10])
    
    except ValueError:
        LOGGER.error(
            "[currency_converter] Invalid exchange-rate date: %s",
            exchange_rate_date,
        )
        return None


@lru_cache(maxsize=1)
def load_quarterly_rates() -> dict[date, dict]:
    try:
        rates = open_json(QUARTERLY_RATES_PATH)

    except (FileNotFoundError, ValueError):
        rates = None

    quarters = rates.get("quarters") if isinstance(rates, dict) else None

    if not quarters:
        LOGGER.warning("[currency_converter] No rates loaded from %s", QUARTERLY_RATES_PATH)
        return {}

    return {
        date.fromisoformat(quarter_end): table
        for quarter_end, table in quarters.items()
    }


def get_quarterly_rate(currency_code: str, selected_date: date) -> float | None:
    # The SGD rate at the quarter-end nearest to the date, or None when the
    # file does not have the currency
    quarterly_rates = load_quarterly_rates()

    if not quarterly_rates:
        return None

    nearest_quarter_end = min(
        quarterly_rates,
        key=lambda quarter_end: abs(quarter_end - selected_date),
    )

    return (quarterly_rates[nearest_quarter_end].get(currency_code) or {}).get("SGD")


@lru_cache(maxsize=None)
def get_latest_currency(
    currency_from: str,
    exchange_rate_date: str | None = None,
) -> float | None:
    # The rates file is used first. A currency it does not have is looked up
    # on the MAS site, which is where the file itself comes from
    currency_code = currency_from.upper()
    selected_date = parse_exchange_rate_date(exchange_rate_date)

    if selected_date is None:
        return None

    if currency_code == "SGD":
        return 1.0

    quarterly_rate = get_quarterly_rate(currency_code, selected_date)

    if quarterly_rate is not None:
        return quarterly_rate

    LOGGER.info(
        "[currency_converter] %s is not in %s, using the MAS site",
        currency_code,
        QUARTERLY_RATES_PATH,
    )

    return get_mas_rate(currency_code, selected_date)


def get_mas_rate(currency_code: str, selected_date: date) -> float | None:
    currency_config = MAS_CURRENCIES.get(currency_code)

    if currency_config is None:
        LOGGER.warning("[currency_converter] Unsupported MAS currency: %s", currency_code)
        return None

    checkbox_name, currency_name, units = currency_config

    try:
        with requests.Session() as session:
            response = session.get(
                BASE_URL,
                headers=DEFAULT_HEADERS,
                timeout=30,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            form = soup.find("form", id="form1")

            if form is None:
                LOGGER.error("[currency_converter] MAS exchange-rate form was not found")
                return None

            request_data = {
                input_tag["name"]: input_tag.get("value", "")
                for input_tag in form.select('input[type="hidden"][name]')
            }
            request_data.update(
                {
                    "ctl00$ContentPlaceHolder1$StartYearDropDownList": str(
                        selected_date.year
                    ),
                    "ctl00$ContentPlaceHolder1$EndYearDropDownList": str(
                        selected_date.year
                    ),
                    "ctl00$ContentPlaceHolder1$StartMonthDropDownList": str(
                        selected_date.month
                    ),
                    "ctl00$ContentPlaceHolder1$EndMonthDropDownList": str(
                        selected_date.month
                    ),
                    "ctl00$ContentPlaceHolder1$FrequencyDropDownList": "D",
                    checkbox_name: "on",
                    "ctl00$ContentPlaceHolder1$DisplayButton": "Display",
                }
            )

            response = session.post(
                BASE_URL,
                headers=DEFAULT_HEADERS,
                data=request_data,
                timeout=30,
            )
            response.raise_for_status()

    except requests.RequestException as error:
        LOGGER.error("[currency_converter] MAS request failed: %s", error)
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    rate_table = next(
        (
            table
            for table in soup.find_all("table")
            if currency_name in table.get_text(" ", strip=True)
        ),
        None,
    )

    if rate_table is None:
        LOGGER.error("[currency_converter] MAS rate table was not found for %s", currency_code)
        return None

    rates = []
    current_year = None
    current_month = None

    for row in rate_table.find_all("tr"):
        cells = row.find_all(["th", "td"])

        if len(cells) != 4:
            continue

        year_text = cells[0].get_text(" ", strip=True)
        month_text = cells[1].get_text(" ", strip=True)
        day_text = cells[2].get_text(" ", strip=True)
        rate_text = cells[3].get_text(" ", strip=True).replace(",", "")

        if year_text:
            current_year = int(year_text)

        if month_text:
            current_month = datetime.strptime(month_text, "%b").month

        try:
            rate_date = date(current_year, current_month, int(day_text))
            rate = float(rate_text)
        except (TypeError, ValueError):
            continue

        if rate_date <= selected_date:
            rates.append(rate)

    if not rates:
        LOGGER.error(
            "[currency_converter] MAS returned no rate on or before %s for %s",
            selected_date,
            currency_code,
        )
        return None

    return rates[-1] / units


def calculate_currency_to_sgd(
    amount: float,
    rate_sgd: float,
    digits: int = 2,
) -> float | None:
    try:
        return round(amount * rate_sgd, digits)
    
    except TypeError as error:
        LOGGER.error("[currency_converter] Failed converting currency: %s", error)
        return None


if __name__ == "__main__":
    rate = get_latest_currency(
        currency_from="USD",
        exchange_rate_date="2026-03-13"
    )

    result = calculate_currency_to_sgd(
        amount=0.9991, 
        rate_sgd=rate 
    )

    print(result)
