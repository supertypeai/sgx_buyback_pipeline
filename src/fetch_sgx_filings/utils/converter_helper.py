import requests 

from src.config.settings import LOGGER


def get_latest_currency(currency_from: str) -> float | None:
    try:
        response = requests.get(f"https://api.frankfurter.app/latest?from={currency_from.upper()}&to=SGD", timeout=10)
        response.raise_for_status()
        
        data = response.json()
        rate = data["rates"]["SGD"]
        return rate

    except requests.exceptions.RequestException as error:
        LOGGER.error(f"[get_latest_currency] Network error occurred: {error}")
        return None

    except (KeyError, ValueError) as error:
        LOGGER.error(f"[get_latest_currency] Error parsing response: {error}")
        return None


def calculate_currency_to_sgd(currency_from: float, rate_sgd: float) -> float | None:
    try:
        converted = currency_from * rate_sgd
        converted = round(converted, 2)
        return converted 
    except Exception as error:
        LOGGER.error(f"[calculate_currency_to_sgd] Error converting currency: {error}")
        return None  