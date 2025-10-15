import requests 

from src.config.settings import LOGGER


def get_latest_currency() -> float | None:
    try:
        response = requests.get("https://api.frankfurter.app/latest?from=USD&to=SGD", timeout=10)
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


def calculate_currency_to_sgd(rate_us: float, rate_sgd: float) -> float | None:
    try:
        converted = rate_us * rate_sgd
        converted = f"{converted:.2f}"
        return converted 
    except Exception as error:
        LOGGER.error(f"[calculate_currency_to_sgd] Error converting currency: {error}")
        return None  