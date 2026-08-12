from datetime import date

from sgx_scraper.fetch_reit_transaction.constant import QUARTERLY_RATES_PATH
from sgx_scraper.utils.json_helper import open_json

import bisect
import logging


LOGGER = logging.getLogger(__name__)

_QUARTERS = None


def load_quarters() -> list[tuple[date, dict]]:
    global _QUARTERS

    if _QUARTERS is None:
        rates = open_json(QUARTERLY_RATES_PATH)
        quarters = rates.get("quarters", {}) if isinstance(rates, dict) else {}

        if not quarters:
            LOGGER.error(f"[REIT TRANSACTION] No rates loaded from {QUARTERLY_RATES_PATH}")

        _QUARTERS = sorted(
            (date.fromisoformat(quarter_end), table)
            for quarter_end, table in quarters.items()
        )

    return _QUARTERS


def to_sgd(amount: float | None, currency: str | None, when: date | None) -> int | None:
    """Production stores SGD, converted at the quarter the deal completed in."""
    if amount is None or not currency or when is None:
        return None

    if currency == "SGD":
        return round(amount)

    quarters = load_quarters()

    if not quarters:
        return None

    index = bisect.bisect_left([quarter_end for quarter_end, _ in quarters], when)

    if index >= len(quarters):
        LOGGER.warning(f"[REIT TRANSACTION] {when} is past the rates file, using its last quarter")
        index = len(quarters) - 1

    rate = quarters[index][1].get(currency, {}).get("SGD")

    if rate is None:
        LOGGER.warning(f"[REIT TRANSACTION] No SGD rate for {currency} at {when}")
        return None

    return round(amount * rate)
