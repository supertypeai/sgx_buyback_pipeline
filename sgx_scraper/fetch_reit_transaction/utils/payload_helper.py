from datetime import date, datetime

from sgx_scraper.fetch_reit_transaction.constant import (
    FIRST_HALF_YEAR_END,
    SECOND_HALF_YEAR_END,
    TRANSACTION_BASIS,
)

import re


NAME_NOISE = (
    r"(the|a|an|of|at|and|property|properties|building|centre|center"
    r"|pte|ltd|limited|located|in|on|no|nos)"
)


def normalize_property_name(name: str | None) -> set[str]:
    cleaned = re.sub(r"[^a-z0-9 ]", " ", (name or "").lower())
    return {word for word in cleaned.split() if not re.fullmatch(NAME_NOISE, word)}


def is_same_property(left: str | None, right: str | None) -> bool:
    """Names drift between the plan and the completion, so shared distinctive
    tokens are the signal rather than an equal string."""
    left_tokens, right_tokens = normalize_property_name(left), normalize_property_name(right)

    if not left_tokens or not right_tokens:
        return False

    shared = left_tokens & right_tokens

    return len(shared) >= min(2, len(left_tokens), len(right_tokens))


def resolve_financial_year(completed: date | None, symbol: str) -> int | None:
    if completed is None:
        return None

    if symbol in FIRST_HALF_YEAR_END:
        return completed.year - (1 if completed.month <= FIRST_HALF_YEAR_END[symbol] else 0)

    if symbol in SECOND_HALF_YEAR_END:
        return completed.year + (1 if completed.month > SECOND_HALF_YEAR_END[symbol] else 0)

    return completed.year


def to_date(value: str | None) -> date | None:
    if not value:
        return None

    text = str(value).strip()

    for pattern, length in (("%Y-%m-%d", 10), ("%Y%m%d", 8)):
        try:
            return datetime.strptime(text[:length], pattern).date()

        except ValueError:
            continue

    return None


def clean_basis(value: str | None) -> str | None:
    return value if value in TRANSACTION_BASIS else None
