from sgx_scraper.filings_ownership.utils.constants import (
    CORPORATE_ACTION_TAGS,
    MIN_TEXT_LENGTH,
    PERCENT_DECIMALS,
    RESTRUCTURING_TAGS,
    ROUTINE_TAGS,
    SUBSTANTIAL,
    THRESHOLDS,
)


def get_interest_value(
    filing: dict,
    side: str,
    part: str,
    field: str,
) -> float | None:
    breakdown = filing.get("interest_breakdown") or {}
    interest = (breakdown.get(side) or {}).get(part) or {}

    return interest.get(field)


def get_interest_amount(filing: dict, side: str, part: str) -> float:
    return get_interest_value(filing, side, part, "amount") or 0


def get_interest_percentage(filing: dict, side: str, part: str) -> float | None:
    return get_interest_value(filing, side, part, "percentage")


def is_still_held(filing: dict) -> bool:
    # Forms round a director's few thousand units to 0.00%, so the unit count decides
    after_units = get_interest_amount(filing, "after", "total")
    after_percentage = get_interest_percentage(filing, "after", "total") or 0

    return after_units > 0 or after_percentage > 0


def get_share_change(filing: dict) -> float:
    return get_interest_amount(filing, "after", "total") - get_interest_amount(filing, "before", "total")


def is_interest_changed(filing: dict, part: str) -> bool:
    before = get_interest_amount(filing, "before", part)
    after = get_interest_amount(filing, "after", part)

    return before != after


def get_interest_role(filing: dict) -> str:
    is_direct_changed = is_interest_changed(filing, "direct")
    is_deemed_changed = is_interest_changed(filing, "deemed")

    if is_direct_changed and is_deemed_changed:
        return "both"

    if is_direct_changed:
        return "direct"

    if is_deemed_changed:
        return "deemed"

    return "none"


def needs_trader(filing: dict) -> bool:
    # A deemed-only move was dealt by someone else; the parser names that
    # party as the acting party when the filing says who it was
    return get_interest_role(filing) == "deemed" and not filing.get("acting_party")


def get_move_kind(filing: dict, interest_role: str) -> str:
    tags = set(filing.get("tags") or [])

    if tags & ROUTINE_TAGS:
        return "routine"

    if tags & CORPORATE_ACTION_TAGS:
        return "corporate_action"

    if tags & RESTRUCTURING_TAGS:
        return "restructuring"

    # The holding is unchanged but the % moved: shares were issued or cancelled
    if interest_role == "none":
        return "passive"

    if filing.get("transaction_type") in ("buy", "sell"):
        return "decision"

    return "other"


def get_thresholds_crossed(before: float | None, after: float | None) -> list[dict]:
    if before is None or after is None:
        return []

    crossed = []

    for level in THRESHOLDS:
        if before < level <= after:
            crossed.append({"level": level, "direction": "up"})

        elif after < level <= before:
            crossed.append({"level": level, "direction": "down"})

    return crossed


def get_substantial_status(
    filing: dict,
    before: float | None,
    after: float | None,
) -> str | None:
    notification_type = filing.get("notification_type")

    if notification_type == "becoming":
        return "became"

    if notification_type == "ceasing":
        return "ceased"

    if before is None or after is None:
        return None

    if before < SUBSTANTIAL <= after:
        return "became"

    if after < SUBSTANTIAL <= before:
        return "ceased"

    return None


def round_fractions(value):
    # Drops float noise like 0.277 / 100 = 0.0027700000000000003 from every number in a row
    if isinstance(value, float):
        return round(value, PERCENT_DECIMALS)

    if isinstance(value, dict):
        return {key: round_fractions(item) for key, item in value.items()}

    if isinstance(value, list):
        return [round_fractions(item) for item in value]

    return value


def get_percentage_change(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None

    return round(after - before, 6)


def join_readable_text(*parts: str | None) -> str | None:
    text = " ".join(
        part
        for part in parts
        if part
    )

    return text if len(text) >= MIN_TEXT_LENGTH else None
