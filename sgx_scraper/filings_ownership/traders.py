from sgx_scraper.filings_ownership.utils.constants import EVIDENCE_SIMILARITY, SELF_HELD_RELATIONS
from sgx_scraper.filings_ownership.utils.name_helper import (
    clean_trader_name,
    get_entity_key,
    normalise_text,
)
from sgx_scraper.filings_ownership.utils.record_helper import get_interest_role

from rapidfuzz import fuzz

import re


# "held by funds managed by one or more of FMR LLC's subsidiaries": the
# subsidiaries are not named, so only the wording shows it
HELD_BY_SUBSIDIARIES = re.compile(r"(\bits\b|\btheir\b|'s)\s+(?:[\w-]+\s+){0,4}subsidiar", re.IGNORECASE)


def holds_through_others(filing: dict, routes: list[dict]) -> bool:
    """True when the filing says the holder's shares sit with companies it
    controls or with relatives, so someone below it decided the trade."""

    if HELD_BY_SUBSIDIARIES.search(filing.get("deemed_interest_text") or ""):
        return True

    for route in routes:
        steps = route.get("steps") or []
        first_step = steps[0] if steps else None

        if first_step is None or first_step["relation"] not in SELF_HELD_RELATIONS:
            return True

    return False


def is_circumstance_evidence(evidence: str | None, filing: dict) -> bool:
    circumstance = normalise_text(filing.get("circumstance_key") or "")
    evidence = normalise_text(evidence or "")

    return bool(circumstance) and bool(evidence) and evidence in circumstance


def names_holder_as_dealer(evidence: str | None, filing: dict, form_definitions: dict[str, str]) -> bool:
    """True when the quoted wording itself names the holder, by name or by a
    short name the form defines for it, as in "HKLIH (as purchaser)"."""
    evidence_text = normalise_text(evidence or "")
    holder_key = get_entity_key(filing["holder_name"])

    if not evidence_text:
        return False

    if normalise_text(filing["holder_name"]) in evidence_text:
        return True

    for short_name, full_name in form_definitions.items():
        if get_entity_key(full_name) != holder_key:
            continue

        if re.search(rf"\b{re.escape(short_name.lower())}\b", evidence_text):
            return True

    return False


def get_named_self_records(
    filings: list[dict],
    answer: dict | None,
    form_definitions: dict[str, str],
) -> set[int]:
    # Record indexes whose own wording names the holder as the party that
    # dealt; such a statement outweighs what the ownership routes suggest
    named = set()

    if not answer:
        return named

    rows = {row.get("record"): row for row in answer.get("traders") or []}

    for index, filing in enumerate(filings):
        row = rows.get(index + 1) or {}
        names = [
            name
            for name in row.get("traders") or []
            if isinstance(name, str)
        ]
        is_self = bool(names) and all(
            get_entity_key(name) == get_entity_key(filing["holder_name"])
            for name in names
        )

        if (
            row.get("basis") == "text"
            and is_self
            and names_holder_as_dealer(row.get("evidence"), filing, form_definitions)
        ):
            named.add(index)

    return named


def get_trader_check_text(
    filings: list[dict],
    filing_text: str,
    form_definitions: dict[str, str],
) -> str:
    # A full name the form spells out elsewhere ("DBS Bank Ltd" for the boxes'
    # "DBS Bank") is as much in the filing as the boxes' own wording
    parts = [filing["holder_name"] for filing in filings]
    parts += [filing.get("change_reason") or "" for filing in filings]
    parts += list(form_definitions.values())

    parts.append(filing_text)

    return normalise_text(" | ".join(parts))


def is_evidence_quoted(evidence: str, check_text: str) -> bool:
    evidence = normalise_text(evidence)

    if not evidence:
        return False

    return evidence in check_text or fuzz.partial_ratio(evidence, check_text) >= EVIDENCE_SIMILARITY


def accept_text_traders(names: list[str], evidence: str | None, check_text: str) -> list[str]:
    # The quote must be in the filing and must say more than the name itself
    if not is_evidence_quoted(evidence or "", check_text):
        return []

    evidence_key = get_entity_key(evidence)

    if any(get_entity_key(name) == evidence_key for name in names):
        return []

    return names


def accept_figure_traders(
    names: list[str],
    filer_types: dict[str, str],
    direct_filer_keys: set[str],
) -> list[str]:
    # The figures can only point at a co-filer whose own direct holding moved,
    # or a fund manager dealing for its portfolios. A parent whose deemed
    # interest moved had the dealing done by someone below it
    if len(filer_types) < 2:
        return []

    return [
        name
        for name in names
        if get_entity_key(name) in filer_types
        and (get_entity_key(name) in direct_filer_keys or filer_types[get_entity_key(name)] == "fund")
    ]


def get_accepted_traders(
    filings: list[dict],
    answer: dict | None,
    filing_text: str,
    filer_types: dict[str, str],
    form_definitions: dict[str, str],
) -> list[list[str]]:
    """Trader names from the model, per record, kept only when the filing backs them.

    Every name must be in the filing and a text answer must quote it.
    filer_types maps each filer's name key to its entity type.
    """
    accepted = [[] for _ in filings]

    if not answer:
        return accepted

    check_text = get_trader_check_text(filings, filing_text, form_definitions)
    filer_keys = set(filer_types)
    direct_filer_keys = {
        get_entity_key(filing["holder_name"])
        for filing in filings
        if get_interest_role(filing) in ("direct", "both")
    }
    rows = {row.get("record"): row for row in answer.get("traders") or []}

    for index in range(len(filings)):
        row = rows.get(index + 1) or {}
        names = [
            clean_trader_name(name)
            for name in row.get("traders") or []
            if isinstance(name, str)
        ]
        names = [
            name
            for name in names
            if name
            and (normalise_text(name) in check_text or get_entity_key(name) in filer_keys)
        ]

        if row.get("basis") == "text":
            accepted[index] = accept_text_traders(names, row.get("evidence"), check_text)

            # A holder dealing for itself, such as a fund manager for its own
            # portfolios, is shown by the circumstance it ticked; whether it
            # may be its own trader is decided once its routes are known
            is_self = bool(names) and all(
                get_entity_key(name) == get_entity_key(filings[index]["holder_name"])
                for name in names
            )

            if (
                not accepted[index]
                and is_self
                and is_circumstance_evidence(row.get("evidence"), filings[index])
            ):
                accepted[index] = names

        elif row.get("basis") == "figures":
            accepted[index] = accept_figure_traders(names, filer_types, direct_filer_keys)

    return accepted
