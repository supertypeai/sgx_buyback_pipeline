from rapidfuzz import fuzz, process

from .json_helper import open_json

import logging
import re


LOGGER = logging.getLogger(__name__)


def get_sgx_company_names():
    companies_path = "data/sgx_companies.json"

    companies = open_json(path=companies_path)

    company_names = [
        value.get('name').strip().lower()
        for _, value in companies.items()
    ]

    return company_names, companies


def get_sgx_company_name_aliases() -> dict[str, str]:
    aliases_path = "data/sgx_company_name_aliases.json"
    aliases = open_json(path=aliases_path)

    return aliases if isinstance(aliases, dict) else {}


def normalize_company_name(
    company_name: str,
    remove_parenthetical: bool = False,
    expand_reit: bool = False,
) -> str:
    normalized_name = company_name.casefold()

    if remove_parenthetical:
        normalized_name = re.sub(r"\s*\([^)]*\)", " ", normalized_name)

    normalized_name = re.sub(r"\bpublic company\b", " ", normalized_name)
    normalized_name = re.sub(r"\bcorporation\b", "corp", normalized_name)
    normalized_name = re.sub(r"\blimited\b", "ltd", normalized_name)

    if expand_reit:
        normalized_name = re.sub(
            r"\breit\b",
            "real estate investment trust",
            normalized_name,
        )

    return " ".join(normalized_name.split())


def build_unique_company_lookup(
    companies: dict,
    remove_parenthetical: bool = False,
    expand_reit: bool = False,
) -> dict[str, str]:
    symbols_by_name = {}
    ambiguous_names = set()

    for company in companies.values():
        company_name = company.get("name")
        symbol = company.get("symbol")

        if not company_name or not symbol:
            continue

        normalized_name = normalize_company_name(
            company_name,
            remove_parenthetical=remove_parenthetical,
            expand_reit=expand_reit,
        )
        existing_symbol = symbols_by_name.get(normalized_name)

        if existing_symbol and existing_symbol != symbol:
            ambiguous_names.add(normalized_name)
            continue

        symbols_by_name[normalized_name] = symbol

    for ambiguous_name in ambiguous_names:
        symbols_by_name.pop(ambiguous_name, None)

    return symbols_by_name


def build_company_alias_lookup(
    aliases: dict[str, str],
    remove_parenthetical: bool = False,
    expand_reit: bool = False,
) -> dict[str, str]:
    return {
        normalize_company_name(
            alias,
            remove_parenthetical=remove_parenthetical,
            expand_reit=expand_reit,
        ): symbol
        for alias, symbol in aliases.items()
        if alias and symbol
    }


def find_fuzzy_symbol(
    input_name: str,
    symbols_by_name: dict[str, str],
    threshold: int,
    minimum_score_margin: int = 5,
) -> str | None:
    matches = process.extract(
        input_name,
        symbols_by_name.keys(),
        scorer=fuzz.ratio,
        limit=2,
    )

    if not matches:
        return None

    matched_name, matched_score, _ = matches[0]
    runner_up_score = matches[1][1] if len(matches) > 1 else 0

    if (
        round(matched_score) < threshold
        or matched_score - runner_up_score < minimum_score_margin
    ):
        return None

    LOGGER.info(
        "Matched company name '%s' to '%s' with score %.2f",
        input_name,
        matched_name,
        matched_score,
    )
    return symbols_by_name[matched_name]


def symbol_from_company_name(input_name: str, threshold: int = 90) -> str | None:
    if not input_name:
        return None

    try:
        _, companies = get_sgx_company_names()
        aliases = get_sgx_company_name_aliases()
        
        lookup_variants = (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        )

        for remove_parenthetical, expand_reit in lookup_variants:
            company_lookup = build_unique_company_lookup(
                companies,
                remove_parenthetical=remove_parenthetical,
                expand_reit=expand_reit,
            )

            normalized_input = normalize_company_name(
                input_name,
                remove_parenthetical=remove_parenthetical,
                expand_reit=expand_reit,
            )
            
            exact_symbol = company_lookup.get(normalized_input)

            if exact_symbol:
                return exact_symbol

        for remove_parenthetical, expand_reit in lookup_variants:
            alias_lookup = build_company_alias_lookup(
                aliases,
                remove_parenthetical=remove_parenthetical,
                expand_reit=expand_reit,
            )
            normalized_input = normalize_company_name(
                input_name,
                remove_parenthetical=remove_parenthetical,
                expand_reit=expand_reit,
            )
            exact_symbol = alias_lookup.get(normalized_input)

            if exact_symbol:
                return exact_symbol

        company_lookup = build_unique_company_lookup(companies)
        normalized_input = normalize_company_name(input_name)

        company_lookup_without_parenthetical = build_unique_company_lookup(
            companies,
            remove_parenthetical=True,
        )

        normalized_input_without_parenthetical = normalize_company_name(
            input_name,
            remove_parenthetical=True,
        )

        fuzzy_symbol = find_fuzzy_symbol(
            input_name=normalized_input,
            symbols_by_name=company_lookup,
            threshold=threshold,
        )

        if fuzzy_symbol:
            return fuzzy_symbol

        return find_fuzzy_symbol(
            input_name=normalized_input_without_parenthetical,
            symbols_by_name=company_lookup_without_parenthetical,
            threshold=threshold,
        )

    except Exception as error:
        LOGGER.error(
            "[symbol_matching_helper] Failed matching '%s': %s", 
            input_name, 
            error,
            exc_info=True
        )
        
        return None


def matching_symbol(issuer_security: str) -> str | None:
    try:
        symbol_matched = symbol_from_company_name(issuer_security)

        if symbol_matched:
            return symbol_matched

    except Exception as error:
        LOGGER.error(f"[matching symbol] Fallback matching symbol failed: {error}")

    return None


if __name__ == '__main__':
    company = symbol_from_company_name("17live group limited")
    print(company)
    # print(SGX_COMPANY_NAMES[:5])


# uv run -m sgx_scraper.utils.symbol_matching_helper
