from tavily import TavilyClient
from rapidfuzz import fuzz, process

from sgx_scraper.config.settings import TAVILY_API_KEYS
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import get_db
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    HOLDER_NAME_ACRONYMS,
    HOLDER_SHORT_NAME_PATTERN,
)
from sgx_scraper.utils.json_helper import open_json

from functools import lru_cache

import re
import logging


LOGGER = logging.getLogger(__name__)


def search_tavily(
    holder_name: str,
    company_name: str, 
    type_question: str = "individual"
) -> str:
    api_keys = [key for key in TAVILY_API_KEYS if key]

    if type_question == "individual":
        query = (
            f"Is '{holder_name}', a shareholder in {company_name}, a person or a company? "
            "Answer with this format: {company or person}, explanation"
        )

    elif type_question == "relation":
        query = (f"""
                Determine the identity and relationship of {holder_name} to
                {company_name}

                Return:
                1. relation_to_issuer
                Return one of:
                - "Parent"
                - "Subsidiary"
                - "Affiliate"
                - "Controller"
                - "Manager"
                - "Trustee-Manager"
                - "Responsible Person"
                - "None"

                Only assign a relation when reliable sources explicitly establish a DIRECT
                relationship between the holder and the listed issuer CapitaLand Integrated Commercial Trust.

                For SGX-listed REITs, trusts, or business trusts:
                - "Manager" means the holder is explicitly the manager of the listed REIT/trust.
                - "Trustee-Manager" means the holder is explicitly the trustee-manager of the
                listed issuer.
                - "Responsible Person" means the holder is explicitly identified as the
                responsible person for the listed issuer.

                The relationship must be between 
                {holder_name} and the {company_name} itself.

                Return concise evidence explaining the classification and cite the source used and using this format: 
                (relation), explanation
            """
        )

    last_error = None

    for key_number, api_key in enumerate(api_keys, start=1):
        if not api_key:
            LOGGER.warning("Tavily API key %d is not configured; skipping it", key_number)
            continue

        try:
            client = TavilyClient(api_key)
            response = client.search(
                query=query,
                include_answer="advanced",
                # topic="finance",
                search_depth="advanced",
                max_results=5,
                chunks_per_source=5,
                include_domains=[
                    "https://www.sgx.com/",
                    "https://ranking.fortuneidn.com/",
                    "https://finance.yahoo.com/"
                ] if type_question == "individual" else None 
            )

            answer = response.get("answer")

            if answer:
                return answer

            raise RuntimeError("Tavily returned no answer")

        except Exception as error:
            last_error = error
            LOGGER.warning(
                "Tavily request with API key %d failed: %s. Trying the next key.",
                key_number,
                error,
            )

    raise RuntimeError("All configured Tavily API keys failed") from last_error


def normalize_holder_name_update(
    holder_name: str,
    remove_parenthetical: bool = False,
):
    normalized_name = holder_name.casefold()

    if remove_parenthetical:
        normalized_name = re.sub(
            r"\s*\([^)]*\)",
            " ",
            normalized_name,
        )

    normalized_name = re.sub(
        r"(?<=[a-z])(?=(?:private|pte|ltd|limited|inc|llc|plc)\b)",
        " ",
        normalized_name,
    )

    normalized_name = re.sub(
        r"\b(private|pte|ltd|limited|inc|llc|plc)\b",
        " ",
        normalized_name,
    )

    normalized_name = re.sub(
        r"[^a-z0-9]+",
        " ",
        normalized_name,
    )

    return " ".join(normalized_name.split())


def create_name_variants(holder_name: str):
    normal_name = normalize_holder_name_update(holder_name)

    parenthetical_removed_name = normalize_holder_name_update(
        holder_name,
        remove_parenthetical=True,
    )

    name_variants = {normal_name}

    # avoid creating extremely broad aliases
    if (
        parenthetical_removed_name
        and parenthetical_removed_name != normal_name
        and len(parenthetical_removed_name.split()) >= 2
    ):
        name_variants.add(parenthetical_removed_name)

    return name_variants


def build_reit_candidates(
    reit_profile: dict | None,
) -> list[dict]:
    if not reit_profile:
        return []

    candidates = []

    management = reit_profile.get("management") or {}

    for management_key, company_names in management.items():
        if isinstance(company_names, str):
            company_names = [company_names]

        for company_name in company_names or []:
            if not company_name:
                continue

            for normalized_name in create_name_variants(company_name):
                if not normalized_name:
                    continue

                candidates.append(
                    {
                        "normalized_name": normalized_name,
                        "candidate_type": "management",
                        "management_key": management_key,
                        "matched_name": company_name,
                    }
                )

    board_members = reit_profile.get("board") or []

    for board_member in board_members:
        board_member_name = board_member.get("name")

        if not board_member_name:
            continue

        for normalized_name in create_name_variants(board_member_name):
            if not normalized_name:
                continue

            candidates.append(
                {
                    "normalized_name": normalized_name,
                    "candidate_type": "board",
                    "matched_name": board_member_name,
                    "position": board_member.get("position"),
                }
            )

    return candidates


def match_filing_to_reit_profile(
    filing_holder_name: str | None,
    reit_profile: dict | None,
    filing_name_variants: set[str] | None = None,
    threshold: int = 90,
) -> bool:
    if not filing_holder_name:
        return False

    candidates = build_reit_candidates(reit_profile)

    if not candidates:
        return False

    if filing_name_variants is None:
        filing_name_variants = create_name_variants(filing_holder_name)

    # First attempt an exact normalized match
    for filing_name_variant in filing_name_variants:
        for candidate in candidates:
            if candidate["normalized_name"] == filing_name_variant:
                return True

    candidate_names = [
        candidate["normalized_name"]
        for candidate in candidates
    ]

    for filing_name_variant in filing_name_variants:
        fuzzy_result = process.extractOne(
            filing_name_variant,
            candidate_names,
            scorer=fuzz.ratio,
        )

        if fuzzy_result is None:
            continue

        _, similarity_score, _ = fuzzy_result

        if similarity_score >= threshold:
            return True

    return False


KNOWN_HOLDER_TYPES_PATH = (
    "sgx_scraper/fetch_sgx_filings/data/processed/processed_final.json"
)


def normalize_holder_key(holder_name: str | None) -> str:
    return (clean_holder_name(holder_name) or "").casefold()


@lru_cache(maxsize=1)
def get_known_holder_types() -> dict[tuple[str, str], str]:
    # Holder types already settled for a holder and company, read from the
    # reviewed records. A holder keeps its type on every later run instead of
    # being asked of Tavily again, whose answers vary from run to run
    try:
        records = open_json(KNOWN_HOLDER_TYPES_PATH)

    except (FileNotFoundError, ValueError):
        return {}

    return {
        (normalize_holder_key(record.get("holder_name")), record.get("symbol")): (
            record["holder_type"]
        )
        for record in records or []
        if record.get("holder_type")
    }


def match_table_holder_type(holder_name: str, symbol: str) -> str | None:
    # The holder's type on its latest stored filing for this company
    name_pattern = re.sub(r"([\\%_])", r"\\\1", clean_holder_name(holder_name))

    try:
        records = get_db(
            table="sgx_filings",
            query_modifier=lambda query: (
                query
                .ilike("holder_name", name_pattern)
                .eq("symbol", symbol)
                .not_.is_("holder_type", "null")
                .order("created_at", desc=True)
                .limit(1)
            ),
            columns="holder_type",
        )

    except Exception as error:
        LOGGER.warning("[holder_type] table lookup failed for %s: %s", holder_name, error)
        return None

    return next(
        (record["holder_type"] for record in records if record.get("holder_type")),
        None,
    )


def match_known_holder_type(holder_name: str, filing_symbol: str | None) -> str | None:
    # A holder keeps the type it already has for this company: first from the
    # reviewed file, then from the sgx_filings table
    if not holder_name or not filing_symbol:
        return None

    symbol = f"{filing_symbol.removesuffix('.SI')}.SI"

    return (
        get_known_holder_types().get((normalize_holder_key(holder_name), symbol))
        or match_table_holder_type(holder_name, symbol)
    )


@lru_cache(maxsize=None)
def classify_holder_type(
    parser_form: str,
    company_name: str,
    filing_symbol: str,
    holder_name: str,
) -> str:
    # "insider" means connected to the issuer: its directors and CEO (Form 1),
    # its manager (Form 6), individuals, and entities related to it. Any other
    # company is an "institution", however large its stake.
    # Cached because the same holders file repeatedly and each lookup costs
    # two Tavily searches.
    if "form1" in parser_form.lower():
        return "insider"

    known_holder_type = match_known_holder_type(holder_name, filing_symbol)

    if known_holder_type:
        return known_holder_type

    if "form6" in parser_form.lower():
        reit_profiles = get_db(
            table="sgx_reit_profile",
            columns="symbol, management, board",
            query_modifier=None,
        )

        reit_profile = next(
            (
                profile
                for profile in reit_profiles
                if profile.get("symbol") == filing_symbol
            ),
            None,
        )

        is_reit_management = match_filing_to_reit_profile(
            filing_holder_name=holder_name,
            reit_profile=reit_profile, 
        )

        if is_reit_management: 
            return "insider"
    
    tavily_raw_output_individual = search_tavily(holder_name, company_name)
    is_individual = tavily_raw_output_individual.split(",", 1)[0].strip().lower()

    if is_individual == "person":
        return "insider"

    tavily_raw_output_relation = search_tavily(
        holder_name, company_name, "relation"
    )
    relation = (
        tavily_raw_output_relation
        .split(",", 1)[0]
        .strip()
        .lower()
        .strip("()")
        .strip()
    )

    LOGGER.info(
        "[holder_type] holder=%s | entity=%s | relation=%s",
        holder_name,
        is_individual,
        relation,
    )

    if relation in (
        "parent",
        "subsidiary",
        "affiliate",
        "controller",
        "manager",
        "trustee-manager",
        "responsible person",
    ):
        return "insider"

    return "institution"


def clean_holder_name(holder_name: str | None) -> str | None:
    if not holder_name:
        return holder_name

    # Remove a bracketed short name such as ("FCAML"), ('JSS') or (the "Manager").
    # These always carry quotation marks; brackets that are part of the legal
    # name, such as (Private) or (Singapore), have none and are kept
    holder_name = re.sub(
        HOLDER_SHORT_NAME_PATTERN, ' ', holder_name
    ).strip()
    holder_name = re.sub(r'\s+', ' ', holder_name).strip()

    # Drop a sentence-style full stop, but not one that closes an abbreviation
    # made of initials, such as "L.P."
    if not re.search(r'\b[A-Za-z]\.[A-Za-z]\.$', holder_name):
        holder_name = holder_name.rstrip('.')

    return holder_name


def format_holder_name(holder_name: str | None) -> str:
    if not holder_name:
        return "A shareholder"

    holder_name = holder_name.replace(" [->] ", " to ")

    # SGX forms are often filled in capitals
    if holder_name.isupper():
        holder_name = " ".join(
            word if word.strip(".,()") in HOLDER_NAME_ACRONYMS else word.title()
            for word in holder_name.split(" ")
        )

    return holder_name


def get_display_holder_name(record: dict[str, any], key: str = "holder_name") -> str:
    # Once the casing step has run, a name still in capitals is one the LLM
    # kept that way (an acronym such as "NTCP SPV VI"), so it is not
    # title-cased again
    holder_name = record.get(key)

    if holder_name and record.get("_holder_name_cased"):
        return holder_name.replace(" [->] ", " to ")

    return format_holder_name(holder_name)
