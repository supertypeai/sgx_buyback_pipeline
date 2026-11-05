from pydantic import BaseModel, ValidationError

from sgx_scraper.filings_ownership.annual_reports.holders import (
    fill_deemed_from_links,
    fill_missing_percentages,
    fill_totals,
    get_issued_shares,
    link_by_share_counts,
    resolve_ultimate_owners,
)
from sgx_scraper.filings_ownership.annual_reports.names import holder_key, is_person_name, normalize_text
from sgx_scraper.filings_ownership.prompts.annual_report import (
    get_system_footnote_owner_prompt,
    get_system_holders_prompt,
    get_system_member_part_prompt,
    get_user_footnote_owner_prompt,
    get_user_holders_prompt,
    get_user_member_part_prompt,
)
from sgx_scraper.filings_ownership.prompts.models import AnnualReportHolders, CompanyOwners, MemberParts
from sgx_scraper.llm.caller import ainvoke_structured_llm

import re
import asyncio
import logging


LOGGER = logging.getLogger(__name__)

# A defined term shorter than this ("CDL") stands for the name it was defined after
SHORT_TERM_LENGTH = 8
# Parts a company outside the table holds, whose count may be printed elsewhere
COUNTED_PART_KINDS = ("family_company", "other")


async def read_structured(
    output_model: type[BaseModel],
    prompts: tuple[str, str],
    log_name: str,
    input_data: dict,
    models: list[str],
    effort: str,
    semaphore: asyncio.Semaphore,
) -> BaseModel | None:
    async with semaphore:
        extraction = await ainvoke_structured_llm(
            pydantic_output=output_model,
            system_prompt=prompts[0],
            user_prompt=prompts[1],
            log_name=log_name,
            input_data=input_data,
            models=models,
            effort=effort,
            temperature=0,
        )

    if extraction is None:
        LOGGER.warning("Nothing extracted for %s", log_name)
        return None

    # The parser does not check the shape, so a reply cut short would otherwise be saved
    try:
        return output_model.model_validate(extraction)
    except ValidationError as error:
        LOGGER.warning("Reply for %s does not fit the model: %s", log_name, error)
        return None


def no_substantial_record(report_pages: dict) -> dict:
    return {
        "symbol": report_pages["symbol"],
        "source_url": report_pages["source_url"],
        "submission_date": report_pages["submission_date"],
        "as_at": None,
        "page": report_pages["no_substantial_page"],
        "holders": [],
        "notes": "The report states there are no substantial shareholders.",
        "ultimate_owners": [],
    }


async def read_report_holders(
    report_pages: dict,
    company_name: str,
    models: list[str],
    effort: str,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    # Saying there are none is an answer too, and keeps the report's link for the page
    if report_pages.get("no_substantial_page"):
        return no_substantial_record(report_pages)

    found = await read_structured(
        output_model=AnnualReportHolders,
        prompts=(get_system_holders_prompt(), get_user_holders_prompt()),
        log_name=f"annual report holders {report_pages['symbol']}",
        input_data={"company_name": company_name, "pages": report_pages["pages"]},
        models=models,
        effort=effort,
        semaphore=semaphore,
    )

    if found is None:
        return None

    extracted = found.model_dump()

    if not extracted["as_at"] and extracted.get("page"):
        extracted["as_at"] = (report_pages.get("page_dates") or {}).get(extracted["page"])

    fill_totals(extracted["holders"])

    if any(holder["total_pct"] is None for holder in extracted["holders"]):
        issued_shares = await asyncio.to_thread(
            get_issued_shares,
            report_pages["symbol"],
            extracted["as_at"],
            extracted["holders"],
        )
        fill_missing_percentages(extracted["holders"], issued_shares)

    fill_deemed_from_links(extracted["holders"])
    link_by_share_counts(extracted["holders"])

    record = {
        "symbol": report_pages["symbol"],
        "source_url": report_pages["source_url"],
        "submission_date": report_pages["submission_date"],
        **extracted,
        "ultimate_owners": resolve_ultimate_owners(extracted["holders"]),
    }

    await read_footnote_owners(report_pages, record, company_name, models, effort, semaphore)
    await read_member_parts(report_pages, record, company_name, models, effort, semaphore)

    return record


def printed_owners(names: list[str], page_text: str, table_keys: set[str]) -> list[str]:
    # Only names printed on the pages and not already table rows; a lower-case start is a
    # term the footnote defines ("the Funds" in T82U), not a name
    return [
        name
        for name in names
        if normalize_text(name) in page_text
        and holder_key(name) not in table_keys
        and not name[:1].islower()
    ]


def people_down_chain(owner: dict, found_by_key: dict[str, list[str]]) -> list[str]:
    # Only people: the companies above a link in the chain are the chain itself (9CI)
    for name in owner["via"]:
        people = [each for each in found_by_key.get(holder_key(name), []) if is_person_name(each)]

        if people:
            return people

    return []


async def read_footnote_owners(
    report_pages: dict,
    record: dict,
    company_name: str,
    models: list[str],
    effort: str,
    semaphore: asyncio.Semaphore,
) -> None:
    # A footnote can name a company's owners without giving them a row (5CF's China Sonangol),
    # including owners of a company down the chain (EB5's Fangiono family)
    tops = [owner for owner in record["ultimate_owners"] if not is_person_name(owner["name"])]
    companies = list(dict.fromkeys(
        name
        for owner in tops
        for name in [owner["name"], *owner["via"]]
        if not is_person_name(name)
    ))

    if not companies:
        return

    found = await read_structured(
        output_model=CompanyOwners,
        prompts=(get_system_footnote_owner_prompt(), get_user_footnote_owner_prompt()),
        log_name=f"annual report footnote owners {record['symbol']}",
        input_data={
            "company_name": company_name,
            "table_holders": "\n".join(holder["name"] for holder in record["holders"]),
            "companies": "\n".join(companies),
            "pages": report_pages["pages"],
        },
        models=models,
        effort=effort,
        semaphore=semaphore,
    )

    if found is None:
        return

    page_text = normalize_text(report_pages["pages"])
    table_keys = {holder_key(holder["name"]) for holder in record["holders"]}
    found_by_key = {
        holder_key(entry.company): printed_owners(entry.owned_by, page_text, table_keys)
        for entry in found.companies
    }

    # A top owner's own footnote first, else the people a company down its chain is held for
    for owner in tops:
        own_footnote = found_by_key.get(holder_key(owner["name"]))
        owner["owned_by"] = own_footnote or people_down_chain(owner, found_by_key)


def count_on_pages(name: str, pages: str) -> float | None:
    # Only a name printed once with a count, so a guess never fills it (H30's
    # "Corporate Development Limited 8,113,776" in the twenty largest holders)
    name = re.sub(r"\s*\(.*$", "", name).strip()

    if len(name) < SHORT_TERM_LENGTH:
        definition = r"([A-Z][\w&.,' -]{7,80}?)\s*\(\s*[\"“”']?" + re.escape(name) + r"[\"“”']?\s*\)"
        defined = re.search(definition, pages)

        if not defined:
            return None

        name = defined.group(1).strip()

    pattern = re.compile(re.escape(normalize_text(name)) + r"[^0-9]{0,40}?(\d{1,3}(?:,\d{3})+)")
    counts = {match.group(1) for match in pattern.finditer(normalize_text(pages))}

    return float(counts.pop().replace(",", "")) if len(counts) == 1 else None


def checked_part(part, page_text: str, count_pages: str) -> dict | None:
    quote = normalize_text(part.quote)

    # A quote not on the pages is the model's own words, so the part goes
    if not quote or quote not in page_text:
        return None

    shares = part.shares
    printed_forms = (f"{int(shares):,}", str(int(shares))) if shares is not None else ()

    # The count must be printed in the quote, with or without its commas
    if shares is not None and not any(form in quote for form in printed_forms):
        shares = None

    checked = {
        "kind": part.kind, 
        "held_by": part.held_by, 
        "shares": shares, 
        "quote": part.quote
    }

    # Only for a count the footnotes leave out; a company already in the table's chain
    # is counted there (P34's McKeeson under Berlian)
    if shares is None and part.kind in COUNTED_PART_KINDS:
        checked["table_count"] = count_on_pages(part.held_by, count_pages)

    return checked


async def read_member_parts(
    report_pages: dict,
    record: dict,
    company_name: str,
    models: list[str],
    effort: str,
    semaphore: asyncio.Semaphore,
) -> None:
    # Parts of a holder's interest no other row holds (H18's spouse shares, OYY's nominee
    # accounts), so a family's total counts each once beside the companies they share
    if not record["holders"]:
        return

    found = await read_structured(
        output_model=MemberParts,
        prompts=(get_system_member_part_prompt(), get_user_member_part_prompt()),
        log_name=f"annual report member parts {record['symbol']}",
        input_data={
            "company_name": company_name,
            "table_holders": "\n".join(holder["name"] for holder in record["holders"]),
            "pages": report_pages["pages"],
        },
        models=models,
        effort=effort,
        semaphore=semaphore,
    )

    if found is None:
        return

    page_text = normalize_text(report_pages["pages"])
    count_pages = f"{report_pages['pages']}\n{report_pages.get('top_holders') or ''}"
    holders_by_key = {holder_key(holder["name"]): holder for holder in record["holders"]}

    for entry in found.holders:
        holder = holders_by_key.get(holder_key(entry.holder))

        if not holder:
            continue

        checked_parts = [checked_part(part, page_text, count_pages) for part in entry.parts]
        holder["parts"] = [part for part in checked_parts if part is not None]


async def read_annual_reports(
    report_pages: list[dict],
    company_names: dict[str, str],
    models: list[str],
    effort: str,
    concurrency: int,
) -> list[dict]:
    semaphore = asyncio.Semaphore(concurrency)
    results = await asyncio.gather(
        *(
            read_report_holders(
                report_pages=pages,
                company_name=company_names.get(pages["symbol"], pages["symbol"]),
                models=models,
                effort=effort,
                semaphore=semaphore,
            )
            for pages in report_pages
        )
    )

    return [result for result in results if result is not None]
