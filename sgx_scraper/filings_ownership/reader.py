from pydantic import BaseModel

from sgx_scraper.filings_ownership.entities import EntityRegistry
from sgx_scraper.filings_ownership.prompts.models import FilingRead, FilingTraders
from sgx_scraper.filings_ownership.prompts.filing import (
    get_system_filing_prompt,
    get_user_filing_prompt,
)
from sgx_scraper.filings_ownership.prompts.trader import (
    get_system_trader_prompt,
    get_user_trader_prompt,
)
from sgx_scraper.filings_ownership.utils.constants import (
    LLM_CONCURRENCY,
    LLM_EFFORT,
    LLM_MODELS,
    LLM_READ_ATTEMPTS,
)
from sgx_scraper.filings_ownership.utils.record_helper import (
    get_interest_amount,
    join_readable_text,
    needs_trader,
)
from sgx_scraper.llm.caller import ainvoke_structured_llm

import asyncio
import logging


LOGGER = logging.getLogger(__name__)

BOX_LABELS = [
    ("deemed_interest_text", "deemed interest"),
    ("relationship_text", "relationship"),
    ("remarks_text", "remarks"),
]


def get_filing_text(filings: list[dict]) -> str:
    # Every box of the filing once, in the order the holders filed them
    texts = []

    for filing in filings:
        for column, _ in BOX_LABELS:
            text = filing.get(column)

            if text and text not in texts:
                texts.append(text)

    return " | ".join(texts)


def needs_reading(filings: list[dict], registry: EntityRegistry) -> bool:
    # Known holders with nothing to read need no call: a direct move is the
    # holder's own trade, and there is no structure to draw
    if join_readable_text(get_filing_text(filings)):
        return True

    for filing in filings:
        if needs_trader(filing):
            return True

        if registry.find(filing["holder_name"]) is None:
            return True

        if filing.get("acting_party") and registry.find(filing["acting_party"]) is None:
            return True

    return False


def describe_share_change(filing: dict, part: str) -> str:
    before = get_interest_amount(filing, "before", part)
    after = get_interest_amount(filing, "after", part)

    return f"{before:,.0f} -> {after:,.0f} ({after - before:+,.0f})"


def describe_record(number: int, filing: dict) -> str:
    marker = " (answer)" if needs_trader(filing) else ""

    return "\n".join([
        f"Record {number}{marker}:",
        f"  holder: {filing['holder_name']}",
        f"  transaction: {filing.get('transaction_type')} on {filing.get('timestamp')}",
        f"  direct shares: {describe_share_change(filing, 'direct')}",
        f"  deemed shares: {describe_share_change(filing, 'deemed')}",
        f"  circumstance ticked: {filing.get('circumstance_key') or 'None'}",
        f"  reason given: {filing.get('change_reason') or 'None'}",
    ])


def describe_boxes(filings: list[dict]) -> str:
    seen = set()
    blocks = []

    for filing in filings:
        for column, label in BOX_LABELS:
            text = filing.get(column)

            if not text or (label, text) in seen:
                continue

            seen.add((label, text))

            blocks.append(f"{filing['holder_name']} - {label}:\n{text}")

    return "\n\n".join(blocks) or "None"


def describe_definitions(definitions: dict[str, str]) -> str:
    lines = [
        f"{short_name} = {full_name}"
        for short_name, full_name in definitions.items()
    ]

    return "\n".join(lines) or "None"


def build_filing_input(
    filings: list[dict],
    company_name: str,
    company_holders: list[str],
    definitions: dict[str, str],
) -> dict:
    holder_names = {filing["holder_name"] for filing in filings}
    acting_parties = sorted({
        filing["acting_party"]
        for filing in filings
        if filing.get("acting_party")
    })

    return {
        "company_name": company_name,
        "records": "\n\n".join(
            describe_record(number, filing)
            for number, filing in enumerate(filings, start=1)
        ),
        "boxes": describe_boxes(filings),
        "definitions": describe_definitions(definitions),
        "company_holders": "\n".join(
            name for name in company_holders if name not in holder_names
        ) or "None",
        "acting_party": "\n".join(acting_parties) or "None",
    }


def is_complete_answer(answer: dict | None, output_model: type[BaseModel]) -> bool:
    # The JSON parser repairs a reply cut off mid-way into a valid object
    # that is missing its later fields; a full reply has every field
    return answer is not None and set(output_model.model_fields) <= set(answer)


async def ask_llm(
    output_model: type[BaseModel],
    system_prompt: str,
    user_prompt: str,
    log_name: str,
    filing_input: dict,
) -> dict | None:
    for attempt in range(1, LLM_READ_ATTEMPTS + 1):
        try:
            answer = await ainvoke_structured_llm(
                pydantic_output=output_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                log_name=log_name,
                input_data=filing_input,
                models=LLM_MODELS,
                effort=LLM_EFFORT,
            )

        except Exception as error:
            LOGGER.warning("[%s] LLM read failed: %s", log_name, error)
            answer = None

        if is_complete_answer(answer, output_model):
            return answer

        LOGGER.warning("[%s] Incomplete LLM answer (attempt %d/%d)", log_name, attempt, LLM_READ_ATTEMPTS)

    return None


async def read_structure(filing_input: dict) -> dict | None:
    return await ask_llm(
        FilingRead,
        get_system_filing_prompt(),
        get_user_filing_prompt(),
        "Filing ownership",
        filing_input,
    )


async def read_traders(filing_input: dict) -> dict | None:
    # Asked on its own: inside the structure read the model often skipped
    # traders, such as a fund manager dealing for its own portfolios
    return await ask_llm(
        FilingTraders,
        get_system_trader_prompt(),
        get_user_trader_prompt(),
        "Filing traders",
        filing_input,
    )


async def read_filing(filing_input: dict, has_trader_records: bool) -> dict | None:
    if not has_trader_records:
        answer = await read_structure(filing_input)

        return {**answer, "traders": []} if answer else None

    answer, traders = await asyncio.gather(
        read_structure(filing_input),
        read_traders(filing_input),
    )

    # A filing saved without its traders would never be read again
    if answer is None or traders is None:
        return None

    return {**answer, "traders": traders["records"]}


async def read_filing_limited(
    semaphore: asyncio.Semaphore,
    source: str,
    filing_input: dict,
    has_trader_records: bool,
) -> tuple[str, dict | None]:
    async with semaphore:
        answer = await read_filing(filing_input, has_trader_records)

    return source, answer


async def read_filings(inputs: dict[str, dict], trader_sources: set[str]) -> dict[str, dict]:
    semaphore = asyncio.Semaphore(LLM_CONCURRENCY)

    results = await asyncio.gather(*[
        read_filing_limited(semaphore, source, filing_input, source in trader_sources)
        for source, filing_input in inputs.items()
    ])

    return {
        source: answer
        for source, answer in results
        if answer is not None
    }
