from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from sgx_scraper.llm.client import get_llm
from sgx_scraper.fetch_sgx_filings.llm_parser.transfer_prompt import (
    SYSTEM_FORM_3_TRANSFER_PROMPT,
    SYSTEM_TRANSFER_PROMPT,
    TransferParties,
    USER_FORM_3_TRANSFER_PROMPT,
    USER_TRANSFER_PROMPT,
)

import json
import logging


LOGGER = logging.getLogger(__name__)


def classify_transfer(holder_name: str | None, circumstances_desc: str) -> dict | None:
    parser = JsonOutputParser(pydantic_object=TransferParties)

    prompt = ChatPromptTemplate.from_messages([
        ('system', SYSTEM_TRANSFER_PROMPT),
        ('user', USER_TRANSFER_PROMPT),
    ])

    input_data = {
        'holder_name': holder_name or '',
        'circumstances_desc': circumstances_desc,
        'format_instructions': parser.get_format_instructions(),
    }

    for model in [
        "nvidia-nemotron-3-ultra",
        "gpt-oss-120b",
    ]:
        try:
            llm = get_llm(model, temperature=0.2)

            if llm is None:
                continue

            LOGGER.info('[transfer] resolving parties with %s', model)

            result = (prompt | llm | parser).invoke(input_data)
            LOGGER.info('[transfer] parties: %s', result)

            return result

        except Exception as error:
            LOGGER.warning('[transfer] model %s failed: %s', model, error)
            continue

    return None


def resolve_transfer_holder(holder_name: str | None, circumstances_desc: str) -> str | None:
    if not circumstances_desc or not circumstances_desc.strip():
        LOGGER.info('[transfer] no circumstance description for %r, skipping', holder_name)
        return None

    parties = classify_transfer(holder_name, circumstances_desc)

    if not parties:
        return None

    transferor = (parties.get('transferor') or '').strip()
    transferee = (parties.get('transferee') or '').strip()

    if not transferor or not transferee:
        LOGGER.info(
            '[transfer] incomplete parties (transferor=%r transferee=%r) for %r, skipping',
            transferor,
            transferee,
            holder_name,
        )
        return None

    return f'{transferor} [->] {transferee}'


def resolve_form_3_part_iii_iv_transfer_holder(records: list[dict]) -> str | None:
    direct_change_records = [
        record
        for record in records
        if record.get('direct_before') != record.get('direct_after')
    ]

    if len(direct_change_records) < 2:
        return None

    parser = JsonOutputParser(pydantic_object=TransferParties)

    prompt = ChatPromptTemplate.from_messages([
        ('system', SYSTEM_FORM_3_TRANSFER_PROMPT),
        ('user', USER_FORM_3_TRANSFER_PROMPT),
    ])

    input_data = {
        'records': json.dumps(records, indent=2),
        'format_instructions': parser.get_format_instructions(),
    }
    
    for model in [
        # "nvidia-nemotron-3-ultra",
        "gpt-oss-120b",
    ]:
        try:
            llm = get_llm(model, temperature=0.2)

            if llm is None:
                continue

            LOGGER.info('[form_3_iii_iv_transfer] resolving parties with %s', model)

            parties = (prompt | llm | parser).invoke(input_data)
            LOGGER.info('[form_3_iii_iv_transfer] parties: %s', parties)

            transferor = (parties.get('transferor') or '').strip()
            transferee = (parties.get('transferee') or '').strip()

            if transferor and transferee:
                return f'{transferor} [->] {transferee}'

        except Exception as error:
            LOGGER.warning('[form_3_transfer] model %s failed: %s', model, error)

    return None
