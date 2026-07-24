from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from sgx_scraper.fetch_sgx_filings.llm.client import get_llm
from sgx_scraper.fetch_sgx_filings.llm.prompts import TransferParties, PromptCollections

import logging


LOGGER = logging.getLogger(__name__)


def classify_transfer(holder_name: str | None, circumstances_desc: str) -> dict | None:
    parser = JsonOutputParser(pydantic_object=TransferParties)

    prompt_collections = PromptCollections()

    prompt = ChatPromptTemplate.from_messages([
        ('system', prompt_collections.get_system_transfer_prompt()),
        ('user', prompt_collections.get_user_transfer_prompt()),
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
