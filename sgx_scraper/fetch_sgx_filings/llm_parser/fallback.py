from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from sgx_scraper.llm.client import get_llm
from sgx_scraper.fetch_sgx_filings.llm_parser.fallback_prompt import (
    RawTransactionExtraction,
    SYSTEM_FALLBACK_PARSER_PROMPT,
    USER_FALLBACK_PARSER_PROMPT,
)
from sgx_scraper.fetch_sgx_filings.parser_forms.base_parser import BaseFormParser
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    safe_convert_float,
    build_value,
    build_price_per_share,
)
from .anchors import region_after_label, region_text

import re
import logging


LOGGER = logging.getLogger(__name__)


def anchor_window(source: BaseFormParser) -> str:
    # The raw share count and consideration live once in Part IV (shared across
    # holders in a multi-holder filing). Anchor to that section so the model can
    # never pick up another transaction's numbers, fall back to the whole document
    # only if the Part IV heading is absent
    blocks = source.ordered_text_blocks()

    part_iv = region_after_label(
        blocks,
        r'Part\s+IV\s*[-–]\s*Transaction',
        end_pattern=r'Particulars of Individual',
    )

    return part_iv if part_iv else region_text(blocks, 0)


def run_extraction(
    source: BaseFormParser,
    window: str,
    holder_name: str | None,
    circumstances_desc: str,
) -> dict | None:
    cache = getattr(source, '_fallback_extraction_cache', None)

    if cache is None:
        cache = {}
        source._fallback_extraction_cache = cache

    cache_key = (window, circumstances_desc)

    if cache_key in cache:
        return cache[cache_key]

    parser = JsonOutputParser(pydantic_object=RawTransactionExtraction)

    prompt = ChatPromptTemplate.from_messages([
        ('system', SYSTEM_FALLBACK_PARSER_PROMPT),
        ('user', USER_FALLBACK_PARSER_PROMPT),
    ])

    input_data = {
        'holder_name': holder_name or '',
        'window': window,
        'circumstances_desc': circumstances_desc or '',
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

            LOGGER.info('extracting raw values with %s', model)

            extraction = (prompt | llm | parser).invoke(input_data)
            LOGGER.info("raw extraction fallback: %s", extraction)

            # the model sometimes wraps the object in a JSON array, unwrap to a dict
            if isinstance(extraction, list):
                extraction = next(
                    (item for item in extraction if isinstance(item, dict)), 
                    None
                )

            if not isinstance(extraction, dict):
                LOGGER.warning('[fallback] %s returned non-object extraction, skipping', model)
                continue

            cache[cache_key] = extraction
            return extraction

        except Exception as error:
            LOGGER.warning('[fallback] model %s failed: %s', model, error)
            continue

    cache[cache_key] = None
    return None


def normalise(text: str) -> str:
    return re.sub(r'\s+', ' ', text or '').strip().lower()


def verified_value(
    value: str | None, 
    source_line: str | None, 
    window: str
) -> str | None:
    if not value:
        return None

    window_normalised = normalise(window)

    if normalise(value) not in window_normalised:
        LOGGER.info('[fallback] rejecting unverifiable value %r (not in window)', value)
        return None

    if source_line and normalise(source_line) not in window_normalised:
        LOGGER.info('[fallback] rejecting value %r (source line not in window)', value)
        return None

    return value


def grounded_type(transaction_type: str | None, circumstances_desc: str) -> str | None:
    if not transaction_type:
        return None

    if not circumstances_desc or not circumstances_desc.strip():
        LOGGER.info(
            '[fallback] rejecting transaction_type %r (no circumstance description supplied)',
            transaction_type,
        )
        return None

    return transaction_type


def compute_fields(
    raw_amount: str | None,
    raw_value: str | None,
    transaction_type: str | None,
    missing_columns: list[str],
) -> dict:
    amount_transaction = safe_convert_float(raw_amount) if raw_amount else None
    price_per_share = build_price_per_share(raw_value, amount_transaction) if raw_value else None

    transaction_value = (
        build_value(raw_value, amount_transaction)
        if (
            'transaction_value' in missing_columns
            and amount_transaction is not None
            and price_per_share is not None
        )
        else None
    )

    filled = {}

    if 'amount_transaction' in missing_columns and amount_transaction is not None:
        filled['amount_transaction'] = int(amount_transaction)

    if 'transaction_value' in missing_columns and transaction_value is not None:
        filled['transaction_value'] = transaction_value

    if 'price_per_share' in missing_columns:
        if price_per_share is not None:
            filled['price_per_share'] = price_per_share

    if 'transaction_type' in missing_columns and transaction_type is not None:
        filled['transaction_type'] = transaction_type

    return filled


def parse_with_llm(
    source: BaseFormParser,
    holder_name: str | None,
    missing_columns: list[str],
    circumstances_desc: str = '',
) -> dict:
    window = anchor_window(source)

    extraction = run_extraction(
        source,
        window,
        holder_name,
        circumstances_desc,
    )

    if not extraction:
        return {}

    raw_amount = verified_value(
        extraction.get('amount_transaction'),
        extraction.get('amount_transaction_source'),
        window,
    )

    raw_value = verified_value(
        extraction.get('consideration'),
        extraction.get('consideration_source'),
        window,
    )

    transaction_type = grounded_type(
        extraction.get('transaction_type'),
        circumstances_desc,
    )

    if (
        raw_amount is None 
        and raw_value is None 
        and transaction_type is None
    ):
        return {}

    return compute_fields(
        raw_amount,
        raw_value,
        transaction_type,
        missing_columns,
    )
