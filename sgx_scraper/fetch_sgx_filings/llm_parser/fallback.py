from datetime import datetime, timedelta

from sgx_scraper.utils.sgx_announcement_html import resolve_document_url
from sgx_scraper.sgx_api.announcements import iter_sgx_announcements
from sgx_scraper.llm.caller import invoke_structured_llm
from sgx_scraper.fetch_sgx_filings.prompts import (
    regular_form,
    form3_part_iii_iv,
    tags as tag_prompts,
    holder,
    price_recovery,
)
from sgx_scraper.fetch_sgx_filings.prompts.models import (
    RawTransactionExtraction,
    AmbiguousTransactionClassification,
    Form3PartIIIIVTransactionExtraction,
    RecoverPricePerShare,
    HolderNameCasing,
    ActingParty,
)
from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.fetch_sgx_filings.parser_forms.base_parser import BaseFormParser
from sgx_scraper.fetch_sgx_filings.utils.amount_helper import (
    safe_convert_float,
    build_value,
    build_price_per_share,
    build_price_tranches,
    clean_amount_text,
    consideration_contradicts_amount,
)
from sgx_scraper.fetch_sgx_filings.utils.holder_helper import (
    format_holder_name,
    clean_holder_name,
)
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    CORPORATE_ACTION_CHECKBOX_KEY,
)
from .anchors import find_label_indices, region_after_label, region_text

import re
import logging
import threading
import fitz


LOGGER = logging.getLogger(__name__)

LLM_EFFORT = "high"

# Calls made and calls that returned nothing after every retry. A run reads
# these to notice an LLM outage, which would otherwise pass silently
LLM_STATS = {"calls": 0, "failed": 0}
LLM_STATS_LOCK = threading.Lock()

# Cased names by original name, shared across a run because the same holders
# file repeatedly
HOLDER_NAME_CASE_CACHE = {}
HOLDER_NAME_CASE_LOCK = threading.Lock()


def call_llm(**kwargs) -> dict | None:
    result = invoke_structured_llm(**kwargs)

    with LLM_STATS_LOCK:
        LLM_STATS["calls"] += 1

        if result is None:
            LLM_STATS["failed"] += 1

    return result


def get_llm_stats() -> dict[str, int]:
    with LLM_STATS_LOCK:
        return dict(LLM_STATS)


def normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


def normalise_source_label(text: str) -> str:
    normalised_text = normalise(text)
    normalised_text = re.sub(r"\s*([()/])\s*", r"\1", normalised_text)

    return normalised_text.rstrip(":;")


def anchor_window(source: BaseFormParser) -> str:
    # The raw share count and consideration live once in Part IV (shared across
    # holders in a multi-holder filing). Anchor to that section so the model can
    # never pick up another transaction"s numbers, fall back to the whole document
    # only if the Part IV heading is absent
    blocks = source.ordered_text_blocks()

    part_iv = region_after_label(
        blocks,
        r"Part\s+IV\s*[-–]\s*Transaction",
        end_pattern=r"Particulars of Individual",
    )

    return part_iv if part_iv else region_text(blocks, 0)


def transaction_window(
    source: BaseFormParser,
    transaction_index: int,
) -> str:
    blocks = source.ordered_text_blocks()

    date_indices = find_label_indices(
        blocks,
        r"Date of acquisition of or change in interest",
    )
    circumstance_indices = find_label_indices(
        blocks,
        r"Circumstance giving rise to the interest",
    )

    return region_text(
        blocks,
        date_indices[transaction_index],
        circumstance_indices[transaction_index],
    )


def run_extraction(
    window: str,
    holder_name: str | None,
    circumstances_desc: str,
    remarks: str,
    deemed_interest_circumstances: str = "",
    ticked_circumstance: str = "",
    models: list[str] = ["nvidia-nemotron-3-ultra", "gpt-oss-120b"],
    max_retry: int = 3
) -> dict | None:
    input_data = {
        "holder_name": holder_name or "",
        "window": window,
        "circumstances_desc": circumstances_desc or "",
        "remarks": remarks,
        "deemed_interest_circumstances": deemed_interest_circumstances or "",
        "ticked_circumstance": ticked_circumstance or "Not available",
    }

    return call_llm(
        pydantic_output=RawTransactionExtraction,
        system_prompt=regular_form.get_system_fallback_parser_prompt(),
        user_prompt=regular_form.get_user_fallback_parser_prompt(),
        log_name="Raw transaction extraction",
        input_data=input_data,
        models=models,
        effort=LLM_EFFORT,
        max_retry=max_retry,
    )


def verified_value(
    value: str | None, 
    source_line: str | None, 
    window: str
) -> str | None:
    if not value:
        return None

    window_normalised = normalise(window)

    if normalise(value) not in window_normalised:
        LOGGER.info("[fallback] rejecting unverifiable value %r (not in window)", value)
        return None

    if (
        source_line 
        and normalise_source_label(source_line) not in normalise_source_label(window)
    ):
        LOGGER.info("[fallback] rejecting value %r (source line not in window)", value)
        return None

    return value


def grounded_type(transaction_type: str | None, circumstances_desc: str) -> str | None:
    if not transaction_type:
        return None

    if not circumstances_desc or not circumstances_desc.strip():
        LOGGER.info(
            "[fallback] rejecting transaction_type %r (no circumstance description supplied)",
            transaction_type,
        )
        return None

    return transaction_type


def clean_change_reason(change_reason: str | None) -> str | None:
    # The phrase is dropped into "The filing attributes the change to ...",
    # so it must not carry its own sentence ending
    if not isinstance(change_reason, str):
        return None

    change_reason = change_reason.strip().rstrip('.').strip()

    return change_reason or None


def get_ticked_circumstance(
    circumstance_key: str | None,
    filing_type: str | None,
) -> str:
    # The box the holder ticked, as the LLM should read it
    if circumstance_key is None:
        return "Others (please specify)"

    if (
        circumstance_key == CORPORATE_ACTION_CHECKBOX_KEY
        and (filing_type or "").startswith("form_3")
    ):
        return (
            "Corporate action by the Listed Issuer which Substantial "
            "Shareholder/Unitholder did not participate in"
        )

    return circumstance_key


def case_holder_name(holder_name: str | None, models: list[str]) -> str | None:
    # A name filed in ALL CAPS is given standard capitalisation by the LLM.
    # The LLM may change case only: any other difference from the original
    # is rejected and plain title-casing is used instead
    if not holder_name or not holder_name.isupper():
        return holder_name

    with HOLDER_NAME_CASE_LOCK:
        cached = HOLDER_NAME_CASE_CACHE.get(holder_name)

    if cached is not None:
        return cached

    result = call_llm(
        pydantic_output=HolderNameCasing,
        system_prompt=holder.get_system_holder_name_casing_prompt(),
        user_prompt=holder.get_user_holder_name_casing_prompt(),
        log_name="Holder name casing",
        input_data={"holder_name": holder_name},
        models=models,
        effort="low",
    )

    cased_name = (result or {}).get("name")

    if (
        not isinstance(cased_name, str)
        or cased_name.casefold() != holder_name.casefold()
    ):
        LOGGER.info(
            "[holder name] LLM casing rejected for %r (got %r), using title case",
            holder_name,
            cased_name,
        )
        cased_name = format_holder_name(holder_name)

    with HOLDER_NAME_CASE_LOCK:
        HOLDER_NAME_CASE_CACHE[holder_name] = cased_name

    return cased_name


def clean_holder_side(holder_side: str | None) -> str | None:
    return holder_side if holder_side in ("receives", "gives") else None


def detect_acting_party(
    holder_name: str | None,
    transaction_type: str | None,
    filing_text: str,
    models: list[str],
) -> str | None:
    # The party the filing says carried out a transaction that moved only the
    # holder's deemed interest. A name must appear in the filing's text; a
    # relative given by relationship is worded "the holder's spouse"
    if not filing_text.strip():
        return None

    result = call_llm(
        pydantic_output=ActingParty,
        system_prompt=holder.get_system_acting_party_prompt(),
        user_prompt=holder.get_user_acting_party_prompt(),
        log_name="Acting party",
        input_data={
            "holder_name": holder_name or "",
            "transaction_type": transaction_type or "",
            "filing_text": filing_text,
        },
        models=models,
        effort=LLM_EFFORT,
    )

    acting_party = (result or {}).get("acting_party")

    if not isinstance(acting_party, str) or not acting_party.strip():
        return None

    acting_party = clean_holder_name(acting_party.strip())

    if acting_party.casefold().startswith("the holder's "):
        return acting_party

    if (
        verified_value(acting_party, None, filing_text) is None
        or acting_party.casefold() == (holder_name or "").casefold()
    ):
        return None

    return case_holder_name(acting_party, models)


def compute_fields(
    raw_amount: str | None,
    raw_value: str | None,
    transaction_type: str | None,
    tags: list[str],
    missing_columns: list[str],
    exchange_rate_date: str | None = None,
) -> dict:
    amount_transaction = safe_convert_float(
        clean_amount_text(raw_amount)
    ) if raw_amount else None

    # The same rules as for values read straight from the form: a
    # consideration for the opposite direction to the amount gives no price,
    # and a consideration written as tranches gives one price per tranche
    if consideration_contradicts_amount(raw_value, raw_amount):
        raw_value = None

    price_per_share = build_price_per_share(
        raw_value,
        amount_transaction,
        exchange_rate_date,
    ) if raw_value else None

    price_tranches = build_price_tranches(
        raw_value,
        amount_transaction,
        exchange_rate_date,
    )

    # A record with no amount or no price has no total either, so the total
    # is worked out whenever one of them had to be recovered
    value_missing = any(
        column in missing_columns
        for column in ("transaction_value", "amount_transaction", "price_per_share")
    )

    transaction_value = (
        build_value(raw_value, amount_transaction, exchange_rate_date)
        if (
            value_missing
            and amount_transaction is not None
            and price_per_share is not None
        )
        else None
    )

    if price_tranches and value_missing:
        transaction_value = sum(
            tranche["price"] * tranche["amount"] for tranche in price_tranches
        )
        price_per_share = round(transaction_value / amount_transaction, 4)

    filled = {}

    if "amount_transaction" in missing_columns and amount_transaction is not None:
        filled["amount_transaction"] = int(amount_transaction)

    if value_missing and transaction_value is not None:
        filled["transaction_value"] = int(transaction_value)

    if "price_per_share" in missing_columns:
        if price_per_share is not None:
            filled["price_per_share"] = price_per_share

            if price_tranches:
                filled["_price_components"] = price_tranches

    if "transaction_type" in missing_columns and transaction_type is not None:
        filled["transaction_type"] = transaction_type

    if "transaction_type" in missing_columns and tags:
        filled["tags"] = tags

    return filled


def parse_with_llm(
    source: BaseFormParser,
    holder_name: str | None,
    missing_columns: list[str],
    models: list[str] = ["nvidia-nemotron-3-ultra", "gpt-oss-120b"],
    circumstances_desc: str = "",
    remarks: str = "",
    deemed_interest_circumstances: str = "",
    ticked_circumstance: str = "",
    transaction_index: int = 0,
    exchange_rate_date: str | None = None,
) -> dict:
    window = transaction_window(source, transaction_index)

    # LOGGER.info("check window: %s", window)

    extraction = run_extraction(
        window,
        holder_name,
        circumstances_desc,
        remarks,
        deemed_interest_circumstances,
        ticked_circumstance,
        models=models
    )

    if not extraction:
        return {}

    # The amount and consideration may be stated in the circumstance text when
    # the form's own fields are "N.A", so a value is accepted from either
    value_text = "\n".join(
        [window, circumstances_desc, remarks, deemed_interest_circumstances]
    )

    raw_amount = verified_value(
        extraction.get("amount_transaction"),
        extraction.get("amount_transaction_source"),
        value_text,
    )

    raw_value = verified_value(
        extraction.get("consideration"),
        extraction.get("consideration_source"),
        value_text,
    )

    transaction_type = grounded_type(
        extraction.get("transaction_type"),
        circumstances_desc,
    )

    tags = extraction.get("tags") or []
    change_reason = clean_change_reason(extraction.get("change_reason"))
    holder_side = clean_holder_side(extraction.get("holder_side"))

    if (
        raw_amount is None 
        and raw_value is None 
        and transaction_type is None
        and not tags
        and change_reason is None
        and holder_side is None
    ):
        return {}

    filled = compute_fields(
        raw_amount,
        raw_value,
        transaction_type,
        tags,
        missing_columns,
        exchange_rate_date,
    )

    if change_reason is not None:
        filled["change_reason"] = change_reason

    if holder_side is not None:
        # Only used to word the title and body, so it is not stored
        filled["_holder_side"] = holder_side

    return filled


def build_all_shareholder_context(contexts: list[dict]) -> str:
    context_sections = []

    for shareholder_position, context in enumerate(contexts):
        shareholder_label = chr(ord("A") + shareholder_position)
        holder_name = context.get("holder_name") or "N.A."
        awareness_explanation = context.get("awareness_explanation") or "N.A."
        deemed_interest_circumstances = (
            context.get("deemed_interest_circumstances") or "N.A."
        )
        shareholder_relationship = context.get("shareholder_relationship") or "N.A."
        remarks = context.get("remarks") or "N.A."

        context_sections.append(
            f"[Shareholder {shareholder_label}: {holder_name}]\n"
            "Awareness explanation:\n"
            f"{awareness_explanation}\n\n"
            "Circumstances giving rise to deemed interests:\n"
            f"{deemed_interest_circumstances}\n\n"
            "Relationship between shareholders:\n"
            f"{shareholder_relationship}\n\n"
            "Remarks:\n"
            f"{remarks}"
        )

    return "\n\n".join(context_sections)


def parse_with_llm_form3_iii_iv(
    source: BaseFormParser,
    holder_name: str | None,
    missing_columns: list[str],
    circumstances_desc: str,
    contexts: list[dict],
    models: list[str] = ["nvidia-nemotron-3-ultra", "gpt-oss-120b"],
    max_retry: int = 3,
    exchange_rate_date: str | None = None,
    ticked_circumstance: str = "",
) -> dict:
    window = anchor_window(source)

    all_shareholder_context = build_all_shareholder_context(contexts)
    # LOGGER.info("all shareholder context:\n%s", all_shareholder_context)

    input_data = {
        "current_shareholder": holder_name or "",
        "part_iv_context": window,
        "all_shareholder_context": all_shareholder_context,
        "ticked_circumstance": ticked_circumstance or "Not available",
    }

    extraction = call_llm(
        pydantic_output=Form3PartIIIIVTransactionExtraction,
        system_prompt=form3_part_iii_iv.get_system_form3_part_iii_iv_prompt(),
        user_prompt=form3_part_iii_iv.get_user_form3_part_iii_iv_prompt(),
        log_name="Form 3 Part III/IV extraction",
        input_data=input_data,
        models=models,
        effort=LLM_EFFORT,
        max_retry=max_retry,
    )

    if not extraction:
        return {}

    value_text = "\n".join([window, circumstances_desc, all_shareholder_context])

    raw_amount = verified_value(
        extraction.get("amount_transaction"),
        extraction.get("amount_transaction_source"),
        value_text,
    )

    raw_value = verified_value(
        extraction.get("consideration"),
        extraction.get("consideration_source"),
        value_text,
    )

    transaction_type = grounded_type(
        extraction.get("transaction_type"),
        circumstances_desc,
    )

    tags = extraction.get("tags") or []
    change_reason = clean_change_reason(extraction.get("change_reason"))
    holder_side = clean_holder_side(extraction.get("holder_side"))

    if (
        raw_amount is None
        and raw_value is None
        and transaction_type is None
        and not tags
        and change_reason is None
        and holder_side is None
    ):
        return {}

    filled = compute_fields(
        raw_amount,
        raw_value,
        transaction_type,
        tags,
        missing_columns,
        exchange_rate_date,
    )

    if change_reason is not None:
        filled["change_reason"] = change_reason

    if holder_side is not None:
        # Only used to word the title and body, so it is not stored
        filled["_holder_side"] = holder_side

    return filled


def detect_tags_ambiguous(
    circumstance_key: str,
    remarks: str, 
    circumstances_deemed: str,
    models: list[str] = ["nvidia-nemotron-3-ultra", "gpt-oss-120b"],
    max_retry: int = 3
) -> list | None:
    input_data = {
        "selected_circumstance": circumstance_key,
        "circumstances_deemed_desc": circumstances_deemed,
        "remarks": remarks,
    }

    extraction = call_llm(
        pydantic_output=AmbiguousTransactionClassification,
        system_prompt=tag_prompts.get_system_ambiguous_tag_prompt(),
        user_prompt=tag_prompts.get_user_ambiguous_tag_prompt(),
        log_name="Ambiguous transaction tags",
        input_data=input_data,
        models=models,
        effort=LLM_EFFORT,
        max_retry=max_retry,
    )

    if not extraction:
        return None

    return extraction.get("tags")


def recover_price_per_share(
    target_transaction_label: str,
    transaction_contexts: list[dict],
    models: list[str] = [
        "nvidia-nemotron-3-ultra",
        "deepsek-v4-flash"
    ],
    is_form3_iii_iv: bool = False
) -> float | None:
    if is_form3_iii_iv:
        input_data = {
            "target_transaction": target_transaction_label,
            "shareholder_context": build_all_shareholder_context(
                transaction_contexts
            ),
        }
        system_prompt = (
            price_recovery.get_system_recover_price_per_share_form3_part_iii_iv()
        )
        user_prompt = (
            price_recovery.get_user_recover_price_per_share_form3_part_iii_iv()
        )
        log_name = "Recover price_per_share Form 3 Part III/IV"

    else:
        transaction_context_parts = []

        for context in transaction_contexts:
            transaction_context_parts.append(
                f"[Transaction {context.get("transaction_number")}]\n"
                f"Amount transaction: {context.get("amount_transaction")}\n"
                f"Amount of consideration: {context.get("raw_consideration")}\n"
                f"Circumstances description: {context.get("circumstances_desc")}\n"
                f"Remarks: {context.get("remarks")}"
            )

        input_data = {
            "target_transaction": target_transaction_label,
            "transaction_context": "\n\n".join(transaction_context_parts),
        }

        system_prompt = price_recovery.get_system_recover_price_per_share()
        user_prompt = price_recovery.get_user_recover_price_per_share()
        log_name = "Recover price_per_share"

    result = call_llm(
        pydantic_output=RecoverPricePerShare,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        log_name=log_name,
        input_data=input_data,
        models=models,
        effort=LLM_EFFORT,
    )

    return (result or {}).get("price_per_share")


def recover_price_from_general_announcement(
    filing_symbol: str,
    timestamp: str,
    models: list[str]
) -> str | None:
    filing_date = datetime.strptime(timestamp, "%Y-%m-%d")
    period_start = (filing_date - timedelta(days=2)).strftime("%Y%m%d")
    period_end = (filing_date + timedelta(days=1)).strftime("%Y%m%d")

    announcements = iter_sgx_announcements(
        sub_category="ANNC18",
        flag_log="general announcement",
        period_start=period_start,
        period_end=period_end,
        page_size=100
    )

    normalized_filing_symbol = filing_symbol.strip().upper()
    title_phrase = "payment of management fees"

    pdf_url = None 

    for announcement in announcements:
        issuers = announcement.get("issuers") or []

        announcement_symbols = {
            issuer.get("stock_code", "").strip().upper()
            for issuer in issuers
            if issuer.get("stock_code")
        }

        if normalized_filing_symbol not in announcement_symbols:
            continue

        title = (announcement.get("title") or "").strip().lower()

        if title_phrase not in title:
            continue

        announcement_url = announcement.get("url")

        if not announcement_url:
            return None

        pdf_url = resolve_document_url(announcement_url)
        break 

    if not pdf_url:
        return None 
    
    response = HTTPCLIENT.get(pdf_url)
    response.raise_for_status()
    pdf_bytes = response.content

    with fitz.open(
        stream=pdf_bytes, 
        filetype="pdf"
    ) as document:
        page_texts = []

        for page in document:
            page_texts.append(page.get_text())

        pdf_text = "".join(page_texts)

    input_data = {
        "announcement_text": pdf_text
    }

    result = call_llm(
        pydantic_output=RecoverPricePerShare,
        system_prompt=price_recovery.get_system_recover_price_general_announcement(),
        user_prompt=price_recovery.get_user_recover_price_general_announcement(),
        log_name="Recover price_per_share from general announcement",
        input_data=input_data,
        models=models,
        effort=LLM_EFFORT,
    )
    
    return (result or {}).get("price_per_share")
