from sgx_scraper.fetch_sgx_filings.parser_forms.router import RouterFormParser
from sgx_scraper.fetch_sgx_filings.parser_forms.base_parser import BaseFormParser
from sgx_scraper.utils.sgx_announcement_html import resolve_document_url
from .llm_parser.fallback import (
    get_ticked_circumstance,
    case_holder_name,
    parse_with_llm, 
    parse_with_llm_form3_iii_iv, 
    detect_tags_ambiguous,
    detect_acting_party,
    build_all_shareholder_context,
    recover_price_per_share,
    recover_price_from_general_announcement
)
from sgx_scraper.fetch_sgx_filings.utils.currency_converter import (
    get_latest_currency, 
    calculate_currency_to_sgd
)
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    CORPORATE_ACTION_CHECKBOX_KEY,
)
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    build_filing_payload,
    needs_acting_party,
    is_unmoved_joint_holder,
)
from sgx_scraper.utils.json_helper import open_json

import logging
import time


LOGGER = logging.getLogger(__name__)

TOP_200_PATH = "sgx_scraper/fetch_sgx_filings/data/db_top_200/records.json"


def process_price_per_share_recover(
    record_filing: dict,
    price_per_share_recovered: dict, 
    timestamp: str 
):
    if "components" in price_per_share_recovered:
        transaction_value = 0

        for price_record in price_per_share_recovered["components"]:
            currency = price_record.get("currency")
            price = price_record.get("price")

            if (
                currency is not None and 
                currency != "SGD"
            ): 
                rate = get_latest_currency(
                    currency_from=currency,
                    exchange_rate_date=timestamp
                )
            
                price_converted = calculate_currency_to_sgd(
                    amount=price, 
                    rate_sgd=rate,
                    digits=4,
                )

                price_record["price"] = price_converted

            transaction_value += (
                price_record["amount"] * price_record["price"]
            )

        record_filing["transaction_value"] = int(transaction_value)

        # Kept so the stored price_per_share can list every tranche
        record_filing["_price_components"] = [
            {
                "price": price_record.get("price"),
                "amount": price_record.get("amount"),
            }
            for price_record in price_per_share_recovered["components"]
        ]
        
    else: 
        price = price_per_share_recovered.get("price")
        currency = price_per_share_recovered.get("currency")

        if (
            currency is not None and 
            currency != "SGD"
        ): 
            rate = get_latest_currency(
                currency_from=currency,
                exchange_rate_date=timestamp
            )
        
            price_converted = calculate_currency_to_sgd(
                amount=price, 
                rate_sgd=rate,
                digits=4,
            ) 

            price_per_share_recovered["price"] = price_converted

        amount_transaction = record_filing.get("amount_transaction")

        if amount_transaction is not None:
            record_filing["transaction_value"] = int(
                amount_transaction * price_per_share_recovered["price"]
            )


def get_recovered_price_per_share(
    record_filing: dict,
    price_per_share_recovered: dict,
) -> float | None:
    # The recovered price is an object, either one price or several components.
    # While the record is being processed it carries a single number: the
    # price itself, or the average across components (total value over total
    # amount). finalize_price_per_share turns it into the stored list
    if "components" not in price_per_share_recovered:
        return price_per_share_recovered.get("price")

    amount_transaction = record_filing.get("amount_transaction")
    transaction_value = record_filing.get("transaction_value")

    if not amount_transaction or not transaction_value:
        return None

    return round(transaction_value / amount_transaction, 4)


def normalize_classification(record: dict) -> None:
    # Buy and sell only come from a ticked checkbox or an explicit LLM answer;
    # anything still unclassified after the fallback is "others"
    if record.get("transaction_type") is None:
        record["transaction_type"] = "others"

    # When the corporate-action box is ticked and none of the holder's own
    # securities moved, only the issuer acted, so a tag describing an action
    # by the holder does not belong. The prompt says so too; this catches the
    # cases where the LLM tags the mechanism anyway. A holder whose securities
    # did move under this box, such as a manager paid in fee units, keeps its
    # tags
    securities_moved = (
        record.get("direct_before") != record.get("direct_after")
        or record.get("deemed_before") != record.get("deemed_after")
    )

    if (
        record.get("circumstance_key") == CORPORATE_ACTION_CHECKBOX_KEY
        and not securities_moved
    ):
        kept_tags = [
            tag
            for tag in record.get("tags") or []
            if tag == "share-buyback"
        ]

        if kept_tags != (record.get("tags") or []):
            LOGGER.info(
                "Dropping tags %s from corporate-action record of %s",
                record.get("tags"),
                record.get("holder_name"),
            )
            record["tags"] = kept_tags

    # A buy or sell must not contradict the form's own table, whether it came
    # from the LLM or from a ticked acquisition or disposal box: filers do
    # tick the wrong box, and one box cannot describe several movements. The
    # table never chooses the type, but a "sell" while the holder's total
    # rose, or a "buy" while it fell, is vetoed and becomes "others"
    holding_before = record.get("holding_before")
    holding_after = record.get("holding_after")

    if (
        holding_before is not None
        and holding_after is not None
        and (
            (record["transaction_type"] == "buy" and holding_after < holding_before)
            or (record["transaction_type"] == "sell" and holding_after > holding_before)
        )
    ):
        LOGGER.info(
            "Type %s of %s contradicts the share table, using others",
            record["transaction_type"],
            record.get("holder_name"),
        )
        record["transaction_type"] = "others"

    # One ticked box in Part IV of a multi-holder Form 3 covers every holder
    # giving notice. A holder none of whose holdings moved took no part in
    # that transaction, so it is not a buy or a sell for that holder and the
    # group's amount and consideration are not carried on its record
    if (
        record["transaction_type"] in ("buy", "sell")
        and is_unmoved_joint_holder(record)
    ):
        LOGGER.info(
            "No holding of %s moved, using others and no amount",
            record.get("holder_name"),
        )
        record["transaction_type"] = "others"
        record["amount_transaction"] = None
        record["price_per_share"] = None
        record["transaction_value"] = None
        record.pop("_price_components", None)

    # A transfer is a non-sale movement such as a gift, so it never applies
    # to a buy or a sell
    if record["transaction_type"] in ("buy", "sell"):
        record["tags"] = [
            tag
            for tag in record.get("tags") or []
            if tag != "transfer"
        ]


def finalize_price_per_share(record: dict) -> None:
    # price_per_share is stored as a list of {"price", "amount"} tranches.
    # Most transactions have one. Fee units issued at several prices, usual on
    # Form 6, have one entry per tranche. With no price and no amount the
    # list is empty
    components = record.get("_price_components")

    if components:
        record["price_per_share"] = components
        return

    # A form that states the amount or the consideration as 0 or "Nil" gives
    # no amount and no price
    price_per_share = record.get("price_per_share") or None
    amount_transaction = record.get("amount_transaction") or None

    if price_per_share is None and not record.get("transaction_value"):
        record["transaction_value"] = None

    if price_per_share is None and amount_transaction is None:
        record["price_per_share"] = []
        return

    record["price_per_share"] = [
        {
            "price": price_per_share,
            "amount": amount_transaction,
        }
    ]


def strip_internal_fields(record: dict) -> None:
    # Underscore keys only carry context for the title and body
    for key in [key for key in record if key.startswith("_")]:
        del record[key]


def fill_acting_party(record: dict, filing_text: str, models: list[str]) -> None:
    # A holder whose interest is only deemed did not carry out the
    # transaction, so the body names the party the filing says did
    if not needs_acting_party(record):
        return

    LOGGER.info("Acting party lookup fires for %s", record.get("holder_name"))

    record["acting_party"] = detect_acting_party(
        holder_name=record.get("holder_name"),
        transaction_type=record.get("transaction_type"),
        filing_text=filing_text,
        models=models,
    )


def apply_holder_name_casing(result_parsed: list[dict], models: list[str]) -> None:
    # Names filed in ALL CAPS get standard capitalisation, on the record and
    # on the link to the direct holder used in other records' bodies
    for record in result_parsed:
        for key in ("holder_name", "_primary_holder"):
            if record.get(key):
                record[key] = case_holder_name(record[key], models)

        record["_holder_name_cased"] = True


def process_form3_part_iii_iv_records(
    parser: BaseFormParser,
    result_parsed: list[dict],
    form_3_part_iii_iv_context_records: list[dict],
    columns_to_check: list[str],
    models: list[str],
) -> None:
    for record in result_parsed:
        circumstances_desc = record.get("circumstances_desc") or ""
        tags = record.get("tags") or []
        raw_values = (record.get("raw_values") or "").strip().lower()
        timestamp = record.get("timestamp")

        missing_columns = [
            column
            for column in columns_to_check
            if record.get(column) is None
        ]

        if missing_columns:
            LOGGER.info(
                "Fallback fires because missing values on %s",
                ", ".join(missing_columns)
            )

            filled = parse_with_llm_form3_iii_iv(
                source=parser,
                holder_name=record.get("holder_name"),
                missing_columns=missing_columns,
                circumstances_desc=circumstances_desc,
                contexts=form_3_part_iii_iv_context_records,
                models=models,
                exchange_rate_date=timestamp,
                ticked_circumstance=get_ticked_circumstance(
                    record.get("circumstance_key"), record.get("filing_type")
                ),
            )

            for column, value in filled.items():
                record[column] = value

            time.sleep(2)

        if (
            record.get("price_per_share") is None and
            (
                "refer" in raw_values or
                "see" in raw_values or
                "management-fees" in tags
            )
        ):
            LOGGER.info("Recover price per share fires for Form 3 Part III/IV")

            price_per_share_recovered = recover_price_per_share(
                target_transaction_label=record.get("holder_name") or "",
                transaction_contexts=form_3_part_iii_iv_context_records,
                models=models,
                is_form3_iii_iv=True,
            )

            if price_per_share_recovered is not None:
                process_price_per_share_recover(
                    record_filing=record,
                    price_per_share_recovered=price_per_share_recovered,
                    timestamp=timestamp,
                )

                record["price_per_share"] = get_recovered_price_per_share(
                    record_filing=record,
                    price_per_share_recovered=price_per_share_recovered,
                )

    share_price_across_holders(result_parsed)

    for record in result_parsed:
        normalize_classification(record)
        fill_acting_party(
            record,
            "Circumstance description:\n"
            f"{record.get('circumstances_desc') or 'N.A.'}\n\n"
            f"{build_all_shareholder_context(form_3_part_iii_iv_context_records)}",
            models,
        )
        parser.generate_title_and_body(record=record)
        finalize_price_per_share(record)
        strip_internal_fields(record)


def share_price_across_holders(result_parsed: list[dict]) -> None:
    # Every holder of a Form 3 Part III/IV filing reports the same Part IV
    # transaction. When the price was found for one holder and not for
    # another with the same amount, they get the same price. Holders whose
    # found prices differ are left as they are
    priced = {}

    for record in result_parsed:
        amount = record.get("amount_transaction")

        if not amount or record.get("price_per_share") is None:
            continue

        price_fields = (
            record.get("price_per_share"),
            record.get("transaction_value"),
            record.get("_price_components"),
        )

        if priced.setdefault(amount, price_fields)[:2] != price_fields[:2]:
            priced[amount] = None

    for record in result_parsed:
        price_fields = priced.get(record.get("amount_transaction"))

        if price_fields and record.get("price_per_share") is None:
            (
                record["price_per_share"],
                record["transaction_value"],
                record["_price_components"],
            ) = price_fields


def process_regular_form_records(
    parser: BaseFormParser,
    result_parsed: list[dict],
    regular_forms_context_records: list[dict],
    columns_to_check: list[str],
    models: list[str],
) -> None:
    for record in result_parsed:
        transaction_index = record.pop("_transaction_index")
        circumstances_desc = record.get("circumstances_desc") or ""
        deemed_interest_circumstances = record.get(
            "deemed_interest_circumstances"
        )
        circumstance_key = record.get("circumstance_key")
        remarks = record.get("remarks")
        tags = record.get("tags")
        price_per_share = record.get("price_per_share")
        raw_values = record.get("raw_values").strip().lower()
        symbol = record.get("symbol")
        timestamp = record.get("timestamp")

        missing_columns = [
            column
            for column in columns_to_check
            if record.get(column) is None
        ]

        if missing_columns:
            LOGGER.info(
                "Fallback fires because missing values on %s",
                ", ".join(missing_columns)
            )

            filled = parse_with_llm(
                source=parser,
                holder_name=record.get("holder_name"),
                missing_columns=missing_columns,
                circumstances_desc=circumstances_desc,
                remarks=remarks or "",
                deemed_interest_circumstances=deemed_interest_circumstances or "",
                ticked_circumstance=get_ticked_circumstance(
                    circumstance_key, record.get("filing_type")
                ),
                transaction_index=transaction_index,
                models=models,
                exchange_rate_date=timestamp,
            )

            for column, value in filled.items():
                record[column] = value

            time.sleep(2)

        if circumstance_key in (
            "Securities via off-market transaction",
            "Securities following conversion/exercise"
        ):
            LOGGER.info(
                "LLM call to detect the tags for %s fires",
                circumstance_key
            )

            tags = detect_tags_ambiguous(
                circumstance_key=circumstance_key,
                circumstances_deemed=deemed_interest_circumstances,
                remarks=remarks,
                models=models,
            )

            if tags is not None:
                record["tags"] = tags

        if (
            price_per_share is None and
            (
                "management-fees" in tags or
                "refer" in raw_values or
                "see" in raw_values
            )
        ):
            LOGGER.info("Recover price per share fires")

            price_per_share_recovered = recover_price_per_share(
                models=models,
                target_transaction_label=chr(ord("A") + transaction_index),
                transaction_contexts=regular_forms_context_records,
            )

            if price_per_share_recovered is None:
                price_per_share_recovered = recover_price_from_general_announcement(
                    filing_symbol=symbol,
                    timestamp=timestamp,
                    models=models,
                )

            if price_per_share_recovered is not None:
                process_price_per_share_recover(
                    record_filing=record,
                    price_per_share_recovered=price_per_share_recovered,
                    timestamp=timestamp,
                )

                record["price_per_share"] = get_recovered_price_per_share(
                    record_filing=record,
                    price_per_share_recovered=price_per_share_recovered,
                )

        normalize_classification(record)
        
        fill_acting_party(
            record,
            "Circumstance description:\n"
            f"{circumstances_desc or 'N.A.'}\n\n"
            "Circumstances giving rise to deemed interests:\n"
            f"{deemed_interest_circumstances or 'N.A.'}\n\n"
            "Remarks:\n"
            f"{remarks or 'N.A.'}",
            models,
        )
        parser.generate_title_and_body(record=record)
        finalize_price_per_share(record)
        strip_internal_fields(record)


def get_symbols_top_200() -> set[str]:
    db_top_200 = open_json(TOP_200_PATH)

    return {
        record["symbol"]
        for record in db_top_200
    }


def is_top_200_symbol(symbol: str | None, symbols_top_200: set[str]) -> bool:
    if not symbol:
        return False

    return f"{symbol}.SI" in symbols_top_200


def is_top_200_announcement(
    announcement: dict,
    symbols_top_200: set[str],
) -> bool:
    return any(
        is_top_200_symbol(issuer.get("stock_code"), symbols_top_200)
        for issuer in announcement.get("issuers") or []
    )


def get_announcement_symbol(
    announcement: dict,
    symbols_top_200: set[str],
) -> str | None:
    stock_codes = {
        issuer.get("stock_code")
        for issuer in announcement.get("issuers") or []
        if is_top_200_symbol(issuer.get("stock_code"), symbols_top_200)
    }

    return stock_codes.pop() if len(stock_codes) == 1 else None


def get_sgx_filings(
    url: str,
    models: list[str] = [
        "nvidia-nemotron-3-ultra",
        "deepsek-v4-flash",
        "gpt-oss-120b"
    ],
    fallback_symbol: str | None = None,
) -> list[dict]:
    pdf_url = resolve_document_url(url)

    if not pdf_url:
        return []

    parser = RouterFormParser.get_parser(pdf_url)

    if parser is None:
        LOGGER.info("[sgx_filings] skipping unsupported form: %s", pdf_url)
        return []

    parser.fallback_symbol = fallback_symbol

    # Voting-shares filtering now happens inside each parser: per-transaction in the
    # builder-based forms, and once per shared Part IV in Form 3 Part III/IV.
    result_parsed = parser.parse_records()

    symbols_top_200 = get_symbols_top_200()

    for record in result_parsed:
        if not is_top_200_symbol(record.get("symbol"), symbols_top_200):
            LOGGER.info(
                "Skipping not in top 200, for pdf url: %s",
                pdf_url
            )
            return []
    
    form_3_part_iii_iv_context_records = getattr(
        parser,
        "transfer_context_records",
        None,
    )

    regular_forms_context_records = getattr(
        parser,
        "transaction_data_contexts",
        None,
    )

    columns_to_check = [
        "price_per_share",
        "amount_transaction",
        "transaction_type"
    ]

    apply_holder_name_casing(result_parsed, models)

    # The Part III context names each holder too, and must match the records
    for context in form_3_part_iii_iv_context_records or []:
        context["holder_name"] = case_holder_name(
            context.get("holder_name"), models
        )

    if form_3_part_iii_iv_context_records:
        process_form3_part_iii_iv_records(
            parser=parser,
            result_parsed=result_parsed,
            form_3_part_iii_iv_context_records=form_3_part_iii_iv_context_records,
            columns_to_check=columns_to_check,
            models=models,
        )
    else:
        process_regular_form_records(
            parser=parser,
            result_parsed=result_parsed,
            regular_forms_context_records=regular_forms_context_records,
            columns_to_check=columns_to_check,
            models=models,
        )

    return [
        build_filing_payload(record)
        for record in result_parsed
    ]
