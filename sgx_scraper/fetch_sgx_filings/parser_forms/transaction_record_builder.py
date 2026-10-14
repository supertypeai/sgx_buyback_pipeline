from sgx_scraper.utils.date_helper import safe_convert_datetime
from .base_parser import BaseFormParser
from sgx_scraper.fetch_sgx_filings.utils.amount_helper import (
    safe_convert_float,
    build_value,
    build_price_per_share,
    clean_amount_text,
    pick_stated_amount,
    consideration_contradicts_amount,
    build_price_tranches,
)
from sgx_scraper.fetch_sgx_filings.utils.holder_helper import (
    clean_holder_name,
    classify_holder_type,
)

import logging


LOGGER = logging.getLogger(__name__)


class TransactionRecordBuilder:
    """
    Single-holder transaction loop shared by Form 1, Form 3 Part II, and Form 6 part II.

    Composed with a `source` parser (any BaseFormParser) that supplies the PDF
    extraction primitives. The form stays responsible for its own holder logic
    and passes the resolved holder in, so the two form classes remain fully
    independent only this one loop is shared.
    """

    def __init__(self, source: BaseFormParser):
        self._source = source

    def build(
        self,
        *,
        symbol: str | None,
        company_name: str | None,
        sector: str | None,
        sub_sector: str | None,
        holder_name: str | None,
    ) -> list[dict]:
        source = self._source
        form_name = type(source).__name__

        circumstances = source.extract_circumstances()

        raw_dates = source.find_values_below_label('Date of acquisition of or change in interest')
        raw_amounts = source.find_values_below_label('acquired or disposed of by')
        raw_values = source.find_values_below_label('Amount of consideration')
        deemed_interest_circumstances = source.find_section_values(
            'Circumstances giving rise to deemed interests',
            r'Attachments\s*\(\s*if any\s*\)',
        )
        remarks = source.find_section_values(
            r'Remarks\s*\(\s*if any\s*\)',
            r'Transaction Reference Number\s*\(\s*auto-generated\s*\)',
        )

        # One entry per transaction, in the same order as the fields above
        transaction_references = source.extract_transaction_references()
        notification_types = source.extract_notification_types()
        security_noun = source.extract_security_noun()

        # These four count EVERY transaction (voting shares + rights/options/etc).
        voting_flags = source.voting_shares_flags()

        counts = {
            'circumstances': len(circumstances),
            'dates': len(raw_dates),
            'amounts': len(raw_amounts),
            'values': len(raw_values),
            'flags': len(voting_flags),
        }

        if len(set(counts.values())) != 1:
            LOGGER.warning(
                '[%s] count mismatch %s for %s',
                form_name,
                counts,
                source.pdf_url
            )

        # Share tables exist only for voting-shares transactions, so they are
        # consumed in order as each voting transaction is reached, never zipped by
        # index (a mixed filing has fewer tables than transactions)
        share_tables = iter(source.extract_share_tables())

        records = []
        transaction_contexts = []

        transaction_data = zip(
            raw_dates, 
            raw_amounts, 
            raw_values, 
            circumstances, 
        )

        for index, (
            raw_date, 
            raw_amount, 
            raw_value, 
            circumstance,
        ) in enumerate(
            transaction_data
        ):
            is_voting = voting_flags[index] if index < len(voting_flags) else True

            if not is_voting:
                LOGGER.info(
                    '[%s] skipping non-voting-shares transaction %d', 
                    form_name, 
                    index
                )
                continue

            transaction_contexts.append({
                "transaction_number": chr(ord("A") + index),
                "amount_transaction": raw_amount,
                "raw_consideration": raw_value,
                "remarks": remarks[index],
                "circumstances_desc": circumstance
            })
            
            share_table = next(share_tables, None)

            if share_table is None:
                LOGGER.warning(
                    '[%s] no share table for voting transaction %d in %s', 
                    form_name, 
                    index, 
                    source.pdf_url
                )
                continue

            transaction = self._extract_transaction(
                raw_date=raw_date,
                raw_amount=raw_amount,
                raw_value=raw_value,
                share_table=share_table,
                circumstance=circumstance,
                deemed_interest_circumstances=deemed_interest_circumstances[index],
                remarks=remarks[index],
            )

            # if not source.is_valid_record(transaction):
            #     LOGGER.info(
            #         '[%s] Skipping, due to no change in direct before and after',
            #         form_name
            #     )
            #     continue

            holder_type = classify_holder_type(
                parser_form=form_name, 
                company_name=company_name,
                filing_symbol=symbol, 
                holder_name=holder_name,
            )
            
            record = {
                "source": source.pdf_url,
                "filing_type": source.FILING_TYPE,
                "transaction_reference": (
                    transaction_references[index]
                    if index < len(transaction_references)
                    else None
                ),
                "notification_type": (
                    notification_types[index]
                    if index < len(notification_types)
                    else None
                ),
                "symbol": symbol,
                "company_name": company_name,
                "sector": sector,
                "sub_sector": sub_sector,
                "raw_values": raw_value,
                "holder_name": clean_holder_name(holder_name),
                "holder_type": holder_type,
                "source_is_manual": False,
                **transaction,
            }

            record['_security_noun'] = security_noun
            record = source.generate_title_and_body(record)
            record['_transaction_index'] = index

            records.append(record)

        source.transaction_data_contexts = transaction_contexts

        return records

    def _extract_transaction(
        self,
        raw_date: str | None,
        raw_amount: str | None,
        raw_value: str | None,
        share_table: list[list[str]],
        circumstance: dict,
        deemed_interest_circumstances: str | None,
        remarks: str | None,
    ) -> dict:
        source = self._source

        share_table_result = source.parse_share_table(share_table=share_table)
        amount_float = safe_convert_float(clean_amount_text(raw_amount))

        amount_transaction = int(amount_float) if amount_float is not None else None
        amount_transaction = (
            pick_stated_amount(raw_amount, share_table_result) or amount_transaction
        )
        timestamp = safe_convert_datetime(raw_date)

        if consideration_contradicts_amount(raw_value, raw_amount):
            raw_value = None

        price_per_share = build_price_per_share(
            raw_value,
            amount_transaction,
            timestamp,
        )

        transaction_value = (
            build_value(raw_value, amount_transaction, timestamp)
            if amount_transaction is not None 
            and price_per_share is not None
            else None
        )

        price_tranches = build_price_tranches(
            raw_value,
            amount_transaction,
            timestamp,
        )

        if price_tranches:
            transaction_value = sum(
                tranche["price"] * tranche["amount"] for tranche in price_tranches
            )
            price_per_share = round(transaction_value / amount_transaction, 4)

        transaction_type, tags, specific_key = source.build_transaction_type_and_tags(
            circumstance
        )
        
        circumstances_desc = source.build_circumstances_description(circumstance)

        return {
            "timestamp": timestamp,
            # One entry per tranche when the form prices the securities in parts
            "_price_components": price_tranches,
            "amount_transaction": amount_transaction,
            "transaction_value": int(transaction_value) if transaction_value is not None else None,
            "price_per_share": price_per_share,
            "transaction_type": transaction_type,
            "tags": tags,
            "circumstances_desc": circumstances_desc,
            "circumstance_key": specific_key, 
            "change_reason": None,
            "deemed_interest_circumstances": deemed_interest_circumstances,
            "remarks": remarks,
            **share_table_result,
        }
