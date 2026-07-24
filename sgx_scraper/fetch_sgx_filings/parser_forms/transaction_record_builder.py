from sgx_scraper.utils.date_helper import safe_convert_datetime
from .base_parser import BaseFormParser
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    safe_convert_float,
    build_value,
    build_price_per_share,
    clean_holder_name
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
        holder_type: str,
    ) -> list[dict]:
        source = self._source
        form_name = type(source).__name__

        circumstances = source.extract_circumstances()

        raw_dates = source.find_values_below_label('Date of acquisition of or change in interest')
        raw_amounts = source.find_values_below_label('acquired or disposed of by')
        raw_values = source.find_values_below_label('Amount of consideration')

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

        for index, (raw_date, raw_amount, raw_value, circumstance) in enumerate(
            zip(raw_dates, raw_amounts, raw_values, circumstances)
        ):
            is_voting = voting_flags[index] if index < len(voting_flags) else True

            if not is_voting:
                LOGGER.info(
                    '[%s] skipping non-voting-shares transaction %d', 
                    form_name, 
                    index
                )
                continue

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
            )

            if not source.is_valid_record(transaction):
                LOGGER.info(
                    '[%s] Skipping, due to no change in direct before and after',
                    form_name
                )
                continue

            record = {
                "source": source.pdf_url,
                "symbol": symbol,
                "company_name": company_name,
                "sector": sector,
                "sub_sector": sub_sector,
                "holder_name": clean_holder_name(holder_name),
                "holder_type": holder_type,
                **transaction,
            }

            record = source.generate_title_and_body(record)

            records.append(record)

        return records

    def _extract_transaction(
        self,
        raw_date: str | None,
        raw_amount: str | None,
        raw_value: str | None,
        share_table: list[list[str]],
        circumstance: dict,
    ) -> dict:
        source = self._source

        share_table_result = source.parse_share_table(share_table=share_table)
        amount_float = safe_convert_float(raw_amount)

        amount_transaction = int(amount_float) if amount_float is not None else None
        transaction_value = build_value(raw_value, amount_transaction)

        tags, circumstances_desc = source.detect_tags(circumstance)
        transaction_type = source.build_transaction_type(circumstance, transaction_value)

        return {
            "timestamp": safe_convert_datetime(raw_date),
            "amount_transaction": amount_transaction,
            "transaction_value": int(transaction_value) if transaction_value is not None else None,
            "price_per_share": build_price_per_share(raw_value, amount_transaction),
            "transaction_type": transaction_type,
            "tags": tags,
            "circumstances_desc": circumstances_desc,
            **share_table_result,
        }
