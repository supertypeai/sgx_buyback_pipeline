from ..base_parser import BaseFormParser
from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    safe_convert_float,
    build_value,
    build_price_per_share,
    classify_holder_type,
)

import logging


LOGGER = logging.getLogger(__name__)


class Form3PartIIIandIV(BaseFormParser):
    """
    Multiple substantial shareholders (Part III) with one shared transaction
    (Part IV).

    The loop is over holders, not transactions: each holder keeps its own name,
    date and before/after share table, while the transaction amount, amount of
    consideration and circumstance come once from Part IV and are broadcast to
    every holder.
    """

    def extract_transaction(
        self,
        raw_date: str | None,
        raw_amount: str | None,
        raw_value: str | None,
        share_table: list[list[str]],
        circumstance: dict,
    ) -> dict:
        share_table_result = self.parse_share_table(share_table=share_table)

        amount_transaction = safe_convert_float(raw_amount)

        price_per_share = build_price_per_share(
            raw_value, 
            amount_transaction
        )

        transaction_value = (
            build_value(raw_value, amount_transaction)
            if amount_transaction is not None 
            and price_per_share is not None
            else None
        )

        tags, circumstances_desc = self.detect_tags(circumstance)
        transaction_type = self.build_transaction_type(circumstance, transaction_value)

        return {
            "timestamp": safe_convert_datetime(raw_date),
            "amount_transaction": amount_transaction,
            "transaction_value": transaction_value,
            "price_per_share": price_per_share,
            "transaction_type": transaction_type,
            "tags": tags,
            "circumstances_desc": circumstances_desc,
            **share_table_result,
        }

    def parse_records(self) -> list[dict]:
        # Part IV holds a single shared transaction, so one filing-level check
        # applies to every holder; skip the whole filing if it is not voting shares.
        if not self.is_voting_shares_transaction():
            LOGGER.info('[Form3PartIIIandIV] skipping %s: not an ordinary voting shares transaction', self.pdf_url)
            return []

        symbol = self.extract_symbol()
        company_name, sector, sub_sector = self.get_sector_and_sub_sector(symbol=symbol)

        # Part III: one name + date + before/after share table per holder
        holder_names = self.find_values_below_label('Name of Substantial Shareholder/Unitholder')
        raw_dates = self.find_values_below_label('Date of acquisition of or change in interest')
        share_tables = self.extract_share_tables()

        # Part IV: a single shared transaction, broadcast to every holder
        raw_amount = self.find_value_below_label('acquired or disposed of by')
        raw_value = self.find_value_below_label('Amount of consideration')
        circumstances = self.extract_circumstances()
        circumstance = circumstances[0] if circumstances else {}

        counts = {
            'holders': len(holder_names),
            'dates': len(raw_dates),
            'tables': len(share_tables),
        }
       
        if len(set(counts.values())) != 1:
            LOGGER.warning('[Form3PartIIIandIV] count mismatch %s for %s', counts, self.pdf_url)

        records = []

        for holder_name, raw_date, share_table in zip(
            holder_names, raw_dates, share_tables
        ):
            transaction = self.extract_transaction(
                raw_date=raw_date,
                raw_amount=raw_amount,
                raw_value=raw_value,
                share_table=share_table,
                circumstance=circumstance,
            )

            if not self.is_valid_record(transaction):
                LOGGER.info(
                    "[Form3PartIIIandIV] Skipping (%s) due to no change in direct before and after",
                    holder_name
                )
                continue

            record = {
                "source": self.pdf_url,
                "symbol": symbol,
                "company_name": company_name,
                "sector": sector,
                "sub_sector": sub_sector,
                "holder_name": holder_name,
                "holder_type": classify_holder_type(holder_name),
                **transaction,
            }

            record = self.generate_title_and_body(record)

            records.append(record)

        return records
