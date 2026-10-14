from ..base_parser import BaseFormParser
from sgx_scraper.utils.date_helper import safe_convert_datetime
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    safe_convert_float,
    build_value,
    build_price_per_share,
    clean_amount_text,
    consideration_contradicts_amount,
    build_price_tranches,
    classify_holder_type,
    clean_holder_name,
)

import logging
import re


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

    FILING_TYPE = "form_3_part_iii_iv"

    def find_part_iii_section_values(
        self,
        label_pattern: str,
        end_pattern: str,
    ) -> list[str | None]:
        label_regex = re.compile(label_pattern, re.IGNORECASE)
        end_regex = re.compile(end_pattern, re.IGNORECASE)
        blocks = self.ordered_text_blocks()
        values = []

        for start_index, block in enumerate(blocks):
            if not label_regex.search(block['text']):
                continue

            end_index = next(
                (
                    candidate_index
                    for candidate_index in range(start_index + 1, len(blocks))
                    if end_regex.search(blocks[candidate_index]['text'])
                ),
                len(blocks),
            )

            value_parts = []

            for candidate in blocks[start_index + 1:end_index]:
                candidate_text = candidate['text'].strip()

                if (
                    not candidate_text
                    or candidate_text.startswith('[')
                    or re.fullmatch(r'(?:\d+\.?|\([a-z0-9]+\))', candidate_text)
                    or re.match(r'^Page \d+ of \d+', candidate_text, re.IGNORECASE)
                    or candidate_text.startswith('FORM 3/')
                    or candidate_text.startswith('Save as')
                    or candidate_text.startswith('Substantial Shareholder/Unitholder')
                ):
                    continue

                value_parts.append(candidate_text)

            values.append(' '.join(value_parts) or None)

        return values

    def _extract_transaction(
        self,
        raw_date: str | None,
        raw_amount: str | None,
        raw_value: str | None,
        share_table: list[list[str]],
        circumstance: dict,
    ) -> dict:
        share_table_result = self.parse_share_table(share_table=share_table)

        amount_float = safe_convert_float(clean_amount_text(raw_amount))
        amount_transaction = int(amount_float) if amount_float is not None else None
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

        transaction_type, tags, specific_key = self.build_transaction_type_and_tags(
            circumstance
        )
        
        circumstances_desc = self.build_circumstances_description(circumstance)

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
        holder_names = [
            clean_holder_name(holder_name)
            for holder_name in self.find_values_below_label(
                'Name of Substantial Shareholder/Unitholder'
            )
        ]
        raw_dates = self.find_values_below_label('Date of acquisition of or change in interest')
        share_tables = self.extract_share_tables()

        awareness_explanations = self.find_values_below_label(
            r'Explanation\s*\(\s*if the date of becoming aware'
        )

        deemed_interest_descriptions = self.find_part_iii_section_values(
            'Circumstances giving rise to deemed interests',
            'Relationship between the Substantial Shareholders/Unitholders',
        )

        remarks_sections = self.find_part_iii_section_values(
            r'Remarks\s*\(\s*if any\s*\)',
            r'^(?:[A-Z]$|Part\s+IV\s*[-–]\s*Transaction\b)',
        )
        
        shareholder_relationships = self.find_part_iii_section_values(
            'Relationship between the Substantial Shareholders/Unitholders',
            r'Attachments\s*\(\s*if any\s*\)',
        )

        # Part IV: a single shared transaction, broadcast to every holder
        raw_amount = self.find_value_below_label('acquired or disposed of by')
        raw_value = self.find_value_below_label('Amount of consideration')
        circumstances = self.extract_circumstances()
        circumstance = circumstances[0] if circumstances else {}

        # Part III ticks a notification type per holder; Part IV's single
        # transaction has one reference shared by every holder
        notification_types = self.extract_notification_types()
        transaction_references = self.extract_transaction_references()
        transaction_reference = (
            transaction_references[0] if transaction_references else None
        )
        security_noun = self.extract_security_noun()

        counts = {
            'holders': len(holder_names),
            'dates': len(raw_dates),
            'tables': len(share_tables),
            'awareness_explanations': len(awareness_explanations),
            'deemed_interest_descriptions': len(deemed_interest_descriptions),
            'remarks': len(remarks_sections),
            'shareholder_relationships': len(shareholder_relationships),
        }
       
        if len(set(counts.values())) != 1:
            LOGGER.warning(
                '[Form3PartIIIandIV] count mismatch %s for %s', 
                counts, 
                self.pdf_url
            )

        records = []
        transfer_context_records = []

        shareholder_data = zip(
            holder_names,
            raw_dates,
            share_tables,
            awareness_explanations,
            deemed_interest_descriptions,
            remarks_sections,
            shareholder_relationships,
        )

        for holder_index, (
            holder_name,
            raw_date,
            share_table,
            awareness_explanation,
            deemed_interest_description,
            remarks,
            shareholder_relationship,
        ) in enumerate(shareholder_data):
            transaction = self._extract_transaction(
                raw_date=raw_date,
                raw_amount=raw_amount,
                raw_value=raw_value,
                share_table=share_table,
                circumstance=circumstance,
            )

            transfer_context_records.append({
                "holder_name": holder_name,
                "direct_before": transaction["direct_before"],
                "direct_after": transaction["direct_after"],
                "remarks": remarks,
                "awareness_explanation": awareness_explanation,
                "deemed_interest_circumstances": deemed_interest_description,
                "shareholder_relationship": shareholder_relationship,
            })

            # if not self.is_valid_record(transaction):
            #     LOGGER.info(
            #         "[Form3PartIIIandIV] Skipping (%s) due to no change in direct before and after",
            #         holder_name
            #     )
            #     continue

            holder_type = classify_holder_type(
                parser_form="form3partiiiandiv", 
                company_name=company_name,
                filing_symbol=symbol, 
                holder_name=holder_name,
            )
            
            record = {
                "source": self.pdf_url,
                "filing_type": self.FILING_TYPE,
                "transaction_reference": transaction_reference,
                "notification_type": (
                    notification_types[holder_index]
                    if holder_index < len(notification_types)
                    else None
                ),
                "symbol": symbol,
                "company_name": company_name,
                "sector": sector,
                "source_is_manual": False,
                "remarks": remarks,
                "raw_values": raw_value,
                "awareness_explanation": awareness_explanation,
                "deemed_interest_circumstances": deemed_interest_description,
                "shareholder_relationship": shareholder_relationship,
                "sub_sector": sub_sector,
                "source_is_manual": False,
                "holder_name": holder_name,
                "holder_type": holder_type,
                **transaction,
            }

            record['_security_noun'] = security_noun

            records.append(record)

        # The holder whose direct interest moved is the one that actually holds
        # the securities; the other holders are deemed interested through it
        direct_holders = [
            record["holder_name"]
            for record in records
            if record.get("direct_before") != record.get("direct_after")
        ]
        primary_holder = direct_holders[0] if len(direct_holders) == 1 else None

        # How far that holder's own total moved. It is named as the party
        # that acted only when this equals the amount in Part IV
        primary_holder_change = next(
            (
                abs(record["holding_after"] - record["holding_before"])
                for record in records
                if record["holder_name"] == primary_holder
                and record.get("holding_before") is not None
                and record.get("holding_after") is not None
            ),
            None,
        )

        for record in records:
            if record["holder_name"] != primary_holder:
                record["_primary_holder"] = primary_holder
                record["_primary_holder_change"] = primary_holder_change

            self.generate_title_and_body(record)

        self.transfer_context_records = transfer_context_records

        return records
