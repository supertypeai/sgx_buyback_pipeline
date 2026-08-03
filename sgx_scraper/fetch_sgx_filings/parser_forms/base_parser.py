from abc import ABC, abstractmethod

from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.symbol_matching_helper import matching_symbol
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    populate_extra_data,
    safe_convert_float,
    shares_percentage_to_decimal,
    generate_title_and_body as generate_title_and_body_helper,
    contains_any_keyword,
    build_transaction_type as build_transaction_type_helper,
)
from sgx_scraper.fetch_sgx_filings.utils.payload_pdf_helper import (
    contains_share_rule,
    extract_circumstance_interest_checkbox,
    extract_type_securities_checkbox,
    extract_type_securities_checkbox_all,
    get_all_text_blocks,
)
from sgx_scraper.fetch_sgx_filings.utils.constants import (
    TYPE_SECURITIES_SECTION_PATTERN,
    TAKEOVER_CHECKBOX_KEYS,
    EMPLOYEE_CHECKBOX_KEYS,
    KEYWORD_DIRECTOR_FEE,
    KEYWORD_EMPLOYEE_PLAN,
    KEYWORD_MANAGEMENT_FEE,
    KEYWORD_DIVIDEND,
    KEYWORD_INHERITANCE,
    KEYWORD_INTERNAL_RESTRUCTURING,
    KEYWORD_GIFT,
)

import io
import re
import logging
import pdfplumber
import fitz


LOGGER = logging.getLogger(__name__)


class BaseFormParser(ABC):
    """
    Abstract base for the per-form SGX filing parsers (Form 1, Form 3 Part II /
    Part III+IV, Form 6).

    Holds the shared, form-agnostic PDF extraction primitives, cached bytes/fitz
    document, share-table parsing, circumstance and type-of-securities checkbox
    reading, reading-order text-block access, symbol/sector lookup and title/body
    generation,  all read deterministically via pymupdf/pdfplumber. Each concrete
    form supplies its own holder logic and implements `parse_records`.
    """

    # Item headers such as '2. Type of Listed Issuer:' or '(a) Name of ...'
    # sit directly below a value in reading order, used to reject them as values
    _ITEM_HEADER_PATTERN = re.compile(
        r'^\s*(?:\d+\.|\([a-z0-9]+\)|Attachments\b)(?:\s|$)'
    )
    _PAGE_HEADER_PATTERN = re.compile(r'^Page \d+ of \d+ FORM ', re.IGNORECASE)

    def __init__(
        self,
        pdf_url: str
    ):
        self.pdf_url = pdf_url
        self.pdf_bytes = None
        self._doc_fitz = None
        self._doc_parsed = None
        self._text_blocks = None

    def get_pdf_bytes(self) -> bytes:
        if self.pdf_bytes is None:
            response = HTTPCLIENT.get(self.pdf_url)
            response.raise_for_status()
            self.pdf_bytes = response.content

        return self.pdf_bytes

    def get_pdf_doc(self) -> fitz.Document:
        if self._doc_fitz is None:
            self._doc_fitz = fitz.open(
                stream=self.get_pdf_bytes(), 
                filetype='pdf'
            )

        return self._doc_fitz

    def extract_share_tables(self) -> list[list[list[str]]]:
        raw_tables = []

        with pdfplumber.open(io.BytesIO(self.get_pdf_bytes())) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    if table and contains_share_rule(table):
                        raw_tables.append(table)

        return self.group_share_tables(raw_tables)

    def group_share_tables(self, raw_tables: list[list[list[str]]]) -> list[list[list[str]]]:
        grouped_tables = []
        current_group = []

        for table in raw_tables:
            starts_new_section = 'immediately before' in str(table[0]).lower()

            if starts_new_section and current_group:
                grouped_tables.append(current_group)
                current_group = []

            current_group.extend(table)

        if current_group:
            grouped_tables.append(current_group)

        return grouped_tables

    def parse_share_table(self, share_table: list[list[str]]) -> dict:
        section = None
        values = {}

        #  Each row is [label, direct, deemed, total]
        for row in share_table:
            label = (row[0] or '').lower()
            direct = row[1] if len(row) > 1 else None
            total = row[-1]

            if 'immediately before' in label:
                section = 'before'

            elif 'immediately after' in label:
                section = 'after'

            elif not section:
                continue

            elif 'as a percentage' in label:
                direct_percentage = safe_convert_float(direct)

                values[f'share_percentage_{section}'] = shares_percentage_to_decimal(
                    safe_convert_float(total)
                )

                values[f"direct_pct_{section}"] = shares_percentage_to_decimal(
                    direct_percentage if direct_percentage is not None else 0
                )

            else:
                total_holding = safe_convert_float(total)
                direct_holding = safe_convert_float(direct)

                values[f'holding_{section}'] = (
                    int(total_holding)
                    if total_holding is not None
                    else None
                )

                values[f'direct_{section}'] = (
                    int(direct_holding)
                    if direct_holding is not None
                    else 0
                )

        share_percentage_before = values.get('share_percentage_before')
        share_percentage_after = values.get('share_percentage_after')

        values['share_percentage_transaction'] = (
            round(abs(share_percentage_after - share_percentage_before), 5)
            if share_percentage_before is not None
            and share_percentage_after is not None
            else None
        )

        return values

    def is_valid_record(self, share_table_result: dict[str, any]) -> bool:
        return (
            share_table_result["direct_before"]
            != share_table_result["direct_after"]
            # and share_table_result["holding_before"]
            # != share_table_result["holding_after"]
        )

    def extract_circumstances(self) -> list[dict]:
        doc_fitz = self.get_pdf_doc()

        circumstances = []

        for page_index in range(len(doc_fitz)):
            page = doc_fitz.load_page(page_index)

            if not page.search_for('Circumstance giving rise to the interest'):
                continue

            full_page_bbox = (0, 0, page.rect.width, page.rect.height)
            
            circumstance = extract_circumstance_interest_checkbox(
                doc_fitz, page_index, full_page_bbox
            )

            if circumstance:
                circumstances.append(circumstance)

        return circumstances

    def extract_type_securities(self) -> dict:
        type_securities = extract_type_securities_checkbox(
            self.get_pdf_doc(),
            section_pattern=TYPE_SECURITIES_SECTION_PATTERN,
        )

        return (type_securities or {}).get('results') or {}

    def is_voting_shares_transaction(self) -> bool:
        # Filing-level check (first Type of securities section). Correct for filings
        # with a single shared transaction, e.g. Form 3 Part III/IV
        return self.extract_type_securities().get('Voting shares/units') is True

    def voting_shares_flags(self) -> list[bool]:
        # Per-transaction 'Voting shares/units' checked state, one per Type of
        # securities section in document (transaction) order
        sections = extract_type_securities_checkbox_all(
            self.get_pdf_doc(),
            section_pattern=TYPE_SECURITIES_SECTION_PATTERN,
        )

        return [
            (section.get('results') or {}).get('Voting shares/units') is True
            for section in sections
        ]

    def build_transaction_type(self, circumstance_raw: dict, value: float | None) -> str | None:
        return build_transaction_type_helper(circumstance_raw, value)

    @staticmethod
    def build_circumstances_description(
        other_circumstances: dict,
        others_specify: dict,
    ) -> str:
        description_parts = []
        
        corporate_action = (
            other_circumstances.get('Corporate action by Listed Issuer') or {}
        )

        if corporate_action.get('checked') is True:
            corporate_action_text = corporate_action.get('description') or ''

            if corporate_action_text:
                description_parts.append(corporate_action_text)

        if others_specify.get('checked') is True:
            others_text = others_specify.get('description') or ''

            if others_text:
                description_parts.append(others_text)

        return ' '.join(description_parts)

    def detect_tags(self, circumstances_raw: dict) -> tuple[list, str]:
        final_tags = []

        results = circumstances_raw.get('results') or {}
        acquisition = results.get('acquisition') or {}
        disposal = results.get('disposal') or {}
        other_circumstances = results.get('other_circumstances') or {}
        others_specify = results.get('others_specify') or {}

        acquisition_ticked = any(value is True for value in acquisition.values())
        disposal_ticked = any(value is True for value in disposal.values())

        if acquisition_ticked:
            final_tags.append('investment')

        elif disposal_ticked:
            final_tags.append('divestment')

        takeover_ticked = any(
            other_circumstances.get(key) is True 
            for key in TAKEOVER_CHECKBOX_KEYS
        )

        if takeover_ticked:
            final_tags.append('takeover')

        corporate_action = other_circumstances.get('Corporate action by Listed Issuer') or {}

        if corporate_action.get('checked') is True:
            final_tags.append('corporate-action')

        employee_checkbox_ticked = any(
            other_circumstances.get(key) is True 
            for key in EMPLOYEE_CHECKBOX_KEYS
        )

        circumstances_text = self.build_circumstances_description(
            other_circumstances,
            others_specify,
        )

        employee_text_match = contains_any_keyword(circumstances_text, KEYWORD_EMPLOYEE_PLAN)

        if contains_any_keyword(circumstances_text, KEYWORD_DIRECTOR_FEE):
            final_tags.append('director-fee-shares')

        if employee_checkbox_ticked or employee_text_match:
            if 'director-fee-shares' not in final_tags:
                final_tags.append('employee-share-plan')

        if contains_any_keyword(circumstances_text, KEYWORD_MANAGEMENT_FEE):
            final_tags.append('management-fee-shares')

        if contains_any_keyword(circumstances_text, KEYWORD_DIVIDEND):
            final_tags.append('dividend-in-specie')

        if contains_any_keyword(circumstances_text, KEYWORD_INHERITANCE):
            final_tags.append('inheritance')

        if contains_any_keyword(circumstances_text, KEYWORD_INTERNAL_RESTRUCTURING):
            final_tags.append('internal-restructuring')

        if contains_any_keyword(circumstances_text, KEYWORD_GIFT):
            final_tags.append('gift')

        return final_tags, circumstances_text

    def get_sector_and_sub_sector(self, symbol: str) -> tuple[str | None, str | None]:
        company_name, sector, sub_sector = populate_extra_data(symbol)

        return company_name, sector, sub_sector
    
    def ordered_text_blocks(self) -> list[dict[str, any]]:
        # All non-empty text blocks across the document in reading order
        # (page, then top-to-bottom, then left-to-right). liteparse mangles or
        # drops labelled values (e.g. 'Amount of consideration'); the raw pymupdf
        # block geometry keeps every value under its own label deterministically
        if self._text_blocks is not None:
            return self._text_blocks

        doc = self.get_pdf_doc()
        blocks = []

        for page_index in range(len(doc)):
            page = doc.load_page(page_index)

            for block in get_all_text_blocks(page.get_text('dict')):
                text = block['text'].strip()

                if text:
                    blocks.append({
                        'page': page_index,
                        'y0': block['y0'],
                        'x0': block['x0'],
                        'text': text,
                    })

        blocks.sort(
            key=lambda item: (item['page'], item['y0'], item['x0'])
        )

        self._text_blocks = blocks
        return blocks

    def find_values_below_label(self, label_pattern: str) -> list[str | None]:
        # For every block matching the label, the value is the block immediately
        # below it in reading order. Returns one entry (possibly None) per label
        # occurrence so callers can align repeated fields per transaction by index
        pattern = re.compile(label_pattern, re.IGNORECASE)
        blocks = self.ordered_text_blocks()

        values = []

        for index, block in enumerate(blocks):
            if not pattern.search(block['text']):
                continue

            value = None

            candidate_index = index + 1

            if (
                candidate_index < len(blocks)
                and self._PAGE_HEADER_PATTERN.match(blocks[candidate_index]['text'])
            ):
                candidate_index += 1

            if candidate_index < len(blocks):
                candidate = blocks[candidate_index]['text']

                # A following item header (e.g. '2. ...') means the field is empty
                if not self._ITEM_HEADER_PATTERN.match(candidate):
                    value = candidate

            values.append(value)

        if not values:
            LOGGER.info('[find_values_below_label] label not found: %s', label_pattern)

        return values

    def find_value_below_label(self, label_pattern: str) -> str | None:
        values = self.find_values_below_label(label_pattern)

        return values[0] if values else None

    def extract_symbol(self) -> str | None:
        company_name = self.find_value_below_label('Name of Listed Issuer')

        if not company_name:
            return None

        return matching_symbol(company_name)

    def generate_title_and_body(self, record: dict) -> dict:
        title, body = generate_title_and_body_helper(
            holder_name=record.get('holder_name'),
            company_name=record.get('company_name'),
            tx_type=record.get('transaction_type'),
            amount=record.get('amount_transaction'),
            holding_before=record.get('holding_before'),
            holding_after=record.get('holding_after'),
            purpose_en=None,
        )

        record['title'] = title
        record['body'] = body

        return record

    @abstractmethod
    def parse_records(self) -> list[dict]:
        ...
