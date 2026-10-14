from abc import ABC, abstractmethod

from sgx_scraper.utils.http_client import HTTPCLIENT
from sgx_scraper.utils.symbol_matching_helper import matching_symbol
from sgx_scraper.fetch_sgx_filings.utils.payload_helper import (
    clean_holder_name,
    populate_extra_data,
    safe_convert_float,
    shares_percentage_to_decimal,
    get_circumstance_interest,
    generate_title_and_body as generate_title_and_body_helper,
    contains_any_keyword,
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
    KEYWORD_SHARE_BUYBACK,
    ACQUISITION_RULES,
    OTHER_CIRCUMSTANCES_RULES,
    CORPORATE_ACTION_CHECKBOX_KEY,
    SHARE_BUYBACK_RULE,
    NOTIFICATION_TYPE_OPTIONS,
    ISSUER_TYPE_OPTIONS,
    TRANSACTION_REFERENCE_LABEL,
    TRANSACTION_REFERENCE_LENGTH,
    STAPLED_SECURITY_PATTERN,
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

    # Stored on every record as filing_type; set by each form class
    FILING_TYPE: str | None = None

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
            deemed = row[2] if len(row) > 2 else None 
            total = row[-1]

            if 'immediately before' in label:
                section = 'before'

            elif 'immediately after' in label:
                section = 'after'

            elif not section:
                continue

            elif 'as a percentage' in label:
                direct_percentage = safe_convert_float(direct)
                deemed_percentage = safe_convert_float(deemed)

                values[f'share_percentage_{section}'] = shares_percentage_to_decimal(
                    safe_convert_float(total)
                )

                values[f"direct_percentage_{section}"] = shares_percentage_to_decimal(
                    direct_percentage 
                )

                values[f"deemed_percentage_{section}"] = shares_percentage_to_decimal(
                    deemed_percentage 
                )

            else:
                total_holding = safe_convert_float(total)
                direct_holding = safe_convert_float(direct)
                deemed_holding = safe_convert_float(deemed)

                values[f'holding_{section}'] = (
                    int(total_holding)
                    if total_holding is not None
                    else None
                )

                values[f'direct_{section}'] = (
                    int(direct_holding)
                    if direct_holding is not None
                    else None 
                )

                values[f'deemed_{section}'] = (
                    int(deemed_holding)
                    if deemed_holding is not None
                    else None 
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

    # def is_valid_record(self, share_table_result: dict[str, any]) -> bool:
    #     return (
    #         share_table_result["direct_before"]
    #         != share_table_result["direct_after"]
    #         # and share_table_result["holding_before"]
    #         # != share_table_result["holding_after"]
    #     )

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

    def find_checked_options(self, options: dict[str, str]) -> list[str | None]:
        # A form repeats the same option set once per transaction or holder.
        # Returns the ticked option of each set in reading order, or None for
        # a set with nothing ticked
        doc_fitz = self.get_pdf_doc()
        checked_by_option = {option: [] for option in options}

        for page_index in range(len(doc_fitz)):
            page = doc_fitz.load_page(page_index)
            drawings = page.get_drawings()

            for block in get_all_text_blocks(page.get_text('dict')):
                text = block['text'].strip()

                for option, pattern in options.items():
                    if not re.search(pattern, text, re.IGNORECASE):
                        continue

                    # Same rule as the circumstance checkboxes: a filled,
                    # non-white box to the left of the label on the same line
                    is_checked = any(
                        drawing['type'] == 'f'
                        and abs(drawing['rect'].y0 - block['y0']) < 10
                        and drawing['rect'].x1 <= block['x0']
                        and drawing.get('fill')
                        and drawing.get('fill') != (1.0, 1.0, 1.0)
                        for drawing in drawings
                    )

                    checked_by_option[option].append(is_checked)

        set_count = max(
            (len(flags) for flags in checked_by_option.values()),
            default=0,
        )

        return [
            next(
                (
                    option
                    for option, flags in checked_by_option.items()
                    if set_index < len(flags) and flags[set_index]
                ),
                None,
            )
            for set_index in range(set_count)
        ]

    def extract_notification_types(self) -> list[str | None]:
        # Form 3 only: why the substantial shareholder is filing
        return self.find_checked_options(NOTIFICATION_TYPE_OPTIONS)

    def extract_issuer_type(self) -> str | None:
        issuer_types = self.find_checked_options(ISSUER_TYPE_OPTIONS)

        return issuer_types[0] if issuer_types else None

    def extract_security_noun(self) -> str | None:
        # What the form calls the securities: a stapled group says "stapled
        # securities" in its own answers, otherwise the "Type of Listed Issuer"
        # box decides between shares and units
        document_text = ' '.join(
            block['text'] for block in self.ordered_text_blocks()
        )

        if re.search(STAPLED_SECURITY_PATTERN, document_text, re.IGNORECASE):
            return "stapled securities"

        issuer_type = self.extract_issuer_type()

        if issuer_type is None:
            return None

        return "shares" if issuer_type == "company" else "units"

    def extract_transaction_references(self) -> list[str | None]:
        # The form prints one auto-generated reference per transaction, as
        # spaced digits in the block below the label
        references = []

        for value in self.find_values_below_label(TRANSACTION_REFERENCE_LABEL):
            digits = re.sub(r'\D', '', value or '')

            references.append(
                digits if len(digits) == TRANSACTION_REFERENCE_LENGTH else None
            )

        return references

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

    @staticmethod
    def build_circumstances_description(
        circumstance_interest_raw: dict,
    ) -> str:
        description_parts = []

        results = circumstance_interest_raw.get('results') or {}
        other_circumstances = results.get('other_circumstances') or {}
        others_specify = results.get('others_specify') or {}
        
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

    def build_transaction_type_and_tags(
        self,
        circumstance_interest_raw: dict[str, any]
    ) -> tuple[str | None, list[str], str | None]:
        if not circumstance_interest_raw:
            LOGGER.warning(
                "[build_transaction_type_and_tags] circumstance_interest_raw is None"
            )
            return None, [], None

        circumstance_interest = get_circumstance_interest(
            circumstance_interest_raw.get('results')
        )

        if not circumstance_interest:
            return None, [], None

        key = circumstance_interest.get('key')
        checked = circumstance_interest.get('checked')
        specific_key = circumstance_interest.get('specific_key')

        if not checked or key == 'others_specify':
            return None, [], None

        if key == 'acquisition':
            rule = ACQUISITION_RULES.get(specific_key)

        elif key == 'disposal':
            return 'sell', [], specific_key

        elif key == 'other_circumstances':
            rule = OTHER_CIRCUMSTANCES_RULES.get(specific_key)

            if (
                specific_key == CORPORATE_ACTION_CHECKBOX_KEY
                and contains_any_keyword(
                    circumstance_interest.get('description') or '',
                    KEYWORD_SHARE_BUYBACK,
                )
            ):
                rule = SHARE_BUYBACK_RULE

        else:
            return None, [], specific_key

        if not rule:
            return None, [], specific_key

        return rule['transaction_type'], list(rule['tags']), specific_key

    def get_sector_and_sub_sector(self, symbol: str) -> tuple[str | None, str | None]:
        company_name, sector, sub_sector = populate_extra_data(symbol)

        if not company_name or company_name == symbol:
            # The company table only has the ticker for this issuer, so the
            # name printed on the form is used
            printed_name = clean_holder_name(
                self.find_value_below_label('Name of Listed Issuer')
            )

            if printed_name:
                company_name = (
                    printed_name.title() if printed_name.isupper() else printed_name
                )

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

    def find_section_values(
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
                    or self._PAGE_HEADER_PATTERN.match(candidate_text)
                    or candidate_text.startswith('FORM ')
                    or candidate_text.startswith('Save as')
                ):
                    continue

                value_parts.append(candidate_text)

            values.append(' '.join(value_parts) or None)

        return values

    def extract_symbol(self) -> str | None:
        company_name = self.find_value_below_label('Name of Listed Issuer')
        symbol = matching_symbol(company_name) if company_name else None

        # The announcement's own stock code covers issuers whose printed
        # name the matcher does not know
        return symbol or getattr(self, 'fallback_symbol', None)
    
    def generate_title_and_body(self, record: dict) -> dict:
        title, body = generate_title_and_body_helper(record)

        record['title'] = title
        record['body'] = body

        return record

    @abstractmethod
    def parse_records(self) -> list[dict]:
        ...
