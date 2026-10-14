# Constants are grouped by what they describe. Everything is re-exported
# here, so "from ...utils.constants import NAME" works for any of them.
from sgx_scraper.fetch_sgx_filings.utils.constants.form_options import (
    ACQUISITION_OPTIONS,
    DISPOSAL_OPTIONS,
    OTHER_OPTIONS,
    ACQUISITION_RULES,
    OTHER_CIRCUMSTANCES_RULES,
    TYPE_SECURITIES_SECTION_PATTERN,
    TYPE_SECURITIES_OPTIONS,
    KEYWORD_SHARE_BUYBACK,
    CORPORATE_ACTION_CHECKBOX_KEY,
    SHARE_BUYBACK_RULE,
    NOTIFICATION_TYPE_OPTIONS,
    ISSUER_TYPE_OPTIONS,
    TRANSACTION_REFERENCE_LABEL,
    TRANSACTION_REFERENCE_LENGTH,
)
from sgx_scraper.fetch_sgx_filings.utils.constants.record_schema import (
    FILING_TABLE_COLUMNS,
    FILING_EXTRA_FIELDS,
)
from sgx_scraper.fetch_sgx_filings.utils.constants.title_body_rules import (
    TITLE_BODY_TAG_PRIORITY,
    TITLE_BODY_RECEIPT_TAGS,
    TITLE_BODY_TAG_RULES,
    TITLE_BODY_SIDED_TAG_RULES,
    TITLE_BODY_CIRCUMSTANCE_RULES,
    TITLE_BODY_TYPE_RULES,
)
from sgx_scraper.fetch_sgx_filings.utils.constants.wording import (
    STAPLED_SECURITY_PATTERN,
    HOLDER_SHORT_NAME_PATTERN,
    HOLDER_NAME_ACRONYMS,
    UNIT_ISSUER_PATTERN,
)
