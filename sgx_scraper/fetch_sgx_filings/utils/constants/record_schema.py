# The stored shape of a filing record.
# Columns of the sgx_filings table, in table order (id and created_at are
# set by the database)
FILING_TABLE_COLUMNS = [
    "source",
    "timestamp",
    "transaction_type",
    "holder_name",
    "transaction_value",
    "share_percentage_transaction",
    "interest_breakdown",
    "symbol",
    "price_per_share",
    "holder_type",
    "title",
    "body",
    "sector",
    "sub_sector",
    "tags",
    "source_is_manual",
    # The form and its boxes, which the ownership pipeline reads instead of the PDF
    "filing_type",
    "notification_type",
    "circumstance_key",
    "change_reason",
    "acting_party",
    "deemed_interest_text",
    "relationship_text",
    "remarks_text",
]

# Read from the parsed record as they are; the box texts are renamed in the payload
FILING_FORM_FIELDS = [
    "filing_type",
    "notification_type",
    "circumstance_key",
    "change_reason",
    "acting_party",
]

# Helper fields with no column in the table. They were used to write the
# title and body, and stay on the batch files so a batch can be reviewed.
# They are removed from the processed files
FILING_EXTRA_FIELDS = [
    "company_name",
    "transaction_reference",
]
