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
]

# Helper fields with no column in the table. They were used to write the
# title and body, and stay on the batch files so a batch can be reviewed
# (which form, which box, why). They are removed from the processed files
FILING_EXTRA_FIELDS = [
    "company_name",
    "filing_type",
    "transaction_reference",
    "notification_type",
    "circumstance_key",
    "change_reason",
    "acting_party",
]
