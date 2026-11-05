from pathlib import Path


FILINGS_TABLE = "sgx_filings"
ENTITIES_TABLE = "sgx_filing_entities"
LINKS_TABLE = "sgx_filing_entity_links"
FILING_OWNERSHIP_TABLE = "sgx_filing_ownership"
COMPANY_VIEW_TABLE = "sgx_filing_company_holders"
ENTITY_VIEW_TABLE = "sgx_filing_entity_positions"
OWNERS_TABLE = "sgx_filing_company_owners"
COMPANY_TABLE = "sgx_company_report"

FILING_COLUMNS = ",".join([
    "id",
    "source",
    "symbol",
    "timestamp",
    "holder_name",
    "holder_type",
    "transaction_type",
    "transaction_value",
    "interest_breakdown",
    "tags",
    "sector",
    "filing_type",
    "notification_type",
    "circumstance_key",
    "change_reason",
    "acting_party",
    "deemed_interest_text",
    "relationship_text",
    "remarks_text",
])

# Conflict keys for upserts, one per table
TABLE_KEYS = {
    ENTITIES_TABLE: "id",
    LINKS_TABLE: "parent_id,child_id,relation",
    FILING_OWNERSHIP_TABLE: "filing_id",
    COMPANY_VIEW_TABLE: "symbol",
    ENTITY_VIEW_TABLE: "id",
    OWNERS_TABLE: "symbol",
}

PREVIEW_PATH = Path("data/scraper_output/filings_ownership/preview.json")
# Read once a month and kept in the repo, since reports change about once a year
ANNUAL_REPORTS_PATH = Path("data/scraper_output/filings_ownership/annual_report_holders.json")
OWNERS_PATH = Path("data/scraper_output/filings_ownership/company_owners_latest.json")

DB_PAGE_SIZE = 1000

LLM_MODELS = ["gpt-6-luna"]
LLM_EFFORT = "medium"
LLM_CONCURRENCY = 8
# Tries per filing; after that a cut-off reply leaves the filing for the next run
LLM_READ_ATTEMPTS = 2
PDF_CONCURRENCY = 6

ANNUAL_REPORT_MODELS = ["claude-haiku-5.5"]
ANNUAL_REPORT_EFFORT = "high"
ANNUAL_REPORT_LLM_CONCURRENCY = 5
ANNUAL_REPORT_PDF_CONCURRENCY = 8
# Filings near the report date give its share base; units change with placements after
ISSUED_SHARES_WINDOW_DAYS = 90

LISTED_ISSUER = "LISTED ISSUER"

# Shorter text is "N.A." or a pointer to an attachment, with nothing to read
MIN_TEXT_LENGTH = 40
MIN_KEY_LENGTH = 2

# Quotes are sometimes trimmed mid-sentence; near-exact still rules out
# wording the filing does not have
EVIDENCE_SIMILARITY = 90
TYPO_SIMILARITY = 94
MAX_EVIDENCE_LENGTH = 400

# 5% makes a substantial holder; 30% triggers a mandatory offer under the
# Singapore Code on Take-overs; 50% and 75% are control and special-resolution lines
THRESHOLDS = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.50, 0.75]
SUBSTANTIAL = 0.05
# Fractions keep 7 decimals: filings print up to 5 in percent (0.91369%)
PERCENT_DECIMALS = 7
RECENT_DAYS = 90

# Large groups list every subsidiary (BlackRock names 15); the caps only
# guard against a runaway search
MAX_ROUTES = 60
MAX_ROUTE_LENGTH = 15

ROUTINE_TAGS = {"management-fees", "award", "director-fees", "scrip-dividend", "option-exercise"}
CORPORATE_ACTION_TAGS = {"rights-offering", "bonus-issue", "dividend-in-specie", "share-buyback"}
RESTRUCTURING_TAGS = {"internal-restructuring", "transfer"}

OWNING_RELATIONS = {"owns", "controls"}
FAMILY_RELATIONS = {"spouse", "family"}
# A holder whose deemed interest runs only through these still made the
# decision itself: its nominee holds the shares, funds it manages do, or it
# is a party to the agreement that gives the interest
SELF_HELD_RELATIONS = {"nominee", "fund_manager", "agreement"}
STRUCTURE_RELATIONS = {
    "owns",
    "controls",
    "beneficiary",
    "trustee",
    "spouse",
    "family",
    "nominee",
    "fund_manager",
    "agreement",
}
ROUTE_RELATIONS = STRUCTURE_RELATIONS | {"deemed_via"}

# One filing can state a pair twice ("controls" and "owns 100%"); a stated
# stake reads best, a summary such as "deemed interest via" least
STEP_ORDER = [
    "owns",
    "holds_directly",
    "controls",
    "fund_manager",
    "trustee",
    "beneficiary",
    "spouse",
    "family",
    "nominee",
    "agreement",
    "deemed_via",
]
