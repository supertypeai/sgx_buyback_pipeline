from pathlib import Path


AGM_BASE_DIR = Path("data/scraper_output/sgx_agm")
AGM_BASE_DIR.mkdir(parents=True, exist_ok=True)

AGM_PATH_TODAY = AGM_BASE_DIR / "agm_today.json"
AGM_PATH_SEEN = AGM_BASE_DIR / "agm_seen.json"

SUB_CATEGORIES = {"ANNC05": "AGM", "ANNC16": "EGM"}
TABLE_NAME = "sgx_agm"
ON_CONFLICT = "symbol,agm_date,meeting_type"

DETAIL_SECTIONS = (
    "Issuer & Securities",
    "Announcement Details",
    "Event Narrative",
    "Event Dates",
    "Event Venue(s)",
)

MEETING_TAGS = (
    "dividend",
    "capital and equity",
    "board and management",
    "financial reporting and profit allocation",
    "corporate governance",
    "acquisitions and disposals",
)

# What was decided is stated in the results document, and failing that in the
# minutes, which some companies file instead.
OUTCOME_ATTACHMENT_PATTERNS = (
    r"result|outcome|poll|resolution.{0,5}pass|pass.{0,5}resolution",
    r"minute",
)

ELECTRONIC_VENUE_PATTERN = r"electronic|virtual|online|webcast|zoom"
PHYSICAL_VENUE_PATTERN = r"singapore|road|street|level|floor|avenue|ballroom|hotel|#\d"

# agm_place_desc follows the IDX vocabulary so both markets read the same.
PLACE_ONSITE = "Onsite"
PLACE_HYBRID = "Hybrid"
PLACE_ONLINE = "Online"

SIAS_BASE_URL = "https://sias.org.sg/qa-on-annual-reports/"
SIAS_DEFAULT_PAGES = 3
SIAS_QUESTION_PATTERN = r"(?im)^[ \t]*Q[ \t]?(\d{1,2})[.)]"
SIAS_ANSWER_PATTERNS = (
    r"(?im)^[ \t]*Question[ \t]+(\d{1,2})\b",
    r"(?im)^[ \t]*Q[ \t]?(\d{1,2})[.):]",
    r"(?m)^[ \t]*(\d{1,2})[.)][ \t]",
)

MAX_DOCUMENT_CHARS = 30000
