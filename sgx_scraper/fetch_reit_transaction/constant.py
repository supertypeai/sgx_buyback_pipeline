from pathlib import Path


REIT_TRANSACTION_BASE_DIR = Path("data/scraper_output/sgx_reit_transaction")
REIT_TRANSACTION_BASE_DIR.mkdir(parents=True, exist_ok=True)

REIT_TRANSACTION_PATH_TODAY = REIT_TRANSACTION_BASE_DIR / "reit_transaction_today.json"
REIT_TRANSACTION_PATH_SEEN = REIT_TRANSACTION_BASE_DIR / "reit_transaction_seen.json"
REIT_TRANSACTION_PATH_CONFLICT = REIT_TRANSACTION_BASE_DIR / "reit_transaction_conflict.json"

QUARTERLY_RATES_PATH = Path("data/quarterly_rates.json")

SUB_CATEGORY = "ANNC06"
TABLE_NAME = "sgx_reit_property_transaction"

REIT_SYMBOLS = {
    "XZL": "ACROPHYTE HOSPITALITY TRUST",
    "O5RU": "AIMS APAC REIT",
    "M1GU": "ALPHA INTEGRATED REIT",
    "BMGU": "BHG RETAIL REIT",
    "A17U": "CAPITALAND ASCENDAS REIT",
    "HMN": "CAPITALAND ASCOTT TRUST",
    "AU8U": "CAPITALAND CHINA TRUST",
    "CY6U": "CAPITALAND INDIA TRUST",
    "C38U": "CAPITALAND INTEGRATED COMMERCIAL TRUST",
    "J85": "CDL HOSPITALITY TRUSTS",
    "8C8U": "CENTURION ACCOMMODATION REIT",
    "DHLU": "DAIWA HOUSE LOGISTICS TRUST",
    "DCRU": "DIGITAL CORE REIT",
    "MXNU": "ELITE UK REIT",
    "9A4U": "ESR-REIT",
    "Q5T": "FAR EAST HOSPITALITY TRUST",
    "AW9U": "FIRST REIT",
    "J69U": "FRASERS CENTREPOINT TRUST",
    "BUOU": "FRASERS LOGISTICS & COMMERCIAL TRUST",
    "UD1U": "IREIT GLOBAL",
    "AJBU": "KEPPEL DC REIT",
    "K71U": "KEPPEL REIT",
    "CMOU": "KORE US REIT",
    "D5IU": "LANDMARK REIT",
    "JYEU": "LENDLEASE GLOBAL COMMERCIAL REIT",
    "BTOU": "MANULIFE US REIT",
    "ME8U": "MAPLETREE INDUSTRIAL TRUST",
    "M44U": "MAPLETREE LOGISTICS TRUST",
    "N2IU": "MAPLETREE PAN ASIA COMMERCIAL TRUST",
    "NTDU": "NTT DC REIT",
    "TS0U": "OUE REIT",
    "C2PU": "PARKWAY LIFE REIT",
    "OXMU": "PRIME US REIT",
    "CRPU": "SASSEUR REIT",
    "P40U": "STARHILL GLOBAL REIT",
    "SET": "STONEWEG EUROPE STAPLED TRUST",
    "T82U": "SUNTEC REIT",
    "UIBU": "UI BOUSTEAD REIT",
    "ODBU": "UNITED HAMPSHIRE US REIT",
}

# financial_year follows each REIT's own label, not the completion year.
# First-half year ends label the year by its start, September by its end.
FIRST_HALF_YEAR_END = {"P40U": 6, "O5RU": 3, "N2IU": 3, "ME8U": 3, "M44U": 3, "JYEU": 6}
SECOND_HALF_YEAR_END = {"BUOU": 9, "J69U": 9}

TRANSACTION_BASIS = (
    "valuation",
    "book_value",
    "purchase_price",
    "net_identifiable_assets",
)

PLAN_LOOKBACK_DAYS = 365

# Financing notices name the acquisitions they fund, so they match on property
# name while their figures are the funding.
FINANCING_TITLE_PATTERN = (
    r"perpetual|securities|financing|private placement|equity fund rais"
    r"|preferential offer|pricing of"
)

PRICE_CONFLICT_TOLERANCE = 0.05
