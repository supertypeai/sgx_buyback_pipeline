# Patterns and word lists used when wording holder names and securities.
# Stapled groups issue stapled securities; the form text says so
STAPLED_SECURITY_PATTERN = r"stapled\s+securit(?:y|ies)"

# A bracketed short name defined after a holder's name: ("FPIH"), ('JSS'),
# (the "Manager"), ("Agapier" or the "Company")
HOLDER_SHORT_NAME_PATTERN = (
    r"\s*\(\s*(?:(?:the|each|each of|collectively|together|hereinafter)\b[\s,]*)*"
    r"[\"\u201c\u201d'\u2018\u2019][^)]*\)\s*"
)

# Words kept in capitals when a holder name written in ALL CAPS is title-cased
HOLDER_NAME_ACRONYMS = {
    "REIT", "DC", "UK", "US", "USA", "BT", "LLC", "LP", "LLP", "PLC", "NV",
    "SA", "AG", "II", "III", "IV", "DBS", "UOB", "OCBC", "HSBC", "UBS",
    "SGX", "HK", "PRC", "GIC", "CDL", "OUE", "ESR", "AIMS", "APAC",
}

# Listed REITs and trusts issue units rather than shares
UNIT_ISSUER_PATTERN = r"\b(?:reit|trust)\b"
