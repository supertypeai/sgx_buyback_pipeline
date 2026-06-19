ACQUISITION_OPTIONS = {
    "Securities via market transaction": r"Securities via market transaction",
    "Securities via off-market transaction": r"Securities via off-market transaction",
    "Securities via physical settlement": r"Securities via physical settlement",
    "Securities pursuant to rights issue": r"Securities pursuant to rights issue",
    "Securities via a placement": r"Securities via a placement",
    "Securities following conversion/exercise": r"Securities following conversion",
    "Securities as part of management": r"Securities as part of management"
}

DISPOSAL_OPTIONS = {
    "Securities via market transaction": r"Securities via market transaction",
    "Securities via off-market transaction": r"Securities via off-market transaction"
}

OTHER_OPTIONS = {
    "Acceptance of take-over offer": r"Acceptance of take-over offer",
    # "Corporate action by Listed Issuer": r"Corporate action.*Listed Issuer",
    "Acceptance of employee share options/share awards": r"Acceptance of employee share options",
    "Vesting of share awards": r"Vesting of share awards",
    "Exercise of employee share options": r"Exercise of employee share options",
    "Acceptance of take-over offer for Listed Issuer": r"Acceptance of take-over offer",
}

OTHER_CIRCUMSTANCES_RULES = {
    "acceptance of employee share options/share awards": "award",
    "vesting of share awards": "award",
    "exercise of employee share options": "buy",
    "acceptance of take-over offer for the listed issuer": "sell"
}

TRANSACTION_KEYWORDS = {
    # For compensation-related acquisitions
    "award": [
        "award", 
        "grant",
    ],
    
    # For acquisitions through special rights
    "buy": [
        'acquisition',
        "exercise of options", 
        "rights allotment", 
        "share buy-back"
    ],

    # For transfers not on the open market
    "transfer": [
        "transfer",
        "trust deed",
        "spousal agreement"
    ],

    # For non-market sales or disposals
    "sell": [
        "disposal",
        "disposed of",
        "disposed"
    ]
}

TYPE_SECURITIES_OPTIONS = {
    "Voting shares/units": r"(?:Ordinary\s+)?voting\s+(?:shares|units)",
    "Rights/Options/Warrants over voting shares/units": r"Rights/Options/Warrants\s+over\s+(?:voting\s+)?(?:shares/)?units",
    "Convertible debentures over voting shares/units": r"(?:Convertible\s+)?[Dd]ebentures",
    "Others": r"Others.*(?:specify|:)"
}

KEYWORD_DIRECTOR_FEE = [
    "director's fee", "directors' fee", "director\u2019s fee", "directors\u2019 fee",
    "non-executive director", "in lieu of cash", "share component of my",
    "directors' remuneration", "directors\u2019 remuneration",
]
KEYWORD_EMPLOYEE_PLAN = [
    "restricted unit plan", "performance unit plan", "restricted share plan",
    "restricted stapled security plan", "performance stapled security plan",
    "long-term incentive plan", "executive share scheme", "restricted share award",
    "vesting of awards", "key management personnel", "eligible employees",
    "share grant",
]
KEYWORD_MANAGEMENT_FEE = [
    "management fee", "base management fee", "performance management fee",
    "divestment fee", "acquisition fee",
]
KEYWORD_DIVIDEND = [
    "dividend in specie", "special dividend",
]
KEYWORD_INHERITANCE = [
    "deceased", "beneficiaries",
]
KEYWORD_INTERNAL_RESTRUCTURING = [
    "internal transfer", "wholly-owned subsidiaries", "amalgamated",
    "amalgamation", "by operation of law", "immediate holding company",
]
KEYWORD_GIFT = [
    "by way of gift", "as a gift",
]

# Checkbox keys that all map to the employee-share-plan tag.
EMPLOYEE_CHECKBOX_KEYS = [
    "Acceptance of employee share options/share awards",
    "Vesting of share awards",
    "Exercise of employee share options",
]
# The take-over key varies between forms; only one appears per filing.
TAKEOVER_CHECKBOX_KEYS = [
    "Acceptance of take-over offer",
    "Acceptance of take-over offer for Listed Issuer",
]
