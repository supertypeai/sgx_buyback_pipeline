# Checkbox labels on SGX Forms 1, 3 and 6, the transaction type and tags each
# one gives, and the keywords and patterns used to read the forms.
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

# OTHER_CIRCUMSTANCES_RULES = {
#     "acceptance of employee share options/share awards": "others",
#     "vesting of share awards": "others",
#     "exercise of employee share options": "buy",
#     "acceptance of take-over offer for the listed issuer": "sell"
# }

# Transaction type and tag mappings keyed by the exact SGX checkbox label.
ACQUISITION_RULES = {
    "Securities via market transaction": {
        "transaction_type": "buy",
        "tags": [],
    },
    "Securities via off-market transaction": {  
        "transaction_type": "buy",
        "tags": [],
    },
    "Securities via physical settlement": {
        "transaction_type": "buy",
        "tags": [],
    },
    "Securities pursuant to rights issue": {
        "transaction_type": "buy",
        "tags": ["rights-offering"],
    },
    "Securities via a placement": {
        "transaction_type": "buy",
        "tags": [],
    },
    "Securities following conversion/exercise": {
        "transaction_type": "buy",
        "tags": [],
    },
    "Securities as part of management": {
        "transaction_type": "others",
        "tags": ["management-fees"],
    },
}

OTHER_CIRCUMSTANCES_RULES = {
    "Acceptance of employee share options/share awards": {
        "transaction_type": "others",
        "tags": ["award"],
    },
    "Vesting of share awards": {
        "transaction_type": "others",
        "tags": ["award"],
    },
    "Exercise of employee share options": {
        "transaction_type": "buy",
        "tags": ["option-exercise"],
    },
    "Acceptance of take-over offer": {
        "transaction_type": "sell",
        "tags": ["takeover"],
    },
    "Acceptance of take-over offer for Listed Issuer": {
        "transaction_type": "sell",
        "tags": [],
    },
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

TYPE_SECURITIES_SECTION_PATTERN = r'Type of securities which are the subject of the transaction'

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
KEYWORD_SHARE_BUYBACK = [
    "share buyback", "share buy back", "share buy-back",
    "equal access scheme", "repurchase of shares", "share repurchase",
]

CORPORATE_ACTION_CHECKBOX_KEY = "Corporate action by Listed Issuer"

# Free-text corporate actions only get a deterministic classification when the
# description explicitly names a share buyback; the rest go to the LLM fallback.
SHARE_BUYBACK_RULE = {
    "transaction_type": "others",
    "tags": ["share-buyback"],
}

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


# Form checkboxes read as option sets: each key is the stored value and each
# pattern matches the option's printed label.
NOTIFICATION_TYPE_OPTIONS = {
    "becoming": r"^Becoming a Substantial",
    "change_in_level": r"^Change in the percentage level",
    "ceasing": r"^Ceasing to be a Substantial",
}
ISSUER_TYPE_OPTIONS = {
    "company": r"^Company/Corporation$",
    "business_trust": r"^Registered/Recognised Business Trust$",
    "reit": r"^Real Estate Investment Trust$",
}
TRANSACTION_REFERENCE_LABEL = r"Transaction Reference Number\s*\(\s*auto-generated\s*\)"
TRANSACTION_REFERENCE_LENGTH = 15
