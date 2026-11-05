import re


COMPANY_NAME = re.compile(
    r"\b(pte|ltd|limited|inc|incorporated|llc|l\.?p|plc|corp|corporation|company|co|holdings?"
    r"|trusts?|trustees?"
    r"|funds?|bank|banking|group|investments?|capital|partners|foundation|authority|management|advisors?"
    r"|assets|enterprises?|international|properties|securities|bhd|berhad|ag|s\.?a|se|nv|bv|gmbh|estate"
    r"|executors?|government|ministry|vcc|pt|tbk|spc|spv|gp|scsp|s\.?à\s?r\.?l|reinsurance|assurances?"
    r"|insurance|mutuelles|cooperations?|platform|participations?|portfolio|\w+co)\b",
    re.IGNORECASE,
)
# A person's name has at least two words; a one-word name is a defined term (BEC's "Esteel")
PERSON_NAME = re.compile(r"^\W*\w[\w.,'@-]*(\s+[\w.,'@()-]+)+\W*$")
ACRONYM = re.compile(r"\b[A-Z]{3,}\b")
HONORIFIC = re.compile(r"^\W*(mr|mrs|ms|mdm|madam|dr|professor|prof|tan sri|dato|datuk)\b", re.IGNORECASE)
FOOTNOTE_MARK = re.compile(r"\(\d+\)")


def is_person_name(name: str) -> bool:
    # An acronym beside ordinary words is a company (MXNU's "MMAF Santé"), but an
    # all-capitals name or a capitalised surname is a person ("Mr. WONG Fong Pak")
    has_acronym = (
        bool(ACRONYM.search(name))
        and name != name.upper()
        and not HONORIFIC.search(name)
    )

    return (
        bool(PERSON_NAME.match(name.strip()))
        and not COMPANY_NAME.search(name)
        and not has_acronym
    )


def holder_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", FOOTNOTE_MARK.sub("", name.lower()))


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).lower()
