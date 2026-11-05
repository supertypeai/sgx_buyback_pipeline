import re

from sgx_scraper.filings_ownership.utils.name_helper import display_name


# "CapitaLand Investment Limited" and "CapitaLand Investment Ltd." are one name
LEGAL_SUFFIX = re.compile(r"\b(limited|ltd|pte|private|inc|llc|plc|berhad|bhd)\b")

# Words too common to say two companies are one group
GENERIC_FIRST = {
    "the", "singapore", "china", "asia", "capital", "global", "international", "first",
    "united", "great", "new", "national", "pt", "mr", "dr",
}

COMPANY_WORD = re.compile(
    r"\b(limited|ltd|pte|private|inc|llc|plc|berhad|bhd|sdn|holdings?|trust|corp|corporation"
    r"|company|co|fund|group|s\.?a|ag|gmbh|lp|l\.p)\b\.?",
    re.IGNORECASE,
)

# "(formerly known as ...)", "(f.k.a. ...)": a former name, not part of the holder's name
FORMER_NAME = re.compile(
    r"\s*\(\s*(formerly|f\.?\s?k\.?\s?a\.?|now known as|previously)\b[^)]*\)",
    re.IGNORECASE,
)

NOMINEE = re.compile(r"nominee|custod|\bDBSN\b", re.IGNORECASE)

# Honorifics and orders a filing or report puts around a person's name
PERSON_TITLES = {
    "mr", "mrs", "ms", "mdm", "madam", "dr", "prof", "professor", "sir", "dato", "datuk",
    "seri", "puan", "kbe", "ao", "obe", "cbe", "pbm", "bbm",
}


def readable_report(report: dict) -> dict:
    # Every name a report table gives, so a holder reads the same wherever it appears
    holders = []

    for holder in report.get("holders") or []:
        parts = [
            {
                **part,
                "held_by": display_name(part["held_by"]) if part.get("held_by") else part.get("held_by"),
            }
            for part in holder.get("parts") or []
        ]

        holders.append({
            **holder,
            "name": display_name(holder["name"]),
            "deemed_through": readable_names(holder.get("deemed_through")),
            "held_through_others": readable_names(holder.get("held_through_others")),
            "parts": parts,
        })

    owners = []

    for owner in report.get("ultimate_owners") or []:
        readable = {
            **owner,
            "name": display_name(owner["name"]),
            "via": readable_names(owner.get("via")),
        }

        if owner.get("owned_by"):
            readable["owned_by"] = readable_names(owner["owned_by"])

        owners.append(readable)

    return {**report, "holders": holders, "ultimate_owners": owners}


def readable_names(names: list[str] | None) -> list[str]:
    return [display_name(name) for name in names or []]


def company_name_key(name: str) -> str:
    key = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    key = LEGAL_SUFFIX.sub(" ", key)

    return re.sub(r"\s+", " ", key).strip()


def register_name_key(name: str) -> str:
    # One key for every spelling of a holder: footnote marks ("Calvin (4)"), a former name, a
    # defined short name ("(FPALP)", "(“Pearl Yard”)") and honorifics are dropped. A longer
    # bracket such as "(acting as trustee of Merit Trust)" tells holders apart, so it stays
    name = FORMER_NAME.sub("", name)
    name = re.sub(r"(\s*,?\s*\(\d+\))+\s*$", "", name)
    name = re.sub(r"\s*\(\s*[\"“”']?[\w&.-]+[\"“”']?\s*\)\s*$", "", name)
    name = re.sub(r"\s*\(\s*[\"“”'‘’][^()\"“”'‘’]+[\"“”'‘’]\s*\)\s*$", "", name)
    name = re.sub(r"^(mr|mrs|ms|madam|dr|professor|prof)\.?\s+", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\d+$", "", name)

    return company_name_key(name).replace(" ", "")


def holding_key(holder_id: str, company_id: str) -> str:
    return f"{holder_id}|{company_id}"


def bare_symbol(symbol: str) -> str:
    return symbol.upper().removesuffix(".SI")


def looks_like_person(name: str, entity_type: str) -> bool:
    return entity_type == "person" or (entity_type == "unknown" and not COMPANY_WORD.search(name))


def person_core(name: str) -> str:
    # One key for a person however titled; "Tan Sri" is a title only as a pair, since Tan is
    # also a surname
    words = re.sub(r"[^a-z ]", " ", name.lower().replace("'", ""))
    words = re.sub(r"\btan sri\b", " ", words)

    return " ".join(sorted(
        word
        for word in words.split()
        if word not in PERSON_TITLES
    ))


def first_word(name: str) -> str:
    match = re.search(r"[a-z0-9]+", name.lower())

    return match.group(0) if match else ""
