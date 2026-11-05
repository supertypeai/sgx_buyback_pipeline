from concurrent.futures import ThreadPoolExecutor

from sgx_scraper.filings_ownership.utils.constants import PDF_CONCURRENCY
from sgx_scraper.filings_ownership.utils.name_helper import LEGAL_SUFFIX
from sgx_scraper.utils.http_client import HTTPCLIENT

import fitz
import logging
import re


LOGGER = logging.getLogger(__name__)

# 'Singapura Developments (Private) Limited ("SDPL")': the run of capitalised
# words, "&", bracketed words and connectors right before the quoted short name
NAME_WORD = r"(?:[A-Z0-9][\w&.,'’\-/]*|\([A-Z][\w]*\)|&|of|and|de|du|la|le|van|der)"
DEFINITION = re.compile(
    rf"((?:{NAME_WORD}\s+)*?{NAME_WORD})\s*\(\s*(?:the\s+)?[\"“”]\s*([^\"“”()]{{2,40}}?)\s*[\"“”]\s*\)"
)
SENTENCE_END = re.compile(r"(\S+)\.\s+(?=\S)")
# "Ltd." and "Inc." end a name, so they are not here
NAME_ABBREVIATIONS = {"pte", "co", "sdn", "no", "st", "mt", "jr", "sr", "bros", "intl", "int'l", "p.t", "s.a", "n.v", "b.v", "l.p", "pt"}
DEFINITION_LEAD = re.compile(r"^(?:(?:of|and|de|du|la|le|van|der|\d+\.?|[IVX]+\.?)\s+)+")
# "FCAM and Frasers Property Retail Trust Holdings" lists two parties
LISTED_SHORT_NAMES = re.compile(r"^(?:[A-Z0-9&]{2,10}(?:,\s+(?:and\s+)?|\s+and\s+))+(?=[A-Z][a-z])")
LIST_SEPARATOR = re.compile(r",\s+(?:and\s+)?")
POSSESSIVE = re.compile(r"['’]s$")
NAME_INITIALS = re.compile(r"[A-Za-z][\w&]*")


def cut_sentence_start(full_name: str) -> str:
    # A full stop ends the sentence before the name ("657,873,900 Units.
    # Centurion Properties Pte. Ltd") unless it closes an abbreviation
    for match in reversed(list(SENTENCE_END.finditer(full_name))):
        if match.group(1).lower() not in NAME_ABBREVIATIONS:
            return full_name[match.end():]

    return full_name


def keep_last_listed_name(full_name: str) -> str:
    # "through DBSH, and Fullerton Fund Management Company Ltd" names Fullerton
    # last; a part after a comma that is only a suffix ("Fraser and Neave,
    # Limited") stays with the name before it
    while True:
        parts = LIST_SEPARATOR.split(full_name, maxsplit=1)

        if len(parts) != 2:
            return full_name

        rest = parts[1]
        is_suffix = LEGAL_SUFFIX.fullmatch(" " + rest.lower().replace(".", ""))

        if len(rest.split()) < 2 or is_suffix:
            return full_name

        full_name = DEFINITION_LEAD.sub("", rest).strip(" ,.")


def is_definition(full_name: str, short_name: str) -> bool:
    # A short name starts with one of its full name's initials: "Transaction"
    # beside "Siriwana Co., Ltd" is a misread, not a definition
    initials = {word[0] for word in NAME_INITIALS.findall(full_name.lower())}

    return (
        len(full_name) > len(short_name)
        and bool(re.match(r"[A-Z]", full_name))
        and not re.match(r"On\s", full_name)
        and short_name[:1].lower() in initials
    )


def clean_definition_name(full_name: str, short_name: str) -> str | None:
    full_name = cut_sentence_start(full_name)
    full_name = LISTED_SHORT_NAMES.sub("", full_name)
    full_name = DEFINITION_LEAD.sub("", full_name).strip(" ,.")
    full_name = POSSESSIVE.sub("", full_name)
    full_name = keep_last_listed_name(full_name)

    return full_name if is_definition(full_name, short_name) else None


def read_form_definitions(source: str) -> dict[str, str]:
    # A box often uses a short name the form spells out in another part, such
    # as "DBSH" in Part I or the circumstance item; the first spelling wins
    try:
        response = HTTPCLIENT.get(source)
        response.raise_for_status()

        with fitz.open(stream=response.content, filetype="pdf") as document:
            text = " ".join(page.get_text() for page in document)

    except Exception as error:
        LOGGER.warning("[Definitions] Could not read %s: %s", source, error)

        return {}

    text = re.sub(r"\s+", " ", text)
    definitions = {}

    for full_name, short_name in DEFINITION.findall(text):
        short_name = short_name.strip()
        key = short_name.lower()

        if key in definitions:
            continue

        name = clean_definition_name(full_name, short_name)

        if name:
            definitions[key] = name

    return definitions


def read_all_form_definitions(sources: list[str]) -> dict[str, dict[str, str]]:
    with ThreadPoolExecutor(max_workers=PDF_CONCURRENCY) as pool:
        results = pool.map(read_form_definitions, sources)

    return dict(zip(sources, results))


def get_used_definitions(definitions: dict[str, str], text: str) -> dict[str, str]:
    # Only short names the boxes actually use are worth showing the model
    return {
        short_name: full_name
        for short_name, full_name in definitions.items()
        if re.search(rf"\b{re.escape(short_name)}\b", text, re.IGNORECASE)
    }
