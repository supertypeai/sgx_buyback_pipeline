import re


# A capacity ("as Joint Executor of ...") or a trailing note of three or more
# words ("(Alternate Director to ...)", "(formerly known as ...)") is not part
# of the name; a short bracket like "(III)" or "(Private)" is
CAPACITY = re.compile(
    r",?\s+(?:as|in (?:his|her) capacity as)\s.*$|\s*\((?:[^()\s]+\s+){2,}[^()]*\)\s*$",
    re.IGNORECASE,
)
# Malaysian titles combine ("Tan Sri Dato' Seri"); bare "Sri" is left alone,
# since it also starts names such as Sri Trang
TITLE_PREFIX = re.compile(
    r"^(?:(?:mr|mrs|ms|mdm|madam|dr|prof|professor|tan sri|puan sri|datuk seri"
    r"|datuk sri|dato seri|dato sri|datin seri|datuk|dato|datin|sir|tun)\s+)+"
)

# Legal forms are stripped only at the end: "Corporation" inside "Hong Leong
# Corporation Holdings" tells it apart from Hong Leong Holdings. Partnership
# forms stay, since a fund (LP) and its general partner (Ltd) are two parties
LEGAL_SUFFIX = re.compile(
    r"(?:\s(?:pte|private|ltd|lim[a-z]*t[a-z]*d|inc|incorporated|llc|plc|co|corp"
    r"|corporation|company|bhd|berhad|sdn|ag|sa|s a|nv|n v))+$"
)
PARTNERSHIP_FORMS = [
    (re.compile(r"\bl l p\b"), "llp"),
    (re.compile(r"\bl p\b"), "lp"),
    (re.compile(r"\bg p\b"), "gp"),
]
SERIAL_MARK = re.compile(r"\b(?:\d+|i{1,3}|iv|vi{0,3}|ix|x)\b")

# The model sometimes copies the definition along with the name:
# 'SAI Pte. Ltd. ("SAI")'
SHORT_NAME_DEFINITION = re.compile(r"\s*\(\s*[\"“][^()]*[\"”]\s*\)\s*$")


# Words a name printed in capitals keeps: legal forms, acronyms and Roman numerals
KEPT_CAPITALS = {
    "LLC", "LP", "LLP", "PLC", "NV", "BV", "SA", "AG", "GP", "VCC", "REIT", "SPV", "PT", "TBK",
    "UK", "US", "USA", "HK", "PRC", "DBS", "UOB", "OCBC", "HSBC", "UBS", "GIC", "SGX", "CDL", "OUE",
    "ESR", "AIMS", "APAC", "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI", "XII",
}
JOINING_WORDS = {
    "of", "and", "alias", "for", "as", "in", "the", "de", "du", "la", "von", "van", "bin", "binti",
}
# Short or vowel-less words that are still words: legal forms and two-letter surnames ("NG", "XU")
CASED_WORDS = {
    "LTD", "SDN", "BHD", "CO", "PTE", "NG", "XU", "HO", "GO", "LI", "OH", "WU", "YU", "LU", "MA",
    "HU", "KO", "TO",
}


def display_name(name: str) -> str:
    # A name printed in capitals reads normally ("LIM ENG HOCK" -> "Lim Eng Hock"); a one-word
    # short name such as "ICG" stays as printed
    if re.search(r"[a-z]", name) or not re.search(r"[A-Z]{3}", name) or " " not in name.strip():
        return name

    words = name.split(" ")

    return " ".join(
        display_word(word, position == 0)
        for position, word in enumerate(words)
    )


def display_word(word: str, first: bool) -> str:
    letters = re.sub(r"[^A-Za-z]", "", word)

    if letters in CASED_WORDS:
        return capitalised(word)

    # Acronyms stay: known ones, two-letter words and words with no vowel ("FMR", "NTCP")
    is_short = len(letters) <= 2 and letters.lower() not in JOINING_WORDS

    if not letters or letters in KEPT_CAPITALS or is_short:
        return word

    if not re.search(r"[AEIOUY]", letters):
        return word

    if not first and letters.lower() in JOINING_WORDS:
        return word.lower()

    return capitalised(word)


def capitalised(word: str) -> str:
    return re.sub(r"[A-Za-z]+", lambda match: match.group(0).capitalize(), word)


def clean_name(name: str | None) -> str:
    return re.sub(r"\s+", " ", name or "").strip(" .,;:")


def clean_trader_name(name: str) -> str:
    return clean_name(SHORT_NAME_DEFINITION.sub("", name))


def get_entity_key(name: str | None) -> str:
    # Name normalisation only, so spellings of one party match
    key = CAPACITY.sub("", name or "").lower()
    key = re.sub(r"[^a-z0-9 ]", " ", key)
    key = re.sub(r"\s+", " ", key).strip()
    key = TITLE_PREFIX.sub("", key)
    key = LEGAL_SUFFIX.sub("", " " + key).strip()
    key = re.sub(r"^the\s", "", key)

    for pattern, form in PARTNERSHIP_FORMS:
        key = pattern.sub(form, key)

    return key


def get_serial_marks(key: str) -> set[str]:
    return set(SERIAL_MARK.findall(key))


def get_word_order_key(key: str) -> str:
    return " ".join(sorted(key.split()))


def normalise_text(text: str | None) -> str:
    text = re.sub(r"[“”]", '"', text or "")
    text = re.sub(r"[‘’]", "'", text)

    return re.sub(r"\s+", " ", text).strip().lower()
