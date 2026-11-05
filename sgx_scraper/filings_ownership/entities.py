from sgx_scraper.filings_ownership.utils.constants import (
    MIN_KEY_LENGTH,
    TYPO_SIMILARITY,
)
from sgx_scraper.filings_ownership.utils.name_helper import (
    clean_name,
    display_name,
    get_entity_key,
    get_serial_marks,
    get_word_order_key,
)

from collections import defaultdict
from rapidfuzz import fuzz

import logging


LOGGER = logging.getLogger(__name__)

ENTITY_ID_PREFIX = "e"


def get_id_number(entity_id: str) -> int:
    return int(entity_id.removeprefix(ENTITY_ID_PREFIX))


class EntityRegistry:
    """
    Every party known so far, matched by name, with new ones added on top.

    Ids continue from the highest one stored, so an existing id never changes.
    """

    def __init__(self, rows: list[dict]) -> None:
        self.rows = {row["id"]: dict(row) for row in rows}
        self.id_by_key = {}
        self.keys_by_prefix = defaultdict(list)
        self.keys_by_word_order = defaultdict(list)
        self.id_by_symbol = {}
        self.changed_ids = set()

        for row in self.rows.values():
            self.index_row(row)

        self.next_number = max(
            (get_id_number(entity_id) for entity_id in self.rows),
            default=0,
        ) + 1

    def index_row(self, row: dict) -> None:
        for name in [row["name"], *(row.get("aliases") or [])]:
            self.index_name(row["id"], name)

        if row.get("listed_symbol"):
            self.id_by_symbol.setdefault(row["listed_symbol"], row["id"])

    def index_name(self, entity_id: str, name: str) -> None:
        key = get_entity_key(name)

        if len(key) < MIN_KEY_LENGTH or key in self.id_by_key:
            return

        self.id_by_key[key] = entity_id

        self.keys_by_prefix[key[:4]].append(key)
        self.keys_by_word_order[get_word_order_key(key)].append(key)

    def find(self, name: str, holder_ids: set[str] | None = None) -> str | None:
        key = get_entity_key(clean_name(name))

        if len(key) < MIN_KEY_LENGTH:
            return None

        if key in self.id_by_key:
            return self.id_by_key[key]

        return self.find_typo(key) or self.find_reordered_person(key, holder_ids or set())

    def find_typo(self, key: str) -> str | None:
        # Typos in filings ("Philip Securities") split one company in two. Names
        # differing in a number or roman numeral ("Holding I", "Holding II") stay
        # apart, as do names where one adds a word. People are never matched on
        # spelling alone
        for other_key in self.keys_by_prefix[key[:4]]:
            entity_id = self.id_by_key[other_key]

            if self.rows[entity_id]["entity_type"] == "person":
                continue

            if get_serial_marks(key) != get_serial_marks(other_key):
                continue

            if len(key.split()) != len(other_key.split()):
                continue

            # One letter more in a one-word name is another company ("Siriwana",
            # "Siriwanan"), and a short word that differs is a designator ("Co-Invest
            # LP", "Co-Invest GP"), not a slip
            if len(key.split()) == 1:
                continue

            changed = [
                (word, other)
                for word, other in zip(key.split(), other_key.split())
                if word != other
            ]

            if any(min(len(word), len(other)) <= 2 for word, other in changed):
                continue

            if fuzz.ratio(key, other_key) >= TYPO_SIMILARITY:
                LOGGER.info("[Entities] '%s' matched '%s' as a typo", key, other_key)

                return entity_id

        return None

    def find_reordered_person(self, key: str, holder_ids: set[str]) -> str | None:
        # "Andrew Lee Kok Keng" and "Lee Kok Keng Andrew" are one director only
        # when that person has already filed for the same issuer; two people can
        # share a short name
        for other_key in self.keys_by_word_order[get_word_order_key(key)]:
            entity_id = self.id_by_key[other_key]

            if entity_id in holder_ids and self.rows[entity_id]["entity_type"] == "person":
                LOGGER.info("[Entities] '%s' matched '%s' in another word order", key, other_key)

                return entity_id

        return None

    def resolve(
        self,
        name: str | None,
        entity_type: str | None = None,
        holder_ids: set[str] | None = None,
    ) -> str | None:
        name = clean_name(name)

        if entity_type == "not_a_party" or len(get_entity_key(name)) < MIN_KEY_LENGTH:
            return None

        entity_id = self.find(name, holder_ids)

        if entity_id is None:
            return self.add(name, entity_type or "unknown")

        self.add_alias(entity_id, name)

        return entity_id

    def add(
        self,
        name: str,
        entity_type: str,
        listed_symbol: str | None = None,
    ) -> str:
        entity_id = f"{ENTITY_ID_PREFIX}{self.next_number}"
        self.next_number += 1

        # A form filled in capitals keeps that spelling as an alias, so it still matches
        readable = display_name(name)
        row = {
            "id": entity_id,
            "name": readable,
            "aliases": [name] if readable != name else [],
            "entity_type": entity_type,
            "listed_symbol": listed_symbol,
        }
        self.rows[entity_id] = row
        self.index_row(row)
        self.changed_ids.add(entity_id)

        LOGGER.info("[Entities] New %s %s: %s", entity_type, entity_id, name)

        return entity_id

    def add_alias(self, entity_id: str, name: str) -> None:
        row = self.rows[entity_id]

        if name == row["name"] or name in row["aliases"]:
            return

        row["aliases"] = [*row["aliases"], name]
        self.index_name(entity_id, name)
        self.changed_ids.add(entity_id)

    def get_company_id(self, symbol: str, company_name: str | None) -> str:
        if symbol in self.id_by_symbol:
            return self.id_by_symbol[symbol]

        name = company_name or symbol
        entity_id = self.find(name)

        if entity_id is None:
            return self.add(name, "company", listed_symbol=symbol)

        if not self.rows[entity_id].get("listed_symbol"):
            self.rows[entity_id]["listed_symbol"] = symbol
            self.id_by_symbol[symbol] = entity_id
            self.changed_ids.add(entity_id)

        return entity_id

    def get_type(self, entity_id: str) -> str:
        return self.rows[entity_id]["entity_type"]

    def get_name(self, entity_id: str) -> str:
        return self.rows[entity_id]["name"]

    def get_changed_rows(self) -> list[dict]:
        return [
            self.rows[entity_id]
            for entity_id in sorted(self.changed_ids, key=get_id_number)
        ]
