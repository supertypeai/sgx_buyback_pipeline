from sgx_scraper.filings_ownership.entities import EntityRegistry
from sgx_scraper.filings_ownership.utils.constants import (
    FAMILY_RELATIONS,
    LISTED_ISSUER,
    MAX_EVIDENCE_LENGTH,
    PERCENT_DECIMALS,
    STRUCTURE_RELATIONS,
)
from sgx_scraper.filings_ownership.utils.graph_helper import reaches
from sgx_scraper.filings_ownership.utils.name_helper import (
    clean_name,
    get_entity_key,
    normalise_text,
)

from collections import defaultdict

import re


WHOLLY_OWNED = re.compile(r"wholly[\s-]*owned", re.IGNORECASE)
NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")


class StructureReader:
    """
    One filing's LLM answer, kept only where the filing itself states it.
    """

    def __init__(
        self,
        registry: EntityRegistry,
        text: str,
        company_id: str,
        known_names: list[str],
        entity_types: dict[str, str],
        holder_ids: set[str],
        form_definitions: dict[str, str],
    ) -> None:
        self.registry = registry
        self.company_id = company_id
        self.entity_types = entity_types
        self.holder_ids = holder_ids
        self.form_definitions = form_definitions
        self.short_names = {}
        # A name the form spells out elsewhere counts as stated by this filing
        known_names = known_names + list(form_definitions.values())
        self.plain_text = normalise_text(text)
        self.numbers = {
            float(number.replace(",", ""))
            for number in NUMBER.findall(text)
        }
        self.known_keys = {get_entity_key(name) for name in known_names} - {""}
        # A holder filed as "X, as Joint Executor of Estate of Y" names the
        # estate only inside its own name, while the text says "the Estate"
        self.known_text = " | ".join(normalise_text(name) for name in known_names)

    def resolve(self, name: str | None) -> str | None:
        if not isinstance(name, str) or not name.strip():
            return None

        if name.strip().upper() == LISTED_ISSUER:
            return self.company_id

        # The boxes' own definitions come first, then the rest of the form's,
        # for when the model still returns a short name such as "DBSH"
        short_name = name.strip().lower()

        if short_name in self.short_names:
            name = self.short_names[short_name]

        elif short_name in self.form_definitions:
            name = self.form_definitions[short_name]

        if not self.is_stated(name):
            return None

        return self.registry.resolve(
            name,
            self.entity_types.get(clean_name(name)),
            self.holder_ids,
        )

    def is_stated(self, name: str) -> bool:
        # A name must be in the text or in a holder name the model was given
        plain_name = normalise_text(name)

        return (
            plain_name in self.plain_text
            or plain_name in self.known_text
            or get_entity_key(name) in self.known_keys
        )

    def get_percentage(self, value: float | None) -> float | None:
        # A percentage must be written in the text; it is stored as a fraction
        if isinstance(value, (int, float)) and value in self.numbers and 0 < value <= 100:
            return round(value / 100, PERCENT_DECIMALS)

        return None

    def build_link(self, item: dict, parent: str, child: str) -> dict:
        relation = item.get("relation")
        percentage = self.get_percentage(item.get("percentage"))
        evidence = (item.get("evidence") or "")[:MAX_EVIDENCE_LENGTH]

        # A route step shows the filing's own words: "wholly owned" is 100%
        # though no figure is written
        stated_relation = relation
        stated_percentage = percentage

        if percentage is None and item.get("percentage") == 100 and WHOLLY_OWNED.search(evidence):
            stated_percentage = 1.0

        if relation == "owns" and percentage is None:
            relation = "controls"

        if relation == "owns" and child == self.company_id:
            relation = "holds_directly"

        if relation == "deemed_via" and self.registry.get_type(child) == "nominee":
            relation = "nominee"

        if relation != "controls" or stated_relation != "owns":
            stated_relation = relation

        return {
            "parent": parent,
            "child": child,
            "relation": relation,
            "percentage": percentage,
            "counts_toward_parent": bool(item.get("counts_toward_parent")) and relation in FAMILY_RELATIONS,
            "evidence": evidence,
            "stated_relation": stated_relation,
            "stated_percentage": stated_percentage,
        }

    def build_direct_holder_link(self, holder_id: str) -> dict:
        return {
            "parent": holder_id,
            "child": self.company_id,
            "relation": "holds_directly",
            "percentage": None,
            "counts_toward_parent": False,
            "evidence": "",
            "stated_relation": "holds_directly",
            "stated_percentage": None,
        }

    def get_nominee_ids(self, links: list[dict]) -> set[str]:
        nominee_ids = {
            link["child"]
            for link in links
            if link["relation"] == "nominee"
        }

        for name, entity_type in self.entity_types.items():
            if entity_type != "nominee":
                continue

            entity_id = self.registry.find(name)

            if entity_id:
                nominee_ids.add(entity_id)

        return nominee_ids

    def read(self, answer: dict) -> tuple[list[dict], dict[str, float], str | None]:
        # A short name like "the Estate" names a different estate in every
        # filing, so only this filing's own definitions are used
        self.short_names = {
            item["short"].strip().lower(): clean_name(item["full"])
            for item in answer.get("short_names") or []
            if item.get("short") and item.get("full")
        }

        links = []

        for item in answer.get("links") or []:
            parent = self.resolve(item.get("parent"))
            child = self.resolve(item.get("child"))

            if parent and child and parent != child:
                links.append(self.build_link(item, parent, child))

        # Shares "registered in the name of UOB Kay Hian" belong to the filer;
        # a nominee is a direct holder only when the filing states its own
        # stake, which arrives as an "owns" link with a percentage
        nominee_ids = self.get_nominee_ids(links)

        for name in answer.get("direct_holders") or []:
            holder_id = self.resolve(name)

            if not holder_id or holder_id == self.company_id:
                continue

            if holder_id in nominee_ids or self.registry.get_type(holder_id) == "nominee":
                continue

            links.append(self.build_direct_holder_link(holder_id))

        # "through its 20% interest in FSML" states the holder's share of the
        # vehicle, not of the listed issuer; a route % equal to a stake in its
        # own vehicle is that stake
        vehicle_stakes = {
            (link["child"], link["percentage"])
            for link in links
            if link["relation"] == "owns"
        }
        route_percentages = {}

        for route in answer.get("route_percentages") or []:
            via = self.resolve(route.get("via"))
            percentage = self.get_percentage(route.get("percentage"))

            if via and percentage and (via, percentage) not in vehicle_stakes:
                route_percentages[via] = percentage

        return links, route_percentages, self.resolve(answer.get("acting_party"))


def drop_restated_links(links: list[dict], company_id: str) -> list[dict]:
    # Filers often add a summary ("each of A, B and C is deemed interested in
    # the units held by D") on top of the full chain; keep the chain. "CLI
    # controls CICT" without a % restates the holding the filing is about
    children = defaultdict(set)

    for link in links:
        if link["relation"] in STRUCTURE_RELATIONS:
            children[link["parent"]].add(link["child"])

    kept = []

    for link in links:
        is_summary = (
            link["relation"] == "deemed_via"
            and reaches(children, link["parent"], link["child"])
        )
        is_restated_holding = (
            link["child"] == company_id
            and link["relation"] in ("controls", "deemed_via")
            and link["percentage"] is None
        )

        if not is_summary and not is_restated_holding:
            kept.append(link)

    return kept
