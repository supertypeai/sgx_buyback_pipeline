from collections import defaultdict
from pathlib import Path

from sgx_scraper.filings_ownership.entities import EntityRegistry
from sgx_scraper.filings_ownership.links import get_link_key, merge_filing_links
from sgx_scraper.filings_ownership.reader import (
    build_filing_input,
    get_filing_text,
    needs_reading,
    read_filings,
)
from sgx_scraper.filings_ownership.routes import build_route_steps, build_routes
from sgx_scraper.filings_ownership.store import (
    normalise_filing,
    normalise_ownership_row,
    read_table,
)
from sgx_scraper.filings_ownership.structure import StructureReader, drop_restated_links
from sgx_scraper.filings_ownership.traders import (
    get_accepted_traders,
    get_named_self_records,
    holds_through_others,
)
from sgx_scraper.filings_ownership.utils.constants import (
    COMPANY_TABLE,
    COMPANY_VIEW_TABLE,
    ENTITIES_TABLE,
    ENTITY_VIEW_TABLE,
    FILING_COLUMNS,
    FILING_OWNERSHIP_TABLE,
    FILINGS_TABLE,
    LINKS_TABLE,
)
from sgx_scraper.filings_ownership.utils.definition_helper import (
    get_used_definitions,
    read_all_form_definitions,
)
from sgx_scraper.filings_ownership.utils.name_helper import clean_name, get_entity_key
from sgx_scraper.filings_ownership.utils.record_helper import (
    get_interest_percentage,
    get_interest_role,
    get_move_kind,
    get_percentage_change,
    get_share_change,
    get_substantial_status,
    get_thresholds_crossed,
    needs_trader,
    round_fractions,
)
from sgx_scraper.filings_ownership.views import build_company_views, build_entity_views

import asyncio
import copy
import json
import logging
import re


LOGGER = logging.getLogger(__name__)

ABBREVIATION = re.compile(r"[A-Z0-9&.\-]{2,10}")


def load_state(state_dir: Path | None) -> dict:
    filings = read_table(FILINGS_TABLE, FILING_COLUMNS, state_dir)
    ownership_rows = read_table(FILING_OWNERSHIP_TABLE, state_dir=state_dir)
    companies = read_table(COMPANY_TABLE, "symbol,name", state_dir)

    return {
        "filings": [normalise_filing(row) for row in filings],
        "ownership_rows": [normalise_ownership_row(row) for row in ownership_rows],
        "entity_rows": read_table(ENTITIES_TABLE, state_dir=state_dir),
        "link_rows": read_table(LINKS_TABLE, state_dir=state_dir),
        "company_views": read_table(COMPANY_VIEW_TABLE, state_dir=state_dir),
        "entity_views": read_table(ENTITY_VIEW_TABLE, state_dir=state_dir),
        "company_names": {row["symbol"]: row["name"] for row in companies},
    }


def get_filing_order(filing: dict) -> tuple[str, int]:
    return filing["timestamp"] or "", filing["id"]


def group_new_filings(
    filings: list[dict],
    ownership_rows: list[dict],
    limit: int | None,
) -> dict[str, list[dict]]:
    # A filing is new until it has an ownership row, so a filing skipped
    # after a failed read is picked up again on the next run
    processed_ids = {row["filing_id"] for row in ownership_rows}
    groups = defaultdict(list)

    for filing in sorted(filings, key=get_filing_order):
        if filing["id"] not in processed_ids and filing.get("holder_name"):
            groups[filing["source"]].append(filing)

    sources = list(groups)[:limit] if limit else list(groups)

    return {source: groups[source] for source in sources}


def get_company_holder_ids(ownership_rows: list[dict]) -> dict[str, set[str]]:
    holder_ids = defaultdict(set)

    for row in ownership_rows:
        holder_ids[row["company_id"]].add(row["holder_id"])

    return holder_ids


def get_answer_types(answer: dict | None) -> dict[str, str]:
    if not answer:
        return {}

    return {
        clean_name(item.get("name")): item.get("entity_type")
        for item in answer.get("entity_types") or []
        if item.get("name")
    }


def get_record_acting_party_id(
    filing: dict,
    group: list[dict],
    answer: dict | None,
    acting_party_id: str | None,
    registry: EntityRegistry,
    entity_types: dict[str, str],
) -> str | None:
    # The model writes the acting party's full name when the filing has one
    # acting party; otherwise each record keeps the name the parser found
    acting_parties = {
        record.get("acting_party")
        for record in group
        if record.get("acting_party")
    }

    if answer and len(acting_parties) == 1:
        return acting_party_id

    name = filing["acting_party"]

    return registry.resolve(name, entity_types.get(clean_name(name)))


def get_trader_ids(
    filing: dict,
    holder_id: str,
    interest_role: str,
    acting_party_id: str | None,
) -> list[str]:
    if interest_role in ("direct", "both"):
        return [holder_id]

    if filing.get("acting_party") and acting_party_id:
        return [acting_party_id]

    return []


def get_unstated_trader_ids(
    filing: dict,
    holder_id: str,
    routes: list[dict],
    direct_filer_ids: set[str],
    accepted_names: list[str],
    short_names: dict[str, str],
    registry: EntityRegistry,
    entity_types: dict[str, str],
    is_named_dealer: bool,
) -> list[str]:
    # With no acting party, a co-filer whose own direct holding moved is
    # proven by the figures when the holder's interest runs through it;
    # otherwise the filing's wording names the traders, or nobody can tell
    candidates = direct_filer_ids - {holder_id}

    if len(candidates) == 1:
        candidate = next(iter(candidates))

        if any(candidate in route["chain"] for route in routes):
            return [candidate]

    trader_ids = []

    for name in accepted_names:
        name = short_names.get(name.lower(), name)
        trader_id = registry.resolve(name, entity_types.get(clean_name(name)))

        if trader_id and trader_id not in trader_ids:
            trader_ids.append(trader_id)

    # A deemed-only holder whose shares sit with its subsidiaries or relatives
    # did not decide the trade itself, unless the filing's own wording names
    # it as the party that dealt
    if holder_id in trader_ids and not is_named_dealer and holds_through_others(filing, routes):
        trader_ids.remove(holder_id)

    return trader_ids


def is_on_routes(entity_id: str, routes: list[dict]) -> bool:
    return any(entity_id in route["chain"] for route in routes)


def get_shared_trader_id(
    filing: dict,
    row: dict,
    traded_rows: list[tuple[dict, dict]],
) -> str | None:
    # A parent deemed interested only because a subsidiary dealt moves by the
    # same shares as that subsidiary's record; the model often names the
    # trader on the first record only. When the parent's routes reach two
    # different traders, the filing cannot show whose dealing it was
    reached_rows = [
        (traded_filing, traded_row)
        for traded_filing, traded_row in traded_rows
        if traded_row["holder_id"] != row["holder_id"]
        and (
            is_on_routes(traded_row["holder_id"], row["deemed_routes"])
            or any(
                is_on_routes(trader_id, row["deemed_routes"])
                for trader_id in traded_row["trader_ids"]
            )
        )
    ]

    trader_ids = {
        trader_id
        for _, traded_row in reached_rows
        for trader_id in traded_row["trader_ids"]
        if trader_id != row["holder_id"]
    }
    is_same_change = any(
        get_share_change(traded_filing) == get_share_change(filing)
        for traded_filing, _ in reached_rows
    )

    if len(trader_ids) != 1 or not is_same_change:
        return None

    return next(iter(trader_ids))


def is_blocked_self_trade(filing: dict, row: dict, named_filing_ids: set[int]) -> bool:
    return (
        filing["id"] not in named_filing_ids
        and needs_trader(filing)
        and row["trader_ids"] == [row["holder_id"]]
        and holds_through_others(filing, row["deemed_routes"])
    )


def fill_shared_traders(group: list[dict], rows: list[dict], named_filing_ids: set[int]) -> None:
    filings_by_id = {filing["id"]: filing for filing in group}
    # A stored parent named as its own trader is not passed down the group
    traded_rows = [
        (filings_by_id[row["filing_id"]], row)
        for row in rows
        if row["trader_ids"]
        and not is_blocked_self_trade(filings_by_id[row["filing_id"]], row, named_filing_ids)
    ]

    for row in rows:
        filing = filings_by_id[row["filing_id"]]

        if row["trader_ids"] or not needs_trader(filing) or not get_share_change(filing):
            continue

        trader_id = get_shared_trader_id(filing, row, traded_rows)

        if trader_id:
            row["trader_ids"] = [trader_id]


def build_ownership_row(
    filing: dict,
    holder_id: str,
    company_id: str,
    trader_ids: list[str],
    interest_role: str,
    routes: list[dict],
) -> dict:
    before = get_interest_percentage(filing, "before", "total")
    after = get_interest_percentage(filing, "after", "total")

    return {
        "filing_id": filing["id"],
        "holder_id": holder_id,
        "company_id": company_id,
        "trader_ids": trader_ids,
        "interest_role": interest_role,
        "move_kind": get_move_kind(filing, interest_role),
        "pct_before": before,
        "pct_after": after,
        "pct_change": get_percentage_change(before, after),
        "thresholds_crossed": get_thresholds_crossed(before, after),
        "substantial_status": get_substantial_status(filing, before, after),
        "deemed_routes": routes,
    }


def build_group_rows(
    group: list[dict],
    answer: dict | None,
    registry: EntityRegistry,
    company_id: str,
    company_holder_ids: set[str],
    form_definitions: dict[str, str],
) -> tuple[list[dict], list[dict]]:
    """
    The ownership rows of one filing and the links its boxes state.
    """
    entity_types = get_answer_types(answer)
    filing_text = get_filing_text(group)

    holder_ids = [
        registry.resolve(
            filing["holder_name"],
            entity_types.get(clean_name(filing["holder_name"])),
            company_holder_ids,
        )
        for filing in group
    ]

    links = []
    route_percentages = {}
    acting_party_id = None
    short_names = dict(form_definitions)

    if answer:
        known_names = [filing["holder_name"] for filing in group]
        known_names += [registry.get_name(holder_id) for holder_id in company_holder_ids]
        known_names += [
            filing["acting_party"]
            for filing in group
            if filing.get("acting_party")
        ]

        reader = StructureReader(
            registry,
            filing_text,
            company_id,
            known_names,
            entity_types,
            company_holder_ids,
            form_definitions,
        )
        links, route_percentages, acting_party_id = reader.read(answer)
        short_names.update(reader.short_names)

    links = drop_restated_links(links, company_id)
    direct_holder_ids = {
        link["parent"]
        for link in links
        if link["child"] == company_id and link["relation"] == "holds_directly"
    }
    direct_filer_ids = {
        holder_id
        for filing, holder_id in zip(group, holder_ids)
        if holder_id and get_interest_role(filing) in ("direct", "both")
    }
    # The model's type for a party can change from one read to the next, so a
    # filer already stored keeps its stored type
    filer_types = {
        get_entity_key(filing["holder_name"]): registry.get_type(holder_id)
        for filing, holder_id in zip(group, holder_ids)
        if holder_id
    }
    accepted_traders = get_accepted_traders(group, answer, filing_text, filer_types, form_definitions)
    named_self_records = get_named_self_records(group, answer, form_definitions)

    rows = []

    for index, filing in enumerate(group):
        holder_id = holder_ids[index]

        if holder_id is None:
            LOGGER.warning(
                "[Filings ownership] No entity for holder '%s', filing %s skipped",
                filing["holder_name"],
                filing["id"],
            )
            continue

        interest_role = get_interest_role(filing)
        record_acting_party_id = None

        if filing.get("acting_party"):
            record_acting_party_id = get_record_acting_party_id(
                filing, group, answer, acting_party_id, registry, entity_types
            )

        trader_ids = get_trader_ids(filing, holder_id, interest_role, record_acting_party_id)
        routes = []

        if interest_role != "direct":
            targets = (direct_filer_ids | direct_holder_ids | set(trader_ids)) - {holder_id}
            routes = build_routes(
                holder_id,
                company_id,
                links,
                route_percentages,
                targets,
                set(trader_ids),
                get_interest_percentage(filing, "after", "total"),
            )

        for route in routes:
            route["steps"] = build_route_steps(route["chain"], links)

        if needs_trader(filing):
            trader_ids = get_unstated_trader_ids(
                filing,
                holder_id,
                routes,
                direct_filer_ids,
                accepted_traders[index],
                short_names,
                registry,
                entity_types,
                index in named_self_records,
            )

        rows.append(build_ownership_row(filing, holder_id, company_id, trader_ids, interest_role, routes))

    fill_shared_traders(group, rows, {group[index]["id"] for index in named_self_records})

    return rows, links


def get_company_holder_names(holder_ids: set[str], registry: EntityRegistry) -> list[str]:
    return sorted(registry.get_name(holder_id) for holder_id in holder_ids)


def get_review_items(new_ids: set[str], registry: EntityRegistry, source: str) -> list[dict]:
    # A new entity named only by a short code ("DBSH") is usually a party the
    # form never spells out; it is listed for a person to check
    return [
        {"entity_id": entity_id, "name": registry.get_name(entity_id), "source": source}
        for entity_id in sorted(new_ids)
        if ABBREVIATION.fullmatch(registry.get_name(entity_id))
    ]


def get_changed_rows(
    new_rows: list[dict],
    stored_rows: list[dict],
    key: str,
) -> list[dict]:
    # A view row is saved only when it differs from the stored one
    stored = {row[key]: row for row in stored_rows}
    changed = []

    for row in new_rows:
        old_row = stored.get(row[key])
        old_values = {column: old_row.get(column) for column in row} if old_row else None

        old_text = json.dumps(old_values, sort_keys=True, default=str)
        new_text = json.dumps(row, sort_keys=True, default=str)

        if old_text != new_text:
            changed.append({"before": old_row, "after": row})

    return changed


def get_company_ids(
    groups: dict[str, list[dict]],
    registry: EntityRegistry,
    company_names: dict[str, str],
) -> dict[str, str]:
    company_ids = {}

    for source, group in groups.items():
        symbol = group[0]["symbol"]
        company_ids[source] = registry.get_company_id(symbol, company_names.get(symbol))

    return company_ids


def read_new_filings(
    groups: dict[str, list[dict]],
    registry: EntityRegistry,
    company_ids: dict[str, str],
    company_holder_ids: dict[str, set[str]],
) -> dict:
    """
    Read the new filings that need the LLM.

    Step 1: keep the PDFs the parsed fields cannot settle on their own
    Step 2: find the short names each PDF's form defines ("DBSH")
    Step 3: build one model input per PDF
    Step 4: mark the PDFs that also need the trader read
    Step 5: send them all to the model
    """
    sources_to_read = [
        source
        for source, group in groups.items()
        if needs_reading(group, registry)
    ]
    LOGGER.info("[Filings ownership] %d PDFs need an LLM read", len(sources_to_read))

    all_definitions = read_all_form_definitions(sources_to_read)
    form_definitions = {
        source: get_used_definitions(all_definitions[source], get_filing_text(groups[source]))
        for source in sources_to_read
    }

    inputs = {}

    for source in sources_to_read:
        company_id = company_ids[source]
        holder_names = get_company_holder_names(company_holder_ids[company_id], registry)

        inputs[source] = build_filing_input(
            groups[source],
            registry.get_name(company_id),
            holder_names,
            form_definitions[source],
        )

    trader_sources = {
        source
        for source in sources_to_read
        if any(needs_trader(filing) for filing in groups[source])
    }
    LOGGER.info("[Filings ownership] %d PDFs need a trader read", len(trader_sources))

    answers = {}

    if inputs:
        answers = asyncio.run(read_filings(inputs, trader_sources))

    return {
        "form_definitions": form_definitions,
        "inputs": inputs,
        "trader_sources": trader_sources,
        "answers": answers,
    }


def build_new_rows(
    groups: dict[str, list[dict]],
    reads: dict,
    registry: EntityRegistry,
    company_ids: dict[str, str],
    company_holder_ids: dict[str, set[str]],
    link_rows: dict[tuple, dict],
) -> dict:
    """
    Turn each new filing PDF into ownership rows and links.

    Step 1: skip a PDF whose LLM read failed, so the next run reads it again
    Step 2: build the PDF's ownership rows and the links its boxes state
    Step 3: list new entities named only by a short code for review
    Step 4: merge the links into the stored ones
    """
    new_ownership_rows = []
    changed_link_keys = set()
    skipped_sources = []
    review = []

    for source, group in groups.items():
        if source in reads["inputs"] and source not in reads["answers"]:
            LOGGER.warning("[Filings ownership] LLM read failed, skipped: %s", source)

            skipped_sources.append(source)
            continue

        company_id = company_ids[source]
        known_ids = set(registry.rows)

        rows, links = build_group_rows(
            group,
            reads["answers"].get(source),
            registry,
            company_id,
            company_holder_ids[company_id],
            reads["form_definitions"].get(source, {}),
        )

        review += get_review_items(set(registry.rows) - known_ids, registry, source)
        new_ownership_rows += round_fractions(rows)
        changed_link_keys |= merge_filing_links(link_rows, links, group[0])

        for row in rows:
            company_holder_ids[company_id].add(row["holder_id"])

    return {
        "ownership_rows": new_ownership_rows,
        "changed_link_keys": changed_link_keys,
        "skipped_sources": skipped_sources,
        "review": review,
    }


def build_views(
    state: dict,
    new_ownership_rows: list[dict],
    registry: EntityRegistry,
    link_rows: dict[tuple, dict],
) -> tuple[list[dict], list[dict]]:
    # Views are worked out from every filing, so a new link also updates
    # companies and holders the new filings did not name
    filings_by_id = {filing["id"]: filing for filing in state["filings"]}
    joined_filings = [
        {**filings_by_id[row["filing_id"]], **row}
        for row in state["ownership_rows"] + new_ownership_rows
        if row["filing_id"] in filings_by_id
    ]

    entity_rows = list(registry.rows.values())
    all_link_rows = list(link_rows.values())

    company_views = build_company_views(
        joined_filings,
        all_link_rows,
        entity_rows,
        state["company_names"],
    )
    entity_views = build_entity_views(joined_filings, all_link_rows, entity_rows)

    return company_views, entity_views


def run_pipeline(state_dir: Path | None, limit: int | None) -> dict:
    """
    Build the before/after preview of every table the new filings change.

    Step 1: load the stored tables
    Step 2: group the filings without ownership rows by PDF
    Step 3: read the PDFs that need the LLM
    Step 4: build ownership rows and links from each PDF
    Step 5: rebuild the company and entity views
    Step 6: keep only the rows that changed
    """
    # Step 1
    state = load_state(state_dir)
    registry = EntityRegistry(state["entity_rows"])
    stored_entities = {row["id"]: row for row in state["entity_rows"]}
    link_rows = {get_link_key(row): row for row in state["link_rows"]}
    stored_links = copy.deepcopy(link_rows)
    company_holder_ids = get_company_holder_ids(state["ownership_rows"])

    # Step 2
    groups = group_new_filings(state["filings"], state["ownership_rows"], limit)
    new_filing_count = sum(map(len, groups.values()))
    LOGGER.info("[Filings ownership] %d new filings in %d PDFs", new_filing_count, len(groups))

    company_ids = get_company_ids(groups, registry, state["company_names"])

    # Step 3
    reads = read_new_filings(groups, registry, company_ids, company_holder_ids)

    # Step 4
    new = build_new_rows(groups, reads, registry, company_ids, company_holder_ids, link_rows)

    # Step 5
    company_views, entity_views = build_views(state, new["ownership_rows"], registry, link_rows)

    # Step 6
    changed_ids = set(registry.changed_ids)

    return {
        "summary": {
            "new_filings": new_filing_count,
            "llm_reads": len(reads["inputs"]),
            "trader_reads": len(reads["trader_sources"]),
            "skipped_sources": new["skipped_sources"],
            "review": new["review"],
            "new_entities": len(changed_ids - set(stored_entities)),
            "changed_entities": len(changed_ids & set(stored_entities)),
            "changed_links": len(new["changed_link_keys"]),
            "new_ownership_rows": len(new["ownership_rows"]),
        },
        ENTITIES_TABLE: [
            {"before": stored_entities.get(row["id"]), "after": row}
            for row in registry.get_changed_rows()
        ],
        LINKS_TABLE: [
            {"before": stored_links.get(key), "after": link_rows[key]}
            for key in sorted(new["changed_link_keys"])
        ],
        FILING_OWNERSHIP_TABLE: [
            {"before": None, "after": row}
            for row in new["ownership_rows"]
        ],
        COMPANY_VIEW_TABLE: get_changed_rows(company_views, state["company_views"], "symbol"),
        ENTITY_VIEW_TABLE: get_changed_rows(entity_views, state["entity_views"], "id"),
        # Kept so a reviewer can see what the model said for each filing
        "llm_answers": reads["answers"],
    }


def get_rows_to_save(preview: dict) -> dict[str, list[dict]]:
    return {
        table: [change["after"] for change in preview[table]]
        for table in (
            ENTITIES_TABLE,
            LINKS_TABLE,
            FILING_OWNERSHIP_TABLE,
            COMPANY_VIEW_TABLE,
            ENTITY_VIEW_TABLE,
        )
    }
