from .blocks import block_rows, build_blocks
from .ceased import mark_ceased
from .family import family_union
from .filings import apply_filings, merge_linked_rows, roll_up_chain_filings
from .names import NOMINEE, looks_like_person, person_core, register_name_key
from .table import REGISTER_MIN, ReportTable
from .tops import nested_layers, split_partial_holders


def resolve(table: ReportTable, ultimate: list[dict]) -> list[dict] | None:
    report_tops = sorted(
        [
            owner
            for owner in table.report.get("ultimate_owners") or []
            if (owner.get("total_pct") or 0) >= REGISTER_MIN * 100
        ],
        key=lambda owner: -owner["total_pct"],
    )

    if not report_tops:
        return None

    tops = nested_layers(table, split_partial_holders(table, report_tops))
    table.tops = tops
    blocks = build_blocks(table, tops)
    owners = block_rows(table, blocks, tops)

    for owner, block in zip(owners, blocks):
        union = family_union(table, block, tops)

        if union:
            owner.update(union)
            owner["_current"] = True

    apply_filings(table, owners, tops, ultimate)
    merge_linked_rows(table, owners, tops)
    roll_up_chain_filings(table, owners)
    mark_ceased(table, owners)

    for owner in owners:
        tidy_entities(table, owner)

    return sorted(owners, key=lambda owner: -owner["share"])


def tidy_entities(table: ReportTable, owner: dict) -> None:
    company_id = table.company["entity_id"]
    entities = owner["entities"]

    # A company below another owner of the same row belongs on that owner's chain
    # (C2PU: Parkway Pantai under IHH's owners)
    below = [
        entity
        for entity in entities
        if entity["type"] not in ("person", "state")
        and not looks_like_person(entity["name"], entity["type"])
        and any(
            other["id"] != entity["id"]
            and (
                table.index.has_ancestor(entity["id"], other["id"])
                or entity["id"] in entity_chain(table, other, company_id)
            )
            for other in entities
        )
    ]

    if below and len(below) < len(entities):
        below_ids = {entity["id"] for entity in below}
        owner["entities"] = [
            entity
            for entity in entities
            if entity not in below
        ]
        owner["via"] = [*below, *(
            each
            for each in owner["via"]
            if each["id"] not in below_ids
        )]

    # The whole chain goes in via, not only the companies holding in their own name (F03);
    # nominees hold for someone else, so they are no link in it
    shown = {entity["id"] for entity in owner["entities"] + owner["via"]}

    for entity in owner["entities"]:
        for name in (table.top_of(table.tops, entity["name"]) or {}).get("via") or []:
            company = table.named(name)

            if company["id"] in shown or looks_like_person(name, company["type"]) or NOMINEE.search(name):
                continue

            owner["via"].append(company)
            shown.add(company["id"])

        for node in entity_chain(table, entity, company_id):
            company = table.index.entity(node)
            is_link = (
                company["id"] not in shown
                and company["type"] not in ("person", "state")
                and node in table.index.entities
                and not NOMINEE.search(company["name"])
            )

            if is_link:
                owner["via"].append(company)
                shown.add(company["id"])

    # An unresolved name beside the resolved person it names is dropped
    # (MXNU: "Tan Sri Sir Dr Jeffrey Cheah Fook Ling KBE AO")
    resolved = {
        person_core(entity["name"])
        for entity in owner["entities"]
        if entity["type"] == "person"
    }
    owner["entities"] = [
        entity
        for entity in owner["entities"]
        if entity["type"] != "unknown" or person_core(entity["name"]) not in resolved
    ]


def entity_chain(table: ReportTable, entity: dict, company_id: str) -> list[str]:
    # The entity's chain in route order, so the page's via comes out the same on every run
    top = table.top_of(table.tops, entity["name"])

    if not top:
        top = next(
            (
                each
                for each in table.tops
                if register_name_key(each["name"]) == register_name_key(entity["name"])
            ),
            None,
        )

    route_nodes = [
        node
        for route in table.index.deemed_routes(entity["id"], company_id)
        for node in route["chain"]
    ]
    report_nodes = [table.named(name)["id"] for name in (top or {}).get("via") or []]

    return list(dict.fromkeys([*route_nodes, *report_nodes]))
