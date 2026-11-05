import re

from .names import GENERIC_FIRST, bare_symbol, holding_key, register_name_key
from .table import REGISTER_MIN, ReportTable


def apply_filings(
    table: ReportTable,
    owners: list[dict],
    tops: list[dict],
    ultimate: list[dict],
) -> None:
    FilingApplier(table, owners, tops, ultimate).apply()


class FilingApplier:
    # Brings each report row up to the filings made since the report's date
    def __init__(self, table: ReportTable, owners: list[dict], tops: list[dict], ultimate: list[dict]):
        self.table = table
        self.owners = owners
        self.tops = tops
        self.ultimate = ultimate
        self.as_at = table.report.get("as_at")

        self.in_report = {
            register_name_key(holder["name"])
            for holder in table.report["holders"]
        }
        self.in_report |= {
            register_name_key(name)
            for top in tops
            for name in [top["name"], *top["via"]]
        }

        self.block_shares = {
            id(owner): {
                shares
                for name in owner["_names"]
                for each in [name, *((table.top_of(tops, name) or {}).get("via") or [])]
                for shares in self.shares_of(each)
            }
            for owner in owners
        }
        # The counts a block's owners hold in their own name
        self.own_name_shares = {
            id(owner): {
                (table.holder(name) or {}).get("direct_shares")
                for name in owner["_names"]
            } - {None, 0}
            for owner in owners
        }
        self.ceased_since = {
            event["holder_id"]
            for event in table.index.ceased_filings
            if event["symbol"] == bare_symbol(table.company["symbol"])
            and self.as_at
            and event["date"] > self.as_at
        }
        self.updated: set[int] = {
            id(owner)
            for owner in owners
            if owner.get("_current")
        }

    def apply(self) -> None:
        # Newest first, so a block takes its latest member's filing (5UF: Koh Wee Seng's 76.19%)
        filed_since = sorted(
            [
                owner
                for owner in self.ultimate
                if owner["share"] >= REGISTER_MIN and self.as_at and owner["as_of"] > self.as_at
            ],
            key=lambda owner: owner["as_of"],
            reverse=True,
        )

        for filed in filed_since:
            self.apply_filing(filed)

        self.name_people_behind_rows()
        self.apply_lead_filings()

    def shares_of(self, name: str) -> list[float]:
        holder = self.table.holder(name) or {}
        direct = holder.get("direct_shares") or 0
        deemed = holder.get("deemed_shares") or 0

        return [
            shares
            for shares in (direct, deemed, direct + deemed)
            if shares > 0
        ]

    def row_with(self, key: str) -> dict | None:
        return next(
            (
                owner
                for owner in self.owners
                if any(
                    register_name_key(entity["name"]) == key
                    for entity in owner["entities"]
                )
            ),
            None,
        )

    def join(self, row: dict, entity: dict) -> None:
        # A person the filings group with a row joins it, unless the report lists them apart
        entity_key = register_name_key(entity["name"])

        if entity["type"] == "person" and entity_key not in self.in_report and not self.row_with(entity_key):
            row["entities"].append(entity)

    def apply_filing(self, filed: dict) -> None:
        lead = filed["entity"]
        key = register_name_key(lead["name"])

        # A family files under whichever member files, but the stake is the largest member's
        # (ADN: the group files as Hong Leong, its 48.21% is Ho Han Leong Calvin's)
        largest = filed["members"][0]["entity"] if filed["members"] else lead
        largest_key = register_name_key(largest["name"])
        same = self.row_with(largest_key) if largest_key != key else None

        if same:
            key = largest_key
            self.join(same, lead)
        else:
            same = self.row_with(key)

        if same and id(same) in self.updated:
            return

        # A deemed-only holder of exactly a block's shares holds that block (G92: Sinopec)
        same_shares = None
        deemed_only = all(not member["direct"] for member in filed["members"])

        if not same and filed["units"] and deemed_only:
            same_shares = next(
                (
                    owner
                    for owner in self.owners
                    if filed["units"] in self.block_shares.get(id(owner), set())
                ),
                None,
            )

        # Another member of the filing group can be the one the report names (5LY: Teo Kee Bock)
        if not same and not same_shares:
            for member in filed["members"][1:]:
                same = self.row_with(register_name_key(member["entity"]["name"]))

                if same:
                    key = register_name_key(member["entity"]["name"])
                    self.join(same, lead)
                    break

            if same and id(same) in self.updated:
                return

        # A count filed before a later sale links a filer to the report's holder of it (P9D)
        if not same and not same_shares and deemed_only:
            earlier = {
                amount
                for member in filed["members"]
                for date, amount in self.table.index.units_by_holding.get(
                    holding_key(member["entity"]["id"], self.table.company["entity_id"]), []
                )
                if self.as_at and date >= self.as_at
            }
            same_shares = next(
                (
                    owner
                    for owner in self.owners
                    if earlier & self.own_name_shares.get(id(owner), set())
                ),
                None,
            )

        if same_shares:
            self.take_block(same_shares, filed)
        elif same:
            self.update_row(same, filed, key)
        elif key not in self.in_report:
            self.owners.append({
                "entities": [filed["entity"]],
                "via": [],
                "owned_by": [],
                "ceased": False,
                "share": filed["share"],
                "as_of": filed["as_of"],
                "source": filed["source"],
                "page": None,
                "from": "filing",
                "_lead": key,
                "_extra": 0,
                "_names": [],
            })

    def take_block(self, row: dict, filed: dict) -> None:
        # The company holding the block ceased since, so the filer's row replaces it
        # (T82U: ESR ceased when Jardine bought the units)
        if any(entity["id"] in self.ceased_since for entity in row["via"]):
            row.update({
                "entities": [filed["entity"]],
                "via": [],
                "owned_by": [],
                "share": filed["share"],
                "as_of": filed["as_of"],
                "source": filed["source"],
                "page": None,
                "from": "filing",
            })
            self.updated.add(id(row))

            return

        if id(row) not in self.updated:
            row.update({
                "share": filed["share"],
                "as_of": filed["as_of"],
                "source": filed["source"],
                "page": None,
                "from": "filing",
            })
            self.updated.add(id(row))

        row["entities"].append(filed["entity"])

    def update_row(self, row: dict, filed: dict, key: str) -> None:
        # A member below the row's largest count files only its own part, so its change is
        # applied to the row's count (CC3: Ooredoo filed AMH's 965,845,290)
        lead = filed["entity"]
        member_count = self.table.held_shares(lead["name"]) if row["_lead"] != key else 0
        row_count = max(
            (
                self.table.held_shares(name)
                for name in row["_names"]
            ),
            default=0,
        )

        files_own_part = (
            filed["units"]
            and member_count
            and row_count > member_count * 1.0005
            and abs(filed["units"] - member_count) <= member_count * 0.05
        )

        if files_own_part:
            units = row_count + filed["units"] - member_count
            share = units * filed["share"] / filed["units"] + row["_extra"]
        else:
            share = filed["share"]

        # The lead's own filing gives only its total, so partners' own shares stay on top
        row.update({
            "share": share + (row["_extra"] if row["_lead"] == key else 0),
            "as_of": filed["as_of"],
            "source": filed["source"],
            "page": None,
            "from": "filing",
        })
        self.updated.add(id(row))

    def name_people_behind_rows(self) -> None:
        # A filing older than the report still names the person behind a row whose count it
        # matches exactly (P9D: James Fitzgerald's family trust); the report's figure stays
        for filed in self.ultimate:
            if not self.as_at or filed["as_of"] > self.as_at or not filed["units"]:
                continue

            if filed["entity"]["type"] != "person":
                continue

            if any(member["direct"] for member in filed["members"]):
                continue

            row = next(
                (
                    owner
                    for owner in self.owners
                    if filed["units"] in self.own_name_shares.get(id(owner), set())
                ),
                None,
            )

            if row:
                self.join(row, filed["entity"])

    def apply_lead_filings(self) -> None:
        # A report row's lead can have a newer filing its group never carried
        # (ADN: Hong Leong filed 32.81% in September)
        company_id = self.table.company["entity_id"]

        for owner in self.owners:
            if id(owner) in self.updated or owner["from"] != "report" or not owner["entities"]:
                continue

            lead = owner["entities"][0]
            holder = next(
                (
                    each
                    for each in self.table.company["holders"]
                    if each["holder_id"] == lead["id"]
                ),
                None,
            )

            if not holder or not self.as_at or holder["as_of"] <= self.as_at:
                continue

            if (holder.get("pct") or 0) < REGISTER_MIN:
                continue

            owner.update({
                "share": holder["pct"] + owner["_extra"],
                "as_of": holder["as_of"],
                "source": self.table.index.latest_filing(lead["id"], company_id)["source"],
                "page": None,
                "from": "filing",
            })


def merge_linked_rows(table: ReportTable, owners: list[dict], tops: list[dict]) -> None:
    # Rows the filings tie together are one owner (C6L: Tembusu is Temasek's subsidiary).
    # Only downward, the bigger row above the smaller one
    company_id = table.company["entity_id"]
    in_report = {register_name_key(holder["name"]) for holder in table.report["holders"]}
    in_report |= {
        register_name_key(name)
        for top in tops
        for name in [top["name"], *top["via"]]
    }

    merged = True

    while merged:
        merged = False

        for first in range(len(owners)):
            for second in range(first + 1, len(owners)):
                owner_a, owner_b = owners[first], owners[second]

                if owner_a["share"] >= owner_b["share"]:
                    larger, smaller = owner_a, owner_b
                else:
                    larger, smaller = owner_b, owner_a

                linked = any(
                    is_above(table, upper["id"], lower["id"], company_id)
                    for upper in larger["entities"]
                    for lower in smaller["entities"]
                )

                if not linked and not same_group(larger, smaller, in_report):
                    continue

                larger["entities"].extend(
                    entity
                    for entity in smaller["entities"]
                    if not any(each["id"] == entity["id"] for each in larger["entities"])
                )
                owners.remove(smaller)
                merged = True
                break

            if merged:
                break


def is_above(table: ReportTable, upper: str, lower: str, company_id: str) -> bool:
    if upper == lower:
        return False

    routes = [
        node
        for route in table.index.deemed_routes(upper, company_id)
        for node in route["chain"]
    ]

    return table.index.has_ancestor(lower, upper) or lower in routes


def group_word(name: str) -> str | None:
    # The first word that names a group (CJLU: "M&G" in M&G plc and M&G Investments)
    match = re.search(r"[a-z0-9&]+", name.lower())
    word = match.group(0) if match else ""

    if (len(word) >= 4 or "&" in word) and word not in GENERIC_FIRST:
        return word

    return None


def same_group(owner_a: dict, owner_b: dict, in_report: set[str]) -> bool:
    # Two companies new since the report under one group name, with no filing linking them
    if owner_a["from"] != "filing" or owner_b["from"] != "filing":
        return False

    if len(owner_a["entities"]) != 1 or len(owner_b["entities"]) != 1:
        return False

    for owner in (owner_a, owner_b):
        entity = owner["entities"][0]

        if entity["type"] == "person" or register_name_key(entity["name"]) in in_report:
            return False

    word = group_word(owner_a["entities"][0]["name"])

    return bool(word) and word == group_word(owner_b["entities"][0]["name"])


def roll_up_chain_filings(table: ReportTable, owners: list[dict]) -> None:
    # A company inside a row's chain can file when the owner at the top does not
    # (JYEU: the manager's 16.1M fee units sit inside Lendlease's deemed interest)
    company_id = table.company["entity_id"]
    holders = {
        key.split("|")[0]
        for key in table.index.holding_moves
        if key.endswith(f"|{company_id}")
    }

    for owner in owners:
        if owner["ceased"] or not owner.get("as_of"):
            continue

        anchors = chain_anchors(table, owner, holders, company_id)
        chain = {
            table.named(company)["id"]
            for name in owner.get("_names") or []
            for company in (table.top_of(table.tops, name) or {}).get("via") or []
        }
        row_units = table.row_units(owner)

        if not row_units:
            continue

        named = {entity["id"] for entity in owner["entities"]}
        moved = chain_moves(table, owner, holders, anchors, named, company_id)
        add_moved_filers(table, owners, owner, moved)

        start, start_date, counted = whole_row_start(
            table, owner, chain, holders, named, row_units, moved, company_id
        )
        change = 0.0

        for holder_id, since in moved.items():
            # The highest filer on each branch only, after its parent's last filing (K71U)
            parent_dates = [
                moved[other][-1]["date"]
                for other in moved
                if other != holder_id and table.index.has_ancestor(holder_id, other)
            ]
            since = [
                move
                for move in since
                if (not parent_dates or move["date"] > max(parent_dates))
                and (not start_date or move["date"] > start_date)
            ]

            if not since:
                continue

            change += since[-1]["after"] - since[0]["before"]
            counted += since

        if counted:
            update_from_moves(owner, counted, start, change, row_units)


def chain_anchors(table: ReportTable, owner: dict, holders: set[str], company_id: str) -> set[str]:
    anchors = {entity["id"] for entity in owner["entities"] + owner["via"]}

    # Companies the report says hold some of the row's units, with no link to the owner
    # (NTDU's and DCRU's managers, SEB's EBT manager)
    anchors |= {
        table.named(company)["id"]
        for name in owner.get("_names") or []
        for member in [name, *((table.top_of(table.tops, name) or {}).get("via") or [])]
        for company in (table.holder(member) or {}).get("held_through_others") or []
    }

    # A filer whose count before its change is exactly what the owner's deemed count leaves
    # unexplained holds that part (ODBU: the manager filed from UOB's other 1,366,292)
    for name in owner.get("_names") or []:
        top = table.top_of(table.tops, name) or {}
        at_ends = sum(
            table.held_shares(end)
            for end in table.holders_of(top.get("via") or [])
        )
        unexplained = ((table.holder(name) or {}).get("deemed_shares") or 0) - at_ends

        if unexplained < 1000:
            continue

        for holder_id in holders:
            first = next(
                (
                    move
                    for move in table.index.holding_moves[holding_key(holder_id, company_id)]
                    if move["date"] > owner["as_of"]
                ),
                None,
            )

            if first and first["before"] and abs(first["before"] - unexplained) <= 1:
                anchors.add(holder_id)

    return anchors


def chain_moves(
    table: ReportTable,
    owner: dict,
    holders: set[str],
    anchors: set[str],
    named: set[str],
    company_id: str,
) -> dict[str, list[dict]]:
    moved = {}

    for holder_id in holders:
        # Only companies down the chain; a named owner's or a person's filing is the row's figure
        if holder_id in named or table.index.entity(holder_id)["type"] == "person":
            continue

        under_anchor = any(
            table.index.has_ancestor(holder_id, anchor)
            for anchor in anchors
        )

        if holder_id not in anchors and not under_anchor:
            continue

        since = [
            move
            for move in table.index.holding_moves[holding_key(holder_id, company_id)]
            if move["date"] > owner["as_of"]
        ]

        if since and since[0]["before"] is not None and since[-1]["after"] is not None:
            moved[holder_id] = since

    return moved


def add_moved_filers(table: ReportTable, owners: list[dict], owner: dict, moved: dict) -> None:
    # A filer holding part of the row's units is on its chain (ODBU's manager), while the row's
    # owners are still the report's, and unless it has since sold out (Y92's Siriwanan)
    listed = {
        entity["id"]
        for each in owners
        for entity in each["entities"] + each["via"]
    }
    own = {register_name_key(entity["name"]) for entity in owner["entities"]}

    if any(register_name_key(name) in own for name in owner.get("_names") or []):
        owner["via"] += [
            table.index.entity(holder_id)
            for holder_id in sorted(moved)
            if holder_id not in listed and moved[holder_id][-1]["after"]
        ]


def whole_row_start(
    table: ReportTable,
    owner: dict,
    chain: set[str],
    holders: set[str],
    named: set[str],
    row_units: float,
    moved: dict,
    company_id: str,
) -> tuple[float, str | None, list[dict]]:
    # A chain company the report gives the row's whole count files the row's total itself, so
    # its latest filing is the new start (BUOU: FPIH filed in September for all of Charoen's units)
    whole = [
        holder_id
        for holder_id in chain & holders
        if holder_id not in named
        and abs(table.held_shares(table.index.entity(holder_id)["name"]) - row_units) <= row_units * 0.001
        and table.index.holding_moves[holding_key(holder_id, company_id)][-1]["date"] > owner["as_of"]
    ]

    if not whole:
        return row_units, None, []

    top = max(
        whole,
        key=lambda holder_id: (
            table.index.holding_moves[holding_key(holder_id, company_id)][-1]["date"],
            holder_id,
        ),
    )
    last = table.index.holding_moves[holding_key(top, company_id)][-1]

    if last["after"] is None:
        return row_units, None, []

    moved.pop(top, None)

    return last["after"], last["date"], [last]


def update_from_moves(
    owner: dict,
    counted: list[dict],
    start: float,
    change: float,
    row_units: float,
) -> None:
    latest = max(counted, key=lambda move: (move["date"], move["id"]))

    # The issued total from the latest filing of a holding of 1% or more, since a manager's
    # small stake is rounded too coarsely (M44U); without one the row's own total stands
    sized = [
        move
        for move in counted
        if move["issued"] and move["after"] and move["after"] / move["issued"] >= 0.01
    ]

    if sized:
        issued = max(sized, key=lambda move: (move["date"], move["after"]))["issued"]
    else:
        issued = row_units / owner["share"]

    share = (start + change) / issued

    # A small difference with no units moved is rounding; units that did move are the row's
    # latest word, however small (SEB: the managers' changes net to -0.02 points)
    moved_units = abs(start + change - row_units) >= 1
    too_small = abs(share - owner["share"]) < 0.0005 and not moved_units

    if too_small or not 0 < share <= 1 or abs(share - owner["share"]) > 0.05:
        return

    owner.update({
        "share": share,
        "as_of": latest["date"],
        "source": latest["source"],
        "page": None,
        "from": "filing",
    })
