import re

from .index import OwnershipIndex
from .names import holding_key, register_name_key


# The most companies a gap the report's table skips is filled across
GAP_HOPS = 4

# The share of a row's stake the direct holdings must still make up once a holder reads as sold out
SOLD_OUT_KEEPS = 0.9

# The links that carry a holding down a chain; relatives and agreements sit beside it
CHAIN_LINKS = ["owns", "controls", "deemed_via", "nominee", "beneficiary", "trustee", "fund_manager"]
OWNING_LINKS = {"owns", "controls"}

# "the 14,120,000 shares held by Meng Tak", or without the noun, "8,113,776 held by ..."
SHARES_HELD_BY = re.compile(
    r"(\d{1,3}(?:,\d{3})+|\d{4,})\s+"
    r"(?:(?:ordinary\s+)?(?:shares|units|stapled securities)\b[^.]*?\s)?"
    r"held (?:by|through)\b",
    re.IGNORECASE,
)

# "More than 20%" is stored as 0.2, so the filing's own words keep it a floor
LOWER_BOUND = re.compile(
    r"\b(more than|at least|not less than|over|in excess of|exceeding)\s+\d",
    re.IGNORECASE,
)

NO_STAKE = {
    "direct": None,
    "deemed": None,
    "direct_units": None,
    "deemed_units": None,
    "as_of": None,
    "source": None,
    "from": None,
}


def chain_of(index: OwnershipIndex, company: dict, report: dict | None, owner: dict) -> list[dict]:
    return Chain(index, company, report, owner).steps()


def link_label(link: dict) -> dict:
    label = {"relation": link["relation"], "percentage": link.get("percentage")}

    if link.get("percentage") is None:
        return label

    evidence = link.get("percentage_evidence")

    if evidence is None:
        evidence = link.get("evidence") or ""

    match = LOWER_BOUND.search(evidence)

    if match:
        label["percentage_floor"] = match.group(1).lower()

    return label


def latest_quote(link: dict) -> dict | None:
    if not link.get("evidence"):
        return None

    sources = link.get("sources") or []

    return {
        "text": link["evidence"],
        "source": sources[-1] if sources else None,
        "date": link.get("last_seen"),
    }


def plain_link(relation: str, target: dict) -> dict:
    return {"label": {"relation": relation, "percentage": None}, "to": target, "quote": None}


def group_key_part(value) -> str:
    # Steps with the same stake share one line; 0 and 0.0 are the same stake
    if value is None:
        return ""

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value)


def table_pct(row: dict, kind: str) -> float | None:
    # A table that prints counts and only a total percentage gives a part's share as its
    # count over the issued total (C09)
    printed = row.get(f"{kind}_pct")

    if printed:
        return printed / 100

    count = row.get(f"{kind}_shares")
    held = (row.get("direct_shares") or 0) + (row.get("deemed_shares") or 0)

    if count and row.get("total_pct") and held:
        return (count / held) * (row["total_pct"] / 100)

    return None


class Chain:
    # A report row's owners and the companies they hold through, each with its links and its
    # own holding of the company. Holders with the same links and stake share one step
    def __init__(
        self, 
        index: OwnershipIndex, 
        company: dict, 
        report: dict | None, 
        owner: dict
    ):
        self.index = index
        self.company = company
        self.report = report or {}
        self.share = owner["share"]

        entities = [self.ref(entity) for entity in owner["entities"]]
        via = [self.ref(entity) for entity in owner["via"]]
        self.row_members = unique([*entities, *via])
        self.owner_ids = list(dict.fromkeys(entity["id"] for entity in entities))

        # The names a footnote puts behind a company head the chain (D05: Minister for Finance)
        self.behind = [
            {
                "company": self.ref(each["company"]),
                "owners": [self.ref(entity) for entity in each["owners"]],
            }
            for each in owner["owned_by"]
        ]
        footnote_owners = [entity for entry in self.behind for entity in entry["owners"]]
        self.members = unique([*self.row_members, *footnote_owners])
        self.by_id = {member["id"]: member for member in self.members}

        self.links_of: dict[str, list[dict]] = {}
        self.parents_of: dict[str, list[str]] = {}
        self.keeps_sold_out = False

    def steps(self) -> list[dict]:
        self.fill_gaps()
        self.fill_table_steps()
        self.draw_links()
        self.link_footnote_owners()
        self.settle_deemed_via()
        self.keeps_sold_out = self.direct_left() >= self.share * SOLD_OUT_KEEPS
        self.hang_from_lead()

        return self.grouped_steps(self.path_to_units())

    # Lookups
    def ref(self, entity: dict) -> dict:
        # A name only the report gives has no entity row and keeps the file's own ref
        known = self.index.entity(entity["id"])

        return entity if known["name"] == entity["id"] else known

    def pick(self, items: list[dict], name_of, name: str) -> dict | None:
        # The exact name first, then the name key only when one item has it
        # (NTDU: "NTT, Inc." and "NTT Limited" share a key but are two holders)
        exact = next(
            (
                item
                for item in items
                if name_of(item) == name
            ),
            None,
        )

        if exact is not None:
            return exact

        keyed = [
            item
            for item in items
            if register_name_key(name_of(item)) == register_name_key(name)
        ]

        return keyed[0] if len(keyed) == 1 else None

    def member_named(self, name: str) -> dict | None:
        return self.pick(self.members, lambda member: member["name"], name)

    def table_row(self, entity: dict) -> dict | None:
        return self.pick(self.report.get("holders") or [], lambda holder: holder["name"], entity["name"])

    def deemed_through(self, member: dict) -> list[dict]:
        names = (self.table_row(member) or {}).get("deemed_through") or []
        targets = [self.member_named(name) for name in names]

        return [
            target
            for target in targets
            if target and target["id"] != member["id"]
        ]

    def add_member(self, entity: dict) -> None:
        self.members.append(entity)
        self.by_id[entity["id"]] = entity

    def add_link(self, parent_id: str, link: dict) -> None:
        self.links_of.setdefault(parent_id, []).append(link)
        self.parents_of.setdefault(link["to"]["id"], []).append(parent_id)

    # Members
    def fill_gaps(self) -> None:
        # The companies between a holder and the one its table row is deemed through, by the
        # shortest owning path (9CI: TJ Holdings (III) between Glenville and CapitaLand Group)
        owning: dict[str, list[str]] = {}

        for link in self.index.links:
            if link["relation"] in OWNING_LINKS and link["parent_id"] != link["child_id"]:
                owning.setdefault(link["parent_id"], []).append(link["child_id"])

        for member in list(self.members):
            if any(child in self.by_id for child in owning.get(member["id"], [])):
                continue

            # Each name is looked up after the companies the one before brought in
            for name in (self.table_row(member) or {}).get("deemed_through") or []:
                target = self.member_named(name)

                if not target or target["id"] == member["id"]:
                    continue

                for entity_id in shortest_path(owning, member["id"], target["id"]) or []:
                    if entity_id not in self.by_id:
                        self.add_member(self.index.entity(entity_id))

    def fill_table_steps(self) -> None:
        # A table row the owners file leaves out joins between two members, below a member the
        # filings link to nobody on the row (SEB: SWI Capital -> ICG -> Icona Swiss Holding)
        for member in list(self.members):
            linked = any(
                link["parent_id"] == member["id"]
                and link["child_id"] in self.by_id
                and link["relation"] in CHAIN_LINKS
                for link in self.index.links
            )

            if linked:
                continue

            for name in (self.table_row(member) or {}).get("deemed_through") or []:
                if self.member_named(name):
                    continue

                row = self.pick(self.report.get("holders") or [], lambda holder: holder["name"], name)
                leads_back = row and any(
                    self.member_named(each)
                    for each in row.get("deemed_through") or []
                )

                if not leads_back:
                    continue

                entity_id = self.index.entity_by_register_name.get(register_name_key(row["name"]))

                if entity_id:
                    entity = self.index.entity(entity_id)
                else:
                    entity = {"id": row["name"], "name": row["name"], "type": "unknown", "symbol": None}

                if entity["id"] not in self.by_id:
                    self.add_member(entity)

    # Links
    def draw_links(self) -> None:
        # One link per pair: the first relation in CHAIN_LINKS, then the most filed of it
        best: dict[str, dict] = {}

        for link in self.index.links:
            on_row = link["parent_id"] in self.by_id and link["child_id"] in self.by_id

            if link["relation"] not in CHAIN_LINKS or link["parent_id"] == link["child_id"] or not on_row:
                continue

            key = holding_key(link["parent_id"], link["child_id"])
            kept = best.get(key)

            if kept is None or link_rank(link) < link_rank(kept):
                best[key] = link
            elif link_rank(link) == link_rank(kept) and kept["filing_count"] < link["filing_count"]:
                best[key] = link

        for link in best.values():
            self.add_link(link["parent_id"], {
                "label": link_label(link),
                "to": self.by_id[link["child_id"]],
                "quote": latest_quote(link),
            })

    def link_footnote_owners(self) -> None:
        # A footnote gives no stake or wording beyond who stands above the company
        for entry in self.behind:
            target = entry["company"]

            for entity in entry["owners"]:
                linked = any(
                    link["to"]["id"] == target["id"]
                    for link in self.links_of.get(entity["id"], [])
                )

                if not linked and entity["id"] != target["id"]:
                    self.add_link(entity["id"], plain_link("owns", target))

    def drawn_reach(self, start: str, end: str, seen: frozenset = frozenset()) -> bool:
        # Down the links other than "interest through", from one holder to another
        for link in self.links_of.get(start, []):
            target_id = link["to"]["id"]

            if link["label"]["relation"] == "deemed_via" or target_id in seen:
                continue

            if target_id == end or self.drawn_reach(target_id, end, seen | {target_id}):
                return True

        return False

    def settle_deemed_via(self) -> None:
        for member in self.members:
            links = self.links_of.get(member["id"], [])

            # "Interest through" a company the other links already reach is dropped; one they do
            # not reach stays (M44U: Temasek is deemed in its manager's units)
            summed = [
                link
                for link in links
                if link["label"]["relation"] == "deemed_via"
                and self.drawn_reach(member["id"], link["to"]["id"])
            ]

            if summed:
                self.links_of[member["id"]] = [
                    link
                    for link in links
                    if not any(link is each for each in summed)
                ]

                for link in summed:
                    target_id = link["to"]["id"]
                    self.parents_of[target_id] = [
                        parent
                        for parent in self.parents_of.get(target_id, [])
                        if parent != member["id"]
                    ]

            if links:
                continue

            # A holder the filings never link takes the table's "deemed through" (CHZ: Aviel Sim)
            for target in self.deemed_through(member):
                self.add_link(member["id"], plain_link("deemed_via", target))

    def hang_from_lead(self) -> None:
        # A row company that holds units, or holds through one that does, hangs from the row's
        # lead when no filing links them (ODBU's manager, ADN's City Developments)
        lead = self.owner_ids[0] if self.owner_ids else None

        for member in self.row_members:
            if not lead or member["id"] in self.owner_ids or self.parents_of.get(member["id"]):
                continue

            if not self.stake_of(member)["direct"] and not self.links_of.get(member["id"]):
                continue

            self.links_of.setdefault(lead, []).append(plain_link("deemed_via", member))
            self.parents_of[member["id"]] = [lead]

    # Stakes
    def stake_of(self, member: dict, sold_out: bool | None = None) -> dict:
        sold_out = self.keeps_sold_out if sold_out is None else sold_out
        as_at = self.report.get("as_at")
        filed = next(
            (
                holder
                for holder in self.company.get("holders") or []
                if holder["holder_id"] == member["id"]
            ),
            None,
        )

        if filed and (not as_at or filed["as_of"] >= as_at):
            latest = self.index.latest_filing(member["id"], self.company["entity_id"])

            return {
                "direct": filed["direct_pct"] if filed.get("direct_pct") is not None else 0,
                "deemed": filed["deemed_pct"] if filed.get("deemed_pct") is not None else 0,
                "direct_units": latest["direct_units"],
                "deemed_units": latest["deemed_units"],
                "as_of": filed["as_of"],
                "source": latest["source"],
                "from": "filing",
            }

        # A company at the chain's end that sold out reads its last filing, or the report's
        # older count would stand (F99: TCC Assets passed its shares to Stellar in August)
        last = self.index.latest_filing(member["id"], self.company["entity_id"])
        holds_on = bool(self.links_of.get(member["id"]))
        sold_out_since = (
            sold_out
            and not filed
            and not holds_on
            and last["date"]
            and last["share"] is not None
            and last["share"] == 0
            and (not as_at or last["date"] >= as_at)
        )

        if sold_out_since:
            return {
                "direct": 0,
                "deemed": 0,
                "direct_units": 0,
                "deemed_units": 0,
                "as_of": last["date"],
                "source": last["source"],
                "from": "filing",
            }

        row = self.table_row(member)

        if not row:
            return self.counted_holding(member) or dict(NO_STAKE)

        # The table's dash is none, not unknown
        direct = table_pct(row, "direct")
        deemed = table_pct(row, "deemed")

        return {
            "direct": direct if direct is not None else 0,
            "deemed": deemed if deemed is not None else 0,
            "direct_units": row.get("direct_shares"),
            "deemed_units": row.get("deemed_shares"),
            "as_of": as_at,
            "source": self.report.get("source_url"),
            "from": "report",
        }

    def counted_holding(self, member: dict) -> dict | None:
        # A company with no filing or table row holds the shares an owner's filing counts for it,
        # over the issued total that filing gives (E28: Vimala's 14,120,000 in Meng Tak)
        member_ids = {each["id"] for each in self.members}
        counted = []

        for link in self.index.links:
            if link["relation"] != "deemed_via" or link["child_id"] != member["id"]:
                continue

            if link["parent_id"] not in member_ids:
                continue

            match = SHARES_HELD_BY.search(link.get("evidence") or "")
            filing = self.index.latest_filing(link["parent_id"], self.company["entity_id"])

            if match and filing["units"] and filing["share"]:
                counted.append((link, match, filing))

        if not counted:
            return None

        # Several owners can count the same shares; the newest filing has the current issued
        # total (H30: Cheong Sim Eng's October filing after the share count fell)
        link, match, filing = max(counted, key=lambda each: each[2]["date"] or "")
        units = float(match.group(1).replace(",", ""))
        sources = link.get("sources") or []
        issued = filing["units"] / filing["share"]

        return {
            "direct": round(units / issued, 6),
            "deemed": 0,
            "direct_units": units,
            "deemed_units": 0,
            "as_of": link.get("last_seen"),
            "source": sources[-1] if sources else None,
            "from": "filing",
        }

    def direct_left(self) -> float:
        # The direct holdings left must still make up most of the row's stake, or the sale is
        # the record's error (F99 keeps 87.31% of 87.36%; Y92 would keep 1.2% of 67.9%)
        total = 0

        for member in self.members:
            direct = self.stake_of(member, sold_out=True)["direct"]
            total += direct if direct is not None else 0

        return total

    # Steps

    def path_to_units(self) -> set[str]:
        # Only the steps that lead down to the units; the row's own owners always stay
        # (K71U: Temasek also controls DBS, which holds none of Keppel REIT)
        on_path: set[str] = set()

        for member in self.members:
            stake = self.stake_of(member)

            # A bottom company filing only a deemed stake holds through nominees (5JK: Hiap Hoe Holdings)
            bottom = not self.links_of.get(member["id"])

            if stake["direct"] or (bottom and stake["deemed"]):
                self.climb(member["id"], on_path)

        return on_path

    def climb(self, entity_id: str, on_path: set[str]) -> None:
        if entity_id in on_path:
            return

        on_path.add(entity_id)

        for parent in self.parents_of.get(entity_id, []):
            self.climb(parent, on_path)

    def depth(self, entity_id: str, seen: frozenset = frozenset()) -> int:
        if entity_id in seen:
            return 0

        parents = self.parents_of.get(entity_id, [])

        if not parents:
            return 0

        return 1 + max(self.depth(parent, seen | {entity_id}) for parent in parents)

    def grouped_steps(self, on_path: set[str]) -> list[dict]:
        steps: dict[str, dict] = {}

        for member in self.members:
            if on_path and member["id"] not in on_path and member["id"] not in self.owner_ids:
                continue

            stake = self.stake_of(member)
            links = [
                link
                for link in self.links_of.get(member["id"], [])
                if not on_path or link["to"]["id"] in on_path
            ]

            # A company no link reaches and with no holding adds an empty line
            isolated = not links and member["id"] not in self.parents_of

            if isolated and not stake["direct"] and not stake["deemed"]:
                continue

            level = self.depth(member["id"])
            key = "|".join([
                str(level),
                *sorted(
                    f"{link['label']['relation']}>{link['to']['id']}"
                    for link in links
                ),
                group_key_part(stake["direct"]),
                group_key_part(stake["deemed"]),
                group_key_part(stake["as_of"]),
            ])

            if key in steps:
                steps[key]["step"]["holders"].append(member)
            else:
                steps[key] = {"depth": level, "step": {"holders": [member], "links": links, **stake}}

        ordered = sorted(steps.values(), key=lambda entry: (entry["depth"], -held(entry["step"])))

        return [entry["step"] for entry in ordered]


def unique(entities) -> list[dict]:
    kept = []

    for entity in entities:
        if all(each["id"] != entity["id"] for each in kept):
            kept.append(entity)

    return kept


def link_rank(link: dict) -> int:
    return CHAIN_LINKS.index(link["relation"])


def shortest_path(owning: dict[str, list[str]], start: str, end: str) -> list[str] | None:
    # The companies between start and end, not counting either
    paths = [[start]]

    for _ in range(GAP_HOPS):
        if not paths:
            break

        next_paths = []

        for path in paths:
            for child in owning.get(path[-1], []):
                if child == end:
                    return path[1:]

                if child not in path:
                    next_paths.append([*path, child])

        paths = next_paths

    return None


def held(step: dict) -> float:
    return (step["direct"] or 0) + (step["deemed"] or 0)
