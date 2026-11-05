from .names import COMPANY_WORD, holding_key, person_core, register_name_key
from .table import ReportTable


def family_union(table: ReportTable, block: dict, tops: list[dict]) -> dict | None:
    # A family block is every member's own shares plus the largest member's deemed interest,
    # not one member's total (H30: Cheong Sim Eng's own 14.57% sits beside Pin Chuan's deemed)
    names = family_names(table, block)

    if len(names) < 2:
        return None

    member_tops = [table.top_of(tops, name) for name in names]

    if any(not top or not top["via"] for top in member_tops):
        return None

    base = report_base(table, names)

    if not base:
        return None

    return FamilyUnion(table, block, tops, names, member_tops, base).result()


def family_names(table: ReportTable, block: dict) -> list[str]:
    # The people in the block, judged by name too, since an entity type can be wrong
    # (Q5T's Ng Chee Tat Philip is typed a trust)
    people = []

    for name in block["names"]:
        entity_type = table.named(name)["type"]
        is_person = entity_type == "person"
        reads_as_person = entity_type in ("unknown", "trust") and not COMPANY_WORD.search(name)

        if is_person or reads_as_person:
            people.append(name)

    return people


def report_base(table: ReportTable, names: list[str]) -> float | None:
    # The report's issued total, from the first member whose count and percentage are both given
    for name in names:
        holder = table.holder(name)
        held = table.held_shares(name)

        if holder and holder.get("total_pct") and held:
            return held / (holder["total_pct"] / 100)

    return None


class FamilyUnion:
    def __init__(
        self,
        table: ReportTable,
        block: dict,
        tops: list[dict],
        names: list[str],
        member_tops: list[dict],
        base: float,
    ):
        self.table = table
        self.block = block
        self.tops = tops
        self.names = names
        self.member_tops = member_tops
        self.base = base

        # A company in the block only adds parts of its own chain that no person counts
        self.companies = [
            name
            for name in block["names"]
            if name not in names
        ]
        self.as_at = table.report.get("as_at")
        self.holders_by_id = {
            holder["holder_id"]: holder
            for holder in table.company["holders"]
        }
        self.derived = self.derived_parts()

    def result(self) -> dict | None:
        names = self.names

        # Count from the member with the largest deemed figure and from the one with the
        # largest total, and keep the larger: each can only miss shares, never double them
        by_deemed = max(range(len(names)), key=lambda each: self.deemed(names[each]))
        by_total = max(range(len(names)), key=lambda each: self.own(names[each]) + self.deemed(names[each]))
        union, _ = max(self.union_from(by_deemed), self.union_from(by_total))

        # Each member at its latest figure, since a placement after the report dilutes them all
        largest = max(
            self.member_figure(name, top)
            for name, top in zip(names, self.member_tops)
        )

        if union < largest - 0.0005 or union > 1:
            return None

        # Equal to the largest member, the block's own figure stands, unless that one is stale
        if abs(union - largest) <= 0.0005 and not any(
            self.newer_alike(name)
            for name in names
        ):
            return None

        used = [
            holder
            for name in names
            if (holder := self.filed(name))
        ]
        latest = max(used, key=lambda holder: holder["as_of"], default=None)

        if latest:
            company_id = self.table.company["entity_id"]
            source = self.table.index.latest_filing(latest["holder_id"], company_id)["source"]
        else:
            source = self.table.report.get("source_url")

        return {
            "share": union,
            "as_of": latest["as_of"] if latest else self.as_at,
            "source": source,
            "page": None if latest else self.table.report.get("page"),
            "from": "filing" if latest else "report",
            "_extra": 0,
        }

    def member_figure(self, name: str, top: dict) -> float:
        if self.newer_alike(name):
            return self.own(name) + self.deemed(name)

        return (self.filed(name) or {}).get("pct") or (top.get("total_pct") or 0) / 100

    # Member figures

    def filed(self, name: str) -> dict | None:
        # The member's filing, when it is newer than the report
        holder = self.holders_by_id.get(self.table.named(name)["id"])

        if holder and self.as_at and holder["as_of"] > self.as_at:
            return holder

        return None

    def moves(self, name: str) -> list[dict]:
        key = holding_key(self.table.named(name)["id"], self.table.company["entity_id"])

        return self.table.index.holding_moves.get(key, [])

    def newer_alike(self, name: str) -> dict | None:
        # A later filing by another member that starts from this member's last deemed count is
        # the same shares moving (5TP: Julie filed Messiah's 28,212,200, then Choo sold from it)
        now = self.filed(name)
        last = (self.moves(name) or [{}])[-1].get("deemed_after") if now else None

        if not last:
            return None

        newer = []

        for other in self.names:
            other_filing = self.filed(other)

            if other == name or not other_filing or other_filing["as_of"] <= now["as_of"]:
                continue

            starts_from_last = any(
                move["date"] >= now["as_of"]
                and move["deemed_before"]
                and abs(move["deemed_before"] - last) <= last * 0.001
                for move in self.moves(other)
            )

            if starts_from_last:
                newer.append(other_filing)

        return max(newer, key=lambda holder: holder["as_of"], default=None)

    def own(self, name: str) -> float:
        now = self.filed(name)

        if now:
            return now.get("direct_pct") or 0

        return ((self.table.holder(name) or {}).get("direct_shares") or 0) / self.base

    def deemed(self, name: str) -> float:
        now = self.newer_alike(name) or self.filed(name)

        if now:
            return now.get("deemed_pct") or 0

        return ((self.table.holder(name) or {}).get("deemed_shares") or 0) / self.base

    def deemed_change(self, name: str) -> float:
        # In shares, since a buyback moves every percentage while no holding changed
        report_deemed = (self.table.holder(name) or {}).get("deemed_shares") or 0

        return (self.table.latest_count(name, "deemed")[0] - report_deemed) / self.base

    def names_member(self, holder: str, member: str) -> bool:
        keys = {register_name_key(member), person_core(member)}

        return any(
            register_name_key(part["held_by"]) in keys or person_core(part["held_by"]) in keys
            for part in self.table.parts_of(holder)
        )

    def part_keys(self, name: str) -> set[str]:
        return {
            register_name_key(part["held_by"])
            for part in self.table.parts_of(name)
            if part["kind"] != "assigned_votes"
        }

    def in_deemed(self, name: str) -> float:
        # The parts inside a holder's deemed count, not the nominee accounts inside its own
        direct = (self.table.holder(name) or {}).get("direct_shares") or 0

        return sum(
            part["shares"]
            for part in self.table.parts_of(name)
            if part.get("shares")
            and part["kind"] != "assigned_votes"
            and not (part["kind"] in ("nominee", "joint_account") and part["shares"] <= direct)
        )

    # Counts the footnotes leave out

    def derived_parts(self) -> dict[tuple[str, str], float]:
        derived = {}
        self.derive_from_chain(derived)
        self.derive_from_other_member(derived)

        return derived

    def derive_from_chain(self, derived: dict) -> None:
        # A spouse's or child's count is what the deemed count leaves after the chain's companies
        # and the other parts (H30: Cheong Sim Eng's 53,659,778 less Goodyear and Corporate
        # Development leaves his wife's 1,060,244)
        for name, top in zip(self.names, self.member_tops):
            parts = [
                part
                for part in self.table.parts_of(name)
                if part["kind"] != "assigned_votes"
            ]
            missing = [
                part
                for part in parts
                if not part.get("shares") and not part.get("table_count")
            ]

            if len(missing) != 1 or missing[0]["kind"] not in ("spouse", "child"):
                continue

            known = sum(
                part.get("shares") or part.get("table_count") or 0
                for part in parts
                if part is not missing[0]
            )
            chain = sum(
                (self.table.holder(company) or {}).get("direct_shares") or 0
                for company in top["via"]
            )
            deemed_shares = (self.table.holder(name) or {}).get("deemed_shares") or 0
            rest = deemed_shares - chain - known

            if chain and 0 < rest < deemed_shares * 0.1:
                derived[(name, register_name_key(missing[0]["held_by"]))] = rest

    def derive_from_other_member(self, derived: dict) -> None:
        # A missing count is the gap to a member with the same sources less that one
        # (MV4: Bianca Cheo 726,416,618 less Michelle's 724,990,618 is Bianca's spouse's 1,426,000)
        for name in self.names:
            missing = [
                part
                for part in self.table.parts_of(name)
                if part["kind"] != "assigned_votes" and not part.get("shares")
            ]

            if len(missing) != 1 or (name, register_name_key(missing[0]["held_by"])) in derived:
                continue

            key = register_name_key(missing[0]["held_by"])

            for other in self.names:
                if other == name or self.part_keys(other) != self.part_keys(name) - {key}:
                    continue

                gap = (
                    ((self.table.holder(name) or {}).get("deemed_shares") or 0)
                    - ((self.table.holder(other) or {}).get("deemed_shares") or 0)
                )

                if gap > 0:
                    derived[(name, key)] = gap
                    break

    # One count of the block, led by one member

    def union_from(self, lead_index: int) -> tuple[float, str]:
        lead = self.names[lead_index]
        lead_top = self.member_tops[lead_index]
        unnamed = self.unnamed_share(lead, lead_top)
        now_base = self.current_base()

        union = self.own_now(lead, now_base) + self.deemed_now(lead, now_base)
        union = self.add_partners_own(union, lead, lead_top, unnamed, now_base)

        # A count another member already reports, as a joint account, is not added again
        counted = {
            part["shares"]
            for part in self.table.parts_of(lead)
            if part.get("shares")
        }
        counted |= {
            (self.table.holder(name) or {}).get("direct_shares")
            for name in self.names
        } - {None}

        union = self.add_member_parts(union, lead, counted, now_base)
        union = self.add_outside_parts(union, lead, lead_top, unnamed, counted)
        union = self.add_company_parts(union, lead, lead_top, counted, now_base)

        return union, lead

    def unnamed_share(self, lead: str, lead_top: dict) -> float:
        # What the lead's deemed count leaves unexplained, where a partner's own shares could
        # hide; every company on its chain and every part its footnote names is explained
        lead_deemed_shares = (self.table.holder(lead) or {}).get("deemed_shares") or 0

        explained = sum(
            (self.table.holder(company) or {}).get("direct_shares") or 0
            for company in lead_top["via"]
        )
        explained += sum(
            part["shares"]
            for part in self.table.parts_of(lead)
            if part.get("shares") and part["kind"] != "assigned_votes"
        )
        at_ends = sum(
            self.table.held_shares(end)
            for end in self.table.holders_of(lead_top["via"])
        )

        return max(lead_deemed_shares - max(explained, at_ends), 0) / self.base

    def current_base(self) -> float:
        # Report counts go over the latest issued total of the filings they join
        # (5UF's units grew from 1,851M to 2,063M)
        return max(
            (
                issued
                for name in self.names
                if self.filed(name) and (issued := self.table.latest_count(name, "deemed")[1])
            ),
            default=self.base,
        )

    def own_now(self, name: str, now_base: float) -> float:
        if self.filed(name):
            return self.own(name)

        return self.own(name) * self.base / now_base

    def deemed_now(self, name: str, now_base: float) -> float:
        # An unfiled deemed count is rebased too, unless it holds a member who filed since
        # (C41: Maria Yu's count holds her husband's 16.8M from before his sale)
        if self.filed(name) or self.newer_alike(name):
            return self.deemed(name)

        sources = set(self.table.through_of(name))

        if any(
            register_name_key(other) in sources and self.filed(other)
            for other in self.block["names"]
        ):
            return self.deemed(name)

        return self.deemed(name) * self.base / now_base

    def add_partners_own(
        self,
        union: float,
        lead: str,
        lead_top: dict,
        unnamed: float,
        now_base: float,
    ) -> float:
        lead_ends = {
            register_name_key(end)
            for end in self.table.holders_of(lead_top["via"])
        }
        lead_via = {register_name_key(company) for company in lead_top["via"]}

        for name, top in zip(self.names, self.member_tops):
            if name == lead or register_name_key(name) in self.table.through_of(lead):
                continue

            # A partner's own shares are added unless they fit inside what the lead's footnote
            # leaves unnamed (MV4: Dr Cheo's footnote lists his sources without his daughters)
            lists_without_partner = self.table.parts_of(lead) and not self.names_member(lead, name)

            if self.own(name) > unnamed or lists_without_partner:
                union += self.own_now(name, now_base)

            # A company on another member's chain only holds its own shares beside the lead's
            # (H18: Tan Teck Lin Holdings' 23.4M and Aditan's 20.5M)
            for company in top["via"]:
                company_key = register_name_key(company)
                on_lead_chain = (
                    company_key in lead_ends
                    or company_key in lead_via
                    or company_key in self.table.through_of(lead)
                )

                if on_lead_chain:
                    continue

                lead_ends.add(company_key)
                union += self.own_now(company, now_base)

        return union

    def add_member_parts(self, union: float, lead: str, counted: set, now_base: float) -> float:
        # The parts each member's footnote gives beside the shared companies, each once
        # (OYY: Lim Tow Huat's 27,819,742 in nominee accounts)
        for name in self.names:
            if name == lead:
                continue

            # Deemed in a brother's shares is deemed in his own, not his spouse's (OV8)
            through = register_name_key(name) in self.table.through_of(lead)
            # A member whose deemed figure moved unlike the lead's may have moved its parts
            moved = abs(self.deemed_change(name) - self.deemed_change(lead)) > 0.0005
            direct = (self.table.holder(name) or {}).get("direct_shares") or 0

            for part in self.table.parts_of(name):
                derived_shares = self.derived.get((name, register_name_key(part["held_by"])))
                part = {**part, "shares": part.get("shares") or derived_shares}

                if part["kind"] == "assigned_votes" or not part.get("shares") or part["shares"] in counted:
                    continue

                # Only a spouse's or child's holding stays apart when the brothers move shares
                if (moved or through) and part["kind"] not in ("spouse", "child"):
                    continue

                # A nominee or joint account inside the member's own shares is those shares
                if part["kind"] in ("nominee", "joint_account") and part["shares"] <= direct:
                    continue

                counted.add(part["shares"])
                union += part["shares"] / now_base

        return union

    def add_outside_parts(
        self,
        union: float,
        lead: str,
        lead_top: dict,
        unnamed: float,
        counted: set,
    ) -> float:
        # A partner deemed through its own companies outside the table holds that part beside
        # the shared ones (ADN: Neo Teck Pheng's 17,008,562 through Ararat and Magnificent)
        lead_chain = {register_name_key(company) for company in lead_top["via"]}
        lead_holding = [
            company
            for company in lead_top["via"]
            if (self.table.holder(company) or {}).get("direct_shares")
        ]
        lead_rest = (
            self.table.latest_count(lead, "deemed")[0]
            - sum(
                self.table.latest_count(company, "direct")[0]
                for company in lead_holding
            )
            - self.in_deemed(lead)
        )

        for name, top in zip(self.names, self.member_tops):
            if name == lead or register_name_key(name) in self.table.through_of(lead):
                continue

            outside = {
                register_name_key(company)
                for company in (self.table.holder(name) or {}).get("held_through_others") or []
            }
            # Not when another member names one of those companies too (H02's Kheng Leong)
            others = {
                register_name_key(company)
                for other in self.block["names"]
                if other != name
                for company in [
                    *((self.table.holder(other) or {}).get("held_through_others") or []),
                    *(part["held_by"] for part in self.table.parts_of(other)),
                ]
            }
            holding = [
                company
                for company in top["via"]
                if (self.table.holder(company) or {}).get("direct_shares")
            ]

            if not outside or outside & others or not holding:
                continue

            if any(register_name_key(company) not in lead_chain for company in holding):
                continue

            deemed_shares, issued = self.table.latest_count(name, "deemed")
            rest = (
                deemed_shares
                - sum(
                    self.table.latest_count(company, "direct")[0]
                    for company in holding
                )
                - self.in_deemed(name)
            )
            # Nor when the lead's own figure leaves something unexplained the rest could be (8C8U)
            adds_shares = (
                rest > max(unnamed * self.base, 0) + 1
                and abs(lead_rest) < rest * 0.01
                and rest not in counted
            )

            if adds_shares:
                counted.add(rest)
                union += rest / (issued or self.base)

        return union

    def add_company_parts(
        self,
        union: float,
        lead: str,
        lead_top: dict,
        counted: set,
        now_base: float,
    ) -> float:
        # A company member's chain can hold a part no person's does (8YZ: the trust company
        # holds Xinyangchuan's 150,000,000), unless the lead's own chain reaches it
        lead_reach = {
            register_name_key(name)
            for company in [lead, *lead_top["via"]]
            for name in [company, *((self.table.holder(company) or {}).get("held_through_others") or [])]
        }

        for company in self.companies:
            for member in [company, *((self.table.top_of(self.tops, company) or {}).get("via") or [])]:
                for part in self.table.parts_of(member):
                    if part["kind"] == "assigned_votes" or not part.get("shares"):
                        continue

                    if part["shares"] in counted:
                        continue

                    if register_name_key(part["held_by"]) in lead_reach:
                        continue

                    counted.add(part["shares"])
                    union += part["shares"] / now_base

        return union
