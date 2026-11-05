from datetime import datetime

import re

from .index import OwnershipIndex
from .names import holding_key, register_name_key


# A substantial holding; smaller register names are mostly retail accounts
REGISTER_MIN = 0.05

# Days before the report's date a trade can still be missing from its table
LATE_FILING_DAYS = 30

LEGAL_WORDS = re.compile(r"\b(co|ltd|limited|pte|inc|corp|plc|the|and|of)\b\.?", re.IGNORECASE)


class ReportTable:
    # One company's report table, corrected for late filings, with the lookups every step reads
    def __init__(self, index: OwnershipIndex, company: dict, report: dict):
        self.index = index
        self.company = company
        self.filers = [index.entity(holder["holder_id"]) for holder in company["holders"]]
        self.late = {}
        self.report = self.after_late_filings(report)

        self.holders_by_key = {}

        for holder in self.report["holders"]:
            self.holders_by_key.setdefault(register_name_key(holder["name"]), holder)

        self.deemed_through = {
            register_name_key(holder["name"]): holder.get("deemed_through")
            for holder in self.report["holders"]
        }
        # The report's owners once split and nested, set by resolve
        self.tops: list[dict] = []

    def after_late_filings(self, report: dict) -> dict:
        # A trade just before the report's date can be filed after the register was read, so a
        # holder still at its pre-trade count takes the count after it (PCT: RAYS's 19,926,000)
        if not report.get("as_at"):
            return report

        holders = []
        totals = {}

        for holder in report["holders"]:
            last = self.late_move(holder, report["as_at"])

            if last:
                deemed = last["deemed_after"] or 0
                holder = {
                    **holder,
                    "direct_shares": last["after"] - deemed,
                    "deemed_shares": deemed,
                    "total_pct": last["after"] / last["issued"] * 100,
                }
                totals[register_name_key(holder["name"])] = holder["total_pct"]
                self.late[register_name_key(holder["name"])] = last

            holders.append(holder)

        if not totals:
            return report

        return {
            **report,
            "holders": holders,
            "ultimate_owners": [
                {
                    **owner,
                    "total_pct": totals.get(register_name_key(owner["name"]), owner.get("total_pct")),
                }
                for owner in report.get("ultimate_owners") or []
            ],
        }

    def late_move(self, holder: dict, as_at: str) -> dict | None:
        count = (holder.get("direct_shares") or 0) + (holder.get("deemed_shares") or 0)
        entity_id = self.named(holder["name"])["id"]
        moves = self.index.holding_moves.get(holding_key(entity_id, self.company["entity_id"])) or []
        last = moves[-1] if moves else None

        if not count or not last or not last["before"]:
            return None

        if last["after"] is None or last["after"] == last["before"]:
            return None

        days = (datetime.fromisoformat(as_at) - datetime.fromisoformat(last["date"])).days

        if 0 <= days <= LATE_FILING_DAYS and abs(last["before"] - count) <= 1 and last["issued"]:
            return last

        return None

    def holder(self, name: str) -> dict | None:
        return self.holders_by_key.get(register_name_key(name))

    def held_shares(self, name: str) -> float:
        holder = self.holder(name) or {}

        return (holder.get("direct_shares") or 0) + (holder.get("deemed_shares") or 0)

    def named(self, name: str) -> dict:
        key = register_name_key(name)
        entity_id = self.index.entity_by_register_name.get(key)

        if entity_id:
            return self.index.entity(entity_id)

        # A report name ending with one filer's name is that filer
        # (BDX: "Dr Sam Goi Seng Hui" is the filer "Goi Seng Hui")
        filers = [
            entity
            for entity in self.filers
            if entity["type"] == "person"
            and len(register_name_key(entity["name"])) >= 8
            and key.endswith(register_name_key(entity["name"]))
        ]

        if len(filers) == 1:
            return filers[0]

        # A report name in capitals can be a filer's initials (T14: "TPH")
        short = name.strip()

        if re.fullmatch(r"[A-Z]{2,6}", short):
            matches = [
                entity
                for entity in self.filers
                if entity["type"] != "person" and initials(entity["name"]) == short
            ]

            if len(matches) == 1:
                return matches[0]

        return {"id": name, "name": name, "type": "unknown", "symbol": None}

    def through_of(self, name: str) -> list[str]:
        return [
            register_name_key(each)
            for each in (self.deemed_through.get(register_name_key(name)) or [])
        ]

    def holders_of(self, via: list[str]) -> list[str]:
        # The companies a chain ends at; family members deemed through each other have no such
        # end, so the chain itself counts
        ends = [
            name
            for name in via
            if not self.deemed_through.get(register_name_key(name))
        ]

        return ends or via

    def latest_count(self, name: str, kind: str) -> tuple[float, float | None]:
        # A holder's count at its latest filing after the report, else the report's, with the
        # issued total that filing gives
        holder = self.holder(name) or {}

        if kind == "deemed":
            reported = holder.get("deemed_shares") or 0
        else:
            reported = holder.get("direct_shares") or 0

        key = holding_key(self.named(name)["id"], self.company["entity_id"])
        moves = self.index.holding_moves.get(key) or []
        last = moves[-1] if moves else None
        as_at = self.report.get("as_at")

        if not last or not as_at or last["date"] <= as_at or last["after"] is None:
            return reported, None

        if kind == "deemed":
            count = last["deemed_after"]
        else:
            count = last["after"] - (last["deemed_after"] or 0)

        return (count if count is not None else reported), last["issued"]

    def parts_of(self, name: str) -> list[dict]:
        return (self.holder(name) or {}).get("parts") or []

    def joint_counts(self, name: str) -> set[float]:
        return {
            part["shares"]
            for part in self.parts_of(name)
            if part["kind"] == "joint_account" and part.get("shares")
        }

    def own_pct(self, name: str) -> float | None:
        # What a holder keeps once the shares whose votes it assigned are taken off
        # (MV4: Cheo Seng Jin assigned 20,912,000 of his 110,990,366 to Unity)
        holder = self.holder(name) or {}
        held = self.held_shares(name)
        assigned = sum(
            part["shares"]
            for part in self.parts_of(name)
            if part["kind"] == "assigned_votes" and part.get("shares")
        )

        if not holder.get("total_pct") or not held or not 0 < assigned < held:
            return holder.get("total_pct")

        return holder["total_pct"] * (held - assigned) / held

    def top_of(self, tops: list[dict], name: str) -> dict | None:
        return next(
            (
                top
                for top in tops
                if top["name"] == name
            ),
            None,
        )

    def row_units(self, owner: dict) -> float | None:
        # The share count behind the row's figure: the report's count, or the filed total
        if owner["from"] == "report":
            counts = [
                (holder.get("direct_shares") or 0) + (holder.get("deemed_shares") or 0)
                for name in owner.get("_names") or []
                if (holder := self.holder(name))
            ]

            if counts and abs(max(counts) / self.issued_at_report() - owner["share"]) < 0.0015:
                return max(counts)

            return None

        for entity in owner["entities"]:
            record = self.index.latest_by_holding.get(holding_key(entity["id"], self.company["entity_id"]))
            filing = self.index.filing_by_id.get(record["filing_id"]) if record else None

            if filing and filing.get("source") == owner["source"]:
                total = ((filing.get("interest_breakdown") or {}).get("after") or {}).get("total") or {}

                return total.get("amount")

        return None

    def issued_at_report(self) -> float:
        counts = []

        for holder in self.report["holders"]:
            held = (holder.get("direct_shares") or 0) + (holder.get("deemed_shares") or 0)

            if holder.get("total_pct") and held:
                counts.append(held / (holder["total_pct"] / 100))

        # The middle count, since one misprinted percentage throws the largest off (DCRU)
        return sorted(counts)[len(counts) // 2] if counts else float("inf")


def initials(full: str) -> str:
    words = LEGAL_WORDS.sub("", full)

    return "".join(
        word[0].upper()
        for word in re.split(r"[^A-Za-z]+", words)
        if word
    )
