from .names import bare_symbol, holding_key, register_name_key


GROUP_ABOVE = {"owns", "controls", "deemed_via"}


class OwnershipIndex:
    def __init__(self, tables: dict, filings: list[dict]):
        self.entities = {row["id"]: row for row in tables["entities"]}
        # A fixed order, so chains list their links the same whether the rows come from the
        # database or a state file; the first of two equal links is kept
        self.links = sorted(
            tables["links"],
            key=lambda link: (link["parent_id"], link["child_id"], link["relation"]),
        )
        self.companies = {bare_symbol(row["symbol"]): row for row in tables["companies"]}
        self.reports = {bare_symbol(row["symbol"]): row for row in tables["reports"]}
        self.filing_by_id = {int(row["id"]): row for row in filings}
        self.entity_by_register_name = self._entities_by_register_name()

        ownership = tables["ownership"]
        self.latest_by_holding = self._latest_by_holding(ownership)
        self.latest_routed_by_holding = self._latest_by_holding(
            [
                record
                for record in ownership
                if record.get("deemed_routes")
            ]
        )
        self.ceased_filings = self._ceased_filings(ownership)
        self.units_by_holding = self._units_by_holding(ownership)
        self.holding_moves = self._holding_moves(ownership)
        self.parents = self._parents()

    def entity(self, entity_id: str) -> dict:
        row = self.entities.get(entity_id) or {}

        return {
            "id": entity_id,
            "name": row.get("name") or entity_id,
            "type": row.get("entity_type") or "company",
            "symbol": bare_symbol(row["listed_symbol"]) if row.get("listed_symbol") else None,
        }

    def _entities_by_register_name(self) -> dict[str, str]:
        ids_by_key: dict[str, set[str]] = {}

        for row in self.entities.values():
            for name in [row["name"], *(row.get("aliases") or [])]:
                ids_by_key.setdefault(register_name_key(name), set()).add(row["id"])

        # A name two entities share names neither
        return {
            key: next(iter(ids))
            for key, ids in ids_by_key.items()
            if len(ids) == 1
        }

    def _latest_by_holding(self, ownership: list[dict]) -> dict[str, dict]:
        latest: dict[str, dict] = {}

        for record in sorted(ownership, key=lambda record: record["filing_id"]):
            filing = self.filing_by_id.get(record["filing_id"])

            if not filing or not filing.get("source") or not filing.get("timestamp"):
                continue

            key = holding_key(record["holder_id"], record["company_id"])
            previous = latest.get(key)
            previous_time = None

            if previous:
                previous_time = self.filing_by_id.get(previous["filing_id"], {}).get("timestamp")

            if previous is None or (previous_time or "") <= filing["timestamp"]:
                latest[key] = record

        return latest

    def deemed_routes(self, holder_id: str, company_id: str) -> list[dict]:
        # A filing that only moves the holder's own shares states no structure, so the last
        # filing that did still holds (E28: Gooi's October purchase left Prime Logic in place)
        key = holding_key(holder_id, company_id)
        record = self.latest_by_holding.get(key) or {}

        if record.get("interest_role") == "direct" and not record.get("deemed_routes"):
            record = self.latest_routed_by_holding.get(key) or record

        return record.get("deemed_routes") or []

    def _ceased_filings(self, ownership: list[dict]) -> list[dict]:
        ceased = []

        for record in ownership:
            filing = self.filing_by_id.get(record["filing_id"])

            if record.get("substantial_status") != "ceased" or not filing or not filing.get("source"):
                continue

            ceased.append({
                "symbol": bare_symbol(filing.get("symbol") or ""),
                "date": (filing.get("timestamp") or "")[:10],
                "holder_id": record["holder_id"],
            })

        return ceased

    def _units_by_holding(self, ownership: list[dict]) -> dict[str, list[tuple[str, float]]]:
        # Every share count a holder filed for a company, before and after each change
        units: dict[str, list[tuple[str, float]]] = {}

        for record in ownership:
            filing = self.filing_by_id.get(record["filing_id"])

            if not filing or not filing.get("timestamp"):
                continue

            breakdown = filing.get("interest_breakdown") or {}

            for side in ("before", "after"):
                amount = ((breakdown.get(side) or {}).get("total") or {}).get("amount")

                if amount:
                    key = holding_key(record["holder_id"], record["company_id"])

                    units.setdefault(key, []).append((filing["timestamp"][:10], amount))

        return units

    def _holding_moves(self, ownership: list[dict]) -> dict[str, list[dict]]:
        moves: dict[str, list[dict]] = {}

        for record in ownership:
            filing = self.filing_by_id.get(record["filing_id"])

            if not filing or not filing.get("timestamp"):
                continue

            breakdown = filing.get("interest_breakdown") or {}
            before = ((breakdown.get("before") or {}).get("total") or {}).get("amount")
            after_total = (breakdown.get("after") or {}).get("total") or {}
            deemed = {
                side: ((breakdown.get(side) or {}).get("deemed") or {}).get("amount")
                for side in ("before", "after")
            }
            issued = None

            if after_total.get("amount") and after_total.get("percentage"):
                issued = after_total["amount"] / after_total["percentage"]

            key = holding_key(record["holder_id"], record["company_id"])

            moves.setdefault(key, []).append({
                "date": filing["timestamp"][:10],
                "id": record["filing_id"],
                "source": filing.get("source"),
                "before": before,
                "after": after_total.get("amount"),
                "deemed_before": deemed["before"],
                "deemed_after": deemed["after"],
                "issued": issued,
            })

        return {
            key: sorted(each, key=lambda move: (move["date"], move["id"]))
            for key, each in moves.items()
        }

    def _parents(self) -> dict[str, list[str]]:
        parents: dict[str, list[str]] = {}

        for link in self.links:
            if link["relation"] in GROUP_ABOVE:
                parents.setdefault(link["child_id"], []).append(link["parent_id"])

        return parents

    def has_ancestor(self, entity_id: str, ancestor: str) -> bool:
        seen = set()
        stack = [entity_id]

        while stack:
            for parent in self.parents.get(stack.pop(), []):
                if parent == ancestor:
                    return True

                if parent in seen:
                    continue

                seen.add(parent)

                stack.append(parent)

        return False

    def latest_filing(self, holder_id: str, company_id: str) -> dict:
        record = self.latest_by_holding.get(holding_key(holder_id, company_id))
        filing = self.filing_by_id.get(record["filing_id"]) if record else None
        after = ((filing or {}).get("interest_breakdown") or {}).get("after") or {}

        return {
            "units": (after.get("total") or {}).get("amount"),
            "direct_units": (after.get("direct") or {}).get("amount"),
            "deemed_units": (after.get("deemed") or {}).get("amount"),
            "source": (filing or {}).get("source"),
            "date": ((filing or {}).get("timestamp") or "")[:10] or None,
            "share": record.get("pct_after") if record else None,
        }
