from .names import holding_key
from .table import ReportTable


def mark_ceased(table: ReportTable, owners: list[dict]) -> None:
    # A ceased notice is under 5%, so no other step reaches the row; the owner stays, smaller
    # and no longer filing (5CF: CS International 14.05% in March, 4.89% in April)
    as_at = table.report.get("as_at")
    company_id = table.company["entity_id"]
    ceased_since_report = ceased_holders(table, as_at, company_id)
    sold_out = sold_out_holders(table, as_at, company_id)

    for owner in list(owners):
        # A member who sold out leaves a shared row the others still hold (9A4U's Starwood)
        gone = [
            entity
            for entity in owner["entities"]
            if entity["id"] in sold_out and not filed_after(owner, sold_out[entity["id"]])
        ]

        if gone and len(gone) < len(owner["entities"]):
            owner["entities"] = [
                entity
                for entity in owner["entities"]
                if entity not in gone
            ]

        if len(owner["entities"]) == 1:
            only = owner["entities"][0]["id"]

            if only in sold_out and not filed_after(owner, sold_out[only]):
                owners.remove(owner)
                continue

        # When the company holding the shares ceased, everyone deemed through it did too
        members = [*owner["entities"], *(owner["via"] if owner["from"] == "report" else [])]
        ceased = sorted(
            [
                ceased_since_report[entity["id"]]
                for entity in members
                if entity["id"] in ceased_since_report
            ],
            key=lambda holder: holder["as_of"],
            reverse=True,
        )

        if not ceased:
            continue

        latest = ceased[0]

        # A member's newer filing is the row's latest word
        if owner["from"] == "filing" and owner["as_of"] and owner["as_of"] > latest["as_of"]:
            continue

        owner.update({
            "share": latest["pct"],
            "as_of": latest["as_of"],
            "source": table.index.latest_filing(latest["holder_id"], company_id)["source"],
            "page": None,
            "from": "filing",
            "ceased": True,
        })


def filed_after(owner: dict, date: str) -> bool:
    return owner["from"] == "filing" and owner["as_of"] > date


def ceased_holders(table: ReportTable, as_at: str | None, company_id: str) -> dict[str, dict]:
    ceased = {}

    for holder in table.company["holders"]:
        record = table.index.latest_by_holding.get(holding_key(holder["holder_id"], company_id)) or {}

        if as_at and holder["as_of"] > as_at and record.get("substantial_status") == "ceased":
            ceased[holder["holder_id"]] = holder

    return ceased


def sold_out_holders(table: ReportTable, as_at: str | None, company_id: str) -> dict[str, str]:
    # The company's holder list leaves out one that sold everything, so those come from the
    # filings (T24: Koh Wee Meng sold all 65,830,000 shares in June)
    current = {holder["holder_id"] for holder in table.company["holders"]}
    sold_out = {}

    for key, record in table.index.latest_by_holding.items():
        holder_id, record_company = key.split("|", 1)
        filing = table.index.filing_by_id.get(record["filing_id"]) or {}
        date = (filing.get("timestamp") or "")[:10]

        is_sold_out = (
            record_company == company_id
            and holder_id not in current
            and record.get("substantial_status") == "ceased"
            and not record.get("pct_after")
            and as_at
            and date > as_at
        )

        if is_sold_out:
            sold_out[holder_id] = date

    return sold_out
