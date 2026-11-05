from .names import looks_like_person, register_name_key
from .table import REGISTER_MIN, ReportTable


def split_partial_holders(table: ReportTable, tops: list[dict]) -> list[dict]:
    # A person at a chain's end can hold more than the owner is deemed in, and is then an owner
    # in their own right (MV4: Cheo Seng Jin holds 110,990,366, the family is deemed in 20,912,000)
    split = set()

    for top in tops:
        deemed = (table.holder(top["name"]) or {}).get("deemed_shares") or 0
        ends = table.holders_of(top["via"])
        held = sum(table.held_shares(end) for end in ends)

        if not deemed or held <= deemed * 1.005:
            continue

        for end in ends:
            holder = table.holder(end) or {}
            holds_alone = (
                looks_like_person(end, table.named(end)["type"])
                and holder.get("direct_shares")
                and not holder.get("deemed_shares")
                and held - table.held_shares(end) <= deemed * 1.005
            )

            if holds_alone:
                split.add(end)

    if not split:
        return tops

    named = {register_name_key(top["name"]) for top in tops}
    kept = [
        {
            **top,
            "via": [
                name
                for name in top["via"]
                if name not in split
            ],
            "total_pct": table.own_pct(top["name"]) if top["name"] in split else top.get("total_pct"),
        }
        for top in tops
    ]
    added = [
        {"name": name, "total_pct": table.own_pct(name), "via": []}
        for name in sorted(split)
        if register_name_key(name) not in named
        and ((table.holder(name) or {}).get("total_pct") or 0) >= REGISTER_MIN * 100
    ]

    return kept + added


def nested_layers(table: ReportTable, tops: list[dict]) -> list[dict]:
    # Unlinked holders deemed in the same shares are one layer; with two or more layers, only
    # the largest stays on top (J85's footnotes never name the companies, but the counts nest)
    members_by_shares: dict[float, int] = {}

    for top in tops:
        shares = (table.holder(top["name"]) or {}).get("deemed_shares")

        if top["via"] or not shares:
            continue

        members_by_shares[shares] = members_by_shares.get(shares, 0) + 1

    layers = [
        shares
        for shares, count in members_by_shares.items()
        if count >= 2
    ]
    
    if len(layers) < 2:
        return tops

    largest = max(table.held_shares(top["name"]) for top in tops)
    kept = []

    for top in tops:
        holder = table.holder(top["name"]) or {}

        if table.held_shares(top["name"]) == largest:
            kept.append(top)
        elif holder.get("deemed_shares") and holder["deemed_shares"] in layers:
            continue
        elif top["via"] or not holder.get("direct_shares"):
            kept.append(top)

    return kept
