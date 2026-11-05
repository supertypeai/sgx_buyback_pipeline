from .names import GENERIC_FIRST, first_word, register_name_key
from .table import ReportTable


def build_blocks(table: ReportTable, tops: list[dict]) -> list[dict]:
    blocks: list[dict] = []

    for top in tops:
        holder = table.holder(top["name"]) or {}
        holder_keys = block_keys(table, top, holder)
        names = relatives_of(table, top, holder) or [top["name"]]

        own = register_name_key(top["name"])
        block = next(
            (
                each
                for each in blocks
                if any(key in each["holders"] for key in holder_keys)
                or own in each["holders"]
                or any(register_name_key(name) in holder_keys for name in each["names"])
            ),
            None,
        )

        # Two holders reporting the same joint account hold it together, the joint units once
        # and each one's own beside them (TS0U: Gordon and Celine Tang's 444,469,642)
        joint = table.joint_counts(top["name"])
        joined = None

        if not block:
            joined = next(
                (
                    each
                    for each in blocks
                    if joint & each.get("joint", set())
                ),
                None,
            )

        if joined:
            held = table.held_shares(top["name"])
            shared = sum(joint & joined["joint"])

            if held and top.get("total_pct") and 0 < shared < held:
                extra = (held - shared) / held * top["total_pct"] / 100
                joined["share"] += extra
                joined["extra"] += extra

                joined["names"].extend(
                    name
                    for name in names
                    if name not in joined["names"]
                )
                joined["joint"] |= joint
                continue

        if block:
            extra = own_shares_beside(table, block, top, tops)
            block["share"] += extra
            block["extra"] += extra

            block["names"].extend(
                name
                for name in names
                if name not in block["names"]
            )
        else:
            blocks.append({
                "names": names,
                "share": top["total_pct"] / 100,
                "holders": holder_keys,
                "extra": 0,
                "joint": joint,
            })

    merge_overlapping(table, blocks)

    return blocks


def block_keys(table: ReportTable, top: dict, holder: dict) -> list[str]:
    # Owners deemed in exactly the same shares hold one block (Z74: Capital Group and Capital
    # Research); a holder with shares in its own name stays apart (ZKX's founders)
    deemed_only = not top["via"] and holder.get("deemed_shares") and not holder.get("direct_shares")

    if deemed_only:
        return [f"deemed {holder['deemed_shares']}", register_name_key(top["name"])]

    if top["via"]:
        return [register_name_key(name) for name in table.holders_of(top["via"])]

    return [register_name_key(top["name"])]


def relatives_of(table: ReportTable, top: dict, holder: dict) -> list[str]:
    # An owner deemed in exactly its family members' shares holds by relation, so the family
    # members are the owners (5TP: Lim Kuoh Yang is deemed in his parents' shares)
    deemed_shares = holder.get("deemed_shares")

    if not deemed_shares:
        return []

    own = register_name_key(top["name"])
    relatives = []

    for name in top["via"]:
        if table.named(name)["type"] != "person":
            continue

        through = table.deemed_through.get(register_name_key(name)) or []

        if any(register_name_key(each) == own for each in through):
            continue

        if (table.holder(name) or {}).get("deemed_shares") == deemed_shares:
            relatives.append(name)

    return relatives


def own_shares_beside(table: ReportTable, block: dict, top: dict, tops: list[dict]) -> float:
    # A member whose deemed interest is all in its chain's end companies holds the rest in its
    # own name (5WJ: Lim Yong Sheng is deemed in exactly Money Farm's shares)
    if not top.get("total_pct") or not explained(table, top):
        return 0

    if any(not is_person_like(table, name) for name in [top["name"], *block["names"]]):
        return 0

    own = register_name_key(top["name"])
    members = [register_name_key(name) for name in block["names"]]

    if any(key in members for key in table.through_of(top["name"])):
        return 0

    if any(own in table.through_of(name) for name in block["names"]):
        return 0

    counted = {
        register_name_key(name)
        for member in block["names"]
        for name in table.holders_of((table.top_of(tops, member) or {}).get("via") or [])
    }
    total = table.held_shares(top["name"])
    own_shares = (table.holder(top["name"]) or {}).get("direct_shares") or 0
    uncounted_ends = sum(
        table.held_shares(name)
        for name in table.holders_of(top["via"])
        if register_name_key(name) not in counted
    )
    extra = own_shares + uncounted_ends

    if not total or extra <= 0 or extra >= total:
        return 0

    return (extra / total) * (top["total_pct"] / 100)


def is_person_like(table: ReportTable, name: str) -> bool:
    return table.named(name)["type"] in ("person", "unknown")


def explained(table: ReportTable, top: dict) -> bool:
    deemed = (table.holder(top["name"]) or {}).get("deemed_shares") or 0
    ends = sum(table.held_shares(name) for name in table.holders_of(top["via"]))

    return bool(top["via"]) and deemed > 0 and abs(deemed - ends) <= deemed * 0.005


def merge_overlapping(table: ReportTable, blocks: list[dict]) -> None:
    merged = True

    while merged:
        merged = False

        for first in range(len(blocks)):
            for second in range(first + 1, len(blocks)):
                if not one_group(table, blocks[first], blocks[second]):
                    continue

                if blocks[first]["share"] >= blocks[second]["share"]:
                    larger, smaller = blocks[first], blocks[second]
                else:
                    larger, smaller = blocks[second], blocks[first]

                larger["names"].extend(
                    name
                    for name in smaller["names"]
                    if name not in larger["names"]
                )
                larger["holders"].extend(
                    key
                    for key in smaller["holders"]
                    if key not in larger["holders"]
                )
                blocks.remove(smaller)
                merged = True
                break

            if merged:
                break


def one_group(table: ReportTable, block_a: dict, block_b: dict) -> bool:
    # Two blocks over 100% together must share shares (P52); companies of one name whose
    # deemed shares nest are one group too (F34: Kerry Holdings inside Kerry Group)
    if block_a["share"] + block_b["share"] > 1.0001:
        return True

    lone_a = lone(table, block_a)
    lone_b = lone(table, block_b)

    return (
        bool(lone_a)
        and bool(lone_b)
        and len(lone_a["word"]) >= 4
        and lone_a["word"] not in GENERIC_FIRST
        and lone_a["word"] == lone_b["word"]
        and lone_a["shares"] != lone_b["shares"]
    )


def lone(table: ReportTable, block: dict) -> dict | None:
    # A single company deemed-only block, with its first name word and deemed count
    name = block["names"][0]
    holder = table.holder(name) or {}
    is_lone_company = (
        len(block["names"]) == 1
        and table.named(name)["type"] != "person"
        and not holder.get("direct_shares")
        and holder.get("deemed_shares")
    )

    if is_lone_company:
        return {"word": first_word(name), "shares": holder["deemed_shares"]}

    return None


def block_rows(table: ReportTable, blocks: list[dict], tops: list[dict]) -> list[dict]:
    report = table.report
    rows = []

    for block in blocks:
        owned_by = []

        for name in block["names"]:
            above = (table.top_of(tops, name) or {}).get("owned_by") or []

            if above:
                owned_by.append({
                    "company": table.named(name),
                    "owners": [table.named(each) for each in above],
                })

        # A row whose count came from a late filing cites that filing, not the report's page
        late = next(
            (
                table.late[register_name_key(name)]
                for name in block["names"]
                if register_name_key(name) in table.late
            ),
            None,
        )

        if late:
            dated = {"as_of": late["date"], "source": late["source"], "page": None, "from": "filing"}
        else:
            dated = {
                "as_of": report.get("as_at"),
                "source": report.get("source_url"),
                "page": report.get("page"),
                "from": "report",
            }

        lead_via = (table.top_of(tops, block["names"][0]) or {}).get("via") or []

        rows.append({
            "entities": [table.named(name) for name in block["names"]],
            "via": [table.named(name) for name in table.holders_of(lead_via)],
            "owned_by": owned_by,
            "ceased": False,
            "share": block["share"],
            **dated,
            "_lead": register_name_key(block["names"][0]),
            "_extra": block["extra"],
            "_names": block["names"],
        })

    return rows
