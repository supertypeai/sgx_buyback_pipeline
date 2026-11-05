from .index import OwnershipIndex
from .names import holding_key


ULTIMATE_MIN_STAKE = 0.001


def ultimate_owners(index: OwnershipIndex, company: dict) -> list[dict]:
    owners = []

    for block in company.get("top_level_holders") or []:
        if block["pct"] < ULTIMATE_MIN_STAKE:
            continue

        member_ids = [block["holder_id"], *block["same_block"]]
        member_rows = [
            holder
            for holder in company["holders"]
            if holder["holder_id"] in member_ids
        ]

        newest = max(member_rows, key=lambda holder: holder["as_of"], default=None)
        stale = stale_members(index, company, member_rows, newest)

        if stale:
            member_rows = [
                {
                    **holder,
                    "pct": newest["pct"],
                    "as_of": newest["as_of"],
                }
                if holder in stale
                else holder
                for holder in member_rows
            ]

        # The group's share is its largest member's stake, dated from that member's filing
        largest = None

        for holder in member_rows:
            if largest is None or (holder.get("pct") or 0) > (largest.get("pct") or 0):
                largest = holder

        if stale:
            largest = newest

        share = block["pct"]

        if stale:
            share = max(holder.get("pct") or 0 for holder in member_rows)

        share = shared_deemed_share(index, company, member_rows) or share

        filer_id = largest["holder_id"] if largest else block["holder_id"]
        filing = index.latest_filing(filer_id, company["entity_id"])

        owners.append({
            "entity": index.entity(block["holder_id"]),
            "share": share,
            "units": filing["units"],
            "source": filing["source"],
            "as_of": largest["as_of"] if largest else "",
            "members": [
                {
                    "entity": index.entity(holder["holder_id"]),
                    "direct": holder.get("direct_pct"),
                    "pct": holder.get("pct") or 0,
                }
                for holder in sorted(member_rows, key=lambda holder: -(holder.get("pct") or 0))
            ],
        })

    return owners


def holder_moves(index: OwnershipIndex, company: dict, holder: dict) -> list[dict]:
    return index.holding_moves.get(holding_key(holder["holder_id"], company["entity_id"])) or []


def stale_members(
    index: OwnershipIndex,
    company: dict,
    member_rows: list[dict],
    newest: dict | None,
) -> list[dict]:
    # A newer filing by another member that starts from this member's last count is the same
    # shares moving on (I07: Keith Toh sold on from NTCP's 6.72% to 6.37% in October)
    if not newest:
        return []

    stale = []

    for holder in member_rows:
        moves = holder_moves(index, company, holder)

        if holder["as_of"] >= newest["as_of"] or not moves:
            continue

        last = moves[-1]["after"]

        if not last:
            continue

        starts_from_last = any(
            move["date"] >= holder["as_of"]
            and move["before"]
            and abs(move["before"] - last) <= last * 0.001
            for move in holder_moves(index, company, newest)
        )

        if starts_from_last:
            stale.append(holder)

    return stale


def shared_deemed_share(index: OwnershipIndex, company: dict, member_rows: list[dict]) -> float | None:
    # Members filing the same deemed count each hold their own shares beside it, so the block
    # is that count plus every member's own (5LY: the Teos are all deemed in Apricot's shares)
    directs = [
        holder["direct_pct"]
        for holder in member_rows
        if holder.get("direct_pct")
    ]
    deemed_counts = [
        (holder_moves(index, company, holder) or [{}])[-1].get("deemed_after")
        for holder in member_rows
    ]
    same_deemed = (
        len(member_rows) >= 2
        and all(deemed_counts)
        and max(deemed_counts) - min(deemed_counts) <= max(deemed_counts) * 0.001
        and len(directs) >= 2
    )

    if not same_deemed:
        return None

    newest = max(member_rows, key=lambda holder: holder["as_of"])

    return (newest.get("deemed_pct") or 0) + sum(
        holder.get("direct_pct") or 0
        for holder in member_rows
    )
