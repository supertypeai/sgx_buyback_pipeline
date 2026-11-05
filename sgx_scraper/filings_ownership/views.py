from sgx_scraper.filings_ownership.utils.constants import RECENT_DAYS
from sgx_scraper.filings_ownership.utils.graph_helper import get_ancestors
from sgx_scraper.filings_ownership.utils.record_helper import get_interest_percentage, is_still_held

from collections import Counter, defaultdict
from datetime import date, timedelta


# Two unrelated holders matching within 0.2 points at 20% or more is not
# plausible; family members differ only by their own direct shares
LARGE_STAKE = 0.20
LARGE_STAKE_TOLERANCE = 0.002
EXACT_TOLERANCE = 0.00005


class BlockGrouper:
    """
    Union-find over holder ids: holders reporting the same shares join one block.
    """

    def __init__(self, holder_ids: list[str]) -> None:
        self.roots = {holder_id: holder_id for holder_id in holder_ids}

    def find(self, holder_id: str) -> str:
        while self.roots[holder_id] != holder_id:
            self.roots[holder_id] = self.roots[self.roots[holder_id]]
            holder_id = self.roots[holder_id]

        return holder_id

    def join(self, first_id: str, second_id: str) -> None:
        self.roots[self.find(first_id)] = self.find(second_id)


def get_pct(filing: dict) -> float:
    return filing["pct_after"] or 0


def get_deemed_pct(filing: dict) -> float:
    return get_interest_percentage(filing, "after", "deemed") or 0


def get_direct_pct(filing: dict) -> float:
    return get_interest_percentage(filing, "after", "direct") or 0


def get_filing_order(filing: dict) -> tuple[str, int]:
    return filing["timestamp"], filing["filing_id"]


def is_same_shares(first: dict, second: dict) -> bool:
    # Deemed interests overlap by law: family members and every level of a
    # group each report the same shares
    total_gap = abs(get_pct(first) - get_pct(second))
    deemed_gap = abs(get_deemed_pct(first) - get_deemed_pct(second))

    is_large_match = (
        get_deemed_pct(first) >= LARGE_STAKE
        and get_deemed_pct(second) >= LARGE_STAKE
        and (deemed_gap <= LARGE_STAKE_TOLERANCE or total_gap <= LARGE_STAKE_TOLERANCE)
    )
    is_exact_match = total_gap < EXACT_TOLERANCE and all(
        filing["interest_role"] != "direct" and get_deemed_pct(filing) >= get_direct_pct(filing)
        for filing in (first, second)
    )
    is_filed_together = (
        first["filing_type"] == "form_3_part_iii_iv"
        and first["source"] == second["source"]
        and first["pct_after"] == second["pct_after"]
    )

    return is_large_match or is_exact_match or is_filed_together


def get_owned_route_nodes(route: dict) -> list[str]:
    # A purchase deal, share loan or option only promises shares: the holders
    # on either side of it stay separate owners once it completes
    steps = route.get("steps") or []
    nodes = []

    for index, node in enumerate(route["chain"][1:]):
        step = steps[index] if index < len(steps) else None

        if step and step.get("relation") == "agreement":
            break

        nodes.append(node)

    return nodes


def group_same_block(
    positions: list[dict],
    parents: dict[str, set[str]],
    reach: Counter,
    nominee_ids: set[str],
    history: dict[tuple, list[dict]],
) -> list[dict]:
    # Holders are one block when they hold through the same vehicle or through
    # each other, filed together with the same stake, or report the same large stake
    grouper = BlockGrouper([filing["holder_id"] for filing in positions])
    holders_via = defaultdict(set)

    for filing in positions:
        # Any of the holder's filings for this company may carry the route
        for past in history[(filing["symbol"], filing["holder_id"])]:
            for route in past["deemed_routes"]:
                for node in get_owned_route_nodes(route):
                    if node not in nominee_ids:
                        holders_via[node].add(filing["holder_id"])

    for node, via_ids in holders_via.items():
        first_id, *other_ids = sorted(via_ids)

        for other_id in other_ids:
            grouper.join(first_id, other_id)

        # A spouse or fund holder routing through another holder of this
        # company reports that holder's shares
        if node in grouper.roots:
            grouper.join(first_id, node)

    for first in positions:
        for second in positions:
            if first is not second and is_same_shares(first, second):
                grouper.join(first["holder_id"], second["holder_id"])

    blocks = defaultdict(list)

    for filing in positions:
        blocks[grouper.find(filing["holder_id"])].append(filing)

    rows = []

    for members in blocks.values():
        member_ids = [filing["holder_id"] for filing in members]
        top_id = pick_block_top(member_ids, parents, reach)

        rows.append({
            "holder_id": top_id,
            "pct": max(get_pct(filing) for filing in members),
            "same_block": [
                member_id
                for member_id in member_ids
                if member_id != top_id
            ],
        })

    return sorted(rows, key=get_row_pct, reverse=True)


def pick_block_top(member_ids: list[str], parents: dict[str, set[str]], reach: Counter) -> str:
    # Label the block with its top: no parent inside the block, then the
    # holder present in the most SGX companies
    members = set(member_ids)
    ranks = [
        (bool(get_ancestors(member_id, parents) & members), -reach[member_id], position, member_id)
        for position, member_id in enumerate(member_ids)
    ]

    return min(ranks)[-1]


def get_row_pct(row: dict) -> float:
    return row["pct"] or 0


def build_company_row(
    symbol: str,
    filings: list[dict],
    positions: list[dict],
    context: dict,
) -> dict:
    holder_ids = {filing["holder_id"] for filing in positions}
    # Top-level holders: no other current holder of this company sits above them
    top_level = [
        filing
        for filing in positions
        if not get_ancestors(filing["holder_id"], context["parents"]) & holder_ids
    ]
    blocks = group_same_block(
        top_level,
        context["parents"],
        context["reach"],
        context["nominee_ids"],
        context["history"],
    )
    recent = [
        filing
        for filing in filings
        if filing["timestamp"] >= context["cutoff"] and filing["move_kind"] == "decision"
    ]
    net_value = 0

    for filing in recent:
        sign = 1 if filing["transaction_type"] == "buy" else -1
        net_value += (filing["transaction_value"] or 0) * sign

    return {
        "symbol": symbol,
        "name": context["company_names"].get(symbol) or context["entity_names"][filings[0]["company_id"]],
        "entity_id": filings[0]["company_id"],
        "sector": next((
            filing["sector"]
            for filing in filings
            if filing.get("sector")
        ), None),
        "holders": [
            {
                "holder_id": filing["holder_id"],
                "pct": filing["pct_after"],
                "as_of": filing["timestamp"],
                "direct_pct": get_interest_percentage(filing, "after", "direct"),
                "deemed_pct": get_interest_percentage(filing, "after", "deemed"),
                "holder_type": filing["holder_type"],
            }
            for filing in positions
        ],
        "top_level_holders": blocks,
        "largest_top_level_pct": max((block["pct"] for block in blocks), default=None),
        "recent_decisions": {
            "buys": sum(filing["transaction_type"] == "buy" for filing in recent),
            "sells": sum(filing["transaction_type"] == "sell" for filing in recent),
            "net_value": net_value,
            "since": context["cutoff"],
        },
        "filing_count": len(filings),
    }


def get_latest_filings(filings: list[dict]) -> dict[tuple, dict]:
    latest = {}

    for filing in sorted(filings, key=get_filing_order):
        latest[(filing["symbol"], filing["holder_id"])] = filing

    return latest


def get_parents(link_rows: list[dict], relations: set[str]) -> dict[str, set[str]]:
    parents = defaultdict(set)

    for row in link_rows:
        if row["relation"] in relations:
            parents[row["child_id"]].add(row["parent_id"])

    return parents


def build_company_views(
    filings: list[dict],
    link_rows: list[dict],
    entity_rows: list[dict],
    company_names: dict[str, str],
) -> list[dict]:
    latest = get_latest_filings(filings)
    history = defaultdict(list)
    by_symbol = defaultdict(list)

    for filing in filings:
        history[(filing["symbol"], filing["holder_id"])].append(filing)
        by_symbol[filing["symbol"]].append(filing)

    last_date = date.fromisoformat(max(filing["timestamp"] for filing in filings))
    context = {
        "parents": get_parents(link_rows, {"owns", "controls", "deemed_via"}),
        "reach": Counter(holder_id for _, holder_id in latest),
        "nominee_ids": {
            row["id"]
            for row in entity_rows
            if row["entity_type"] == "nominee"
        },
        "history": history,
        "cutoff": (last_date - timedelta(days=RECENT_DAYS)).isoformat(),
        "company_names": company_names,
        "entity_names": {row["id"]: row["name"] for row in entity_rows},
    }

    positions_by_symbol = defaultdict(list)

    for (symbol, _), filing in latest.items():
        if is_still_held(filing):
            positions_by_symbol[symbol].append(filing)

    rows = []

    for symbol in sorted(by_symbol):
        positions = sorted(positions_by_symbol[symbol], key=get_pct, reverse=True)

        rows.append(build_company_row(symbol, by_symbol[symbol], positions, context))

    return rows


def get_ultimate_parent(entity_id: str, parent_links: dict[str, list[dict]]) -> str | None:
    seen = {entity_id}
    current_id = entity_id

    while parent_links.get(current_id):
        best = max(parent_links[current_id], key=get_parent_rank)

        if best["parent_id"] in seen:
            break

        seen.add(best["parent_id"])
        current_id = best["parent_id"]

    return current_id if current_id != entity_id else None


def get_parent_rank(link: dict) -> tuple[float, int]:
    # A stated majority stake outranks a bare "controls", taken as 50%
    return link["percentage"] or 0.5, link["filing_count"]


def get_position_pct(position: dict) -> float:
    return position["pct"] or 0


def build_entity_views(
    filings: list[dict],
    link_rows: list[dict],
    entity_rows: list[dict],
) -> list[dict]:
    positions = defaultdict(list)

    for (symbol, holder_id), filing in get_latest_filings(filings).items():
        positions[holder_id].append({
            "symbol": symbol,
            "pct": filing["pct_after"],
            "as_of": filing["timestamp"],
            "interest_role": filing["interest_role"],
        })

    filing_counts = Counter(filing["holder_id"] for filing in filings)
    trade_counts = Counter(
        trader_id
        for filing in filings
        for trader_id in filing["trader_ids"]
        if trader_id != filing["holder_id"]
    )
    parent_links = defaultdict(list)

    for row in link_rows:
        if row["relation"] in ("owns", "controls"):
            parent_links[row["child_id"]].append(row)

    return [
        {
            "id": row["id"],
            "positions": sorted(positions[row["id"]], key=get_position_pct, reverse=True),
            "filing_count": filing_counts[row["id"]],
            "trades_for_others": trade_counts[row["id"]],
            "ultimate_parent": get_ultimate_parent(row["id"], parent_links),
        }
        for row in entity_rows
    ]
