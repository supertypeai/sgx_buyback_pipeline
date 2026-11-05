from sgx_scraper.filings_ownership.utils.constants import (
    FAMILY_RELATIONS,
    MAX_ROUTE_LENGTH,
    MAX_ROUTES,
    ROUTE_RELATIONS,
    STEP_ORDER,
)

from collections import defaultdict, deque


# A route % above the holder's own stake by more than this is misread
PERCENTAGE_TOLERANCE = 0.0005
ROUTE_TOTAL_TOLERANCE = 1.05


def build_route_graph(links: list[dict], company_id: str) -> tuple[dict, set, set]:
    # Family and beneficiary links run both ways but only as the holder's first
    # step: the relative's relatives (or a trust's other beneficiaries) never
    # count. Past that first step the route goes on only through a relative
    # whose interest the filing says counts for the holder
    children = defaultdict(list)
    end_pairs = set()

    for link in links:
        if link["relation"] in ROUTE_RELATIONS:
            children[link["parent"]].append(link["child"])

        if link["relation"] in FAMILY_RELATIONS | {"beneficiary"}:
            children[link["child"]].append(link["parent"])
            end_pairs.add((link["child"], link["parent"]))

        if link["relation"] in FAMILY_RELATIONS:
            end_pairs.add((link["parent"], link["child"]))

    direct_holders = {
        link["parent"]
        for link in links
        if link["child"] == company_id
        and link["relation"] in ("holds_directly", "owns", "controls")
    }

    return children, end_pairs, direct_holders


def get_next_nodes(
    path: list[str],
    children: dict[str, list[str]],
    end_pairs: set[tuple],
    counted_pairs: set[tuple],
    company_id: str,
) -> list[str]:
    last_pair = (path[-2], path[-1]) if len(path) >= 2 else None

    if last_pair in end_pairs and last_pair not in counted_pairs:
        return []

    return [
        child
        for child in children.get(path[-1], [])
        if child not in path
        and child != company_id
        and (len(path) == 1 or (path[-1], child) not in end_pairs)
    ]


def find_chains(
    holder_id: str,
    company_id: str,
    links: list[dict],
    targets: set[str],
    counted_pairs: set[tuple],
) -> list[list[str]]:
    children, end_pairs, direct_holders = build_route_graph(links, company_id)
    targets = targets | direct_holders

    chains = []
    queue = deque([[holder_id]])

    while queue and len(chains) < MAX_ROUTES:
        path = queue.popleft()

        if len(path) > MAX_ROUTE_LENGTH:
            continue

        next_nodes = get_next_nodes(path, children, end_pairs, counted_pairs, company_id)
        is_end = path[-1] in targets or not next_nodes

        if len(path) > 1 and is_end and path not in chains:
            chains.append(path)

        # A direct holder can also own another direct holder further down
        # (BPREI holds units and, through BP-Unity, part of UIB Holdings)
        queue.extend(path + [child] for child in next_nodes)

    # A filer often states a shortcut ("through CLA Real Estate") next to the
    # full chain; keep only the longest route to each end point
    longest = {}

    for chain in chains:
        if len(chain) > len(longest.get(chain[-1], [])):
            longest[chain[-1]] = chain

    return list(longest.values())


def get_pairs(links: list[dict], relations: set[str] | None = None) -> set[tuple]:
    pairs = set()

    for link in links:
        if relations is None and not link["counts_toward_parent"]:
            continue

        if relations is not None and link["relation"] not in relations:
            continue

        pairs.add((link["parent"], link["child"]))
        pairs.add((link["child"], link["parent"]))

    return pairs


def is_family_only_route(
    chain: list[str],
    family_pairs: set[tuple],
    counted_pairs: set[tuple],
    trader_ids: set[str],
) -> bool:
    # A relative is a route only when the relative traded or the filing says
    # the relative's own shares count for the holder, not when they just
    # co-own a vehicle
    if len(chain) != 2:
        return False

    pair = (chain[0], chain[1])

    return pair in family_pairs and pair not in counted_pairs and chain[1] not in trader_ids


def build_routes(
    holder_id: str,
    company_id: str,
    links: list[dict],
    route_percentages: dict[str, float],
    targets: set[str],
    trader_ids: set[str],
    pct_after: float | None,
) -> list[dict]:
    counted_pairs = get_pairs(links)
    family_pairs = get_pairs(links, FAMILY_RELATIONS)

    chains = [
        chain
        for chain in find_chains(holder_id, company_id, links, targets, counted_pairs)
        if not is_family_only_route(chain, family_pairs, counted_pairs, trader_ids)
    ]

    # A route group ("via CLA Real Estate 20.214%") can split into branches
    # ("SBR 7.320%", "PHSIPL 5.442%"); each branch keeps its direct holder's
    # own %, the group total is kept once as via_percentage
    held_percentages = {
        link["parent"]: link["percentage"]
        for link in links
        if link["child"] == company_id
        and link["relation"] == "holds_directly"
        and link["percentage"] is not None
    }

    routes = []

    for chain in chains:
        via = next((
            node
            for node in chain
            if node in route_percentages
        ), None)
        is_shared = via is not None and sum(via in other for other in chains) > 1
        own_percentage = held_percentages.get(chain[-1])

        if own_percentage is not None:
            percentage = own_percentage

        elif via and not is_shared:
            percentage = route_percentages[via]

        else:
            percentage = None

        routes.append({
            "chain": chain,
            "percentage": percentage,
            "via": via,
            "via_percentage": route_percentages.get(via),
        })

    clear_misread_percentages(routes, pct_after)

    return routes


def clear_misread_percentages(routes: list[dict], pct_after: float | None) -> None:
    # A route % is a share of the listed issuer; one above the holder's own
    # stake is a vehicle's ownership misread as a route
    ceiling = (pct_after or 0) + PERCENTAGE_TOLERANCE

    for route in routes:
        if route["percentage"] is not None and route["percentage"] > ceiling:
            route["percentage"] = None

    totals = {}

    for route in routes:
        if route["via"]:
            totals[route["via"]] = route["via_percentage"]

        else:
            totals[tuple(route["chain"])] = route["percentage"]

    routed_total = sum(
        value
        for value in totals.values()
        if value is not None
    )

    if routed_total > ceiling * ROUTE_TOTAL_TOLERANCE:
        for route in routes:
            route["percentage"] = None
            route["via_percentage"] = None

    for route in routes:
        if route["via_percentage"] is not None and route["via_percentage"] > ceiling:
            route["via_percentage"] = None


def get_step_rank(link: dict) -> tuple[int, bool]:
    relation = link["stated_relation"]
    rank = STEP_ORDER.index(relation) if relation in STEP_ORDER else len(STEP_ORDER)

    return rank, link["stated_percentage"] is None


def build_route_steps(chain: list[str], links: list[dict]) -> list[dict | None]:
    # Each step as this filing states it, so a route never shows another
    # filing's wording as if it were this one's
    steps = []

    for parent, child in zip(chain, chain[1:]):
        stated = [
            link
            for link in links
            if link["parent"] == parent and link["child"] == child
        ]

        if not stated:
            steps.append(None)
            continue

        best = min(stated, key=get_step_rank)

        steps.append({
            "relation": best["stated_relation"],
            "percentage": best["stated_percentage"],
            "evidence": best["evidence"] or None,
        })

    return steps
