from sgx_scraper.filings_ownership.annual_reports.names import holder_key, is_person_name
from sgx_scraper.filings_ownership.utils.constants import ISSUED_SHARES_WINDOW_DAYS
from sgx_scraper.utils.cli_helper import get_db

from datetime import date


# Rounding in the table can leave direct + deemed this far from the printed total
TOTAL_TOLERANCE = 0.06


def fill_totals(holders: list[dict]) -> None:
    # H13's table has no total column, and Ho Bee Holdings' total was read as its direct
    # 75.31% alone, leaving out the 0.21% it holds through Kingdom Investment
    for holder in holders:
        direct, deemed, total = holder["direct_pct"], holder["deemed_pct"], holder["total_pct"]
        is_off = (
            direct is not None
            and deemed is not None
            and total is not None
            and abs(direct + deemed - total) > TOTAL_TOLERANCE
        )

        if is_off:
            holder["total_pct"] = round(direct + deemed, 2)


def get_issued_shares(symbol: str, as_at: str | None, holders: list[dict]) -> float | None:
    filings = get_db(
        table="sgx_filings",
        columns="timestamp,interest_breakdown",
        query=lambda query: query.eq("symbol", f"{symbol}.SI").not_.is_("interest_breakdown", "null"),
    )

    if not as_at:
        return None

    report_counts = {
        (holder["direct_shares"] or 0) + (holder["deemed_shares"] or 0)
        for holder in holders
    } - {0}

    # The biggest holding near the report date gives the closest count, since filings
    # round to two decimals (T82U's 317,981,810 units at 10.76%)
    candidates = []
    # Outside the window, a filing that starts from the report's own count (A30's MLHS)
    matching_counts = []

    for filing in filings:
        filed_on = date.fromisoformat(filing["timestamp"][:10])
        days_away = abs((filed_on - date.fromisoformat(as_at)).days)

        for side in ("before", "after"):
            total = ((filing["interest_breakdown"] or {}).get(side) or {}).get("total") or {}

            if not (total.get("amount") and total.get("percentage")):
                continue

            issued = total["amount"] / total["percentage"]

            if days_away <= ISSUED_SHARES_WINDOW_DAYS:
                candidates.append((total["amount"], issued))
            elif total["amount"] in report_counts:
                matching_counts.append((days_away, -total["amount"], issued))

    if candidates:
        return max(candidates)[1]

    return min(matching_counts)[2] if matching_counts else None


def fill_missing_percentages(holders: list[dict], issued_shares: float | None) -> None:
    if not issued_shares:
        return

    for holder in holders:
        for shares_field, pct_field in (("direct_shares", "direct_pct"), ("deemed_shares", "deemed_pct")):
            if holder[pct_field] is None and holder[shares_field]:
                holder[pct_field] = round(holder[shares_field] / issued_shares * 100, 2)

        shares = (holder["direct_shares"] or 0) + (holder["deemed_shares"] or 0)

        if holder["total_pct"] is None and (holder["direct_shares"] or holder["deemed_shares"]):
            holder["total_pct"] = round(shares / issued_shares * 100, 2)


def fill_deemed_from_links(holders: list[dict]) -> None:
    # P9D's footnote says Patrick John Tallon holds Goldfirm's shares, but his row was
    # read with no figures, leaving Goldfirm's 16.5% with an owner at 0%
    by_key = {holder_key(holder["name"]): holder for holder in holders}

    for holder in holders:
        if holder["direct_shares"] or holder["deemed_shares"] or holder["total_pct"] is not None:
            continue

        sources = [
            by_key[key]
            for key in map(holder_key, holder["deemed_through"])
            if key in by_key
        ]
        shares = sum((source["direct_shares"] or 0) + (source["deemed_shares"] or 0) for source in sources)
        pct = sum(source["total_pct"] or 0 for source in sources)

        if shares and pct:
            holder.update(deemed_shares=shares, deemed_pct=pct, total_pct=pct)


def link_by_share_counts(holders: list[dict]) -> None:
    # Footnotes do not always name the company in between, but the counts can: SEB's UBS
    # Group is deemed in exactly UBS AG's direct plus deemed shares
    for holder in holders:
        if holder["deemed_through"] or holder["direct_shares"] or not holder["deemed_shares"]:
            continue

        holder["deemed_through"] = [
            other["name"]
            for other in holders
            if other is not holder
            and other["direct_shares"]
            and other["deemed_shares"]
            and other["direct_shares"] + other["deemed_shares"] == holder["deemed_shares"]
        ]


def is_top_owner(key: str, deemed_through: dict[str, set[str]], is_person: dict[str, bool]) -> bool:
    # A person others are deemed through is a relative with a stake of their own, not a
    # vehicle (MZH's Jin Xiao Qun); family deemed through each other are all at the top (1MZ)
    if is_person[key]:
        return True

    return not any(
        key in others and other not in deemed_through[key]
        for other, others in deemed_through.items()
        if other != key
    )


def chain_of(key: str, deemed_through: dict[str, set[str]]) -> list[str]:
    seen, stack = set(), list(deemed_through[key])

    while stack:
        current = stack.pop()

        if current in seen or current == key:
            continue

        seen.add(current)
        stack.extend(deemed_through.get(current, ()))

    return sorted(seen)


def resolve_ultimate_owners(holders: list[dict]) -> list[dict]:
    deemed_through = {
        holder_key(holder["name"]): {holder_key(name) for name in holder["deemed_through"]}
        for holder in holders
    }
    is_person = {holder_key(holder["name"]): is_person_name(holder["name"]) for holder in holders}
    names = {holder_key(holder["name"]): holder["name"] for holder in holders}
    owners = []

    for holder in holders:
        key = holder_key(holder["name"])
        total = holder["total_pct"]
        has_parts = holder["direct_pct"] is not None or holder["deemed_pct"] is not None

        if total is None and has_parts:
            total = round((holder["direct_pct"] or 0) + (holder["deemed_pct"] or 0), 4)

        if is_top_owner(key, deemed_through, is_person):
            owners.append({
                "name": holder["name"],
                "total_pct": total,
                "via": [names.get(each, each) for each in chain_of(key, deemed_through)],
            })

    return owners
