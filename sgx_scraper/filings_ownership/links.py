from sgx_scraper.filings_ownership.utils.constants import OWNING_RELATIONS
from sgx_scraper.filings_ownership.utils.graph_helper import reaches

from collections import defaultdict

import logging


LOGGER = logging.getLogger(__name__)


def get_link_key(row: dict) -> tuple[str, str, str]:
    return row["parent_id"], row["child_id"], row["relation"]


def build_link_row(link: dict, filing: dict) -> dict:
    return {
        "parent_id": link["parent"],
        "child_id": link["child"],
        "relation": link["relation"],
        "percentage": None,
        "counts_toward_parent": False,
        "first_seen": filing["timestamp"],
        "last_seen": filing["timestamp"],
        "filing_count": 0,
        "symbols": [],
        "sources": [],
        "evidence": None,
        "percentage_evidence": None,
        "percentage_source": None,
        "percentage_date": None,
    }


def is_reversed(link: dict, link_rows: dict[tuple, dict]) -> bool:
    # Ownership cannot run both ways; the side already stated keeps its place
    if link["relation"] not in OWNING_RELATIONS:
        return False

    return any(
        (link["child"], link["parent"], relation) in link_rows
        for relation in OWNING_RELATIONS
    )


def is_shortcut(link: dict) -> bool:
    if link["relation"] in ("controls", "deemed_via"):
        return link["percentage"] is None

    return link["relation"] == "owns" and link["percentage"] == 1.0


def is_covered(link: dict, link_rows: dict[tuple, dict]) -> bool:
    # "CLI controls Albert Complex" next to CLI -> CLI Singapore -> Albert
    # Complex draws a redundant shortcut; "wholly owned" also describes an
    # indirect subsidiary, so a 100% link with a longer path goes too
    if link["relation"] == "controls" and (link["parent"], link["child"], "owns") in link_rows:
        return True

    if not is_shortcut(link):
        return False

    children = defaultdict(set)

    for parent_id, child_id, relation in link_rows:
        if relation in OWNING_RELATIONS:
            children[parent_id].add(child_id)

    return reaches(children, link["parent"], link["child"], skip_direct=True)


def update_link_row(row: dict, link: dict, filing: dict) -> None:
    date = filing["timestamp"]

    # The latest percentage keeps the wording and filing it came from, so a
    # page can show where a figure was stated
    if link["percentage"] is not None and date >= (row["percentage_date"] or ""):
        row["percentage"] = link["percentage"]
        row["percentage_evidence"] = link["evidence"] or None
        row["percentage_source"] = filing["source"]
        row["percentage_date"] = date

    row["counts_toward_parent"] = row["counts_toward_parent"] or link["counts_toward_parent"]
    row["first_seen"] = min(row["first_seen"], date)
    row["last_seen"] = max(row["last_seen"], date)
    row["filing_count"] += 1
    row["symbols"] = sorted(set(row["symbols"]) | {filing["symbol"]})
    row["sources"] = [*row["sources"], filing["source"]]

    if link["evidence"] and (not row["evidence"] or date >= row["last_seen"]):
        row["evidence"] = link["evidence"]


def merge_filing_links(
    link_rows: dict[tuple, dict],
    links: list[dict],
    filing: dict,
) -> set[tuple]:
    """
    Adds one filing's links to the stored rows and returns the keys it changed.
    """
    changed_keys = set()

    for link in links:
        key = (link["parent"], link["child"], link["relation"])
        row = link_rows.get(key)

        if row is None:
            if is_reversed(link, link_rows) or is_covered(link, link_rows):
                LOGGER.info("[Links] Skipped new link already covered: %s", key)
                continue

            row = build_link_row(link, filing)
            link_rows[key] = row

        # A filing counts once per link, even when it states the link twice
        if filing["source"] in row["sources"]:
            continue

        update_link_row(row, link, filing)
        changed_keys.add(key)

    return changed_keys
