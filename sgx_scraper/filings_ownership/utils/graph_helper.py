from sgx_scraper.filings_ownership.utils.constants import MAX_ROUTE_LENGTH


def reaches(
    children: dict[str, set[str]],
    start: str,
    goal: str,
    skip_direct: bool = False,
) -> bool:
    stack = [
        (child, 1)
        for child in children.get(start, ())
        if not (skip_direct and child == goal)
    ]
    seen = set()

    while stack:
        node, depth = stack.pop()

        if node == goal:
            return True

        if node in seen or depth > MAX_ROUTE_LENGTH:
            continue

        seen.add(node)

        stack.extend((child, depth + 1) for child in children.get(node, ()))

    return False


def get_ancestors(entity_id: str, parents: dict[str, set[str]]) -> set[str]:
    found = set()
    stack = [entity_id]

    while stack:
        for parent in parents.get(stack.pop(), ()):
            if parent not in found and parent != entity_id:
                found.add(parent)

                stack.append(parent)

    return found
