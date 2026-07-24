import re


def find_label_indices(blocks: list[dict], label_pattern: str) -> list[int]:
    # Reading-order block indices of every block matching the label. Used as
    # section boundaries when slicing the document into anchor windows
    pattern = re.compile(label_pattern, re.IGNORECASE)

    return [
        index
        for index, block in enumerate(blocks)
        if pattern.search(block['text'])
    ]


def region_text(blocks: list[dict], start_index: int, end_index: int | None = None) -> str:
    # Reading-order text of blocks[start_index:end_index]. end_index=None runs to
    # the end of the document
    end = len(blocks) if end_index is None else end_index

    return '\n'.join(block['text'] for block in blocks[start_index:end])


def split_regions_by_label(
    blocks: list[dict],
    start_pattern: str,
    end_pattern: str | None = None,
) -> list[str]:
    # One text window per occurrence of start_pattern, each running until the next
    # start occurrence. The final window ends at the first end_pattern match after
    # the last start (or document end). Gives a clean per-section anchor window
    # without the surrounding boilerplate
    starts = find_label_indices(blocks, start_pattern)

    if not starts:
        return []

    final_boundary = None

    if end_pattern is not None:
        final_boundary = next(
            (index for index in find_label_indices(blocks, end_pattern) if index > starts[-1]),
            None,
        )

    windows = []

    for position, start_index in enumerate(starts):
        is_last = position == len(starts) - 1
        end_index = final_boundary if is_last else starts[position + 1]

        windows.append(region_text(blocks, start_index, end_index))

    return windows


def region_after_label(
    blocks: list[dict],
    start_pattern: str,
    end_pattern: str | None = None,
    occurrence: int = 0,
) -> str | None:
    # Single window from the nth occurrence of start_pattern to the first
    # end_pattern match after it (or document end). Returns None if the start
    # label is absent
    starts = find_label_indices(blocks, start_pattern)

    if occurrence >= len(starts):
        return None

    start_index = starts[occurrence]

    end_index = None

    if end_pattern is not None:
        end_index = next(
            (index for index in find_label_indices(blocks, end_pattern) if index > start_index),
            None,
        )

    return region_text(blocks, start_index, end_index)
