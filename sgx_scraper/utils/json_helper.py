from pathlib import Path

import pandas as pd
import logging
import json
import re


LOGGER = logging.getLogger(__name__)


def open_json(path: str):
    path = Path(path)

    if not path.exists() or path.stat().st_size == 0:
        return []

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    except json.JSONDecodeError:
        LOGGER.warning(f"Failed to decode JSON from {path}, returning empty list")
        return []


def write_json(path: str, payload: list[dict[str, any]]):
    path = Path(path)

    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    LOGGER.info(f"Saved all sgx scraped to {path}")


def write_to_csv(path: str, payload: list[dict[str]]):
    df = pd.DataFrame(payload)

    if df.empty:
        return

    path = Path(path)
    file_exists = path.is_file()
    df.to_csv(path, mode='a', index=False, header=not file_exists)

    LOGGER.info(f'Saved payload to {path}')


def parse_json_reply(reply: str) -> dict:
    cleaned = re.sub(r"^```(?:json)?|```$", "", reply.strip(), flags=re.MULTILINE)
    return json.loads(cleaned.strip())
