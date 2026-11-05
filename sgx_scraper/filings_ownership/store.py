from sgx_scraper.filings_ownership.utils.constants import (
    COMPANY_VIEW_TABLE,
    ENTITIES_TABLE,
    FILINGS_TABLE,
    ENTITY_VIEW_TABLE,
    FILING_OWNERSHIP_TABLE,
    LINKS_TABLE,
    TABLE_KEYS,
)
from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.filings_ownership.utils.db_helper import read_all_rows
from sgx_scraper.utils.cli_helper import upsert_to_db
from sgx_scraper.utils.json_helper import open_json, write_json

from pathlib import Path

import json
import logging


LOGGER = logging.getLogger(__name__)

# Parents before children, so every id a row points to is already stored
SAVE_ORDER = [
    ENTITIES_TABLE,
    LINKS_TABLE,
    FILING_OWNERSHIP_TABLE,
    COMPANY_VIEW_TABLE,
    ENTITY_VIEW_TABLE,
]


def read_table(
    table: str,
    columns: str = "*",
    state_dir: Path | None = None,
) -> list[dict]:
    # A state folder holds one JSON file per table, for runs that must not
    # touch the database
    if state_dir is None:
        return read_all_rows(table, columns)

    rows = open_json(state_dir / f"{table}.json")
    LOGGER.info("[State] Read %d rows from %s.json", len(rows), table)

    return rows


def parse_json_value(value):
    # Older rows keep jsonb values as JSON text
    if isinstance(value, str):
        return json.loads(value)

    return value


def normalise_filing(row: dict) -> dict:
    return {
        **row,
        "id": int(row["id"]),
        "interest_breakdown": parse_json_value(row.get("interest_breakdown")) or {},
        "tags": parse_json_value(row.get("tags")) or [],
    }


def normalise_ownership_row(row: dict) -> dict:
    return {
        **row,
        "filing_id": int(row["filing_id"]),
        "trader_ids": row.get("trader_ids") or [],
        "deemed_routes": parse_json_value(row.get("deemed_routes")) or [],
    }


def read_owner_filings(filing_ids: list[int], state_dir: Path | None) -> list[dict]:
    if state_dir is not None:
        rows = read_table(FILINGS_TABLE, state_dir=state_dir)
    else:
        rows = []

        # Only the filings an ownership row points to, and only the columns the owners need
        for start in range(0, len(filing_ids), 300):
            response = (
                SUPABASE_CLIENT
                .table(FILINGS_TABLE)
                .select("id,symbol,timestamp,source,interest_breakdown")
                .in_("id", filing_ids[start:start + 300])
                .execute()
            )

            rows.extend(response.data)

    return [
        {
            **row,
            "id": int(row["id"]),
            "interest_breakdown": parse_json_value(row.get("interest_breakdown")) or {},
        }
        for row in rows
    ]


def read_owner_inputs(state_dir: Path | None) -> tuple[dict, list[dict]]:
    ownership = [
        normalise_ownership_row(row)
        for row in read_table(FILING_OWNERSHIP_TABLE, state_dir=state_dir)
    ]
    companies = [
        {
            **row,
            "holders": parse_json_value(row.get("holders")) or [],
            "top_level_holders": parse_json_value(row.get("top_level_holders")) or [],
        }
        for row in read_table(COMPANY_VIEW_TABLE, state_dir=state_dir)
    ]
    tables = {
        "entities": read_table(ENTITIES_TABLE, state_dir=state_dir),
        "links": read_table(LINKS_TABLE, state_dir=state_dir),
        "ownership": ownership,
        "companies": companies,
    }
    filings = read_owner_filings(sorted({
        row["filing_id"]
        for row in ownership
    }), state_dir)

    return tables, filings


def save_changes(changes: dict[str, list[dict]]) -> None:
    for table in SAVE_ORDER:
        rows = changes.get(table) or []

        if rows:
            upsert_to_db(rows, table, on_conflict=TABLE_KEYS[table])


def write_preview(preview: dict, path: Path) -> None:
    write_json(path, preview)
    LOGGER.info("[Preview] Changes written to %s", path)
