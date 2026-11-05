from sgx_scraper.config.settings import SUPABASE_CLIENT
from sgx_scraper.filings_ownership.utils.constants import DB_PAGE_SIZE

import logging


LOGGER = logging.getLogger(__name__)


def read_all_rows(table: str, columns: str) -> list[dict]:
    # Supabase returns at most 1,000 rows per request
    rows = []
    start = 0

    while True:
        response = (
            SUPABASE_CLIENT
            .table(table)
            .select(columns)
            .range(start, start + DB_PAGE_SIZE - 1)
            .execute()
        )

        rows.extend(response.data)

        if len(response.data) < DB_PAGE_SIZE:
            break

        start += DB_PAGE_SIZE

    LOGGER.info("[DB] Read %d rows from %s", len(rows), table)

    return rows
