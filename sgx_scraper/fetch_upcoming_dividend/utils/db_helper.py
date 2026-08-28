from datetime import date, timedelta

from sgx_scraper.config.settings import SUPABASE_CLIENT

import logging


LOGGER = logging.getLogger(__name__)


def dedup_payload(payload: list[dict]):
    deduped = {
        record.get("reference"): record  
        for record in payload 
    }

    deduped_payload = list(deduped.values())
    
    if len(deduped_payload) < len(payload):
        LOGGER.info(
            "[dedupe_by_key] removed %d duplicate(s) on key reference, %d -> %d records",
            len(payload) - len(deduped_payload),
            len(payload),
            len(deduped_payload),
        )

    return deduped_payload


def delete_past_dividends(table_name: str, retention_days: int = 14) -> bool:
    deletion_date = (
        date.today() - timedelta(days=retention_days)
    ).isoformat()

    try:
        response = (
            SUPABASE_CLIENT
            .table(table_name)
            .delete()
            .lt('payment_date', deletion_date)
            .execute()
        )

        LOGGER.info(
            '[delete_past_dividends] removed %d records from %s with payment_date before %s',
            len(response.data),
            table_name,
            deletion_date,
        )
        
        return True

    except Exception as error:
        LOGGER.error('[delete_past_dividends] failed on %s: %s', table_name, error)
        return False
