from dataclasses import asdict

from sgx_scraper.fetch_reit_transaction.constant import (
    PRICE_CONFLICT_TOLERANCE,
    REIT_SYMBOLS,
    REIT_TRANSACTION_PATH_CONFLICT,
    REIT_TRANSACTION_PATH_SEEN,
    REIT_TRANSACTION_PATH_TODAY,
    SUB_CATEGORY,
    TABLE_NAME,
)
from sgx_scraper.fetch_reit_transaction.models import ReitPropertyTransaction
from sgx_scraper.fetch_reit_transaction.parser import extract_properties
from sgx_scraper.fetch_reit_transaction.utils.fx_helper import to_sgd
from sgx_scraper.fetch_reit_transaction.utils.payload_helper import (
    clean_basis,
    resolve_financial_year,
    to_date,
)
from sgx_scraper.fetch_reit_transaction.utils.plan_lookup import find_plan_property, is_completion
from sgx_scraper.sgx_api.scraper_sgx_api import iter_sgx_announcements
from sgx_scraper.utils.cli_helper import upsert_to_db
from sgx_scraper.utils.json_helper import open_json, write_json

import logging
import random
import time
import typer


app = typer.Typer(help="SGX REIT property transaction scraper pipeline")

LOGGER = logging.getLogger(__name__)

DB_EXCLUDED_COLUMNS = {"ref_id", "source_url"}


def load_seen_refs() -> set[str]:
    if not REIT_TRANSACTION_PATH_SEEN.exists():
        return set()

    return set(open_json(REIT_TRANSACTION_PATH_SEEN) or [])


def resolve_symbol(announcement: dict) -> str | None:
    issuers = announcement.get("issuers") or []
    return issuers[0].get("stock_code") if issuers else None


def build_record(
    announcement: dict,
    symbol: str,
    prop: dict,
    plan: dict,
    plan_announcement: dict | None,
) -> ReitPropertyTransaction:

    def pick(field):
        value = prop.get(field)
        return plan.get(field) if value is None else value

    completed = to_date(prop.get("completed_date")) or to_date(
        announcement.get("submission_date")
    )
    currency = pick("currency")

    return ReitPropertyTransaction(
        symbol=symbol,
        property_name=prop.get("property_name"),
        transaction_type=pick("transaction_type"),
        financial_year=resolve_financial_year(completed, symbol),
        counterparty=pick("counterparty"),
        interest_pct=pick("interest_pct"),
        completed_date=completed.isoformat() if completed else None,
        deal_id=(
            f"{symbol.lower()}:{plan_announcement['ref_id']}"
            if plan_announcement else None
        ),
        basis_value=to_sgd(pick("basis_value"), currency, completed),
        basis=clean_basis(pick("basis")),
        transaction_price=to_sgd(pick("transaction_price"), currency, completed),
        source_url=announcement.get("url"),
        ref_id=announcement.get("ref_id"),
    )


def price_conflict(prop: dict, plan: dict) -> float | None:
    completion_price, plan_price = prop.get("transaction_price"), plan.get("transaction_price")

    if completion_price is None or not plan_price:
        return None

    gap = abs(completion_price - plan_price) / plan_price

    return gap if gap > PRICE_CONFLICT_TOLERANCE else None


@app.command(name="scraper_reit_transaction")
def run_reit_transaction_scraper(
    period_start: str = typer.Option(None, help="Start period in format YYYYMMDD"),
    period_end: str = typer.Option(None, help="End period in format YYYYMMDD"),
    page_size: int = typer.Option(100, help="Number of records per listing page"),
    model_name: str = typer.Option("deepseek-v4-flash", help="Model key in MODEL_CONFIG"),
    limit: int = typer.Option(None, help="Stop after this many completion filings"),
    ignore_seen: bool = typer.Option(False, help="Reprocess filings already in the seen list"),
    is_push_db: bool = typer.Option(True, help="Flag to push to db or not"),
    is_proxy: bool = typer.Option(None, help="Flag to use proxy or not"),
):
    seen_refs = load_seen_refs()
    processed = 0
    payload, conflicts = [], []

    announcements = iter_sgx_announcements(
        sub_category=SUB_CATEGORY,
        flag_log="REIT Transaction",
        period_start=period_start,
        period_end=period_end,
        page_size=page_size,
        is_proxy=is_proxy,
    )

    for announcement in announcements:
        symbol = resolve_symbol(announcement)
        ref_id = announcement.get("ref_id")
        detail_url = announcement.get("url")
        title = announcement.get("title") or ""

        if symbol not in REIT_SYMBOLS or not detail_url:
            continue

        if not is_completion(title):
            continue

        if ref_id in seen_refs and not ignore_seen:
            LOGGER.info(f"[REIT TRANSACTION] Already processed {ref_id}, skipping")
            continue

        try:
            properties = extract_properties(detail_url, model_name)

        except Exception as error:
            LOGGER.error(
                f"[REIT TRANSACTION] Failed parsing {symbol} - {detail_url}: {error}",
                exc_info=True,
            )
            continue

        for prop in properties:
            if prop.get("status") == "terminated":
                continue

            plan, plan_announcement = {}, None

            if prop.get("transaction_price") is None or prop.get("basis_value") is None:
                found = find_plan_property(
                    company=announcement.get("issuer_name"),
                    property_name=prop.get("property_name"),
                    completed_on=to_date(prop.get("completed_date"))
                    or to_date(announcement.get("submission_date")),
                    model_name=model_name,
                    is_proxy=is_proxy,
                )

                if found:
                    plan, plan_announcement = found

            gap = price_conflict(prop, plan)

            if gap is not None:
                conflicts.append({
                    "symbol": symbol,
                    "property_name": prop.get("property_name"),
                    "completion_price": prop.get("transaction_price"),
                    "plan_price": plan.get("transaction_price"),
                    "gap_pct": round(gap, 3),
                    "source_url": detail_url,
                    "plan_url": plan_announcement.get("url") if plan_announcement else None,
                })

            payload.append(
                asdict(build_record(announcement, symbol, prop, plan, plan_announcement))
            )

        if not properties:
            LOGGER.warning(
                f"[REIT TRANSACTION] Nothing extracted from {ref_id}, leaving it unseen to retry"
            )
            continue

        seen_refs.add(ref_id)
        processed += 1

        if limit and processed >= limit:
            LOGGER.info(f"[REIT TRANSACTION] Reached limit of {limit} filings")
            break

        time.sleep(random.uniform(1, 3))

    LOGGER.info(f"[REIT TRANSACTION] Scraping completed. Total records: {len(payload)}")

    write_json(REIT_TRANSACTION_PATH_TODAY, payload)
    write_json(REIT_TRANSACTION_PATH_SEEN, sorted(seen_refs))

    if conflicts:
        LOGGER.warning(
            f"[REIT TRANSACTION] {len(conflicts)} rows where the completion and plan "
            f"prices disagree, review {REIT_TRANSACTION_PATH_CONFLICT}"
        )
        write_json(REIT_TRANSACTION_PATH_CONFLICT, conflicts)

    if not is_push_db:
        LOGGER.info(f"[REIT TRANSACTION] Dry run, {len(payload)} records written to file only")
        return

    upsert_to_db(
        payload=payload,
        table_name=TABLE_NAME,
        on_conflict="symbol,financial_year,transaction_type,property_name",
        exclude_columns=DB_EXCLUDED_COLUMNS,
    )
