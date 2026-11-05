from sgx_scraper.filings_ownership.annual_reports import get_previous_month, refresh_annual_reports
from sgx_scraper.filings_ownership.pipeline import get_rows_to_save, run_pipeline
from sgx_scraper.filings_ownership.report_owners import build_report_owners, owner_table_rows
from sgx_scraper.filings_ownership.store import read_owner_inputs, save_changes, write_preview
from sgx_scraper.filings_ownership.utils.constants import (
    ANNUAL_REPORT_EFFORT,
    ANNUAL_REPORT_MODELS,
    ANNUAL_REPORTS_PATH,
    COMPANY_TABLE,
    OWNERS_PATH,
    OWNERS_TABLE,
    PREVIEW_PATH,
    TABLE_KEYS,
)
from sgx_scraper.utils.cli_helper import get_db, upsert_to_db
from sgx_scraper.utils.json_helper import open_json, write_json

from datetime import date
from pathlib import Path

import typer
import logging


LOGGER = logging.getLogger(__name__)


app = typer.Typer(help="SGX filings ownership pipeline")


@app.command(name="filings_ownership")
def run_filings_ownership(
    state_dir: str = typer.Option(None, help="Read the tables from JSON files in this folder instead of the database"),
    limit: int = typer.Option(None, help="Cap the number of new filing PDFs processed (for testing)"),
    preview_path: str = typer.Option(str(PREVIEW_PATH), help="Where to write the before/after preview"),
    is_push_db: bool = typer.Option(False, help="Write the changes to the database"),
):
    if state_dir and is_push_db:
        raise typer.BadParameter(
            "A run that reads local JSON tables cannot write to the database"
        )

    preview = run_pipeline(Path(state_dir) if state_dir else None, limit)
    write_preview(preview, Path(preview_path))

    LOGGER.info("[Filings ownership] %s", preview["summary"])

    if is_push_db:
        save_changes(get_rows_to_save(preview))


@app.command(name="report_owners")
def run_report_owners(
    reports_path: str = typer.Option(
        str(ANNUAL_REPORTS_PATH),
        help="The annual report holders JSON the owners start from",
    ),
    output_path: str = typer.Option(str(OWNERS_PATH), help="Where to write each company's owner rows"),
    state_dir: str = typer.Option(None, help="Read the tables from JSON files in this folder instead of the database"),
    is_push_db: bool = typer.Option(False, help="Write the owner rows to sgx_filing_company_owners"),
):
    if state_dir and is_push_db:
        raise typer.BadParameter(
            "A run that reads local JSON tables cannot write to the database"
        )

    tables, filings = read_owner_inputs(Path(state_dir) if state_dir else None)
    tables["reports"] = open_json(Path(reports_path))
    result = build_report_owners(tables, filings)
    write_json(Path(output_path), result)

    LOGGER.info(
        "[Report owners] Wrote owners for %d companies to %s", 
        len(result["companies"]), output_path
    )

    if is_push_db:
        upsert_to_db(
            owner_table_rows(result), 
            OWNERS_TABLE, 
            on_conflict=TABLE_KEYS[OWNERS_TABLE]
        )


@app.command(name="annual_reports")
def run_annual_reports(
    period_start: str = typer.Option(None, help="Start in format YYYYMMDD; the previous month when omitted"),
    period_end: str = typer.Option(None, help="End in format YYYYMMDD; the previous month when omitted"),
    refresh_all: bool = typer.Option(False, help="Read every report again, not only newly published ones"),
    model: str = typer.Option(ANNUAL_REPORT_MODELS[0], help="Model name from MODEL_CONFIG"),
    effort: str = typer.Option(ANNUAL_REPORT_EFFORT, help="Reasoning effort"),
):
    top_companies = get_db(
        table=COMPANY_TABLE,
        columns="name,symbol,market_cap",
        query=lambda query: query.not_.is_("market_cap", "null").order("market_cap", desc=True).limit(200),
    )
    company_names = {
        company["symbol"]: company["name"]
        for company in top_companies
        if company.get("symbol")
    }

    previous_start, previous_end = get_previous_month(date.today())
    stored = open_json(ANNUAL_REPORTS_PATH) or []
    reports = refresh_annual_reports(
        stored=stored,
        company_names=company_names,
        period=(period_start or previous_start, period_end or previous_end),
        refresh_all=refresh_all,
        models=[model],
        effort=effort,
    )
    write_json(ANNUAL_REPORTS_PATH, reports)

    LOGGER.info("[Annual reports] %d reports stored in %s", len(reports), ANNUAL_REPORTS_PATH)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    app()
