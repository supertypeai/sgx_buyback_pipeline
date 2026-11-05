from sgx_scraper.filings_ownership.annual_reports.pages import find_report_urls, get_report_pages
from sgx_scraper.filings_ownership.annual_reports.reader import read_annual_reports
from sgx_scraper.filings_ownership.utils.constants import (
    ANNUAL_REPORT_LLM_CONCURRENCY,
    ANNUAL_REPORT_PDF_CONCURRENCY,
)

from datetime import date, timedelta

import asyncio
import logging


LOGGER = logging.getLogger(__name__)


def get_reports_to_read(
    report_urls: dict[str, dict],
    stored: list[dict],
    refresh_all: bool,
) -> dict[str, dict]:
    # Only a report published after the stored one: some stored reports were read before
    # the managements pipeline found them (42C's 2026 report), and an older one must not win
    stored_dates = {report["symbol"]: report.get("submission_date") or "" for report in stored}

    return {
        symbol: report
        for symbol, report in report_urls.items()
        if refresh_all or report["submission_date"] > stored_dates.get(symbol, "")
    }


def get_previous_month(today: date) -> tuple[str, str]:
    # The same window the managements scrape reads, so a run after it sees the same reports
    month_start = today.replace(day=1)
    period_end = month_start - timedelta(days=1)

    return period_end.replace(day=1).strftime("%Y%m%d"), period_end.strftime("%Y%m%d")


def merge_reports(stored: list[dict], fresh: list[dict]) -> list[dict]:
    by_symbol = {report["symbol"]: report for report in stored}
    by_symbol.update({report["symbol"]: report for report in fresh})

    return [by_symbol[symbol] for symbol in sorted(by_symbol)]


def refresh_annual_reports(
    stored: list[dict],
    company_names: dict[str, str],
    period: tuple[str, str],
    refresh_all: bool,
    models: list[str],
    effort: str,
) -> list[dict]:
    found = find_report_urls(*period)
    report_urls = get_reports_to_read(found, stored, refresh_all)
    LOGGER.info("[Annual reports] %d of %d reports found are new", len(report_urls), len(found))

    if not report_urls:
        return stored

    report_pages = asyncio.run(get_report_pages(report_urls, ANNUAL_REPORT_PDF_CONCURRENCY))
    LOGGER.info("[Annual reports] %d reports have shareholding pages", len(report_pages))

    fresh = asyncio.run(
        read_annual_reports(
            report_pages=report_pages,
            company_names=company_names,
            models=models,
            effort=effort,
            concurrency=ANNUAL_REPORT_LLM_CONCURRENCY,
        )
    )
    LOGGER.info("[Annual reports] Read holders from %d reports", len(fresh))

    return merge_reports(stored, fresh)
