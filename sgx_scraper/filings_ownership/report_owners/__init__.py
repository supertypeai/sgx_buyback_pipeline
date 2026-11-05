from datetime import datetime, timezone

from sgx_scraper.filings_ownership.utils.record_helper import round_fractions

from .chain import chain_of
from .index import OwnershipIndex
from .names import readable_report
from .resolver import resolve
from .table import ReportTable
from .ultimate import ultimate_owners


def build_report_owners(tables: dict, filings: list[dict]) -> dict:
    reports = [readable_report(report) for report in tables["reports"]]
    index = OwnershipIndex({**tables, "reports": reports}, filings)
    companies = {}

    for symbol, company in sorted(index.companies.items()):
        report = index.reports.get(symbol)

        if not report:
            continue

        owners = resolve(ReportTable(index, company, report), ultimate_owners(index, company))

        if owners is None:
            continue

        companies[symbol] = {
            "symbol": symbol,
            "report": {
                "source": report.get("source_url"),
                "page": report.get("page"),
                "as_at": report.get("as_at"),
            },
            "owners": round_fractions([
                {**row, "chain": chain_of(index, company, report, row)}
                for row in (public_row(owner) for owner in owners)
            ]),
        }

    return {
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "companies": companies,
    }


def public_row(owner: dict) -> dict:
    return {
        key: value
        for key, value in owner.items()
        if not key.startswith("_")
    }


def owner_table_rows(result: dict) -> list[dict]:
    # One sgx_filing_company_owners row per company
    return [
        {
            "symbol": f"{symbol}.SI",
            "report_source": company["report"]["source"],
            "report_page": company["report"]["page"],
            "report_as_at": company["report"]["as_at"],
            "owners": company["owners"],
            "built_at": result["built_at"],
        }
        for symbol, company in result["companies"].items()
    ]
